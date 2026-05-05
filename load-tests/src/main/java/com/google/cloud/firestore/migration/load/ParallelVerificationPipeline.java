package com.google.cloud.firestore.migration.load;

import com.google.firestore.v1.Document;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ParallelVerificationPipeline {
    private static final Logger LOG = Logger.getLogger(ParallelVerificationPipeline.class.getName());

    public interface VerificationOptions extends PipelineOptions {
        @Description("Source Project ID")
        @Required
        String getSourceProject();
        void setSourceProject(String value);

        @Description("Source Database ID")
        @Required
        String getSourceDatabase();
        void setSourceDatabase(String value);

        @Description("Destination Project ID")
        @Required
        String getDestProject();
        void setDestProject(String value);

        @Description("Destination Database ID")
        @Required
        String getDestDatabase();
        void setDestDatabase(String value);

        @Description("Collection Name to verify")
        String getCollectionName();
        void setCollectionName(String value);

        @Description("Mode: hash or diff")
        @Default.String("hash")
        String getMode();
        void setMode(String value);

        @Description("Comma separated list of shard IDs to diff (e.g. 1,23,45). Empty means all.")
        String getShards();
        void setShards(String value);

        @Description("Output Report Path (use gs://... for Dataflow)")
        @Default.String("detailed-diff-report.txt")
        String getReportPath();
        void setReportPath(String value);
    }

    public static void main(String[] args) {
        VerificationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(VerificationOptions.class);
        String collection = (options.getCollectionName() != null) ? options.getCollectionName() : "test_data";

        Pipeline p = Pipeline.create(options);

        PCollection<Document> srcDocsRaw = readFirestore(p, options.getSourceProject(), options.getSourceDatabase(), collection, "Read Source DB");
        PCollection<Document> dstDocsRaw = readFirestore(p, options.getDestProject(), options.getDestDatabase(), collection, "Read Dest DB");

        // 1. Run Hash Verification and get mismatching shards
        PCollection<Integer> mismatchingShards = runHashVerification(srcDocsRaw, dstDocsRaw);

        // Count total mismatching shards
        PCollection<Long> mismatchCount = mismatchingShards.apply("Count Mismatches", Count.globally());

        // 2. Sample up to 5 mismatching shards
        PCollection<Iterable<Integer>> sampledShards = mismatchingShards.apply("Sample 5 Mismatches", Sample.fixedSizeGlobally(5));
        PCollectionView<Iterable<Integer>> targetShardsView = sampledShards.apply("Create Target Shards View", View.asSingleton());

        // 3. Filter docs by mismatching shards (using side input)
        PCollection<Document> filteredSrcDocs = srcDocsRaw.apply("Filter Src Docs By Mismatch Shards",
                ParDo.of(new FilterByShardsFn(targetShardsView)).withSideInputs(targetShardsView));

        PCollection<Document> filteredDstDocs = dstDocsRaw.apply("Filter Dst Docs By Mismatch Shards",
                ParDo.of(new FilterByShardsFn(targetShardsView)).withSideInputs(targetShardsView));

        // 4. Run detailed diff on filtered docs (returns PCollection of result lines)
        PCollection<String> diffResults = runDetailedDiff(filteredSrcDocs, filteredDstDocs);

        // 5. Generate Warning Header if total mismatches > 5
        PCollection<String> warningHeader = mismatchCount.apply("Generate Warning Header", ParDo.of(new DoFn<Long, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                long count = c.element();
                if (count > 5) {
                    c.output("==================================================================");
                    c.output("WARNING: ONLY A SAMPLE OF MISMATCHING SHARDS IS SHOWN BELOW!");
                    c.output("Total mismatching shards: " + count + ". Only 5 were sampled for detailed diff.");
                    c.output("==================================================================");
                    c.output("");
                }
            }
        }));

        // 6. Flatten warning and diff results, then write to single file
        PCollectionList<String> collections = PCollectionList.of(warningHeader).and(diffResults);
        PCollection<String> mergedResults = collections.apply("Merge Results", Flatten.pCollections());

        mergedResults.apply("Write Detailed Report", TextIO.write().to(options.getReportPath()).withoutSharding());

        p.run().waitUntilFinish();
    }

    private static PCollection<Document> readFirestore(Pipeline p, String project, String dbId, String collection, String stepName) {
        String parent = String.format("projects/%s/databases/%s/documents", project, dbId);

        com.google.firestore.v1.StructuredQuery query = com.google.firestore.v1.StructuredQuery.newBuilder()
                .addFrom(com.google.firestore.v1.StructuredQuery.CollectionSelector.newBuilder().setCollectionId(collection).setAllDescendants(true).build())
                .build();

        com.google.firestore.v1.PartitionQueryRequest request = com.google.firestore.v1.PartitionQueryRequest.newBuilder()
                .setParent(parent)
                .setStructuredQuery(query)
                .setPartitionCount(100) // Adjust as needed
                .build();

        PCollection<com.google.firestore.v1.PartitionQueryRequest> requests = p.apply("Create Request " + stepName, org.apache.beam.sdk.transforms.Create.of(request));

        PCollection<com.google.firestore.v1.RunQueryRequest> runQueryRequests = requests.apply("Partition " + stepName,
                FirestoreIO.v1().read().partitionQuery().build());

        PCollection<com.google.firestore.v1.RunQueryResponse> responses = runQueryRequests.apply("Run " + stepName,
                FirestoreIO.v1().read().runQuery().build());

        return responses.apply("Extract Doc " + stepName, ParDo.of(new DoFn<com.google.firestore.v1.RunQueryResponse, Document>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                com.google.firestore.v1.RunQueryResponse resp = c.element();
                if (resp.hasDocument()) {
                    c.output(resp.getDocument());
                }
            }
        }));
    }

    private static PCollection<Integer> runHashVerification(PCollection<Document> srcDocs, PCollection<Document> dstDocs) {
        PCollection<KV<Integer, ShardMetrics>> srcShards = srcDocs.apply("Extract Source Shard Hash", ParDo.of(new ShardHashFn()))
                .apply("Sum Source Shards", Combine.perKey(new ShardMetricsCombineFn()));

        PCollection<KV<Integer, ShardMetrics>> dstShards = dstDocs.apply("Extract Dest Shard Hash", ParDo.of(new ShardHashFn()))
                .apply("Sum Dest Shards", Combine.perKey(new ShardMetricsCombineFn()));

        final TupleTag<ShardMetrics> srcTag = new TupleTag<>();
        final TupleTag<ShardMetrics> dstTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> joinedShards = KeyedPCollectionTuple
                .of(srcTag, srcShards)
                .and(dstTag, dstShards)
                .apply("Join Shards Checksum", CoGroupByKey.create());

        PCollection<Integer> mismatchingShardIds = joinedShards.apply("Compare Shard Checksums", ParDo.of(new DoFn<KV<Integer, CoGbkResult>, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                int shardId = c.element().getKey();
                CoGbkResult result = c.element().getValue();

                ShardMetrics srcMetric = result.getOnly(srcTag, null);
                ShardMetrics dstMetric = result.getOnly(dstTag, null);

                long srcCount = (srcMetric != null) ? srcMetric.count : 0;
                long dstCount = (dstMetric != null) ? dstMetric.count : 0;
                String srcHash = (srcMetric != null) ? srcMetric.hash : "0";
                String dstHash = (dstMetric != null) ? dstMetric.hash : "0";

                if (srcCount != dstCount || !Objects.equals(srcHash, dstHash)) {
                    LOG.severe(String.format("SHARD MISMATCH: ShardId=%d | SrcCount=%d DstCount=%d | HashMatch=%b",
                            shardId, srcCount, dstCount, Objects.equals(srcHash, dstHash)));
                    c.output(shardId);
                } else {
                    LOG.info(String.format("SHARD MATCH: ShardId=%d | Count=%d | Hash=%s", shardId, srcCount, srcHash));
                }
            }
        }));

        // Add total count write to file
        PCollection<Long> totalSrcCount = srcShards.apply("Extract Source Counts", ParDo.of(new DoFn<KV<Integer, ShardMetrics>, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().getValue().count);
            }
        })).apply("Sum Total Source Counts", Combine.globally(Sum.ofLongs()));

        totalSrcCount.apply("Format Total Source Count", MapElements.into(TypeDescriptors.strings())
                .via(count -> "TOTAL SOURCE DOC COUNT: " + count))
           .apply("Write Total Source Count", TextIO.write().to("total-src-count.txt").withoutSharding());

        PCollection<Long> totalDstCount = dstShards.apply("Extract Dest Counts", ParDo.of(new DoFn<KV<Integer, ShardMetrics>, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().getValue().count);
            }
        })).apply("Sum Total Dest Counts", Combine.globally(Sum.ofLongs()));

        totalDstCount.apply("Format Total Dest Count", MapElements.into(TypeDescriptors.strings())
                .via(count -> "TOTAL DESTINATION DOC COUNT: " + count))
           .apply("Write Total Dest Count", TextIO.write().to("total-dst-count.txt").withoutSharding());

        return mismatchingShardIds;
    }

    private static void runDiffVerification(PCollection<Document> srcDocs, PCollection<Document> dstDocs, Set<Integer> targetShards) {
        PCollection<KV<String, Document>> srcMapped = srcDocs
                .apply("Filter Source Shards", Filter.by(doc -> targetShards == null || targetShards.contains(getShardId(doc))))
                .apply("Map Source by ID", ParDo.of(new MapByIdFn()));

        PCollection<KV<String, Document>> dstMapped = dstDocs
                .apply("Filter Dest Shards", Filter.by(doc -> targetShards == null || targetShards.contains(getShardId(doc))))
                .apply("Map Dest by ID", ParDo.of(new MapByIdFn()));

        final TupleTag<Document> srcTag = new TupleTag<>();
        final TupleTag<Document> dstTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
                .of(srcTag, srcMapped)
                .and(dstTag, dstMapped)
                .apply("Join Docs", CoGroupByKey.create());

        joined.apply("Diff Docs", ParDo.of(new DoFn<KV<String, CoGbkResult>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String id = c.element().getKey();
                CoGbkResult result = c.element().getValue();

                Document src = result.getOnly(srcTag, null);
                Document dst = result.getOnly(dstTag, null);

                if (src == null && dst != null) {
                    LOG.severe("DIFF MISMATCH: Doc found in Destination but NOT in Source. ID: " + id);
                } else if (src != null && dst == null) {
                    LOG.severe("DIFF MISMATCH: Doc found in Source but NOT in Destination. ID: " + id);
                } else if (src != null && dst != null) {
                    if (!src.getFieldsMap().equals(dst.getFieldsMap())) {
                        LOG.severe("DIFF MISMATCH: Content mismatch for Doc ID: " + id);
                    }
                }
            }
        }));
    }

    private static int getShardId(Document doc) {
        String path = doc.getName();
        String id = path.substring(path.lastIndexOf('/') + 1);
        return Math.abs(id.hashCode() % 100);
    }

    private static class MapByIdFn extends DoFn<Document, KV<String, Document>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            String path = doc.getName();
            String id = path.substring(path.lastIndexOf('/') + 1);
            c.output(KV.of(id, doc));
        }
    }

    public static BigInteger hashContent(Document doc) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            TreeMap<String, String> sortedFields = new TreeMap<>();
            doc.getFieldsMap().forEach((k, v) -> sortedFields.put(k, v.toString()));
            
            String canonical = sortedFields.toString();
            byte[] bytes = md.digest(canonical.getBytes());
            return new BigInteger(1, bytes);
        } catch (Exception e) {
            return BigInteger.ZERO;
        }
    }

    private static class ShardHashFn extends DoFn<Document, KV<Integer, KV<Long, BigInteger>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            int shardId = getShardId(doc);
            
            // Hash content
            BigInteger hash = hashContent(doc);
            c.output(KV.of(shardId, KV.of(1L, hash)));
        }
    }

    public static class DocInfo implements java.io.Serializable {
        public String hash;
        public DocInfo(String hash) {
            this.hash = hash;
        }
    }

    private static class MapDocInfoFn extends DoFn<Document, KV<String, DocInfo>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            String path = doc.getName();
            String id = path.substring(path.lastIndexOf('/') + 1);
            String hash = hashContent(doc).toString(16);
            c.output(KV.of(id, new DocInfo(hash)));
        }
    }

    private static class FilterByShardsFn extends DoFn<Document, Document> {
        private final PCollectionView<Iterable<Integer>> targetShardsView;

        public FilterByShardsFn(PCollectionView<Iterable<Integer>> targetShardsView) {
            this.targetShardsView = targetShardsView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            Iterable<Integer> targetShards = c.sideInput(targetShardsView);
            if (targetShards == null) {
                return;
            }
            int shardId = getShardId(doc);
            boolean match = false;
            for (int s : targetShards) {
                if (s == shardId) {
                    match = true;
                    break;
                }
            }
            if (match) {
                c.output(doc);
            }
        }
    }

    private static PCollection<String> runDetailedDiff(PCollection<Document> srcDocs, PCollection<Document> dstDocs) {
        PCollection<KV<String, DocInfo>> srcMapped = srcDocs.apply("Map Src Doc Info", ParDo.of(new MapDocInfoFn()));
        PCollection<KV<String, DocInfo>> dstMapped = dstDocs.apply("Map Dst Doc Info", ParDo.of(new MapDocInfoFn()));

        final TupleTag<DocInfo> srcTag = new TupleTag<>();
        final TupleTag<DocInfo> dstTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
                .of(srcTag, srcMapped)
                .and(dstTag, dstMapped)
                .apply("Join Filtered Docs", CoGroupByKey.create());

        return joined.apply("Diff Filtered Docs", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String id = c.element().getKey();
                CoGbkResult result = c.element().getValue();

                DocInfo src = result.getOnly(srcTag, null);
                DocInfo dst = result.getOnly(dstTag, null);

                if (src == null && dst != null) {
                    c.output("EXTRA: " + id + " | DestHash=" + dst.hash);
                } else if (src != null && dst == null) {
                    c.output("MISSING: " + id + " | SrcHash=" + src.hash);
                } else if (src != null && dst != null) {
                    if (!Objects.equals(src.hash, dst.hash)) {
                        c.output("MISMATCH: " + id + " | SrcHash=" + src.hash + " DstHash=" + dst.hash);
                    }
                }
            }
        }));
    }

    public static class ShardMetrics implements java.io.Serializable {
        public long count;
        public String hash;

        public ShardMetrics(long count, String hash) {
            this.count = count;
            this.hash = hash;
        }
    }

    private static class ShardMetricsCombineFn extends Combine.CombineFn<KV<Long, BigInteger>, ShardMetricsAccum, ShardMetrics> {

        @Override
        public ShardMetricsAccum createAccumulator() {
            return new ShardMetricsAccum();
        }

        @Override
        public ShardMetricsAccum addInput(ShardMetricsAccum accum, KV<Long, BigInteger> input) {
            accum.count += input.getKey();
            accum.hash = accum.hash.xor(input.getValue());
            return accum;
        }

        @Override
        public ShardMetricsAccum mergeAccumulators(Iterable<ShardMetricsAccum> accums) {
            ShardMetricsAccum merged = new ShardMetricsAccum();
            for (ShardMetricsAccum accum : accums) {
                merged.count += accum.count;
                merged.hash = merged.hash.xor(accum.hash);
            }
            return merged;
        }

        @Override
        public ShardMetrics extractOutput(ShardMetricsAccum accum) {
            return new ShardMetrics(accum.count, accum.hash.toString(16));
        }
    }

    public static class ShardMetricsAccum implements java.io.Serializable {
        public long count = 0;
        public BigInteger hash = BigInteger.ZERO;
    }
}
