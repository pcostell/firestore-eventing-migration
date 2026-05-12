package com.google.cloud.firestore.migration.load;

import com.google.firestore.v1.Document;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

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

        @Description("Read Timestamp (ISO-8601 format, e.g., 2026-05-08T00:00:00Z)")
        String getReadTimestamp();
        void setReadTimestamp(String value);
    }

    public static void main(String[] args) {
        VerificationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(VerificationOptions.class);
        String collection = (options.getCollectionName() != null) ? options.getCollectionName() : "test_data";

        com.google.protobuf.Timestamp readTime = null;
        if (options.getReadTimestamp() != null && !options.getReadTimestamp().isEmpty()) {
            java.time.Instant instant = java.time.Instant.parse(options.getReadTimestamp());
            readTime = com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(instant.getEpochSecond())
                    .setNanos(instant.getNano())
                    .build();
        }

        Pipeline p = Pipeline.create(options);

        PCollection<Document> srcDocsRaw = readFirestore(p, options.getSourceProject(), options.getSourceDatabase(), collection, "Read Source DB", readTime);
        PCollection<Document> dstDocsRaw = readFirestore(p, options.getDestProject(), options.getDestDatabase(), collection, "Read Dest DB", readTime);

        // Extract Shards Metrics
        PCollection<KV<Integer, ShardMetrics>> srcShards = srcDocsRaw.apply("Extract Source Shard Hash", ParDo.of(new ShardHashFn()))
                .apply("Sum Source Shards", Combine.perKey(new ShardMetricsCombineFn()));

        PCollection<KV<Integer, ShardMetrics>> dstShards = dstDocsRaw.apply("Extract Dest Shard Hash", ParDo.of(new ShardHashFn()))
                .apply("Sum Dest Shards", Combine.perKey(new ShardMetricsCombineFn()));

        // Join checksums
        final TupleTag<ShardMetrics> srcTag = new TupleTag<>();
        final TupleTag<ShardMetrics> dstTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> joinedShards = KeyedPCollectionTuple
                .of(srcTag, srcShards)
                .and(dstTag, dstShards)
                .apply("Join Shards Checksum", CoGroupByKey.create());

        // 1. Generate the complete sharded manifest breakdown
        PCollection<String> shardBreakdown = joinedShards.apply("Format Shard Metrics Lines", ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
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
                boolean mismatch = srcCount != dstCount || !Objects.equals(srcHash, dstHash);

                c.output(String.format("  - Shard %02d: SrcCount=%-8d DstCount=%-8d Mismatch=%b", shardId, srcCount, dstCount, mismatch));
            }
        }));

        PCollection<Integer> mismatchingShards = joinedShards.apply("Compare Shard Checksums", ParDo.of(new DoFn<KV<Integer, CoGbkResult>, Integer>() {
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
                    c.output(shardId);
                }
            }
        }));

        // Compute total counts (as side inputs)
        PCollection<Long> totalSrcCount = srcShards.apply("Extract Source Counts", ParDo.of(new DoFn<KV<Integer, ShardMetrics>, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().getValue().count);
            }
        })).apply("Sum Total Source Counts", Combine.globally(Sum.ofLongs()));
        PCollectionView<Long> totalSrcCountView = totalSrcCount.apply("Create Src View", View.asSingleton());

        PCollection<Long> totalDstCount = dstShards.apply("Extract Dest Counts", ParDo.of(new DoFn<KV<Integer, ShardMetrics>, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().getValue().count);
            }
        })).apply("Sum Total Dest Counts", Combine.globally(Sum.ofLongs()));
        PCollectionView<Long> totalDstCountView = totalDstCount.apply("Create Dst View", View.asSingleton());

        PCollection<Long> mismatchCount = mismatchingShards.apply("Count Mismatches", Count.globally());
        PCollectionView<Long> mismatchCountView = mismatchCount.apply("Create Mismatch View", View.asSingleton());

        // Sample up to 5 mismatching shards for detailed diff
        PCollection<Iterable<Integer>> sampledShards = mismatchingShards.apply("Sample 100 Mismatches", Sample.fixedSizeGlobally(100));
        PCollectionView<Iterable<Integer>> targetShardsView = sampledShards.apply("Create Target Shards View", View.asSingleton());

        // Filter docs by mismatching shards (using side input)
        PCollection<Document> filteredSrcDocs = srcDocsRaw.apply("Filter Src Docs By Mismatch Shards",
                ParDo.of(new FilterByShardsFn(targetShardsView)).withSideInputs(targetShardsView));

        PCollection<Document> filteredDstDocs = dstDocsRaw.apply("Filter Dst Docs By Mismatch Shards",
                ParDo.of(new FilterByShardsFn(targetShardsView)).withSideInputs(targetShardsView));

        // Run detailed diff
        PCollection<String> diffResults = runDetailedDiff(filteredSrcDocs, filteredDstDocs);

        // Generate Unified Header
        PCollection<String> unifiedHeader = p.apply("Create Header Trigger", Create.of(1L))
                .apply("Generate Unified Summary Header", ParDo.of(new DoFn<Long, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        long srcTotal = c.sideInput(totalSrcCountView);
                        long dstTotal = c.sideInput(totalDstCountView);
                        long mismatches = c.sideInput(mismatchCountView);

                        c.output("==================================================================");
                        c.output("            MIGRATION LOAD TEST VERIFICATION REPORT               ");
                        c.output("==================================================================");
                        c.output(String.format("* Total Source Documents Examined      : %,d", srcTotal));
                        c.output(String.format("* Total Destination Documents Examined : %,d", dstTotal));
                        c.output(String.format("* Total Mismatching Shards             : %,d", mismatches));
                        c.output("==================================================================");
                        c.output("");
                        c.output("------------------------------------------------------------------");
                        c.output("                    COMPLETE SHARDED MANIFEST BREAKDOWN           ");
                        c.output("------------------------------------------------------------------");
                    }
                }).withSideInputs(totalSrcCountView, totalDstCountView, mismatchCountView));

        PCollection<String> unifiedFooter = p.apply("Create Footer Trigger", Create.of(1L))
                .apply("Generate Detailed Diff Transition Lines", ParDo.of(new DoFn<Long, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        long mismatches = c.sideInput(mismatchCountView);
                        c.output("------------------------------------------------------------------");
                        c.output("");

                        if (mismatches > 0) {
                            c.output("==================================================================");
                            c.output("              DETAILED MISMATCHING DOCUMENT SAMPLES              ");
                            c.output("==================================================================");
                            if (mismatches > 5) {
                                c.output("WARNING: ONLY A SAMPLE OF MISMATCHING SHARDS IS SHOWN BELOW!");
                                c.output("Total mismatching shards: " + mismatches + ". Only 5 were sampled for detailed diff.");
                                c.output("==================================================================");
                                c.output("");
                            }
                        }
                    }
                }).withSideInputs(mismatchCountView));

        // Merge header, shard breakdown, separator, and detailed diff results
        PCollectionList<String> collections = PCollectionList.of(unifiedHeader)
                .and(shardBreakdown)
                .and(unifiedFooter)
                .and(diffResults);
                
        PCollection<String> mergedResults = collections.apply("Merge Results", Flatten.pCollections());

        mergedResults.apply("Write Detailed Report", TextIO.write().to(options.getReportPath()).withoutSharding());

        p.run().waitUntilFinish();
    }

    private static PCollection<Document> readFirestore(Pipeline p, String project, String dbId, String collection, String stepName, com.google.protobuf.Timestamp readTime) {
        String parent = String.format("projects/%s/databases/%s/documents", project, dbId);

        com.google.firestore.v1.StructuredQuery query = com.google.firestore.v1.StructuredQuery.newBuilder()
                .addFrom(com.google.firestore.v1.StructuredQuery.CollectionSelector.newBuilder().setCollectionId(collection).setAllDescendants(true).build())
                .addOrderBy(
                        com.google.firestore.v1.StructuredQuery.Order.newBuilder()
                                .setField(com.google.firestore.v1.StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__").build())
                                .setDirection(com.google.firestore.v1.StructuredQuery.Direction.ASCENDING)
                                .build())
                .build();

        com.google.firestore.v1.PartitionQueryRequest.Builder requestBuilder = com.google.firestore.v1.PartitionQueryRequest.newBuilder()
                .setParent(parent)
                .setStructuredQuery(query)
                .setPartitionCount(5000);
                
        if (readTime != null) {
            requestBuilder.setReadTime(readTime);
        }

        com.google.firestore.v1.PartitionQueryRequest request = requestBuilder.build();

        PCollection<com.google.firestore.v1.PartitionQueryRequest> requests = p.apply("Create Request " + stepName, Create.of(request));

        PCollection<com.google.firestore.v1.RunQueryRequest> runQueryRequests = requests.apply("Partition " + stepName,
                org.apache.beam.sdk.io.gcp.firestore.FirestoreIO.v1().read().partitionQuery().build());

        PCollection<com.google.firestore.v1.RunQueryResponse> responses = runQueryRequests.apply("Run " + stepName,
                org.apache.beam.sdk.io.gcp.firestore.FirestoreIO.v1().read().runQuery().build());

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

    static int getShardId(Document doc) {
        String path = doc.getName();
        int docIndex = path.indexOf("/documents/");
        String relativePath = path.substring(docIndex + "/documents/".length());
        return Math.abs(relativePath.hashCode() % 100);
    }

    private static class MapByIdFn extends DoFn<Document, KV<String, Document>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            String path = doc.getName();
            int docIndex = path.indexOf("/documents/");
            String relativePath = path.substring(docIndex + "/documents/".length());
            c.output(KV.of(relativePath, doc));
        }
    }

    public static void hashLogicalValue(MessageDigest md, com.google.firestore.v1.Value value) {
        switch (value.getValueTypeCase()) {
            case STRING_VALUE:
                md.update((byte) 1);
                md.update(value.getStringValue().getBytes());
                break;
            case INTEGER_VALUE:
                md.update((byte) 2);
                long iv = value.getIntegerValue();
                md.update(java.nio.ByteBuffer.allocate(8).putLong(iv).array());
                break;
            case DOUBLE_VALUE:
                md.update((byte) 3);
                double dv = value.getDoubleValue();
                md.update(java.nio.ByteBuffer.allocate(8).putDouble(dv).array());
                break;
            case BOOLEAN_VALUE:
                md.update((byte) 4);
                md.update((byte) (value.getBooleanValue() ? 1 : 0));
                break;
            case TIMESTAMP_VALUE:
                md.update((byte) 5);
                md.update(java.nio.ByteBuffer.allocate(8).putLong(value.getTimestampValue().getSeconds()).array());
                md.update(java.nio.ByteBuffer.allocate(4).putInt(value.getTimestampValue().getNanos()).array());
                break;
            case NULL_VALUE:
                md.update((byte) 6);
                break;
            case REFERENCE_VALUE:
                md.update((byte) 7);
                md.update(value.getReferenceValue().getBytes());
                break;
            case GEO_POINT_VALUE:
                md.update((byte) 8);
                md.update(java.nio.ByteBuffer.allocate(8).putDouble(value.getGeoPointValue().getLatitude()).array());
                md.update(java.nio.ByteBuffer.allocate(8).putDouble(value.getGeoPointValue().getLongitude()).array());
                break;
            case ARRAY_VALUE:
                md.update((byte) 9);
                for (com.google.firestore.v1.Value v : value.getArrayValue().getValuesList()) {
                    hashLogicalValue(md, v);
                }
                break;
            case MAP_VALUE:
                md.update((byte) 10);
                java.util.TreeMap<String, com.google.firestore.v1.Value> sortedMap = new java.util.TreeMap<>(value.getMapValue().getFieldsMap());
                for (java.util.Map.Entry<String, com.google.firestore.v1.Value> entry : sortedMap.entrySet()) {
                    md.update(entry.getKey().getBytes());
                    hashLogicalValue(md, entry.getValue());
                }
                break;
            case BYTES_VALUE:
                md.update((byte) 11);
                md.update(value.getBytesValue().toByteArray());
                break;
            case VALUETYPE_NOT_SET:
                md.update((byte) 0);
                break;
        }
    }

    public static BigInteger hashContent(Document doc) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            TreeMap<String, com.google.firestore.v1.Value> sortedFields = new TreeMap<>(doc.getFieldsMap());
            
            for (java.util.Map.Entry<String, com.google.firestore.v1.Value> entry : sortedFields.entrySet()) {
                md.update(entry.getKey().getBytes());
                hashLogicalValue(md, entry.getValue());
            }
            
            byte[] bytes = md.digest();
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
            
            BigInteger hash = hashContent(doc);
            c.output(KV.of(shardId, KV.of(1L, hash)));
        }
    }

    public static class DocInfo implements java.io.Serializable {
        public String hash;
        public Document doc;
        public DocInfo(String hash, Document doc) {
            this.hash = hash;
            this.doc = doc;
        }
    }

    private static class MapDocInfoFn extends DoFn<Document, KV<String, DocInfo>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Document doc = c.element();
            String path = doc.getName();
            int docIndex = path.indexOf("/documents/");
            String relativePath = path.substring(docIndex + "/documents/".length());
            String hash = hashContent(doc).toString(16);
            c.output(KV.of(relativePath, new DocInfo(hash, doc)));
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
                String path = c.element().getKey();
                CoGbkResult result = c.element().getValue();

                DocInfo src = result.getOnly(srcTag, null);
                DocInfo dst = result.getOnly(dstTag, null);

                if (src == null && dst != null) {
                    c.output("EXTRA: " + path + " | DestHash=" + dst.hash);
                } else if (src != null && dst == null) {
                    c.output("MISSING: " + path + " | SrcHash=" + src.hash);
                } else if (src != null && dst != null) {
                    if (!src.doc.getFieldsMap().equals(dst.doc.getFieldsMap())) {
                        c.output("MISMATCH: " + path + " | Fields differ");
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
