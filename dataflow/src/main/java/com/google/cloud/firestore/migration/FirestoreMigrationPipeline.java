package com.google.cloud.firestore.migration;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.transforms.Create;

public class FirestoreMigrationPipeline {

  public interface MigrationOptions extends DataflowPipelineOptions {
    @Description("Source project ID")
    @Required
    String getSourceProject();
    void setSourceProject(String value);

    @Description("Source database ID")
    @Required
    String getSourceDatabase();
    void setSourceDatabase(String value);

    @Description("Destination project ID")
    @Required
    String getDestinationProject();
    void setDestinationProject(String value);

    @Description("Destination database ID")
    @Required
    String getDestinationDatabase();
    void setDestinationDatabase(String value);
  }

  public static void main(String[] args) {
    MigrationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MigrationOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("CreateSignal", Create.of(1))
        .apply("ListCollections", ParDo.of(new DoFn<Integer, PartitionQueryRequest>() {
          private transient Firestore db;

          @ProcessElement
          public void processElement(ProcessContext c) {
            MigrationOptions opts = c.getPipelineOptions().as(MigrationOptions.class);
            Firestore db = FirestoreOptions.newBuilder()
                .setProjectId(opts.getSourceProject())
                .setDatabaseId(opts.getSourceDatabase())
                .build()
                .getService();

            for (com.google.cloud.firestore.CollectionReference coll : db.listCollections()) {
              PartitionQueryRequest request = PartitionQueryRequest.newBuilder()
                  .setParent("projects/" + opts.getSourceProject() + "/databases/" + opts.getSourceDatabase() + "/documents")
                  .setStructuredQuery(StructuredQuery.newBuilder()
                      .addFrom(StructuredQuery.CollectionSelector.newBuilder()
                          .setCollectionId(coll.getId())
                          .setAllDescendants(true)
                          .build()))
                  .setPartitionCount(5000)
                  .build();
              c.output(request);
            }
          }
        }))
        .apply("PartitionQuery", FirestoreIO.v1().read().partitionQuery().build())
        .apply("RunQuery", FirestoreIO.v1().read().runQuery().build())
        .apply("ExtractDocument", ParDo.of(new DoFn<RunQueryResponse, Document>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            RunQueryResponse resp = c.element();
            if (resp.hasDocument()) {
              c.output(resp.getDocument());
            }
          }
        }))
        .apply("FilterShadowJournal", ParDo.of(new DoFn<Document, Document>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            Document doc = c.element();
            if (!doc.getName().contains("_ShadowJournal")) {
              c.output(doc);
            }
          }
        }))
        .apply("WriteWithHWM", ParDo.of(new WriteWithHWMFn(options.getDestinationProject(), options.getDestinationDatabase())));

    p.run();
  }

  public static class WriteWithHWMFn extends DoFn<Document, Void> {
    private final String destProject;
    private final String destDatabase;
    private transient Firestore db;
    private transient MigrationMetrics metrics;
    private transient FirestoreSink sink;

    public WriteWithHWMFn(String destProject, String destDatabase) {
      this.destProject = destProject;
      this.destDatabase = destDatabase;
    }

    @Setup
    public void setup() {
      this.db = FirestoreOptions.newBuilder()
          .setProjectId(destProject)
          .setDatabaseId(destDatabase)
          .build()
          .getService();
      this.metrics = new DataflowMetricsImpl("backfill");
      this.sink = new FirestoreSink(db, metrics);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      sink.process(c.element());
    }
  }
}
