package com.google.cloud.firestore.migration;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.v1.FirestoreClient;
import java.io.IOException;
import com.google.firestore.v1.Document;
import com.google.protobuf.util.Timestamps;

import io.cloudevents.CloudEvent;
import com.google.cloud.functions.CloudEventsFunction;
import java.time.OffsetDateTime;

import java.util.logging.Logger;

public class LiveSinkFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(LiveSinkFunction.class.getName());

  private final FirestoreSink sink;

  public LiveSinkFunction() {
    String destProject = System.getenv("DEST_PROJECT");
    String destDatabase = System.getenv("DEST_DB");

    try {
      FirestoreClient client = FirestoreClient.create();
      MigrationMetrics metrics = new CloudFunctionMetrics("live", destProject);
      this.sink = new FirestoreSink(client, metrics, destProject, destDatabase);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create FirestoreClient", e);
    }
  }

  // Visible for testing
  LiveSinkFunction(FirestoreSink sink) {
    this.sink = sink;
  }

  @Override
  public void accept(CloudEvent event) throws Exception {
    logger.info("Received event type: " + event.getType());

    OffsetDateTime odt = event.getTime();
    if (odt == null) {
      logger.severe("Fatal: CloudEvent time attribute is missing!");
      throw new RuntimeException("CloudEvent time attribute is missing");
    }
    Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(odt.toEpochSecond(), odt.getNano());

    if (event.getData() == null) {
      logger.warning("Event data is null");
      return;
    }

    byte[] dataBytes = event.getData().toBytes();
    com.google.events.cloud.firestore.v1.DocumentEventData documentEventData = com.google.events.cloud.firestore.v1.DocumentEventData.parseFrom(dataBytes);

    com.google.events.cloud.firestore.v1.Document originalDoc = documentEventData.getValue();
    
    if (originalDoc != null && !originalDoc.getName().isEmpty()) {
        // Insert or Update
        Document firestoreDoc = Document.parseFrom(originalDoc.toByteArray());
        logger.info("Processing document: " + firestoreDoc.getName());
        
        sink.process(firestoreDoc.getName(), commitTime, firestoreDoc);
    } else {
        // Delete
        com.google.events.cloud.firestore.v1.Document oldDoc = documentEventData.getOldValue();
        if (oldDoc != null && !oldDoc.getName().isEmpty()) {
            String fullPath = oldDoc.getName();
            logger.info("Processing delete: " + fullPath);
            
            sink.process(fullPath, commitTime, null);
        } else {
            logger.warning("Event has no value and no old_value");
        }
    }
  }
}
