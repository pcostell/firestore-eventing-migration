package com.google.cloud.firestore.migration;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.firestore.v1.Document;
import com.google.protobuf.util.Timestamps;

import io.cloudevents.CloudEvent;
import com.google.cloud.functions.CloudEventsFunction;

import java.util.logging.Logger;

public class LiveSinkFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(LiveSinkFunction.class.getName());

  private final Firestore db;
  private final FirestoreSink sink;

  public LiveSinkFunction() {
    String destProject = System.getenv("DEST_PROJECT");
    String destDatabase = System.getenv("DEST_DB");

    this.db = FirestoreOptions.newBuilder()
        .setProjectId(destProject)
        .setDatabaseId(destDatabase)
        .build()
        .getService();
    MigrationMetrics metrics = new CloudFunctionMetrics("live", destProject);
    this.sink = new FirestoreSink(db, metrics);
  }

  // Visible for testing
  LiveSinkFunction(Firestore db, FirestoreSink sink) {
    this.db = db;
    this.sink = sink;
  }

  @Override
  public void accept(CloudEvent event) throws Exception {
    logger.info("Received event type: " + event.getType());

    if (event.getData() == null) {
      logger.warning("Event data is null");
      return;
    }

    byte[] dataBytes = event.getData().toBytes();
    com.google.events.cloud.firestore.v1.DocumentEventData documentEventData = com.google.events.cloud.firestore.v1.DocumentEventData.parseFrom(dataBytes);

    com.google.events.cloud.firestore.v1.Document originalDoc = documentEventData.getValue();
    
    if (originalDoc != null && !originalDoc.getName().isEmpty()) {
        // Insert or Update
        // We convert from events...Document to firestore...Document via byte serialization to avoid manual mapping
        Document firestoreDoc = Document.parseFrom(originalDoc.toByteArray());
        logger.info("Processing document: " + firestoreDoc.getName());
        
        if (!firestoreDoc.hasUpdateTime()) {
            logger.severe("Fatal: Document update_time is missing for insert/update!");
            throw new RuntimeException("Document update_time is missing");
        }
        
        sink.process(firestoreDoc);
    } else {
        // Delete
        com.google.events.cloud.firestore.v1.Document oldDoc = documentEventData.getOldValue();
        if (oldDoc != null && !oldDoc.getName().isEmpty()) {
            String fullPath = oldDoc.getName();
            logger.info("Processing delete: " + fullPath);
            
            Document firestoreOldDoc = Document.parseFrom(oldDoc.toByteArray());
            if (!firestoreOldDoc.hasUpdateTime()) {
                logger.severe("Fatal: Old Document update_time is missing for delete!");
                throw new RuntimeException("Old Document update_time is missing");
            }
            
            // The commit timestamp of the deletion is not propagated through the event. However, we know that:
            // 1) The timestamp is guaranteed to be greater than the old update.
            // 2) The timestamp is guaranteed to be less than any future new insertion.
            // 3) The timestamp for a future new insertion must be at least 2 full nanos after the old update,
            //    since we are guarateed external serializable ordering of the timestamps.
            // Therefore, ts_old < ts_old+1 < ts_insert, and ts_old+1 may be used as a proxy for guaranteeing ordering of the deletion.
            com.google.protobuf.Timestamp oldUpdateTime = firestoreOldDoc.getUpdateTime();
            java.time.Instant deleteTime = java.time.Instant.ofEpochSecond(oldUpdateTime.getSeconds(), oldUpdateTime.getNanos()).plusNanos(1);
            
            sink.process(fullPath, deleteTime, null);
        } else {
            logger.warning("Event has no value and no old_value");
        }
    }
  }
}
