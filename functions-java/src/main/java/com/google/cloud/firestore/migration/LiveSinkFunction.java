/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.firestore.migration;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.v1.FirestoreClient;
import java.io.IOException;
import com.google.firestore.v1.Document;
import com.google.protobuf.util.Timestamps;

import io.cloudevents.CloudEvent;
import com.google.cloud.functions.CloudEventsFunction;
import java.time.Instant;
import java.time.OffsetDateTime;

import java.util.logging.Logger;

public class LiveSinkFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(LiveSinkFunction.class.getName());

  private final FirestoreSink sink;
  private final MigrationMetrics metrics;
  private final Instant syncStart;

  public LiveSinkFunction() {
    String destProject = System.getenv("DEST_PROJECT");
    String destDatabase = System.getenv("DEST_DB");
    String syncStartEnv = System.getenv("SYNC_START");

    this.metrics = new CloudFunctionMetrics("live", destProject);
    if (syncStartEnv != null && !syncStartEnv.isEmpty()) {
      try {
        this.syncStart = Instant.parse(syncStartEnv);
      } catch (Exception e) {
        logger.severe("Invalid SYNC_START environment variable: " + syncStartEnv);
        throw new RuntimeException("Invalid SYNC_START format", e);
      }
    } else {
      this.syncStart = null;
    }

    try {
      FirestoreClient client = FirestoreClient.create();
      this.sink = new FirestoreSink(client, metrics, destProject, destDatabase);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create FirestoreClient", e);
    }
  }

  // Visible for testing
  LiveSinkFunction(FirestoreSink sink) {
    this.sink = sink;
    this.metrics = new CloudFunctionMetrics("live", "test-project");
    this.syncStart = Instant.MIN;
  }

  // Visible for testing
  LiveSinkFunction(FirestoreSink sink, MigrationMetrics metrics, String syncStartEnv) {
    this.sink = sink;
    this.metrics = metrics;
    if (syncStartEnv != null && !syncStartEnv.isEmpty()) {
      this.syncStart = Instant.parse(syncStartEnv);
    } else {
      this.syncStart = null;
    }
  }

  @Override
  public void accept(CloudEvent event) throws Exception {
    logger.info("Received event type: " + event.getType());

    OffsetDateTime odt = event.getTime();
    if (odt == null) {
      logger.severe("Fatal: CloudEvent time attribute is missing!");
      throw new RuntimeException("CloudEvent time attribute is missing");
    }

    if (syncStart == null) {
      logger.info("SYNC_START is not set. Discarding event as NOOP_TIMEDELAY.");
      metrics.recordOperation(MigrationMetrics.Operation.NOOP_TIMEDELAY);
      return;
    }

    Instant eventInstant = odt.toInstant();
    if (!eventInstant.isAfter(syncStart)) {
      logger.info("Event commit time " + eventInstant + " is not after SYNC_START " + syncStart + ". Discarding as NOOP_TIMEDELAY.");
      metrics.recordOperation(MigrationMetrics.Operation.NOOP_TIMEDELAY);
      return;
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
