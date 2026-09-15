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

package com.google.cloud.firestore.migration.load;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ResourceExhaustedException;

public class TrafficGeneratorApp implements HttpFunction {
    
    private final Firestore db;
    private final String collectionName;
    private static final String STATIC_PAYLOAD = "A".repeat(1024);

    public TrafficGeneratorApp() {
        String project = System.getenv("SOURCE_PROJECT");
        String databaseId = System.getenv("SOURCE_DB");
        this.collectionName = System.getenv().getOrDefault("COLLECTION_NAME", "test_data");

        if (project == null || databaseId == null) {
            throw new RuntimeException("SOURCE_PROJECT and SOURCE_DB must be set");
        }

        this.db = FirestoreOptions.newBuilder()
            .setProjectId(project)
            .setDatabaseId(databaseId)
            .build()
            .getService();
    }

    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException {
        // Read body
        StringBuilder sb = new StringBuilder();
        try (var reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        String body = sb.toString().trim();

        if (body.isEmpty()) {
            response.setStatusCode(400);
            response.getWriter().write("Empty request body");
            return;
        }

        String[] lines = body.split("\n");
        WriteBatch batch = db.batch();
        int count = 0;

        Map<String, Object> data = new HashMap<>();
        data.put("stage", "load_test");
        data.put("time", System.currentTimeMillis());
        data.put("randomPayload", STATIC_PAYLOAD);

        for (String line : lines) {
            String[] parts = line.split("=");
            if (parts.length != 2) {
                response.setStatusCode(400);
                response.getWriter().write("Invalid format in line: " + line);
                return;
            }
            String docId = parts[0];
            String op = parts[1];

            switch (op) {
                case "insert":
                case "update":
                    batch.set(db.collection(collectionName).document(docId), data);
                    break;
                case "delete":
                    batch.delete(db.collection(collectionName).document(docId));
                    break;
                default:
                    response.setStatusCode(400);
                    response.getWriter().write("Invalid operation: " + op + " in line: " + line);
                    return;
            }
            count++;
        }

        int maxRetries = 5;
        long baseDelayMs = 1000;
        int retries = 0;

        while (true) {
            try {
                batch.commit().get();
                response.setStatusCode(200);
                response.getWriter().write("OK processed " + count + " operations");
                break; // Success, exit loop
            } catch (InterruptedException | ExecutionException e) {
                Throwable cause = e.getCause();
                boolean shouldRetry = false;
                
                if (cause instanceof ApiException) {
                    ApiException apiException = (ApiException) cause;
                    if (apiException.isRetryable() || apiException instanceof ResourceExhaustedException) {
                        shouldRetry = true;
                    }
                }

                if (shouldRetry && retries < maxRetries) {
                    retries++;
                    // Exponential backoff with jitter
                    long delay = baseDelayMs * (long) Math.pow(2, retries) + (long) (Math.random() * 1000);
                    System.err.println("Retryable error writing to Firestore (e.g., RESOURCE_EXHAUSTED). Retrying in " + delay + " ms (retry " + retries + "/" + maxRetries + ")... Error: " + cause.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        response.setStatusCode(500);
                        response.getWriter().write("Interrupted during retry backoff: " + ie.getMessage());
                        return;
                    }
                } else {
                    System.err.println("ERROR writing to Firestore after " + retries + " retries: " + e.getMessage());
                    e.printStackTrace(System.err);
                    response.setStatusCode(500);
                    response.getWriter().write("Error writing to Firestore: " + e.getMessage());
                    break; // Fatal error or max retries exceeded, exit loop
                }
            }
        }
    }
}
