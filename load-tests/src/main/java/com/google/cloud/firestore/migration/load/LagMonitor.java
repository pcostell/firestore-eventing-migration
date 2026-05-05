package com.google.cloud.firestore.migration.load;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.FieldValue;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class LagMonitor {

    private static final int CYCLE_COUNT = 10;
    private static final int DOCS_PER_CYCLE = 100;
    private static final int BATCH_SIZE = 100;

    public static void main(String[] args) throws Exception {
        String srcProject = System.getenv("SOURCE_PROJECT");
        String srcDatabaseId = System.getenv("SOURCE_DB");
        String dstProject = System.getenv("DEST_PROJECT");
        String dstDatabaseId = System.getenv("DEST_DB");
        String collectionName = System.getenv().getOrDefault("LAG_COLLECTION", "lag_metrics");

        if (srcProject == null || dstProject == null) {
            throw new RuntimeException("SOURCE_PROJECT and DEST_PROJECT must be set");
        }

        Firestore srcDb = FirestoreOptions.newBuilder()
                .setProjectId(srcProject)
                .setDatabaseId(srcDatabaseId)
                .build()
                .getService();

        Firestore dstDb = FirestoreOptions.newBuilder()
                .setProjectId(dstProject)
                .setDatabaseId(dstDatabaseId)
                .build()
                .getService();

        System.out.println("Starting Cyclic Lag Monitor (10 cycles of 1000 docs each)...");

        List<Long> allLags = new ArrayList<>();

        for (int cycle = 0; cycle < CYCLE_COUNT; cycle++) {
            System.out.println("\n--- Starting Cycle " + (cycle + 1) + " ---");
            List<Long> cycleLags = runCycle(srcDb, dstDb, collectionName, cycle);
            allLags.addAll(cycleLags);
            printStats("Cycle " + (cycle + 1), cycleLags);
            Thread.sleep(30000); // Sleep 30s between cycles
        }

        System.out.println("\n=== Final Aggregated Report ===");
        printStats("All Cycles", allLags);
    }

    private static List<Long> runCycle(Firestore srcDb, Firestore dstDb, String collection, int cycleId) throws Exception {
        List<Long> lags = Collections.synchronizedList(new ArrayList<>());

        // 1. Write Docs to Source in Batches
        for (int i = 0; i < DOCS_PER_CYCLE; i += BATCH_SIZE) {
            WriteBatch batch = srcDb.batch();
            int end = Math.min(i + BATCH_SIZE, DOCS_PER_CYCLE);
            for (int j = i; j < end; j++) {
                String docId = String.format("lag-%d-%d", cycleId, j);
                Map<String, Object> data = new HashMap<>();
                data.put("writtenAt", FieldValue.serverTimestamp());
                data.put("cycleId", cycleId);
                batch.set(srcDb.collection(collection).document(docId), data);
            }
            batch.commit().get();
        }

        System.out.println("Cycle " + cycleId + " writes complete. Polling for propagation...");

        // 2. Poll Query on Dest for this cycle
        long startTime = System.currentTimeMillis();
        long timeout = 10 * 60 * 1000; // 10 minutes
        Set<String> receivedDocs = new HashSet<>();

        while (receivedDocs.size() < DOCS_PER_CYCLE && (System.currentTimeMillis() - startTime) < timeout) {
            QuerySnapshot snapshot = dstDb.collection(collection)
                    .whereEqualTo("cycleId", cycleId)
                    .get().get();

            for (QueryDocumentSnapshot doc : snapshot) {
                String docId = doc.getId();
                if (!receivedDocs.contains(docId)) {
                    receivedDocs.add(docId);
                    if (doc.contains("writtenAt")) {
                        Timestamp updateTime = doc.getUpdateTime();
                        Timestamp writeTime = doc.getTimestamp("writtenAt");
                        if (writeTime != null && updateTime != null) {
                            long updateMillis = updateTime.getSeconds() * 1000 + updateTime.getNanos() / 1000000;
                            long writeMillis = writeTime.getSeconds() * 1000 + writeTime.getNanos() / 1000000;
                            lags.add(updateMillis - writeMillis);
                        }
                    }
                }
            }

            if (receivedDocs.size() < DOCS_PER_CYCLE) {
                Thread.sleep(30000); // Poll every 30 seconds
            }
        }

        if (receivedDocs.size() < DOCS_PER_CYCLE) {
            System.err.println("Warning: Cycle " + cycleId + " timed out. Not all docs arrived. Found " + receivedDocs.size() + " of " + DOCS_PER_CYCLE);
        }

        return new ArrayList<>(lags);
    }

    private static void printStats(String label, List<Long> lags) {
        if (lags.isEmpty()) {
            System.out.println(label + ": No data points recorded.");
            return;
        }

        Collections.sort(lags);
        int size = lags.size();
        long p50 = lags.get(size / 2);
        long p90 = lags.get((int) (size * 0.9));
        long p99 = lags.get((int) (size * 0.99));
        long p100 = lags.get(size - 1);

        System.out.println(label + " Stats (ms):");
        System.out.println("  Count: " + size);
        System.out.println("  P50  : " + p50);
        System.out.println("  P90  : " + p90);
        System.out.println("  P99  : " + p99);
        System.out.println("  P100 : " + p100);
    }
}
