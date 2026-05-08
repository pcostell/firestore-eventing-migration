package com.google.cloud.firestore.migration;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BeginTransactionRequest;
import com.google.firestore.v1.BeginTransactionResponse;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.RollbackRequest;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.util.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

public class FirestoreSink {
  private static final Logger logger = Logger.getLogger(FirestoreSink.class.getName());

  private final FirestoreClient client;
  private final MigrationMetrics metrics;
  private final String destDbPath;

  public FirestoreSink(FirestoreClient client, MigrationMetrics metrics, String destProject, String destDatabase) {
    this.client = client;
    this.metrics = metrics;
    this.destDbPath = "projects/" + destProject + "/databases/" + destDatabase;
  }

  public static class Mutation {
    private final String fullPath;
    private final Timestamp commitTime;
    private final Document doc; // null for delete

    public Mutation(String fullPath, Timestamp commitTime, Document doc) {
      this.fullPath = fullPath;
      this.commitTime = commitTime;
      this.doc = doc;
    }

    public String getFullPath() { return fullPath; }
    public Timestamp getCommitTime() { return commitTime; }
    public Document getDoc() { return doc; }
    public boolean isDelete() { return doc == null; }
  }

  public void process(Document doc) {
    com.google.protobuf.Timestamp updateTime = doc.getUpdateTime();
    Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(updateTime.getSeconds(), updateTime.getNanos());
    process(doc.getName(), commitTime, doc);
  }

  public void process(String fullPath, Timestamp commitTime, Document doc) {
    processBatch(java.util.Collections.singletonList(new Mutation(fullPath, commitTime, doc)));
  }

  public void processBatch(java.util.List<Mutation> mutations) {
    if (mutations == null || mutations.isEmpty()) {
      return;
    }

    RetryHelper.runWithRetries((context) -> {
      ByteString transactionId = null;
      try {
        BeginTransactionRequest.Builder beginRequestBuilder = BeginTransactionRequest.newBuilder()
            .setDatabase(destDbPath);
            
        if (context.getPreviousTransactionId() != null) {
          com.google.firestore.v1.TransactionOptions.ReadWrite readWriteOptions = 
              com.google.firestore.v1.TransactionOptions.ReadWrite.newBuilder()
                  .setRetryTransaction(context.getPreviousTransactionId())
                  .build();
                  
          com.google.firestore.v1.TransactionOptions options = 
              com.google.firestore.v1.TransactionOptions.newBuilder()
                  .setReadWrite(readWriteOptions)
                  .build();
                  
          beginRequestBuilder.setOptions(options);
        }

        BeginTransactionResponse beginResponse = client.beginTransaction(beginRequestBuilder.build());
        transactionId = beginResponse.getTransaction();
        context.setTransactionId(transactionId); // Store for subsequent retries

        // Build BatchGet request for all journals
        BatchGetDocumentsRequest.Builder getRequestBuilder = BatchGetDocumentsRequest.newBuilder()
            .setDatabase(destDbPath)
            .setTransaction(transactionId);
            
        java.util.Map<String, Mutation> mutationMap = new java.util.HashMap<>();
        java.util.List<String> journalPaths = new java.util.ArrayList<>();
        
        for (Mutation mut : mutations) {
          String fullPath = mut.getFullPath();
          
          String hashedName = hashDocName(fullPath);
          String journalPath = destDbPath + "/documents/_ShadowJournal/" + hashedName;
          
          mutationMap.put(journalPath, mut);
          journalPaths.add(journalPath);
          getRequestBuilder.addDocuments(journalPath);
        }

        Iterator<BatchGetDocumentsResponse> responseIterator = client.batchGetDocumentsCallable().call(getRequestBuilder.build()).iterator();
        
        java.util.Map<String, Timestamp> hwmMap = new java.util.HashMap<>();
        while (responseIterator.hasNext()) {
          BatchGetDocumentsResponse resp = responseIterator.next();
          if (resp.hasFound()) {
            Document journalDoc = resp.getFound();
            Value hwmVal = journalDoc.getFieldsMap().get("hwm");
            if (hwmVal != null && hwmVal.hasTimestampValue()) {
              com.google.protobuf.Timestamp ts = hwmVal.getTimestampValue();
              hwmMap.put(journalDoc.getName(), Timestamp.ofTimeSecondsAndNanos(ts.getSeconds(), ts.getNanos()));
            }
          }
        }

        CommitRequest.Builder commitBuilder = CommitRequest.newBuilder()
            .setDatabase(destDbPath)
            .setTransaction(transactionId);

        java.util.List<Mutation> committedMutations = new java.util.ArrayList<>();
        int writeCount = 0;

        for (String journalPath : journalPaths) {
          Mutation mut = mutationMap.get(journalPath);
          String fullPath = mut.getFullPath();
          String destFullPath = getDestFullPath(fullPath);
          
          Timestamp commitTime = mut.getCommitTime();
          Timestamp existingHwm = hwmMap.getOrDefault(journalPath, Timestamp.ofTimeSecondsAndNanos(0, 0));

          if (commitTime.compareTo(existingHwm) > 0) {
            MigrationMetrics.Operation op;
            if (mut.isDelete()) {
              commitBuilder.addWrites(Write.newBuilder().setDelete(destFullPath));
              op = MigrationMetrics.Operation.DELETE;
            } else {
              Document destDoc = Document.newBuilder(mut.getDoc())
                  .setName(destFullPath)
                  .build();
              commitBuilder.addWrites(Write.newBuilder().setUpdate(destDoc));
              op = MigrationMetrics.Operation.WRITE;
            }
            
            Document journalDoc = Document.newBuilder()
                .setName(journalPath)
                .putFields("hwm", Value.newBuilder().setTimestampValue(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(commitTime.getSeconds())
                        .setNanos(commitTime.getNanos())
                        .build()
                ).build())
                .build();
            commitBuilder.addWrites(Write.newBuilder().setUpdate(journalDoc));
            
            committedMutations.add(mut);
            metrics.recordOperation(op);
            writeCount++;
          } else {
            MigrationMetrics.Operation op = mut.isDelete() ? MigrationMetrics.Operation.NOOP_DELETE : MigrationMetrics.Operation.NOOP_WRITE;
            metrics.recordOperation(op);
          }
        }

        if (writeCount > 0) {
          com.google.firestore.v1.CommitResponse commitResponse = client.commit(commitBuilder.build());
          com.google.protobuf.Timestamp destCommitTime = commitResponse.getCommitTime();
          
          long destCommitMillis = timestampToMillis(destCommitTime);
          
          for (Mutation mut : committedMutations) {
            metrics.recordLag(destCommitMillis - timestampToMillis(mut.getCommitTime()));
          }
          logger.info("Batch committed successfully with " + writeCount + " writes.");
        } else {
          client.rollback(RollbackRequest.newBuilder()
              .setDatabase(destDbPath)
              .setTransaction(transactionId)
              .build());
          logger.info("Batch rolled back: 0 documents required update.");
        }
        return null;

      } catch (Exception e) {
        if (transactionId != null && !transactionId.isEmpty()) {
          try {
            client.rollback(RollbackRequest.newBuilder()
                .setDatabase(destDbPath)
                .setTransaction(transactionId)
                .build());
          } catch (Exception re) {
            logger.warning("Failed to rollback transaction " + transactionId + " after failure: " + re.getMessage());
          }
        }
        throw e;
      }
    }, "batch commit");
  }

  private String getDestFullPath(String sourceFullPath) {
    int documentsIdx = sourceFullPath.indexOf("/documents/");
    if (documentsIdx == -1) {
        throw new IllegalArgumentException("Invalid document path: " + sourceFullPath);
    }
    return destDbPath + "/documents/" + sourceFullPath.substring(documentsIdx + 11);
  }

  private long timestampToMillis(com.google.protobuf.Timestamp ts) {
    return (ts.getSeconds() * 1000) + (ts.getNanos() / 1000000);
  }

  private long timestampToMillis(Timestamp ts) {
    return (ts.getSeconds() * 1000) + (ts.getNanos() / 1000000);
  }

  String hashDocName(String name) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hashValue = md.digest(name.getBytes(StandardCharsets.UTF_8));
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(hashValue);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not found", e);
    }
  }
}
