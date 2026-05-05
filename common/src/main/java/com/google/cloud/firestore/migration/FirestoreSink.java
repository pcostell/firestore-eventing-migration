package com.google.cloud.firestore.migration;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicReference;

public class FirestoreSink {

  private final Firestore db;
  private final MigrationMetrics metrics;

  public FirestoreSink(Firestore db, MigrationMetrics metrics) {
    this.db = db;
    this.metrics = metrics;
  }

  public void process(Document doc) {
    com.google.protobuf.Timestamp updateTime = doc.getUpdateTime();
    Instant commitTime = Instant.ofEpochSecond(updateTime.getSeconds(), updateTime.getNanos());
    process(doc.getName(), commitTime, doc);
  }

  public void process(String fullPath, Instant commitTime, Document doc) {
    String path = fullPath.substring(fullPath.indexOf("/documents/") + 11);
    DocumentReference destDocRef = db.document(path);
    String hashedName = hashDocName(fullPath);
    DocumentReference journalRef = db.collection("_ShadowJournal").document(hashedName);

    final AtomicReference<MigrationMetrics.Operation> operation = new AtomicReference<>();
    try {
      db.runTransaction(transaction -> {
        DocumentSnapshot journalSnap = transaction.get(journalRef).get();
        Instant existingHwm = journalSnap.exists() && journalSnap.contains("hwm")
            ? journalSnap.getDate("hwm").toInstant() : Instant.EPOCH;

        if (commitTime.isAfter(existingHwm)) {
          if (doc == null) {
            transaction.delete(destDocRef);
            operation.set(MigrationMetrics.Operation.DELETE);
          } else {
            Map<String, Object> data = new HashMap<>();
            doc.getFieldsMap().forEach((k, v) -> data.put(k, convertValue(v)));
            transaction.set(destDocRef, data);
            operation.set(MigrationMetrics.Operation.WRITE);
          }

          Map<String, Object> journalData = new HashMap<>();
          journalData.put("hwm", Date.from(commitTime));

          transaction.set(journalRef, journalData);
        } else {
          if (doc == null) {
            operation.set(MigrationMetrics.Operation.NOOP_DELETE);
          } else {
            operation.set(MigrationMetrics.Operation.NOOP_WRITE);
          }
        }
        return null;
      }).get();

      // Record metrics after successful transaction
      if (operation.get() != null) {
        metrics.recordOperation(operation.get());
      }

      // We use the current time as an approximation of the destination write time
      // because the transaction API does not return the commit timestamp.
      long lagMillis = Instant.now().toEpochMilli() - commitTime.toEpochMilli();
      metrics.recordLag(lagMillis);

    } catch (Exception e) {
      throw new RuntimeException("Transaction failed for " + path, e);
    }
  }

  private Object convertValue(Value v) {
    if (v.hasStringValue()) return v.getStringValue();
    if (v.hasBooleanValue()) return v.getBooleanValue();
    if (v.hasIntegerValue()) return v.getIntegerValue();
    if (v.hasDoubleValue()) return v.getDoubleValue();
    if (v.hasTimestampValue()) return new Date(v.getTimestampValue().getSeconds() * 1000);
    return null;
  }

  private String hashDocName(String name) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hashValue = md.digest(name.getBytes(StandardCharsets.UTF_8));
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(hashValue);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not found", e);
    }
  }
}
