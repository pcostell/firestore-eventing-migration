package com.google.cloud.firestore.migration;

import io.grpc.StatusRuntimeException;
import java.util.logging.Logger;

public class RetryHelper {
  private static final Logger logger = Logger.getLogger(RetryHelper.class.getName());
  private static final long BASE_DELAY_MS = 100;
  private static final long MAX_DELAY_MS = 10000;

  public static class RetryContext {
    private com.google.protobuf.ByteString previousTransactionId;

    public com.google.protobuf.ByteString getPreviousTransactionId() {
      return previousTransactionId;
    }

    public void setTransactionId(com.google.protobuf.ByteString transactionId) {
      this.previousTransactionId = transactionId;
    }
  }

  @FunctionalInterface
  public interface RetryableTask<T> {
    T run(RetryContext context) throws Exception;
  }

  // Retries 10 times internally with exponential backoff.
  // After 10 internal retries are exhausted, the exception is thrown permanently
  // to let the GCF container fail, relying on GCF's Eventarc/Pub/Sub trigger retry 
  // policy (RETRY_POLICY_RETRY) to perform the global retry.
  private static final int MAX_RETRIES = 10;

  public static <T> T runWithRetries(RetryableTask<T> task, String operationName) {
    int retries = 0;
    RetryContext context = new RetryContext();
    while (true) {
      try {
        return task.run(context);
      } catch (Exception e) {
        if (retries < MAX_RETRIES) {
          retries++;
          long delay = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * (long) Math.pow(2, retries)) + (long) (Math.random() * 100);
          logger.warning("Error in " + operationName + " (Class: " + e.getClass().getName() + ", Msg: " + e.getMessage() + "). Retrying " + retries + "/" + MAX_RETRIES + " in " + delay + " ms...");
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", ie);
          }
        } else {
          logger.severe("Operation " + operationName + " failed permanently after exhausting all " + MAX_RETRIES + " retries. Error: " + e.getMessage());
          throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
      }
    }
  }
}
