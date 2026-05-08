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

  public static <T> T runWithRetries(RetryableTask<T> task, String operationName) {
    int retries = 0;
    RetryContext context = new RetryContext();
    while (true) {
      try {
        return task.run(context);
      } catch (Exception e) {
        boolean isRetryable = false;
        Throwable cause = e;
        while (cause != null) {
          if (cause instanceof StatusRuntimeException) {
            StatusRuntimeException se = (StatusRuntimeException) cause;
            io.grpc.Status.Code code = se.getStatus().getCode();
            if (code == io.grpc.Status.Code.ABORTED) {
              isRetryable = true;
              break;
            } else if (code == io.grpc.Status.Code.INVALID_ARGUMENT && 
                       se.getMessage() != null && 
                       se.getMessage().contains("transaction has expired")) {
              isRetryable = true;
              break;
            }
          }
          cause = cause.getCause();
        }

        if (isRetryable) {
          retries++;
          long delay = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * (long) Math.pow(2, retries)) + (long) (Math.random() * 100);
          logger.warning("Retryable error in " + operationName + ". Retrying in " + delay + " ms (retry " + retries + ")...");
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", ie);
          }
        } else {
          logger.severe("Operation " + operationName + " failed due to non-retryable error: " + e.getMessage());
          throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
      }
    }
  }
}
