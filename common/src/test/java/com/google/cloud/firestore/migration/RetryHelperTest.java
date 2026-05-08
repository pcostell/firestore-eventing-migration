package com.google.cloud.firestore.migration;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RetryHelperTest {

    @Test
    public void testRunWithRetries_successFirstAttempt() {
        AtomicInteger executions = new AtomicInteger(0);
        String result = RetryHelper.runWithRetries((context) -> {
            executions.incrementAndGet();
            return "success";
        }, "test operation");

        assertEquals("success", result);
        assertEquals(1, executions.get());
    }

    @Test
    public void testRunWithRetries_retryOnContentionThenSuccess() {
        AtomicInteger executions = new AtomicInteger(0);
        String result = RetryHelper.runWithRetries((context) -> {
            int attempt = executions.incrementAndGet();
            if (attempt == 1) {
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Aborted due to contention"));
            }
            return "success";
        }, "test contention");

        assertEquals("success", result);
        assertEquals(2, executions.get());
    }

    @Test
    public void testRunWithRetries_retryOnTransactionExpiredThenSuccess() {
        AtomicInteger executions = new AtomicInteger(0);
        String result = RetryHelper.runWithRetries((context) -> {
            int attempt = executions.incrementAndGet();
            if (attempt == 1) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("The referenced transaction has expired or is no longer valid."));
            }
            return "success";
        }, "test expiry");

        assertEquals("success", result);
        assertEquals(2, executions.get());
    }

    @Test
    public void testRunWithRetries_failOnNonRetryableError() {
        AtomicInteger executions = new AtomicInteger(0);
        try {
            RetryHelper.runWithRetries((context) -> {
                executions.incrementAndGet();
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("No permission"));
            }, "test non-retryable");
            fail("Expected StatusRuntimeException to be thrown");
        } catch (Exception e) {
            assertTrue(e instanceof StatusRuntimeException);
            assertEquals(Status.Code.PERMISSION_DENIED, ((StatusRuntimeException) e).getStatus().getCode());
        }
        assertEquals(1, executions.get());
    }
}
