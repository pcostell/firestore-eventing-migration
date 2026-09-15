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
