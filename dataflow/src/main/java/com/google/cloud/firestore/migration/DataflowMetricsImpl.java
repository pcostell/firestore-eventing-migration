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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;

public class DataflowMetricsImpl implements MigrationMetrics {
    private final Counter writeCounter;
    private final Counter deleteCounter;
    private final Counter noopWriteCounter;
    private final Counter noopDeleteCounter;
    private final Counter noopTimeDelayCounter;
    private final Distribution lagDistribution;
    private final Distribution potentialLagDistribution;

    public DataflowMetricsImpl(String source) {
        String namespace = "FirestoreMigration." + source;
        this.writeCounter = Metrics.counter(namespace, "doc_count_write");
        this.deleteCounter = Metrics.counter(namespace, "doc_count_delete");
        this.noopWriteCounter = Metrics.counter(namespace, "doc_count_noop_write");
        this.noopDeleteCounter = Metrics.counter(namespace, "doc_count_noop_delete");
        this.noopTimeDelayCounter = Metrics.counter(namespace, "doc_count_noop_time_delay");
        this.lagDistribution = Metrics.distribution(namespace, "lag_ms");
        this.potentialLagDistribution = Metrics.distribution(namespace, "potential_lag_ms");
    }

    @Override
    public void recordOperation(Operation op) {
        switch (op) {
            case WRITE: writeCounter.inc(); break;
            case DELETE: deleteCounter.inc(); break;
            case NOOP_WRITE: noopWriteCounter.inc(); break;
            case NOOP_DELETE: noopDeleteCounter.inc(); break;
            case NOOP_TIMEDELAY: noopTimeDelayCounter.inc(); break;
        }
    }

    @Override
    public void recordLag(long lagMillis) {
        lagDistribution.update(lagMillis);
    }

    @Override
    public void recordPotentialLag(long lagMillis) {
        potentialLagDistribution.update(lagMillis);
    }
}
