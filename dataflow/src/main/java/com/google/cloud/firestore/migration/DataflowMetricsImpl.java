package com.google.cloud.firestore.migration;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;

public class DataflowMetricsImpl implements MigrationMetrics {
    private final Counter writeCounter;
    private final Counter deleteCounter;
    private final Counter noopWriteCounter;
    private final Counter noopDeleteCounter;
    private final Distribution lagDistribution;

    public DataflowMetricsImpl(String source) {
        String namespace = "FirestoreMigration." + source;
        this.writeCounter = Metrics.counter(namespace, "doc_count_write");
        this.deleteCounter = Metrics.counter(namespace, "doc_count_delete");
        this.noopWriteCounter = Metrics.counter(namespace, "doc_count_noop_write");
        this.noopDeleteCounter = Metrics.counter(namespace, "doc_count_noop_delete");
        this.lagDistribution = Metrics.distribution(namespace, "lag_ms");
    }

    @Override
    public void recordOperation(Operation op) {
        switch (op) {
            case WRITE: writeCounter.inc(); break;
            case DELETE: deleteCounter.inc(); break;
            case NOOP_WRITE: noopWriteCounter.inc(); break;
            case NOOP_DELETE: noopDeleteCounter.inc(); break;
        }
    }

    @Override
    public void recordLag(long lagMillis) {
        lagDistribution.update(lagMillis);
    }
}
