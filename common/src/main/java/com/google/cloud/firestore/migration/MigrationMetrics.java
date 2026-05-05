package com.google.cloud.firestore.migration;

public interface MigrationMetrics {
    enum Operation {
        WRITE,
        DELETE,
        NOOP_WRITE,
        NOOP_DELETE
    }

    void recordOperation(Operation op);
    void recordLag(long lagMillis);
}
