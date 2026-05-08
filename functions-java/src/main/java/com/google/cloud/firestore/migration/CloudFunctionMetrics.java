package com.google.cloud.firestore.migration;

import java.util.logging.Logger;

public class CloudFunctionMetrics implements MigrationMetrics {
    private static final Logger logger = Logger.getLogger(CloudFunctionMetrics.class.getName());

    private final String source;
    private final String projectId;

    public CloudFunctionMetrics(String source, String projectId) {
        this.source = source;
        this.projectId = projectId;
    }

    @Override
    public void recordOperation(Operation op) {
        // Log-based metric format: MIGRATION_METRIC: doc_count source=<source> op=<op>
        logger.info("MIGRATION_METRIC: doc_count source=" + source + " op=" + op.name());
    }

    @Override
    public void recordLag(long lagMillis) {
        // Log-based metric format: MIGRATION_METRIC: lag_ms source=<source> lag=<lag_ms>
        logger.info("MIGRATION_METRIC: lag_ms source=" + source + " lag=" + lagMillis);
    }

    @Override
    public void recordPotentialLag(long lagMillis) {
        // Log-based metric format: MIGRATION_METRIC: potential_lag_ms source=<source> lag=<lag_ms>
        logger.info("MIGRATION_METRIC: potential_lag_ms source=" + source + " lag=" + lagMillis);
    }
}
