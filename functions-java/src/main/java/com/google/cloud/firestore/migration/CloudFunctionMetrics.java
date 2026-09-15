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
