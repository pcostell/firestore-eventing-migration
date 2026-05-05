package com.google.cloud.firestore.migration;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeSeries;
import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import com.google.monitoring.v3.CreateTimeSeriesRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Logger;

public class CloudFunctionMetrics implements MigrationMetrics {
    private static final Logger logger = Logger.getLogger(CloudFunctionMetrics.class.getName());

    private final String source;
    private final String projectId;
    private final MetricServiceClient client;

    public CloudFunctionMetrics(String source, String projectId) {
        this.source = source;
        this.projectId = projectId;
        try {
            this.client = MetricServiceClient.create();
        } catch (IOException e) {
            logger.severe("Failed to create MetricServiceClient: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recordOperation(Operation op) {
        ProjectName name = ProjectName.of(projectId);
        
        TimeSeries timeSeries = TimeSeries.newBuilder()
            .setMetric(Metric.newBuilder()
                .setType("custom.googleapis.com/migration/doc_count")
                .putLabels("source", source)
                .putLabels("op", op.name())
                .build())
            .setResource(MonitoredResource.newBuilder()
                .setType("global")
                .putLabels("project_id", projectId)
                .build())
            .addPoints(Point.newBuilder()
                .setInterval(TimeInterval.newBuilder()
                    .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build())
                .setValue(TypedValue.newBuilder()
                    .setInt64Value(1)
                    .build())
                .build())
            .build();
            
        try {
            client.createTimeSeriesCallable().futureCall(CreateTimeSeriesRequest.newBuilder()
                .setName(name.toString())
                .addAllTimeSeries(Collections.singletonList(timeSeries))
                .build());
        } catch (Exception e) {
            logger.warning("Failed to initiate time series creation for operation: " + e.getMessage());
        }
    }

    @Override
    public void recordLag(long lagMillis) {
        ProjectName name = ProjectName.of(projectId);
        
        TimeSeries timeSeries = TimeSeries.newBuilder()
            .setMetric(Metric.newBuilder()
                .setType("custom.googleapis.com/migration/lag_ms")
                .putLabels("source", source)
                .build())
            .setResource(MonitoredResource.newBuilder()
                .setType("global")
                .putLabels("project_id", projectId)
                .build())
            .addPoints(Point.newBuilder()
                .setInterval(TimeInterval.newBuilder()
                    .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build())
                .setValue(TypedValue.newBuilder()
                    .setDoubleValue((double) lagMillis)
                    .build())
                .build())
            .build();
            
        try {
            client.createTimeSeriesCallable().futureCall(CreateTimeSeriesRequest.newBuilder()
                .setName(name.toString())
                .addAllTimeSeries(Collections.singletonList(timeSeries))
                .build());
        } catch (Exception e) {
            logger.warning("Failed to initiate time series creation for lag: " + e.getMessage());
        }
    }
}
