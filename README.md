# Firestore Shadow-Journal Migration Tool

## Access Notice
Access is temporary (30 days). Ensure you have cloned the content before 2026-06-20.

This tool provides a robust, online migration path between two Firestore databases (Standard or Enterprise). It ensures data correctness through a distributed **High Water Mark (HWM)** synchronization pattern, allowing for a live cutover without requiring Point-in-Time Recovery (PITR).

## Architecture

For detailed information about the system architecture, race condition prevention, data correctness guarantees, and the load test framework, please see [architecture.md](file:///usr/local/google/home/pcostello/migration/architecture.md).

## Prerequisites

*   `gcloud` CLI installed and authenticated.
*   Java 11+ and Maven (for Dataflow).
*   Node.js 18+ (for Cloud Functions).
*   Permissions:
    *   `datastore.viewer` and `cloudfunctions.admin` on the **Source** project.
    *   `datastore.user` and `dataflow.admin` on the **Destination** project.

## Usage

### 1. Run Migration
This command deploys the live sink, waits 10 minutes for propagation, and starts the Dataflow backfill.

```bash
./migrate.sh run \
  --source-project SOURCE_PROJECT_ID \
  --source-db SOURCE_DATABASE_ID \
  --source-region REGION \
  --dest-project DEST_PROJECT_ID \
  --dest-db DEST_DATABASE_ID \
  --workers 10
```

### 2. Monitor
*   Check the **Dataflow UI** in the Google Cloud Console for backfill progress.
*   View **Cloud Monitoring** in the Destination project. You can use the following filters or PromQL queries in the Metrics Explorer to monitor progress:

    **Live Traffic Document Count (Log-Based):**
    *   **Filter**: `metric.type="logging.googleapis.com/user/migration_doc_count" AND metric.labels.source="live"`
    *   **PromQL**:
        ```promql
        sum by (op) ({"logging.googleapis.com/user/migration_doc_count", monitored_resource="global", source="live"})
        ```

    **Migration Lag (Log-Based):**
    *   **Filter**: `metric.type="logging.googleapis.com/user/migration_lag_ms"`
    *   **PromQL**:
        ```promql
        {"logging.googleapis.com/user/migration_lag_ms", monitored_resource="global"}
        ``` 
*   Generate a **Monitoring Dashboard** on Cloud Monitoring.
    *   ```bash
         gcloud monitoring dashboards create --config-from-file=monitoring-dashboard.json --project=PROJECT_ID
        ```


### 3. Cleanup
Once the Dataflow job is complete and you have verified the migration, run cleanup to remove the migration infrastructure and perform a bulk delete of the migration metadata.

```bash
./migrate.sh cleanup \
  --source-region REGION \
  --dest-project DEST_PROJECT_ID \
  --dest-db DEST_DATABASE_ID
```

## Configuration Details

*   **Standard vs. Enterprise:** For Standard Edition databases, the tool automatically creates index exclusions for the `_ShadowJournal` fields to minimize write costs.
