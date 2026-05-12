#!/bin/bash
set -xeo pipefail

echo "==========================================================="
echo "   Firestore Migration End-to-End Scale & Validation Test  "
echo "==========================================================="

if [ -z "$PROJECT_ID" ]; then
    echo "ERROR: PROJECT_ID environment variable is missing."
    exit 1
fi

# Parse arguments
POPULATE_ONLY=false
EXISTING_SOURCE_DB=""

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --populate_only          Create and populate the source database, then exit."
  echo "  --existing_source_db ID  Use an existing source database ID. Skips population."
  echo "  --no_cleanup             Do not delete the databases at the end of the test."
  exit 1
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --populate_only) POPULATE_ONLY=true ;;
        --existing_source_db) EXISTING_SOURCE_DB="$2"; shift ;;
        --no_cleanup) NO_CLEANUP=true ;;
        -h|--help) usage ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

export SOURCE_PROJECT="$PROJECT_ID"
export DEST_PROJECT="$PROJECT_ID"
TIMESTAMP=$(date +%s)

if [ -n "$EXISTING_SOURCE_DB" ]; then
    export SOURCE_DB="$EXISTING_SOURCE_DB"
else
    export SOURCE_DB="migration-test-src-$TIMESTAMP"
fi

export DEST_DB="migration-test-dst-$TIMESTAMP"
export COLLECTION_NAME="load_test_data"

# Default to False for 20M docs, set to true via env for 10k test
export DRY_RUN="${DRY_RUN:-false}"

# --- Helper Functions ---

log_step_start() {
  local step_name="$1"
  echo "-----------------------------------------------------------" >&2
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] STARTING STEP: $step_name" >&2
  echo "-----------------------------------------------------------" >&2
  date +%s
}

log_step_end() {
  local step_name="$1"
  local start_time="$2"
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  local min=$((duration / 60))
  local sec=$((duration % 60))
  echo "-----------------------------------------------------------"
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] COMPLETED STEP: $step_name"
  echo "Step $step_name completed in $min minutes and $sec seconds."
  echo "-----------------------------------------------------------"
}

# --- Step Functions ---

step_create_databases() {
  local start=$(log_step_start "Create Databases")
  if [ -z "$EXISTING_SOURCE_DB" ]; then
    echo "Creating Source Database: $SOURCE_DB"
    gcloud firestore databases create --project="$PROJECT_ID" --database="$SOURCE_DB" --type=firestore-native --location=us-central1 --quiet
  else
    echo "Using existing source database: $SOURCE_DB"
  fi
  
  if [ "$POPULATE_ONLY" = "false" ]; then
    echo "Creating Destination Database: $DEST_DB"
    gcloud firestore databases create --project="$PROJECT_ID" --database="$DEST_DB" --type=firestore-native --location=us-central1 --quiet
  else
    echo "Populate only mode: Skipping destination database creation."
  fi
  log_step_end "Create Databases" "$start"
}

step_build_artifacts() {
  local start=$(log_step_start "Build Artifacts")
  (
    cd load-tests
    mvn clean package -DskipTests
  )
  log_step_end "Build Artifacts" "$start"
}

step_deploy_resources() {
  local start=$(log_step_start "Deploy Resources")
  (
    cd load-tests
    echo "Deploying Traffic Generator (Endpoint) to Cloud Run..."
    gcloud run deploy firestore-traffic-generator \
        --source . \
        --project "$PROJECT_ID" \
        --region us-central1 \
        --set-env-vars "SOURCE_PROJECT=$SOURCE_PROJECT,SOURCE_DB=$SOURCE_DB,COLLECTION_NAME=$COLLECTION_NAME,GOOGLE_FUNCTION_TARGET=com.google.cloud.firestore.migration.load.TrafficGeneratorApp" \
        --max-instances=500 \
        --quiet
        
    PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
    gcloud run services add-iam-policy-binding firestore-traffic-generator \
        --region=us-central1 \
        --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --role=roles/run.invoker \
        --project="$PROJECT_ID" \
        --quiet
        
    TRAFFIC_URL=$(gcloud run services describe firestore-traffic-generator --project "$PROJECT_ID" --region us-central1 --format="value(status.url)")
    
    echo "Deploying k6 Load Generator Cloud Run Jobs..."
    mkdir -p k6-build
    cp load_test.js k6-build/
    chmod 644 k6-build/load_test.js
    
    # Write entrypoint wrapper to map exit code 108 (k6 abort) to 0 (success) for Cloud Run
    cat <<'EOF' > k6-build/entrypoint.sh
#!/bin/sh
k6 "$@"
EXIT_CODE=$?
if [ $EXIT_CODE -eq 108 ]; then
  echo "k6 aborted successfully (exit 108). Mapping to exit 0 for Cloud Run success."
  exit 0
else
  exit $EXIT_CODE
fi
EOF
    chmod 755 k6-build/entrypoint.sh

    cat <<EOF > k6-build/Dockerfile
FROM grafana/k6:latest
USER root
COPY load_test.js /load_test.js
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER k6
ENTRYPOINT ["/entrypoint.sh"]
EOF

    gcloud builds submit --tag gcr.io/"$PROJECT_ID"/firestore-k6-load:latest k6-build/
    rm -rf k6-build

    # Configure QPS based on test scale (Option B vs Dry Run)
    if [ "$DRY_RUN" = "true" ]; then
        LOADER_QPS=100
        TRAFFIC_QPS=10
    else
        # Flat 10K QPS Data Loader: 250 QPS/task * 40 tasks = 10,000 QPS
        LOADER_QPS=250
        # Static Base Live Traffic: 250 QPS/task * 40 tasks = 10,000 QPS
        TRAFFIC_QPS=250
    fi

    echo "Deploying firestore-k6-loader job (QPS per task: $LOADER_QPS)..."
    gcloud run jobs deploy firestore-k6-loader \
        --image gcr.io/"$PROJECT_ID"/firestore-k6-load:latest \
        --project "$PROJECT_ID" \
        --region us-central1 \
        --tasks 40 \
        --task-timeout 168h \
        --cpu 2 \
        --memory 8Gi \
        --set-env-vars "TARGET_URL=$TRAFFIC_URL,DRY_RUN=$DRY_RUN,QPS_PER_TASK=$LOADER_QPS,MODE=setup" \
        --args="run,/load_test.js,--summary-mode=disabled" \
        --quiet

    echo "Deploying firestore-k6-traffic job (QPS per task: $TRAFFIC_QPS)..."
    gcloud run jobs deploy firestore-k6-traffic \
        --image gcr.io/"$PROJECT_ID"/firestore-k6-load:latest \
        --project "$PROJECT_ID" \
        --region us-central1 \
        --tasks 40 \
        --task-timeout 168h \
        --cpu 2 \
        --memory 8Gi \
        --set-env-vars "TARGET_URL=$TRAFFIC_URL,DRY_RUN=$DRY_RUN,QPS_PER_TASK=$TRAFFIC_QPS,MODE=run" \
        --args="run,/load_test.js,--summary-mode=disabled" \
        --quiet


  )
  log_step_end "Deploy Resources" "$start"
}

step_load_initial_data() {
  local start=$(log_step_start "Load Initial Data")
  echo "Executing Setup Cloud Run Job (k6 Data Loader)..."
  gcloud run jobs execute firestore-k6-loader \
      --project "$PROJECT_ID" \
      --region us-central1 \
      --wait
  log_step_end "Load Initial Data" "$start"
}

step_start_traffic() {
  local start=$(log_step_start "Start Traffic")
  echo "Starting Live Traffic (k6 in run mode asynchronously)..."
  gcloud run jobs execute firestore-k6-traffic \
      --project "$PROJECT_ID" \
      --region us-central1
  log_step_end "Start Traffic" "$start"
}

step_initiate_migration() {
  local start=$(log_step_start "Initiate Migration")
  echo "Initiating Migration Pipeline..."
  NON_INTERACTIVE=true ./migrate.sh run --source-project "$SOURCE_PROJECT" --source-db "$SOURCE_DB" --dest-project "$DEST_PROJECT" --dest-db "$DEST_DB" --workers 50
  log_step_end "Initiate Migration" "$start"
}

step_wait_for_dataflow() {
  local start=$(log_step_start "Wait for Dataflow")
  echo "Waiting for Dataflow Backfill to appear..."
  JOB_ID=""
  for i in {1..60}; do
    JOB_ID=$(gcloud dataflow jobs list --project="$PROJECT_ID" --region=us-central1 --status=active --format="value(id)" --limit=1)
    if [ -n "$JOB_ID" ]; then
      break
    fi
    echo "Waiting for Dataflow job to appear ($i/60)..."
    sleep 10
  done

  if [ -z "$JOB_ID" ]; then
    echo "ERROR: Failed to find active Dataflow job within 10 minutes!"
    exit 1
  fi

  echo "Found Dataflow job: $JOB_ID. Waiting for completion..."
  while true; do
    STATUS=$(gcloud dataflow jobs describe "$JOB_ID" --project="$PROJECT_ID" --region=us-central1 --format="value(currentState)")
    echo "Dataflow job state: $STATUS"
    if [ "$STATUS" = "JOB_STATE_DONE" ]; then
      break
    elif [ "$STATUS" = "JOB_STATE_FAILED" ] || [ "$STATUS" = "JOB_STATE_CANCELLED" ]; then
      echo "Dataflow job failed or was cancelled!"
      exit 1
    fi
    sleep 30
  done
  log_step_end "Wait for Dataflow" "$start"
}

step_validate_metrics() {
  local start=$(log_step_start "Validate Metrics Reporting")
  echo "Validating that metrics are being reported..."
  for i in {1..40}; do
    START_TIME=$(date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%SZ")
    END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    TOKEN=$(gcloud auth application-default print-access-token 2>/dev/null || gcloud auth print-access-token)
    METRICS_OUT=$(curl -s -H "Authorization: Bearer $TOKEN" "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?filter=metric.type%3D%22logging.googleapis.com%2Fuser%2Fmigration_doc_count%22%20AND%20metric.labels.source%3D%22live%22&interval.startTime=${START_TIME}&interval.endTime=${END_TIME}")
    
    echo "Debug: Metrics response: $METRICS_OUT"
    
    if [[ "$METRICS_OUT" == *"\"points\""* ]]; then
      echo "Metrics are reporting successfully."
      break
    fi
    
    echo "Waiting for metrics to appear ($i/40)..."
    sleep 30
  done
  
  if [[ "$METRICS_OUT" != *"\"points\""* ]]; then
    echo "ERROR: Metrics failed to report within 20 minutes!"
    exit 1
  fi
  
  log_step_end "Validate Metrics Reporting" "$start"
}

step_run_lag_monitor() {
  local start=$(log_step_start "Run Lag Monitor")
  (
    cd load-tests
    mvn exec:java -Dexec.mainClass="com.google.cloud.firestore.migration.load.LagMonitor" -Dexec.classpathScope=runtime
  )
  log_step_end "Run Lag Monitor" "$start"
}

step_stop_load_generator() {
  local start=$(log_step_start "Stop Load Generator")
  echo "Stopping all active k6 Live Traffic executions..."
  ACTIVE_EXECS=$(gcloud run jobs executions list --job firestore-k6-traffic --project="$PROJECT_ID" --region us-central1 --format="json" | jq -r '.[] | select(.status.completionTime == null) | .metadata.name')
  for exec_name in $ACTIVE_EXECS; do
    echo "Cancelling active execution: $exec_name"
    gcloud run jobs executions cancel "$exec_name" --project="$PROJECT_ID" --region us-central1 --quiet || true
  done
  log_step_end "Stop Load Generator" "$start"
}

step_wait_for_propagation() {
  local start=$(log_step_start "Wait for Propagation")
  echo "Waiting for writes to stop propagating (monitoring metrics)..."
  for i in {1..10}; do
    START_TIME=$(date -u -d "2 minutes ago" +"%Y-%m-%dT%H:%M:%SZ")
    END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    METRICS_OUT=$(curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?filter=metric.type%3D%22logging.googleapis.com%2Fuser%2Fmigration_doc_count%22%20AND%20metric.labels.source%3D%22live%22&interval.startTime=${START_TIME}&interval.endTime=${END_TIME}")
    
    if [[ "$METRICS_OUT" == *"\"points\""* ]]; then
      echo "Writes are still propagating ($i/10)..."
    else
      echo "No writes detected in the last 2 minutes. Propagation complete."
      break
    fi
    sleep 30
  done
  if [[ $i -eq 10 ]]; then
    echo "Timed out waiting for propagation to stop or metrics are continuous. Proceeding."
  fi
  log_step_end "Wait for Propagation" "$start"
}

step_verification() {
  local start=$(log_step_start "Verification")
  (
    cd load-tests
    mkdir -p ../output
    echo "Running Pass 1: Sharded Hashing on Cloud Dataflow..."
    mvn exec:java -Dexec.mainClass="com.google.cloud.firestore.migration.load.ParallelVerificationPipeline" \
      -Dexec.args="--runner=DataflowRunner \
      --project=$PROJECT_ID \
      --region=us-central1 \
      --tempLocation=gs://run-sources-pcostello-cloud-us-central1/dataflow/temp \
      --gcpTempLocation=gs://run-sources-pcostello-cloud-us-central1/dataflow/gcp-temp \
      --stagingLocation=gs://run-sources-pcostello-cloud-us-central1/dataflow/staging \
      --sourceProject=$SOURCE_PROJECT \
      --sourceDatabase=$SOURCE_DB \
      --destProject=$DEST_PROJECT \
      --destDatabase=$DEST_DB \
      --collectionName=$COLLECTION_NAME \
      --reportPath=gs://run-sources-pcostello-cloud-us-central1/dataflow/reports/detailed-diff-report.txt"
    
    # Safely download the unified GCS report locally to output directory
    gcloud storage cp gs://run-sources-pcostello-cloud-us-central1/dataflow/reports/detailed-diff-report.txt ../output/detailed-diff-report.txt || true
  )
  log_step_end "Verification" "$start"
}

step_cleanup_databases() {
  if [ "$NO_CLEANUP" = "true" ]; then
    echo "Skipping database cleanup as --no_cleanup is specified."
    return 0
  fi
  local start=$(log_step_start "Cleanup Databases")
  if [ -z "$EXISTING_SOURCE_DB" ]; then
    echo "Deleting Source Database: $SOURCE_DB"
    gcloud firestore databases delete --project="$PROJECT_ID" --database="$SOURCE_DB" --quiet || echo "Warning: Failed to delete source DB $SOURCE_DB"
  else
    echo "Skipping deletion of existing source database: $SOURCE_DB"
  fi
  echo "Deleting Destination Database: $DEST_DB"
  gcloud firestore databases delete --project="$PROJECT_ID" --database="$DEST_DB" --quiet || echo "Warning: Failed to delete destination DB $DEST_DB"
  log_step_end "Cleanup Databases" "$start"
}

# --- Main Execution ---

TOTAL_START=$(date +%s)

if [ "$POPULATE_ONLY" = "true" ]; then
  step_create_databases
  step_build_artifacts
  step_deploy_resources
  step_load_initial_data
  TOTAL_END=$(date +%s)
  TOTAL_DUR=$((TOTAL_END - TOTAL_START))
  TOTAL_MIN=$((TOTAL_DUR / 60))
  TOTAL_SEC=$((TOTAL_DUR % 60))
  echo "==========================================================="
  echo "Population Complete in $TOTAL_MIN minutes and $TOTAL_SEC seconds."
  echo "--populate_only specified, exiting without running tests or cleaning up."
  echo "==========================================================="
  exit 0
fi

step_create_databases
step_build_artifacts
step_deploy_resources

if [ -z "$EXISTING_SOURCE_DB" ]; then
  step_start_traffic       # 1. Start background Live Traffic asynchronously (2K -> 10K QPS)
  step_load_initial_data   # 2. Start Data Loader (10K QPS flat) and WAIT (blocks until 100M docs are loaded)
else
  step_start_traffic &
fi

step_initiate_migration  # 3. Deploy GCF Live Sink and start Dataflow backfill AFTER loader finishes
step_wait_for_dataflow
step_validate_metrics
step_run_lag_monitor
step_stop_load_generator # 4. Stop background Live Traffic
step_wait_for_propagation
step_verification

step_cleanup_databases

TOTAL_END=$(date +%s)
TOTAL_DUR=$((TOTAL_END - TOTAL_START))
TOTAL_MIN=$((TOTAL_DUR / 60))
TOTAL_SEC=$((TOTAL_DUR % 60))

echo "==========================================================="
echo "Test Complete in $TOTAL_MIN minutes and $TOTAL_SEC seconds."
echo "==========================================================="
