#!/bin/bash
set -xeo pipefail

echo "==========================================================="
echo "   Firestore Migration End-to-End Scale & Validation Test  "
echo "==========================================================="

if [ -z "$PROJECT_ID" ]; then
    echo "ERROR: PROJECT_ID environment variable is missing."
    exit 1
fi

export SOURCE_PROJECT="$PROJECT_ID"
export DEST_PROJECT="$PROJECT_ID"
TIMESTAMP=$(date +%s)
export SOURCE_DB="migration-test-src-$TIMESTAMP"
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
  echo "Creating Databases: $SOURCE_DB and $DEST_DB"
  gcloud firestore databases create --project="$PROJECT_ID" --database="$SOURCE_DB" --type=firestore-native --location=us-central1 --quiet
  gcloud firestore databases create --project="$PROJECT_ID" --database="$DEST_DB" --type=firestore-native --location=us-central1 --quiet
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
    
    echo "Deploying k6 Load Generator Cloud Run Job..."
    mkdir -p k6-build
    cp load_test.js k6-build/
    chmod 644 k6-build/load_test.js
    cat <<EOF > k6-build/Dockerfile
FROM grafana/k6:latest
COPY load_test.js /load_test.js
ENTRYPOINT ["k6"]
EOF

    gcloud builds submit --tag gcr.io/"$PROJECT_ID"/firestore-k6-load:latest k6-build/
    rm -rf k6-build

    gcloud run jobs deploy firestore-k6-load \
        --image gcr.io/"$PROJECT_ID"/firestore-k6-load:latest \
        --project "$PROJECT_ID" \
        --region us-central1 \
        --tasks 40 \
        --task-timeout 168h \
        --cpu 2 \
        --memory 8Gi \
        --set-env-vars "TARGET_URL=$TRAFFIC_URL,DRY_RUN=$DRY_RUN" \
        --quiet
  )
  log_step_end "Deploy Resources" "$start"
}

step_load_initial_data() {
  local start=$(log_step_start "Load Initial Data")
  echo "Executing Setup Cloud Run Job (k6 in setup mode)..."
  gcloud run jobs execute firestore-k6-load \
      --project "$PROJECT_ID" \
      --region us-central1 \
      --args="run,/load_test.js,-e,MODE=setup,--summary-mode=disabled" \
      --wait
  log_step_end "Load Initial Data" "$start"
}

step_start_traffic() {
  local start=$(log_step_start "Start Traffic")
  echo "Starting Live Traffic (k6 in run mode)..."
  gcloud run jobs execute firestore-k6-load \
      --project "$PROJECT_ID" \
      --region us-central1 \
      --args="run,/load_test.js,-e,MODE=run,--summary-mode=disabled"
  log_step_end "Start Traffic" "$start"
}

step_initiate_migration() {
  local start=$(log_step_start "Initiate Migration")
  echo "Initiating Migration Pipeline..."
  NON_INTERACTIVE=true ./migrate.sh run --source-project "$SOURCE_PROJECT" --source-db "$SOURCE_DB" --dest-project "$DEST_PROJECT" --dest-db "$DEST_DB" --workers 10
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
    METRICS_OUT=$(curl -s -H "Authorization: Bearer $TOKEN" "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?filter=metric.type%3D%22custom.googleapis.com%2Fmigration%2Fdoc_count%22%20AND%20metric.labels.source%3D%22live%22&interval.startTime=${START_TIME}&interval.endTime=${END_TIME}")
    
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
  echo "Stopping all active k6 Load Generator executions..."
  ACTIVE_EXECS=$(gcloud run jobs executions list --job firestore-k6-load --project="$PROJECT_ID" --region us-central1 --filter="NOT (status.conditions.type=Completed AND (status.conditions.status=True OR status.conditions.status=False))" --format="value(name)")
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
    
    METRICS_OUT=$(curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?filter=metric.type%3D%22custom.googleapis.com%2Fmigration%2Fdoc_count%22%20AND%20metric.labels.source%3D%22live%22&interval.startTime=${START_TIME}&interval.endTime=${END_TIME}")
    
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
      --reportPath=gs://run-sources-pcostello-cloud-us-central1/dataflow/reports/detailed-diff-report-scale.txt"
  )
  log_step_end "Verification" "$start"
}

# --- Main Execution ---

TOTAL_START=$(date +%s)

step_create_databases
step_build_artifacts
step_deploy_resources
step_load_initial_data
step_start_traffic
step_initiate_migration
step_wait_for_dataflow
step_validate_metrics
step_run_lag_monitor
step_stop_load_generator
step_wait_for_propagation
step_verification

TOTAL_END=$(date +%s)
TOTAL_DUR=$((TOTAL_END - TOTAL_START))
TOTAL_MIN=$((TOTAL_DUR / 60))
TOTAL_SEC=$((TOTAL_DUR % 60))

echo "==========================================================="
echo "Test Complete in $TOTAL_MIN minutes and $TOTAL_SEC seconds."
echo "==========================================================="
