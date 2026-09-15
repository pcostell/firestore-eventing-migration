#!/bin/bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# migrate.sh - Orchestrator for Firestore Shadow-Journal Migration

set -e

# --- Helper Functions ---

log() {
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] $1"
}

error() {
    echo "[ERROR] $1" >&2
    exit 1
}

confirm() {
    if [[ "$NON_INTERACTIVE" == "true" ]]; then
        return 0
    fi
    read -p "$1 (y/n): " choice || true
    case "$choice" in
        y|Y ) return 0;;
        * ) return 1;;
    esac
}

usage() {
    echo "Usage: $0 [run|cleanup] [options]"
    echo ""
    echo "Commands:"
    echo "  run        Deploy Live Sink and start Dataflow Backfill"
    echo "  cleanup    Remove migration infra and perform bulk delete of metadata"
    echo ""
    echo "Options for 'run':"
    echo "  --source-project ID    Source GCP Project ID"
    echo "  --source-db ID         Source Firestore Database ID"
    echo "  --dest-project ID      Destination GCP Project ID"
    echo "  --dest-db ID           Destination Firestore Database ID"
    echo "  --workers N            Number of Dataflow workers (default: 5)"
    echo ""
    echo "Options for 'cleanup':"
    echo "  --source-project ID    Source GCP Project ID (optional, defaults to dest)"
    echo "  --dest-project ID      Destination GCP Project ID"
    echo "  --dest-db ID           Destination Firestore Database ID"
    exit 1
}

# --- Argument Parsing ---

COMMAND=$1
shift

SOURCE_PROJECT=""
SOURCE_DB=""
DEST_PROJECT=""
DEST_DB=""
WORKERS=5

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --source-project) SOURCE_PROJECT="$2"; shift ;;
        --source-db) SOURCE_DB="$2"; shift ;;
        --dest-project) DEST_PROJECT="$2"; shift ;;
        --dest-db) DEST_DB="$2"; shift ;;
        --workers) WORKERS="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# --- Core Logic ---

run_migration() {
    [[ -z "$SOURCE_PROJECT" || -z "$SOURCE_DB" || -z "$DEST_PROJECT" || -z "$DEST_DB" ]] && usage

    log "Starting configuration and migration run..."

    # 1. IAM Permissions Confirmation
    DEST_PROJECT_NUMBER=$(gcloud projects describe "$DEST_PROJECT" --format="value(projectNumber)")
    # Using the default compute engine service account for Dataflow and Cloud Functions
    COMPUTE_SA="${DEST_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

    log "This tool requires the following IAM permissions:"
    log " - Destination Compute Service Account ($COMPUTE_SA) must have 'roles/datastore.viewer' on Source Project ($SOURCE_PROJECT)."
    
    if confirm "Would you like to grant this permission now?"; then
        gcloud projects add-iam-policy-binding "$SOURCE_PROJECT" \
            --member="serviceAccount:${COMPUTE_SA}" \
            --role="roles/datastore.viewer" \
            --condition=None --quiet
    else
        log "Continuing, assuming permissions are already set."
    fi

    # Grant permission for Source SA on Dest Project
    SOURCE_PROJECT_NUMBER=$(gcloud projects describe "$SOURCE_PROJECT" --format="value(projectNumber)")
    SOURCE_COMPUTE_SA="${SOURCE_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

    log "This tool also requires the following IAM permissions for the Live Sink:"
    log " - Source Compute Service Account ($SOURCE_COMPUTE_SA) must have 'roles/datastore.user' on Destination Project ($DEST_PROJECT)."
    
    if confirm "Would you like to grant this permission now?"; then
        gcloud projects add-iam-policy-binding "$DEST_PROJECT" \
            --member="serviceAccount:${SOURCE_COMPUTE_SA}" \
            --role="roles/datastore.user" \
            --condition=None --quiet
    else
        log "Continuing, assuming permissions are already set."
    fi


    # 2. Check Database edition for Indexing
    log "Checking destination database edition..."
    DB_DESC=$(gcloud firestore databases describe --project="$DEST_PROJECT" --database="$DEST_DB" --format=json)
    DB_EDITION=$(echo "$DB_DESC" | grep -oP '"databaseEdition":\s*"\K[^"]+' || echo "STANDARD")

    if [[ "$DB_EDITION" == "STANDARD" ]]; then
        log "Destination is Standard/Native. Configuring index exclusions for _ShadowJournal..."
        if confirm "Proceed with creating index exclusions for 'hwm'?"; then
            # Exclusion for hwm
            gcloud firestore indexes fields update hwm \
                --collection-group="_ShadowJournal" \
                --project="$DEST_PROJECT" \
                --database="$DEST_DB" \
                --disable-indexes --quiet || log "Warning: Could not set hwm exclusion. It might already exist."
        fi
    fi

    # 2.5. Deploy Log-Based Metrics for monitoring
    log "Deploying Log-Based Metrics to Source Project ($SOURCE_PROJECT)..."
    gcloud logging metrics create migration_doc_count \
        --config-from-file=functions-java/monitoring/metric-doc-count.json \
        --project="$SOURCE_PROJECT" \
        --quiet || log "Warning: Could not create metric 'migration_doc_count'. It may already exist."

    gcloud logging metrics create migration_lag_ms \
        --config-from-file=functions-java/monitoring/metric-lag-ms.json \
        --project="$SOURCE_PROJECT" \
        --quiet || log "Warning: Could not create metric 'migration_lag_ms'. It may already exist."

    gcloud logging metrics create migration_potential_lag_ms \
        --config-from-file=functions-java/monitoring/metric-potential-lag-ms.json \
        --project="$SOURCE_PROJECT" \
        --quiet || log "Warning: Could not create metric 'migration_potential_lag_ms'. It may already exist."

    # Build fat jar locally
    log "Building Fat JAR..."
    mvn clean package -Pshade -pl functions-java -am -DskipTests -q || error "Failed to build fat jar."

    # Create staging directory in workspace for deployment
    STAGING_DIR="staging-deploy"
    mkdir -p "$STAGING_DIR"
    cp functions-java/target/firestore-migration-functions-1.0-SNAPSHOT-shaded.jar "$STAGING_DIR/function.jar"

    # 3. Deploy Live Stream Sink (Cloud Function) from Fat JAR
    log "Deploying Live Journal Sink (Cloud Function) from Fat JAR..."
    
    CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "pcostello-cloud")
    log "Setting gcloud project to $SOURCE_PROJECT for deployment (was $CURRENT_PROJECT)"
    gcloud config set project "$SOURCE_PROJECT" --quiet
    
    gcloud functions deploy firestore-migration-sink \
        --project="$SOURCE_PROJECT" \
        --runtime=java17 \
        --source "$STAGING_DIR" \
        --trigger-event-filters="type=google.cloud.firestore.document.v1.written,database=$SOURCE_DB" \
        --entry-point=com.google.cloud.firestore.migration.LiveSinkFunction \
        --set-env-vars="DEST_PROJECT=$DEST_PROJECT,DEST_DB=$DEST_DB" \
        --region=us-central1 \
        --gen2 --quiet \
        --retry \
        --concurrency=50 \
        --max-instances=250 \
        --cpu=4 \
        --memory=4Gi

    rm -rf "$STAGING_DIR"
    
    log "Restoring gcloud project to $CURRENT_PROJECT"
    gcloud config set project "$CURRENT_PROJECT" --quiet

    log "Live Sink deployed. Waiting 10 minutes for Eventarc propagation and write-catching..."
    sleep 600

    # 4. Start Dataflow Backfill
    log "Running Dataflow Backfill using exec:java..."
    (
        cd dataflow
        mvn compile exec:java -Dexec.mainClass="com.google.cloud.firestore.migration.FirestoreMigrationPipeline" \
            -Dexec.args="--project=\"$DEST_PROJECT\" \
            --sourceProject=\"$SOURCE_PROJECT\" \
            --sourceDatabase=\"$SOURCE_DB\" \
            --destinationProject=\"$DEST_PROJECT\" \
            --destinationDatabase=\"$DEST_DB\" \
            --runner=DataflowRunner \
            --experiments=use_runner_v2 \
            --numWorkers=\"$WORKERS\" \
            --maxNumWorkers=2000 \
            --region=us-central1"
    )

    log "Migration run initiated successfully. Monitor the Dataflow job in the Cloud Console."
}

cleanup_migration() {
    SOURCE_PROJECT="${SOURCE_PROJECT:-$DEST_PROJECT}"
    [[ -z "$SOURCE_PROJECT" || -z "$DEST_PROJECT" || -z "$DEST_DB" ]] && usage

    log "Starting cleanup for $DEST_PROJECT / $DEST_DB..."

    log "Checking for Eventarc triggers..."
    TRIGGERS=$(gcloud eventarc triggers list --project="$SOURCE_PROJECT" --region=us-central1 --filter="destination.cloudRun.service=firestore-migration-sink" --format="value(name)" 2>/dev/null || true)
    if [[ -n "$TRIGGERS" ]]; then
        if confirm "Delete Eventarc triggers associated with firestore-migration-sink?"; then
            for TRIGGER in $TRIGGERS; do
                log "Deleting trigger $TRIGGER..."
                gcloud eventarc triggers delete "$TRIGGER" --project="$SOURCE_PROJECT" --region=us-central1 --quiet || log "Warning: Could not delete trigger $TRIGGER"
            done
        fi
    fi

    if confirm "Delete the Live Journal Sink (Cloud Function)?"; then
        gcloud functions delete firestore-migration-sink --project="$SOURCE_PROJECT" --region=us-central1 --gen2 --quiet || log "Function not found, skipping."
    fi

    log "Checking for Log-Based Metrics..."
    if gcloud logging metrics describe migration_doc_count --project="$SOURCE_PROJECT" &>/dev/null; then
        if confirm "Delete Log-Based Metric 'migration_doc_count'?"; then
            gcloud logging metrics delete migration_doc_count --project="$SOURCE_PROJECT" --quiet
        fi
    fi
    if gcloud logging metrics describe migration_lag_ms --project="$SOURCE_PROJECT" &>/dev/null; then
        if confirm "Delete Log-Based Metric 'migration_lag_ms'?"; then
            gcloud logging metrics delete migration_lag_ms --project="$SOURCE_PROJECT" --quiet
        fi
    fi
    if gcloud logging metrics describe migration_potential_lag_ms --project="$SOURCE_PROJECT" &>/dev/null; then
        if confirm "Delete Log-Based Metric 'migration_potential_lag_ms'?"; then
            gcloud logging metrics delete migration_potential_lag_ms --project="$SOURCE_PROJECT" --quiet
        fi
    fi

    log "Performing bulk delete of _ShadowJournal collection group..."
    if confirm "Delete all documents in the '_ShadowJournal' collection group?"; then
        gcloud alpha firestore bulk-delete \
            --collection-ids="_ShadowJournal" \
            --project="$DEST_PROJECT" \
            --database="$DEST_DB" \
            --quiet
    fi

    log "Cleanup complete."
}

# --- Execution ---

case "$COMMAND" in
    run) run_migration ;;
    cleanup) cleanup_migration ;;
    *) usage ;;
esac
