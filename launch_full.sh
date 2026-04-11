#!/bin/bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found."
    echo "Please run ./run_local.sh first to install pipeline dependencies."
    exit 1
fi

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo "ERROR: Google Cloud project ID not found."
    echo "Run 'gcloud config set project YOUR_PROJECT_ID' to set your default project."
    exit 1
fi

REGION="us-central1"
BUCKET="gs://file-cache-bucket"

echo "======================================================"
echo "Launching global Kotogram Corpus Pipeline on Dataflow "
echo "Project: $PROJECT_ID"
echo "Region:  $REGION"
echo "Bucket:  $BUCKET"
echo "======================================================"

venv/bin/python scripts/corpus_pipeline.py \
  --runner DataflowRunner \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --temp_location "$BUCKET/dataflow/temp" \
  --staging_location "$BUCKET/dataflow/staging" \
  --output_dir "$BUCKET/corpus_outputs" \
  --requirements_file "${SCRIPT_DIR}/requirements.txt" \
  --setup_file "${SCRIPT_DIR}/setup.py" \
  --experiments=use_runner_v2

echo "Command submitted! Job URL should be printed above."
