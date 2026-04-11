#!/bin/bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BUCKET="gs://file-cache-bucket/corpus_outputs"
DEST="output_diag"

echo "Syncing diagnostic files from $BUCKET to $DEST..."

mkdir -p "$DEST/train"
mkdir -p "$DEST/validation"

# 1. Root reports
echo "Downloading core reports..."
gsutil cp "$BUCKET/split_comparison_report.md" "$DEST/" || true
gsutil cp "$BUCKET/train_only_disjoint_tokens.tsv" "$DEST/" || true
gsutil cp "$BUCKET/val_only_disjoint_tokens.tsv" "$DEST/" || true

# 2. Train diagnostics
echo "Downloading Train diagnostics..."
gsutil cp "$BUCKET/train/metadata.json" "$DEST/train/" || true
gsutil cp "$BUCKET/train/sample_sentences.txt" "$DEST/train/" || true
gsutil cp "$BUCKET/train/rejected_sentences_watch.txt" "$DEST/train/" || true
gsutil cp "$BUCKET/train/token_frequencies.tsv" "$DEST/train/" || true

# 3. Validation diagnostics
echo "Downloading Validation diagnostics..."
gsutil cp "$BUCKET/validation/metadata.json" "$DEST/validation/" || true
gsutil cp "$BUCKET/validation/sample_sentences.txt" "$DEST/validation/" || true
gsutil cp "$BUCKET/validation/rejected_sentences_watch.txt" "$DEST/validation/" || true
gsutil cp "$BUCKET/validation/token_frequencies.tsv" "$DEST/validation/" || true

echo ""
echo "Done! All lightweight diagnostic files synced to ./$DEST/"
