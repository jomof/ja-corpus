#!/bin/bash
set -e

REQUIRED_MAJOR=3
REQUIRED_MINOR=10
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Find python3.10
PYTHON=""
for candidate in python3.10 /opt/homebrew/bin/python3.10 /usr/local/bin/python3.10; do
  if command -v "$candidate" &>/dev/null; then
    PYTHON="$candidate"
    break
  fi
done

if [ -z "$PYTHON" ]; then
  echo "ERROR: Python ${REQUIRED_MAJOR}.${REQUIRED_MINOR} not found."
  echo "Install with: brew install python@${REQUIRED_MAJOR}.${REQUIRED_MINOR}"
  exit 1
fi

# Check existing venv version
if [ -d venv ]; then
  VENV_VERSION=$(venv/bin/python3 --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | head -1)
  if [ "$VENV_VERSION" != "${REQUIRED_MAJOR}.${REQUIRED_MINOR}" ]; then
    echo "venv is Python ${VENV_VERSION}, need ${REQUIRED_MAJOR}.${REQUIRED_MINOR}. Recreating..."
    rm -rf venv
  fi
fi

# Create venv if needed
if [ ! -d venv ]; then
  echo "Creating venv with $PYTHON..."
  "$PYTHON" -m venv venv
  venv/bin/pip install --upgrade pip -q
  venv/bin/pip install -r requirements.txt
fi

echo "Running pipeline..."
# Default to 2020 total docs locally, randomly splitting 10% strictly into validation natively.
if [ $# -eq 0 ]; then
  set -- --max_docs 2020 --val_ratio 0.1
fi
venv/bin/python scripts/corpus_pipeline.py "$@"
