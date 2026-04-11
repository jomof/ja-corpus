# Running Locally

This document describes how to run the `corpus_pipeline.py` pipeline locally for development and testing.

## Prerequisites

1.  **Python 3.10**: SudachiPy requires Python ≤ 3.13 (PyO3 compatibility).
    ```bash
    brew install python@3.10  # macOS
    ```
2.  **GCP Authentication**: `gcloud auth application-default login` (needed to read from GCS).
3.  **Virtual Environment**:
    ```bash
    brew install python@3.10  # macOS
    # or
    sudo apt install python3.10-venv  # Linux

    python3.10 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

## Running the Pipeline

```bash
# Quick test: 100 docs from each split
venv/bin/python scripts/corpus_pipeline.py --max_docs 100

# Validation only
venv/bin/python scripts/corpus_pipeline.py --splits validation --max_docs 500

# Full validation run
venv/bin/python scripts/corpus_pipeline.py --splits validation

# Full run (both splits, all data — takes a while)
venv/bin/python scripts/corpus_pipeline.py
```

### Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--splits` | `train,validation` | Comma-separated splits to process |
| `--max_docs` | `0` (all) | Limit docs per split for testing |
| `--output_dir` | `output/` | Base output directory |

### Automatic Parallelism
The script automatically detects the number of CPU cores and sets
`--direct_num_workers` to `num_cores - 2` to maximize performance.

## Data Source

The pipeline reads pre-cached parquet files from GCS:
```
gs://file-cache-bucket/mc4/ja/train/*.parquet      (10 files, ~40 GB)
gs://file-cache-bucket/mc4/ja/validation/*.parquet  (2 files, ~500 MB)
```

These were populated from HuggingFace (`allenai/c4`, Japanese subset) via
Google Storage Transfer Service. See `GCP.md` for details.

## Output Files

```
output/
├── train/
│   ├── sentences-00000-of-NNNNN.txt.gz  # Full deduplicated corpus (sharded, gzipped)
│   ├── token_frequencies.tsv             # Word frequencies (Sudachi A, B, C)
│   ├── sample_sentences.txt             # 100 accepted sentence samples
│   ├── rejected_sentences.txt           # 100 rejected sentence samples
│   └── metadata.json                    # Corpus stats (sentence count, retention ratio)
└── validation/
    ├── sentences-00000-of-NNNNN.txt.gz
    ├── token_frequencies.tsv
    ├── sample_sentences.txt
    ├── rejected_sentences.txt
    └── metadata.json
```
