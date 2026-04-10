# Running Locally

This document describes how to run the `corpus_pipeline.py` pipeline locally for development and testing.

## Prerequisites

1.  **Python**: Make sure you have Python 3.10 or higher installed.
2.  **Virtual Environment**: It is recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/Mac
    pip install -r requirements.txt
    ```

## Running the Pipeline

To run the pipeline locally, use the following command:

```bash
venv/bin/python scripts/corpus_pipeline.py \
  --num_shards 8 \
  --limit 500 \
  --skip 0
```

### Key Arguments:
*   `--num_shards`: Number of parallel shards to use for initial data fetching.
*   `--limit`: Maximum number of records to process per shard (0 for no limit).
*   `--skip`: Number of records to skip per shard (currently not used in the smart cache but available for future use).

### Automatic Parallelism
The script automatically detects the number of CPU cores on your machine and sets `--direct_num_workers` to `num_cores - 2` to maximize performance without locking up your machine.

## Smart Caching
The pipeline implements a local cache in the `cache/` directory to avoid repeated downloads from HuggingFace.
*   On the first run, it will stream data from HuggingFace and save it to `cache/`.
*   On subsequent runs, it will read directly from the local cache. If you increase the `--limit`, it will read what it has from the cache and fetch only the remaining records from HuggingFace.

The `cache/` directory is ignored by Git.

## Output Files
*   `word_frequencies.tsv`: The main output containing word frequencies sorted by Sudachi Mode C descending.
*   `cache/sample_sentences.txt`: A sample of 100 accepted sentences.
*   `cache/rejected_sentences.txt`: A sample of 100 rejected sentences.
*   `output.txt`: Final results summary (total sentences and Kanji/Kana retention ratio).
