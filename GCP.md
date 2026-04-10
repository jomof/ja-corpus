# Running on Google Cloud Platform (GCP)

This document describes how to run the `corpus_pipeline.py` on Google Cloud Dataflow for scalable processing.

## Prerequisites

1.  **GCP Project**: You need a GCP project with the Dataflow API enabled.
2.  **GCS Bucket**: You need a Google Cloud Storage bucket for staging files and temporary data.
3.  **Authentication**: Make sure you are authenticated with GCP (e.g., via `gcloud auth application-default login`).

## Running on Dataflow

To run the pipeline on Dataflow, you need to switch the runner to `DataflowRunner` and provide the required GCP parameters.

Because we are using third-party libraries (like Sudachi and HuggingFace Datasets) and a custom local module (`ja_sentence_extractor.py`), we must tell Dataflow to install these dependencies on the workers.

Here is the command to run the pipeline on Dataflow:

```bash
venv/bin/python scripts/corpus_pipeline.py \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region YOUR_REGION \
  --temp_location gs://YOUR_BUCKET/temp \
  --staging_location gs://YOUR_BUCKET/staging \
  --output gs://YOUR_BUCKET/output/word_frequencies.tsv \
  --requirements_file requirements.txt \
  --py_file scripts/ja_sentence_extractor.py \
  --num_shards 8 \
  --limit 500
```

### Key Arguments:
*   `--runner DataflowRunner`: Tells Beam to use the managed Google Cloud Dataflow service.
*   `--project`: Your GCP Project ID.
*   `--region`: The GCP region to run the job in (e.g., `us-central1`).
*   `--temp_location`: A GCS path for temporary files created during execution.
*   `--staging_location`: A GCS path for staging code packages.
*   `--output`: The GCS path where the final `word_frequencies.tsv` will be written.
*   `--requirements_file`: Points to `requirements.txt` so Dataflow workers install `sudachipy`, `datasets`, etc.
*   `--py_file`: Includes our custom `ja_sentence_extractor.py` module so workers can import it.

## Monitoring the Job

Once you launch the command, it will output a URL to the Google Cloud Console. You can follow that link to see:
*   A visual graph of the pipeline execution.
*   Time spent in each phase (requested in previous discussion).
*   Worker logs and autoscaling details.

## Note on Cache

When running on Dataflow, the local `cache/` directory created during local runs will **not** be used. Workers will stream data directly from HuggingFace or read from the cached dataset on the workers if configured, but they won't share the local cache you built on your machine.
