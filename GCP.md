# Running on Google Cloud Platform (GCP)

## Data Setup (one-time)

The pipeline reads mc4 Japanese parquet files from GCS. These were
populated from HuggingFace using Google Storage Transfer Service.

### GCS Layout

```
gs://file-cache-bucket/mc4/ja/
├── train/
│   ├── 0.parquet  ...  9.parquet    (10 files, ~40 GB total)
└── validation/
    ├── 0.parquet                     (2 files, ~500 MB total)
    └── 1.parquet
```

### Populating GCS from HuggingFace

If the GCS cache is empty, re-populate using Storage Transfer Service:

1. Create a manifest TSV with all HuggingFace parquet URLs (see below).
2. Upload the manifest:
   ```bash
   gsutil cp transfer_manifest.tsv gs://file-cache-bucket/transfer_manifest.tsv
   gsutil acl ch -u AllUsers:R gs://file-cache-bucket/transfer_manifest.tsv
   ```
3. Create the transfer job:
   ```bash
   gcloud transfer jobs create \
     "https://storage.googleapis.com/file-cache-bucket/transfer_manifest.tsv" \
     "gs://file-cache-bucket/mc4/ja/" \
     --name=mc4-ja-import
   ```
4. After it completes, flatten the URL-derived paths:
   ```bash
   for split in train validation; do
     gsutil -m mv \
       "gs://file-cache-bucket/mc4/ja/huggingface.co/api/datasets/allenai/c4/parquet/ja/${split}/*" \
       "gs://file-cache-bucket/mc4/ja/${split}/"
   done
   gsutil -m rm -r "gs://file-cache-bucket/mc4/ja/huggingface.co/"
   ```

#### Manifest TSV

```tsv
TsvHttpData-1.0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/0.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/1.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/2.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/3.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/4.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/5.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/6.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/7.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/8.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/train/9.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/validation/0.parquet	0
https://huggingface.co/api/datasets/allenai/c4/parquet/ja/validation/1.parquet	0
```

### Prerequisite: STS Service Account Permissions

The Storage Transfer Service account needs access to the bucket:
```bash
gsutil iam ch \
  serviceAccount:project-YOUR_PROJECT_NUM@storage-transfer-service.iam.gserviceaccount.com:roles/storage.admin \
  gs://file-cache-bucket
```

### Populating Wikipedia from HuggingFace (Optional)

We also support dynamically merging the high-density Japanese Wikipedia corpus into your dataset using the exact same STS mechanism! 

1. Use the pre-generated `wiki_manifest.tsv`.
2. Upload the manifest:
   ```bash
   gsutil cp wiki_manifest.tsv gs://file-cache-bucket/wiki_manifest.tsv
   gsutil acl ch -u AllUsers:R gs://file-cache-bucket/wiki_manifest.tsv
   ```
3. Create the transfer job:
   ```bash
   gcloud transfer jobs create \
     "https://storage.googleapis.com/file-cache-bucket/wiki_manifest.tsv" \
     "gs://file-cache-bucket/wikipedia/ja/" \
     --name=wiki-ja-import
   ```
4. After it completes, clean up the nested directories natively:
   ```bash
   gsutil -m mv \
     "gs://file-cache-bucket/wikipedia/ja/huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ja/*" \
     "gs://file-cache-bucket/wikipedia/ja/"
   gsutil -m rm -r "gs://file-cache-bucket/wikipedia/ja/huggingface.co/"
   ```

## Prerequisites

- **Python 3.10**: SudachiPy requires Python ≤ 3.13 (PyO3 compatibility).
  Dataflow workers default to the Python version of the submitting machine,
  so ensure you submit from a Python 3.10 venv.

## Running on Dataflow

```bash
venv/bin/python scripts/corpus_pipeline.py \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region YOUR_REGION \
  --temp_location gs://YOUR_BUCKET/temp \
  --staging_location gs://YOUR_BUCKET/staging \
  --output_dir gs://YOUR_BUCKET/output \
  --requirements_file requirements.txt \
  --setup_file setup.py
```

### Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--splits` | `train,validation` | Comma-separated splits to process |
| `--max_docs` | `0` (all) | Limit docs per split for testing |
| `--output_dir` | `output/` | Base output directory (local or GCS) |

## Monitoring

Once launched, the command outputs a URL to the Google Cloud Console
where you can see the pipeline DAG, per-step timings, worker logs, and
autoscaling details.
