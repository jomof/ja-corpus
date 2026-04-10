"""Corpus pipeline: extract and analyze Japanese sentences from mc4.

Reads pre-cached parquet files from GCS (populated via Storage Transfer
Service from HuggingFace).  Processes both train and validation splits
with separate output paths.

Usage:
    # Quick local test: 100 docs per split
    python scripts/corpus_pipeline.py --max_docs 100

    # Validation only, all docs
    python scripts/corpus_pipeline.py --splits validation

    # Full run on Dataflow
    python scripts/corpus_pipeline.py --runner DataflowRunner \\
        --project YOUR_PROJECT --region us-central1 \\
        --temp_location gs://YOUR_BUCKET/temp \\
        --staging_location gs://YOUR_BUCKET/staging
"""

import argparse
import hashlib
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from ja_sentence_extractor import SentenceExtractor

GCS_BASE = "gs://file-cache-bucket/mc4/ja"
SPLITS = ("train", "validation")


class ProcessDocumentDoFn(beam.DoFn):
    def setup(self):
        self.extractor = SentenceExtractor()

    def process(self, text):
        accepted, rejected, orig_count, recv_count = (
            self.extractor.process_document(text)
        )
        for s in accepted:
            yield s
        for s in rejected:
            yield beam.pvalue.TaggedOutput("rejected", s)
        yield beam.pvalue.TaggedOutput("counts", (orig_count, recv_count))


def hash_sentence(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


class TakeOneFn(beam.CombineFn):
    def create_accumulator(self):
        return None

    def add_input(self, accumulator, element):
        if accumulator is None:
            return element
        return accumulator

    def merge_accumulators(self, accumulators):
        for acc in accumulators:
            if acc is not None:
                return acc
        return None

    def extract_output(self, accumulator):
        return accumulator


class TokenizeAllModesFn(beam.DoFn):
    def setup(self):
        from sudachipy import dictionary
        from sudachipy import tokenizer

        self.dict = dictionary.Dictionary()
        self.tokenizer = self.dict.create()
        self.mode_a = tokenizer.Tokenizer.SplitMode.A
        self.mode_b = tokenizer.Tokenizer.SplitMode.B
        self.mode_c = tokenizer.Tokenizer.SplitMode.C

    def process(self, sentence):
        def clean(text):
            return (
                text.replace("\u2028", " ")
                .replace("\u2029", " ")
                .replace("\n", " ")
                .replace("\r", " ")
            )

        try:
            for t in self.tokenizer.tokenize(sentence, self.mode_a):
                yield beam.pvalue.TaggedOutput(
                    "A", (clean(t.surface()), clean(t.normalized_form()))
                )
            for t in self.tokenizer.tokenize(sentence, self.mode_b):
                yield beam.pvalue.TaggedOutput(
                    "B", (clean(t.surface()), clean(t.normalized_form()))
                )
            for t in self.tokenizer.tokenize(sentence, self.mode_c):
                yield beam.pvalue.TaggedOutput(
                    "C", (clean(t.surface()), clean(t.normalized_form()))
                )
        except Exception as e:
            logging.error(f"Error in TokenizeAllModesFn: {e}")


def _build_split_pipeline(
    p, split, output_dir, max_docs
):
    """Build the processing sub-pipeline for a single split."""
    tag = split.capitalize()

    # Read text column from parquet files on GCS.
    # When max_docs is set, only read the first file for speed.
    if max_docs > 0:
        file_pattern = f"{GCS_BASE}/{split}/0.parquet"
    else:
        file_pattern = f"{GCS_BASE}/{split}/*.parquet"
    texts = (
        p
        | f"Read{tag}" >> beam.io.ReadFromParquet(
            file_pattern=file_pattern,
            columns=["text"],
        )
        | f"ExtractText{tag}" >> beam.Map(lambda row: row["text"])
    )

    if max_docs > 0:
        # Deterministic limit: pick the same N documents every run
        # by sorting on text hash (not random sampling).
        texts = (
            texts
            | f"Limit{tag}"
            >> beam.combiners.Top.Of(max_docs, key=lambda t: hashlib.md5(t.encode()).hexdigest())
            | f"Flatten{tag}" >> beam.FlatMap(lambda x: x)
        )

    # Process documents
    results = (
        texts
        | f"ProcessDocs{tag}"
        >> beam.ParDo(ProcessDocumentDoFn()).with_outputs(
            "counts", "rejected", main="sentences"
        )
    )
    sentences = results.sentences
    counts = results.counts
    rejected = results.rejected

    # Deduplicate
    deduplicated = (
        sentences
        | f"PairHash{tag}" >> beam.Map(lambda s: (hash_sentence(s), s))
        | f"DedupHash{tag}" >> beam.CombinePerKey(TakeOneFn())
        | f"ExtractSent{tag}" >> beam.Map(lambda x: x[1])
    )

    split_dir = os.path.join(output_dir, split)

    # Write full deduplicated corpus (gzipped, shuffled)
    (
        deduplicated
        | f"Shuffle{tag}" >> beam.Reshuffle()
        | f"WriteCorpus{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "sentences"),
            file_name_suffix=".txt.gz",
            compression_type=beam.io.filesystem.CompressionTypes.GZIP,
        )
    )

    # Tokenize and compute token frequencies.
    # Each token yields a (surface, normalized_form) pair as the key.
    mode_tokens = (
        deduplicated
        | f"Tokenize{tag}"
        >> beam.ParDo(TokenizeAllModesFn()).with_outputs("A", "B", "C")
    )

    count_a = mode_tokens.A | f"CountA{tag}" >> beam.combiners.Count.PerElement()
    count_b = mode_tokens.B | f"CountB{tag}" >> beam.combiners.Count.PerElement()
    count_c = mode_tokens.C | f"CountC{tag}" >> beam.combiners.Count.PerElement()

    joined = (
        {"A": count_a, "B": count_b, "C": count_c}
        | f"CoGroup{tag}" >> beam.CoGroupByKey()
    )

    def get_sort_key(element):
        _, data = element
        c_a = data["A"][0] if data["A"] else 0
        c_b = data["B"][0] if data["B"] else 0
        c_c = data["C"][0] if data["C"] else 0
        return (c_c, c_b, c_a)

    def format_tsv_row(element):
        key, data = element
        surface, normalized = key
        c_a = data["A"][0] if data["A"] else 0
        c_b = data["B"][0] if data["B"] else 0
        c_c = data["C"][0] if data["C"] else 0
        return f"{surface}\t{normalized}\t{c_a}\t{c_b}\t{c_c}"


    # Write token frequencies
    (
        joined
        | f"TopN{tag}"
        >> beam.transforms.combiners.Top.Of(1000000, key=get_sort_key)
        | f"FlatTop{tag}" >> beam.FlatMap(lambda x: x)
        | f"FormatTSV{tag}" >> beam.Map(format_tsv_row)
        | f"WriteFreq{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "token_frequencies.tsv"),
            shard_name_template="",
            header=(
                f"# Token frequencies (Sudachi A, B, C)\n"
                f"# Source: mc4 Japanese ({split})\n"
                f"# Surface\tNormalized\tSudachi_A\tSudachi_B\tSudachi_C"
            ),
        )
    )

    # Sample sentences
    (
        deduplicated
        | f"TakeSample{tag}" >> beam.transforms.combiners.Top.Of(100)
        | f"FmtSample{tag}" >> beam.FlatMap(lambda x: x)
        | f"WriteSample{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "sample_sentences.txt"),
            shard_name_template="",
        )
    )

    # Rejected samples
    (
        rejected
        | f"TakeReject{tag}" >> beam.transforms.combiners.Top.Of(100)
        | f"FmtReject{tag}" >> beam.FlatMap(lambda x: x)
        | f"WriteReject{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "rejected_sentences.txt"),
            shard_name_template="",
        )
    )

    # Summary stats
    total_sentences = (
        deduplicated
        | f"CountSent{tag}" >> beam.CombineGlobally(beam.combiners.CountCombineFn())
    )
    orig_sum = (
        counts
        | f"GetOrig{tag}" >> beam.Map(lambda x: x[0])
        | f"SumOrig{tag}" >> beam.CombineGlobally(sum)
    )
    recv_sum = (
        counts
        | f"GetRecv{tag}" >> beam.Map(lambda x: x[1])
        | f"SumRecv{tag}" >> beam.CombineGlobally(sum)
    )

    class ComputeRatioFn(beam.DoFn):
        def process(self, recv, orig):
            if orig == 0:
                yield 0.0
            else:
                yield recv / orig

    ratio = (
        recv_sum
        | f"Ratio{tag}"
        >> beam.ParDo(ComputeRatioFn(), orig=beam.pvalue.AsSingleton(orig_sum))
    )

    results_output = p | f"Dummy{tag}" >> beam.Create([None])
    (
        results_output
        | f"FmtMetadata{tag}"
        >> beam.ParDo(
            lambda _, c, r, split=split: [
                json.dumps(
                    {
                        "split": split,
                        "sentence_count": c,
                        "kanji_kana_retention_ratio": round(r, 6),
                        "source": "allenai/c4 (mc4 Japanese)",
                        "gcs_base": GCS_BASE,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            ],
            c=beam.pvalue.AsSingleton(total_sentences),
            r=beam.pvalue.AsSingleton(ratio),
        )
        | f"WriteMetadata{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "metadata.json"),
            shard_name_template="",
        )
    )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--splits",
        default="train,validation",
        help="Comma-separated splits to process (default: train,validation)",
    )
    parser.add_argument(
        "--max_docs",
        type=int,
        default=0,
        help="Limit docs per split for testing (0 = all)",
    )
    parser.add_argument(
        "--output_dir",
        default="output",
        help="Base output directory (default: output/)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    splits = [s.strip() for s in known_args.splits.split(",")]

    # Auto-configure parallelism based on runner.
    is_dataflow = any("DataflowRunner" in arg for arg in pipeline_args)

    if not is_dataflow:
        # Local runs: PrismRunner (Beam 2.69+ default).
        # --sdk_worker_parallelism controls SDK-side thread count.
        # --direct_num_workers is for the old DirectRunner (ignored by Prism).
        num_cores = os.cpu_count() or 4
        default_workers = max(1, num_cores - 2)
        if not any("--sdk_worker_parallelism" in a for a in pipeline_args):
            pipeline_args.append(f"--sdk_worker_parallelism={default_workers}")
        if not any("--direct_num_workers" in a for a in pipeline_args):
            pipeline_args.append(f"--direct_num_workers={default_workers}")
        logging.info(f"Local run: parallelism={default_workers} (cores={num_cores})")

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        for split in splits:
            if split not in SPLITS:
                logging.warning(f"Unknown split '{split}', skipping")
                continue
            _build_split_pipeline(
                p, split, known_args.output_dir, known_args.max_docs
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
