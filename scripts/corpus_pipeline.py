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
        accepted, rejected, orig_count, recv_count = self.extractor.process_document(text)
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
        from sudachipy import dictionary, tokenizer

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


class FilterByChiveDoFn(beam.DoFn):
    def setup(self):
        import gzip
        import logging
        from apache_beam.io.filesystems import FileSystems
        from sudachipy import dictionary, tokenizer
        
        self.chive_vocab = set()
        targets = [
            "../.cache/chive/tokens.txt.gz",
            ".cache/chive/tokens.txt.gz",
            "gs://file-cache-bucket/chIve-1.3-mc5/tokens.txt.gz"
        ]
        loaded = False
        last_err = None
        for target in targets:
            try:
                with FileSystems.open(target) as f_in:
                    # GCS and Local FileSystems handle .gz decompression inconsistently 
                    # Try reading raw; if it's not gzipped (e.g. auto-decompressed by Beam), it's UTF-8.
                    raw_bytes = f_in.read()
                    if raw_bytes.startswith(b'\x1f\x8b'):
                        import io
                        import gzip
                        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="r") as gz:
                            raw_data = gz.read().decode("utf-8", errors="replace")
                    else:
                        raw_data = raw_bytes.decode("utf-8", errors="replace")
                            
                    lines = raw_data.split("\n")
                    for line in lines:
                        line = line.strip()
                        if line:
                            self.chive_vocab.add(line)
                    loaded = True
                    break
            except Exception as e:
                logging.warning(f"FilterByChive: Could not instantiate target {target}: {e}")
        
        if not loaded:
            logging.error("Failed to load any chiVe dictionary. Falling back to ALL pass!")

        self.dict = dictionary.Dictionary()
        self.tokenizer = self.dict.create()
        self.mode_c = tokenizer.Tokenizer.SplitMode.C

    def process(self, sentence):
        if not self.chive_vocab:
            yield sentence
            return
            
        try:
            tokens = self.tokenizer.tokenize(sentence, self.mode_c)
            for t in tokens:
                surface = t.surface()
                normalized = t.normalized_form()
                if surface not in self.chive_vocab and normalized not in self.chive_vocab:
                    yield beam.pvalue.TaggedOutput("rejected", sentence)
                    return
            yield sentence
        except Exception as e:
            logging.error(f"Error in FilterByChiveDoFn parsing sentence: {e}")
            yield beam.pvalue.TaggedOutput("rejected", sentence)


def _write_split(p, deduplicated, counts, rejected, split, output_dir):
    """Processes a deduplicated split to compute and format outputs."""
    tag = split.capitalize()
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

    mode_tokens = deduplicated | f"Tokenize{tag}" >> beam.ParDo(TokenizeAllModesFn()).with_outputs(
        "A", "B", "C"
    )

    count_a = mode_tokens.A | f"CountA{tag}" >> beam.combiners.Count.PerElement()
    count_b = mode_tokens.B | f"CountB{tag}" >> beam.combiners.Count.PerElement()
    count_c = mode_tokens.C | f"CountC{tag}" >> beam.combiners.Count.PerElement()

    joined = {"A": count_a, "B": count_b, "C": count_c} | f"CoGroup{tag}" >> beam.CoGroupByKey()

    def get_sort_key(element):
        _, data = element
        c_a = data["A"][0] if data["A"] else 0
        c_b = data["B"][0] if data["B"] else 0
        c_c = data["C"][0] if data["C"] else 0
        return (c_c, c_b, c_a)

    class FormatTSVRowDoFn(beam.DoFn):
        def setup(self):
            import io
            import gzip
            import logging
            from apache_beam.io.filesystems import FileSystems
            self.percentiles = {}
            targets = [
                "../.cache/chive/tokens.txt.gz",
                ".cache/chive/tokens.txt.gz",
                "gs://file-cache-bucket/chIve-1.3-mc5/tokens.txt.gz"
            ]
            loaded = False
            last_err = None
            for target in targets:
                try:
                    with FileSystems.open(target) as f_in:
                        raw_bytes = f_in.read()
                        if raw_bytes.startswith(b'\x1f\x8b'):
                            import io
                            import gzip
                            with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="r") as gz:
                                raw_data = gz.read().decode("utf-8", errors="replace")
                        else:
                            raw_data = raw_bytes.decode("utf-8", errors="replace")
                                
                        lines = raw_data.split("\n")
                        total = sum(1 for line in lines if line.strip())
                        for r, line in enumerate(lines):
                            line = line.strip()
                            if line:
                                p = 100.0 * (1.0 - (r / total))
                                self.percentiles[line] = f"{p:.2f}"
                        loaded = True
                        break
                except Exception as e:
                    last_err = e
                    logging.warning(f"Could not instantiate chive target {target}: {e}")
            
            if not loaded:
                raise RuntimeError(f"Failed to load chive dictionary fallback loop. Last error: {last_err}")

        def process(self, element):
            key, data = element
            surface, normalized = key
            c_a = data["A"][0] if data["A"] else 0
            c_b = data["B"][0] if data["B"] else 0
            c_c = data["C"][0] if data["C"] else 0
            p_val = self.percentiles.get(surface)
            if p_val is None:
                p_val = self.percentiles.get(normalized, "x")
            yield f"{surface}\t{normalized}\t{c_a}\t{c_b}\t{c_c}\t{p_val}"

    (
        joined
        | f"TopN{tag}" >> beam.transforms.combiners.Top.Of(1000000, key=get_sort_key)
        | f"FlatTop{tag}" >> beam.FlatMap(lambda x: x)
        | f"FormatTSV{tag}" >> beam.ParDo(FormatTSVRowDoFn())
        | f"WriteFreq{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "token_frequencies.tsv"),
            shard_name_template="",
            header=(
                f"# Token frequencies (Sudachi A, B, C)\n"
                f"# Source: mc4 Japanese ({split})\n"
                f"# Surface\tNormalized\tSudachi_A\tSudachi_B\tSudachi_C\tchiVe_Percentile"
            ),
        )
    )

    (
        deduplicated
        | f"TakeSample{tag}" >> beam.combiners.Sample.FixedSizeGlobally(100)
        | f"FmtSample{tag}" >> beam.FlatMap(lambda x: x)
        | f"WriteSample{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "sample_sentences.txt"),
            shard_name_template="",
        )
    )

    (
        rejected
        | f"TakeReject{tag}" >> beam.combiners.Sample.FixedSizeGlobally(100)
        | f"FmtReject{tag}" >> beam.FlatMap(lambda x: x)
        | f"WriteReject{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "rejected_sentences.txt"),
            shard_name_template="",
        )
    )

    def filter_watch_tokens(text):
        watch_tokens = ["2022", "2023", "⭐", "▶"]
        return any(w in text for w in watch_tokens)

    (
        rejected
        | f"FilterWatch{tag}" >> beam.Filter(filter_watch_tokens)
        | f"TakeWatchReject{tag}" >> beam.combiners.Sample.FixedSizeGlobally(500)
        | f"FmtWatchReject{tag}" >> beam.FlatMap(lambda x: x)
        | f"WriteWatchReject{tag}"
        >> beam.io.WriteToText(
            os.path.join(split_dir, "rejected_sentences_watch.txt"),
            shard_name_template="",
        )
    )

    total_sentences = deduplicated | f"CountSent{tag}" >> beam.CombineGlobally(
        beam.combiners.CountCombineFn()
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

    ratio = recv_sum | f"Ratio{tag}" >> beam.ParDo(
        ComputeRatioFn(), orig=beam.pvalue.AsSingleton(orig_sum)
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


def _compare_splits(output_dir: str):
    """Post-processing stage to calculate distributional differences."""
    import math
    from apache_beam.io.filesystems import FileSystems
    
    train_path = os.path.join(output_dir, "train", "token_frequencies.tsv")
    val_path = os.path.join(output_dir, "validation", "token_frequencies.tsv")
    
    if not (FileSystems.exists(train_path) and FileSystems.exists(val_path)):
        logging.warning("Skipping split comparison: requires both train and validation token frequencies.")
        return
        
    def read_tsv(path):
        data = {}
        with FileSystems.open(path) as f:
            for raw_line in f:
                line = raw_line.decode('utf-8')
                if line.startswith("#"):
                    continue
                parts = line.strip("\n").split("\t")
                if len(parts) >= 5:
                    surf = parts[0]
                    try:
                        c_a, c_b, c_c = int(parts[2]), int(parts[3]), int(parts[4])
                        if surf not in data:
                            data[surf] = {
                                "normalized": parts[1] if len(parts) > 1 else surf,
                                "A": c_a,
                                "B": c_b,
                                "C": c_c,
                                "chive": parts[5] if len(parts) >= 6 else "x"
                            }
                        else:
                            data[surf]["A"] += c_a
                            data[surf]["B"] += c_b
                            data[surf]["C"] += c_c
                    except ValueError:
                        continue
        return data

    logging.info("Analyzing token distribution alignments...")
    train_data = read_tsv(train_path)
    val_data = read_tsv(val_path)
    
    train_vocab = set(w for w in train_data if train_data[w].get("C", 0) > 0)
    val_vocab = set(w for w in val_data if val_data[w].get("C", 0) > 0)
    
    union_vocab = train_vocab | val_vocab
    intersection_vocab = train_vocab & val_vocab
    
    only_train = train_vocab - val_vocab
    only_val = val_vocab - train_vocab
    
    # Sort disjoint sets by highest Frequency (Mode C)
    only_train_sorted = sorted(list(only_train), key=lambda x: train_data[x]["C"], reverse=True)
    only_val_sorted = sorted(list(only_val), key=lambda x: val_data[x]["C"], reverse=True)
    
    modes = ["C"]
    
    def cosine_sim(mode):
        dot = 0.0
        norm_t = 0.0
        norm_v = 0.0
        for w in union_vocab:
            t = train_data.get(w, {}).get(mode, 0)
            v = val_data.get(w, {}).get(mode, 0)
            dot += t * v
            norm_t += t * t
            norm_v += v * v
        if norm_t == 0 or norm_v == 0:
            return 0.0
        return dot / (math.sqrt(norm_t) * math.sqrt(norm_v))

    jaccard = len(intersection_vocab) / len(union_vocab) if union_vocab else 0.0
    
    CHIVE_VOCAB_SIZE = 2530791
    train_in_chive = sum(1 for w in train_vocab if train_data[w].get("chive", "x") != "x")
    val_in_chive = sum(1 for w in val_vocab if val_data[w].get("chive", "x") != "x")
    
    jaccard_train_chive = train_in_chive / (len(train_vocab) + CHIVE_VOCAB_SIZE - train_in_chive) if train_vocab else 0.0
    jaccard_val_chive = val_in_chive / (len(val_vocab) + CHIVE_VOCAB_SIZE - val_in_chive) if val_vocab else 0.0
    
    train_chive_cov = train_in_chive / len(train_vocab) if train_vocab else 0.0
    val_chive_cov = val_in_chive / len(val_vocab) if val_vocab else 0.0
    
    train_missing_chive_sorted = sorted([w for w in train_vocab if train_data[w].get("chive", "x") == "x"], key=lambda x: train_data[x]["C"], reverse=True)
    val_missing_chive_sorted = sorted([w for w in val_vocab if val_data[w].get("chive", "x") == "x"], key=lambda x: val_data[x]["C"], reverse=True)
    
    chive_missing_list = []
    import gzip
    targets = [
        "../.cache/chive/tokens.txt.gz",
        ".cache/chive/tokens.txt.gz",
        "gs://file-cache-bucket/chIve-1.3-mc5/tokens.txt.gz"
    ]
    loaded = False
    last_err = None
    for target in targets:
        try:
            with FileSystems.open(target) as f_in:
                raw_bytes = f_in.read()
                if raw_bytes.startswith(b'\x1f\x8b'):
                    import io
                    import gzip
                    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="r") as gz:
                        raw_data = gz.read().decode("utf-8", errors="replace")
                else:
                    raw_data = raw_bytes.decode("utf-8", errors="replace")
                        
                lines = raw_data.split("\n")
                total = sum(1 for line in lines if line.strip())
                if total > 0:
                    union_vocab_all = set(union_vocab)
                    for w in train_vocab:
                        union_vocab_all.add(train_data[w]["normalized"])
                    for w in val_vocab:
                        union_vocab_all.add(val_data[w]["normalized"])
                    
                    for r, line in enumerate(lines):
                        line = line.strip()
                        if not line: continue
                        if line not in union_vocab_all:
                            p = 100.0 * (1.0 - (r / total))
                            chive_missing_list.append((line, p))
                            if len(chive_missing_list) >= 50:
                                break
                    loaded = True
                break
        except Exception as e:
            last_err = e
            print(f"FAILED TO LOAD {target}: {e}")
            continue
            
    if not loaded:
        raise RuntimeError(f"Failed to load chiVe dictionary in comparison loop. Last error: {last_err}")
    
    report_lines = []
    report_lines.append("# Kotogram Train vs Validation Split Comparison")
    report_lines.append("")
    report_lines.append("## Metrics Key")
    report_lines.append("- **Jaccard Similarity ([0.0 - 1.0])**: Measures strictly Boolean vocabulary overlap. `0.0` means the two splits share absolutely no words in common. `1.0` means they contain the exact same vocabulary. Note: this ignores word frequencies! Because large scrapes contain thousands of unique misspellings and single-occurrence URLs (the 'long tail'), Jaccard typically stays low across splits!")
    report_lines.append("- **Cosine Similarity ([0.0 - 1.0])**: Measures the alignment of token *proportionality*. `0.0` means the corpuses are orthogonally unmatched (no shared distribution), while `1.0` means perfectly identically structured subsets. If word 'X' makes up 5% of both corpuses naturally, the Cosine score pushes strongly to 1.0. This handles noise exceptionally well.")
    report_lines.append("")
    report_lines.append("## Distribution Similarity")
    report_lines.append(f"- Jaccard Similarity (Vocabulary boolean overlap): {jaccard:.6f}")
    for m in modes:
        report_lines.append(f"- Cosine Similarity (Mode {m} vectors): {cosine_sim(m):.6f}")
        
    report_lines.append("")
    report_lines.append("## External Baseline Similarity (chiVe mc5)")
    report_lines.append(f"- Train vs chiVe Jaccard Similarity: {jaccard_train_chive:.6f}")
    report_lines.append(f"- Validation vs chiVe Jaccard Similarity: {jaccard_val_chive:.6f}")
    report_lines.append(f"- Train Vocabulary mapped to chiVe: {train_chive_cov*100:.2f}% coverage")
    report_lines.append(f"- Validation Vocabulary mapped to chiVe: {val_chive_cov*100:.2f}% coverage")
    
    report_lines.append("")
    report_lines.append("## Vocabulary Topography")
    report_lines.append(f"- Total unique tokens across both corpuses: {len(union_vocab):,}")
    report_lines.append(f"- Intersection (Shared by both): {len(intersection_vocab):,}")
    report_lines.append(f"- Only in Train: {len(only_train):,}")
    report_lines.append(f"- Only in Validation: {len(only_val):,}")
    
    report_lines.append("")
    report_lines.append("### Top 50 Missing Tokens (In Train, Missing from Validation)")
    for w in only_train_sorted[:50]:
        report_lines.append(f"  {w} (Train Set count: {train_data[w]['C']})")
        
    report_lines.append("")
    report_lines.append("### Top 50 Missing Tokens (In Validation, Missing from Train)")
    for w in only_val_sorted[:50]:
        report_lines.append(f"  {w} (Validation Set count: {val_data[w]['C']})")
        
    report_lines.append("")
    report_lines.append("### Top 50 Unknown to chiVe (Train Set)")
    for w in train_missing_chive_sorted[:50]:
        report_lines.append(f"  {w} (Train Set count: {train_data[w]['C']})")

    report_lines.append("")
    report_lines.append("### Top 50 Unknown to chiVe (Validation Set)")
    for w in val_missing_chive_sorted[:50]:
        report_lines.append(f"  {w} (Validation Set count: {val_data[w]['C']})")
        
    if chive_missing_list:
        report_lines.append("")
        report_lines.append("### Top 50 chiVe Tokens Missing from Corpus")
        for w, p in chive_missing_list:
            report_lines.append(f"  {w} (chiVe Percentile: {p:.2f})")
        
    report_path = os.path.join(output_dir, "split_comparison_report.md")
    with FileSystems.create(report_path) as f:
        f.write("\n".join(report_lines).encode('utf-8'))
        
    # Write exhaustives tracking lists for hard-coding review
    train_only_path = os.path.join(output_dir, "train_only_disjoint_tokens.tsv")
    with FileSystems.create(train_only_path) as f:
        for w in only_train_sorted:
            f.write(f"{w}\t{train_data[w]['C']}\n".encode('utf-8'))
            
    val_only_path = os.path.join(output_dir, "val_only_disjoint_tokens.tsv")
    with FileSystems.create(val_only_path) as f:
        for w in only_val_sorted:
            f.write(f"{w}\t{val_data[w]['C']}\n".encode('utf-8'))
            
    logging.info(f"Comparison report comprehensively extracted to: {report_path}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max_docs",
        type=int,
        default=0,
        help="Limit overall documents processed (0 = all)",
    )
    parser.add_argument(
        "--val_ratio",
        type=float,
        default=0.1,
        help="Float percentage of deduplicated sentences dynamically partitioned to validation branch. (0.1 = 10%)",
    )
    parser.add_argument(
        "--include_mc4",
        type=lambda x: str(x).lower() == 'true',
        default=True,
        help="Whether to include the mC4 dataset."
    )
    parser.add_argument(
        "--include_wikipedia",
        type=lambda x: str(x).lower() == 'true',
        default=True,
        help="Whether to include Wikipedia in the dataset."
    )
    parser.add_argument(
        "--output_dir",
        default="output",
        help="Base output directory (default: output/)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Auto-configure parallelism based on runner.
    is_dataflow = any("DataflowRunner" in arg for arg in pipeline_args)

    if not is_dataflow:
        num_cores = os.cpu_count() or 4
        default_workers = max(1, num_cores - 2)
        if not any("--sdk_worker_parallelism" in a for a in pipeline_args):
            pipeline_args.append(f"--sdk_worker_parallelism={default_workers}")
        if not any("--direct_num_workers" in a for a in pipeline_args):
            pipeline_args.append(f"--direct_num_workers={default_workers}")
        logging.info(f"Local run: parallelism={default_workers} (cores={num_cores})")

    pipeline_options = PipelineOptions(pipeline_args)

    def hash_partition_fn(sentence, num_partitions, val_ratio):
        """Stable hashing algorithm to divide strings proportionally and permanently"""
        h = int(hashlib.md5(sentence.encode("utf-8")).hexdigest()[:8], 16)
        return 1 if (h % 1000) < (val_ratio * 1000) else 0

    with beam.Pipeline(options=pipeline_options) as p:
        texts_combined = []
        if known_args.include_mc4:
            if known_args.max_docs > 0:
                file_pattern = f"{GCS_BASE}/train/0.parquet"
            else:
                file_pattern = f"{GCS_BASE}/*/*.parquet"

            texts_mc4 = (
                p
                | "ReadGloballyMC4"
                >> beam.io.ReadFromParquet(file_pattern=file_pattern, columns=["text"])
                | "ExtractTextMC4" >> beam.Map(lambda row: row["text"])
            )
            texts_combined.append(texts_mc4)
        
        if known_args.include_wikipedia:
            WIKI_GCS_BASE = "gs://file-cache-bucket/wikipedia/ja"
            if known_args.max_docs > 0:
                wiki_file_pattern = f"{WIKI_GCS_BASE}/train-00000-of-00015.parquet"
            else:
                wiki_file_pattern = f"{WIKI_GCS_BASE}/*.parquet"
                
            texts_wiki = (
                p
                | "ReadGloballyWiki"
                >> beam.io.ReadFromParquet(file_pattern=wiki_file_pattern, columns=["text"])
                | "ExtractTextWiki" >> beam.Map(lambda row: row["text"])
            )
            texts_combined.append(texts_wiki)
            
        texts = (
            texts_combined
            | "MergeSources" >> beam.Flatten()
        )

        if known_args.max_docs > 0:
            texts = (
                texts
                | "LimitGlobally"
                >> beam.combiners.Top.Of(known_args.max_docs, key=lambda t: hashlib.md5(t.encode()).hexdigest())
                | "FlattenGlobally" >> beam.FlatMap(lambda x: x)
            )

        results = texts | "ProcessDocsGlobally" >> beam.ParDo(ProcessDocumentDoFn()).with_outputs(
            "counts", "rejected", main="sentences"
        )
        
        deduplicated_raw = (
            results.sentences
            | "PairHashGlobal" >> beam.Map(lambda s: (hash_sentence(s), s))
            | "DedupHashGlobal" >> beam.CombinePerKey(TakeOneFn())
            | "ExtractSentGlobal" >> beam.Map(lambda x: x[1])
        )

        chive_filtered = deduplicated_raw | "FilterChiveOOV" >> beam.ParDo(FilterByChiveDoFn()).with_outputs("rejected", main="sentences")
        deduplicated = chive_filtered.sentences

        rejected_all = (
            (results.rejected, chive_filtered.rejected) | "FlattenRejects" >> beam.Flatten()
        )

        # Apply proportional native partition
        partitions = deduplicated | "HashPartition" >> beam.Partition(
            hash_partition_fn, 2, val_ratio=known_args.val_ratio
        )
        
        train_partition = partitions[0]
        val_partition = partitions[1]

        _write_split(p, train_partition, results.counts, rejected_all, "train", known_args.output_dir)
        _write_split(p, val_partition, results.counts, rejected_all, "validation", known_args.output_dir)

    _compare_splits(known_args.output_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
