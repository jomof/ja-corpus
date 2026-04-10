import argparse
import logging
import hashlib
import os
import json
from datasets import load_dataset
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Import the new extractor module
from ja_sentence_extractor import SentenceExtractor

class FetchShardDoFn(beam.DoFn):
    def process(self, shard_index, num_shards, limit, skip=0):
        cache_dir = "cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"shard_{shard_index}_of_{num_shards}.jsonl")
        
        cached_count = 0
        if os.path.exists(cache_file):
            logging.info(f"Shard {shard_index}/{num_shards}: Reading from cache (requested limit: {limit})")
            try:
                with open(cache_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            record = json.loads(line)
                            yield record["text"]
                            cached_count += 1
                            if limit > 0 and cached_count >= limit:
                                logging.info(f"Shard {shard_index}/{num_shards}: Finished reading from cache")
                                return
            except Exception as e:
                logging.warning(f"Error reading cache file {cache_file}: {e}. Will fall back to fetching.")
                cached_count = 0
                
        if limit > 0 and cached_count >= limit:
            logging.info(f"Shard {shard_index}/{num_shards}: Finished (already met limit from cache)")
            return
            
        logging.info(f"Shard {shard_index}/{num_shards}: Fetching remaining {limit - cached_count if limit > 0 else 'all'} records from HF")
        try:
            dataset = load_dataset("mc4", languages=["ja"], streaming=True)
            
            with open(cache_file, 'a') as f:
                matched_count = 0
                count = 0
                for record in dataset["validation"]:
                    if count % num_shards == shard_index:
                        if matched_count < cached_count:
                            matched_count += 1
                        else:
                            yield record["text"]
                            f.write(json.dumps(record) + "\n")
                            matched_count += 1
                            if limit > 0 and matched_count >= limit:
                                break
                    count += 1
                        
        except Exception as e:
            logging.error(f"Error processing shard {shard_index}: {e}")
            
        logging.info(f"Shard {shard_index}/{num_shards}: Finished fetching from HF")

class ProcessDocumentDoFn(beam.DoFn):
    def setup(self):
        # Initialize the extractor once per worker
        self.extractor = SentenceExtractor()

    def process(self, text):
        # Use the clean API for extraction and filtering
        accepted, rejected, orig_count, recv_count = self.extractor.process_document(text)
        
        for s in accepted:
            yield s  # Main output
            
        for s in rejected:
            yield beam.pvalue.TaggedOutput("rejected", s)
            
        yield beam.pvalue.TaggedOutput("counts", (orig_count, recv_count))

def hash_sentence(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()

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
        try:
            # Helper to clean unusual line terminators and newlines
            def clean_surface(surface):
                return surface.replace('\u2028', ' ').replace('\u2029', ' ').replace('\n', ' ').replace('\r', ' ')

            # Mode A
            for t in self.tokenizer.tokenize(sentence, self.mode_a):
                yield beam.pvalue.TaggedOutput("A", clean_surface(t.surface()))
            # Mode B
            for t in self.tokenizer.tokenize(sentence, self.mode_b):
                yield beam.pvalue.TaggedOutput("B", clean_surface(t.surface()))
            # Mode C
            for t in self.tokenizer.tokenize(sentence, self.mode_c):
                yield beam.pvalue.TaggedOutput("C", clean_surface(t.surface()))
        except Exception as e:
            logging.error(f"Error in TokenizeAllModesFn: {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="output.txt", help="Output file")
    parser.add_argument("--num_shards", type=int, default=2, help="Number of shards")
    parser.add_argument("--limit", type=int, default=50, help="Limit records per shard (0 for no limit)")
    parser.add_argument("--skip", type=int, default=0, help="Skip records per shard")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Auto-set direct_num_workers to num_cores - 2 if not specified
    has_workers = False
    for arg in pipeline_args:
        if "--direct_num_workers" in arg:
            has_workers = True
            break
            
    if not has_workers:
        num_cores = os.cpu_count()
        if num_cores:
            default_workers = max(1, num_cores - 2)
            pipeline_args.append(f"--direct_num_workers={default_workers}")
            logging.info(f"Auto-setting --direct_num_workers to {default_workers} (Total cores: {num_cores})")
    
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Generate shard indices
        shard_indices = p | "CreateShardIndices" >> beam.Create(list(range(known_args.num_shards)))
        
        # Step 2: Fetch data in parallel shards
        texts = shard_indices | "FetchShards" >> beam.ParDo(
            FetchShardDoFn(),
            num_shards=known_args.num_shards,
            limit=known_args.limit,
            skip=known_args.skip
        )
        
        # Step 3: Process documents (Extract sentences and count Kanji/Kana)
        results = texts | "ProcessDocs" >> beam.ParDo(ProcessDocumentDoFn()).with_outputs("counts", "rejected", main="sentences")
        
        sentences = results.sentences
        counts = results.counts
        rejected = results.rejected
        
        # Step 3.5: Global Deduplication (Option 1)
        deduplicated_sentences = (
            sentences
            | "PairWithHash" >> beam.Map(lambda s: (hash_sentence(s), s))
            | "DeduplicateByHash" >> beam.CombinePerKey(TakeOneFn())
            | "ExtractSentence" >> beam.Map(lambda x: x[1])
        )
        
        # Step 4: Compute Histograms
        mode_tokens = deduplicated_sentences | "TokenizeAllModes" >> beam.ParDo(TokenizeAllModesFn()).with_outputs("A", "B", "C")
        
        count_a = mode_tokens.A | "CountA" >> beam.combiners.Count.PerElement()
        count_b = mode_tokens.B | "CountB" >> beam.combiners.Count.PerElement()
        count_c = mode_tokens.C | "CountC" >> beam.combiners.Count.PerElement()
        
        joined_frequencies = (
            {'A': count_a, 'B': count_b, 'C': count_c}
            | "CoGroupFrequencies" >> beam.CoGroupByKey()
        )
        
        def get_sort_key(element):
            word, data = element
            c_a = data['A'][0] if data['A'] else 0
            c_b = data['B'][0] if data['B'] else 0
            c_c = data['C'][0] if data['C'] else 0
            return (c_c, c_b, c_a) # C descending, then B, then A
            
        def format_tsv_row(element):
            word, data = element
            c_a = data['A'][0] if data['A'] else 0
            c_b = data['B'][0] if data['B'] else 0
            c_c = data['C'][0] if data['C'] else 0
            return f"{word}\t{c_a}\t{c_b}\t{c_c}"
            
        (
            joined_frequencies
            | "TopN" >> beam.transforms.combiners.Top.Of(1000000, key=get_sort_key)
            | "FlattenTop" >> beam.FlatMap(lambda x: x)
            | "FormatTSV" >> beam.Map(format_tsv_row)
            | "WriteHistograms" >> beam.io.WriteToText(
                "word_frequencies.tsv", 
                shard_name_template="",
                header="# Word frequencies generated using Sudachi tokenization (Modes A, B, C)\n# Data Provenance: HuggingFace 'mc4' dataset (validation split)\n# Word\tSudachi_A\tSudachi_B\tSudachi_C"
            )
        )
        
        # Step 5a: Output Samples (Branch) - Use deduplicated sentences
        (
            deduplicated_sentences 
            | "TakeSample" >> beam.transforms.combiners.Top.Of(100)
            | "FormatSample" >> beam.FlatMap(lambda sample_list: sample_list)
            | "WriteSample" >> beam.io.WriteToText("cache/sample_sentences.txt", shard_name_template="")
        )
        
        # Step 5b: Output Rejected Samples (Branch)
        (
            rejected
            | "TakeRejectedSample" >> beam.transforms.combiners.Top.Of(100)
            | "FormatRejectedSample" >> beam.FlatMap(lambda sample_list: sample_list)
            | "WriteRejectedSample" >> beam.io.WriteToText("cache/rejected_sentences.txt", shard_name_template="")
        )
        
        # Step 6: Count sentences - Use deduplicated sentences
        total_sentences = deduplicated_sentences | "CountSentences" >> beam.CombineGlobally(beam.combiners.CountCombineFn())
        
        # Step 7: Calculate Kanji/Kana Ratio
        orig_sum = counts | "GetOrig" >> beam.Map(lambda x: x[0]) | "SumOrig" >> beam.CombineGlobally(sum)
        recv_sum = counts | "GetRecv" >> beam.Map(lambda x: x[1]) | "SumRecv" >> beam.CombineGlobally(sum)
        
        class ComputeRatioFn(beam.DoFn):
            def process(self, recv, orig):
                if orig == 0:
                    yield 0.0
                else:
                    yield recv / orig
                    
        ratio = recv_sum | "ComputeRatio" >> beam.ParDo(ComputeRatioFn(), orig=beam.pvalue.AsSingleton(orig_sum))
        
        # Step 8: Write result to output file
        results_output = p | "CreateDummy" >> beam.Create([None])
        
        (
            results_output 
            | "FormatFinalResult" >> beam.ParDo(
                lambda _, c, r: [f"Total sentences: {c}\nKanji/Kana retention ratio: {r:.4f}"],
                c=beam.pvalue.AsSingleton(total_sentences),
                r=beam.pvalue.AsSingleton(ratio)
            )
            | "WriteOutput" >> beam.io.WriteToText(known_args.output, shard_name_template="")
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
