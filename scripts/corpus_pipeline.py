import argparse
import logging
from datasets import load_dataset
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Import the new extractor module
from ja_sentence_extractor import SentenceExtractor

class FetchShardDoFn(beam.DoFn):
    def process(self, shard_index, num_shards, limit, skip=0):
        logging.info(f"Processing shard {shard_index} of {num_shards} with limit {limit}, skip {skip}")
        try:
            dataset = load_dataset("mc4", languages=["ja"], streaming=True)
            
            count = 0
            yielded = 0
            skipped = 0
            for record in dataset["validation"]:  # Using validation as control
                if count % num_shards == shard_index:
                    if skipped < skip:
                        skipped += 1
                    else:
                        yield record["text"]
                        yielded += 1
                        if limit > 0 and yielded >= limit:
                            break
                count += 1
                
        except Exception as e:
            logging.error(f"Error processing shard {shard_index}: {e}")

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

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="output.txt", help="Output file")
    parser.add_argument("--num_shards", type=int, default=2, help="Number of shards")
    parser.add_argument("--limit", type=int, default=50, help="Limit records per shard (0 for no limit)")
    parser.add_argument("--skip", type=int, default=0, help="Skip records per shard")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
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
        
        # Step 4a: Output Samples (Branch)
        (
            sentences 
            | "TakeSample" >> beam.transforms.combiners.Top.Of(100)
            | "FormatSample" >> beam.FlatMap(lambda sample_list: sample_list)
            | "WriteSample" >> beam.io.WriteToText("sample_sentences.txt", shard_name_template="")
        )
        
        # Step 4b: Output Rejected Samples (Branch)
        (
            rejected
            | "TakeRejectedSample" >> beam.transforms.combiners.Top.Of(100)
            | "FormatRejectedSample" >> beam.FlatMap(lambda sample_list: sample_list)
            | "WriteRejectedSample" >> beam.io.WriteToText("rejected_sentences.txt", shard_name_template="")
        )
        
        # Step 5: Count sentences
        total_sentences = sentences | "CountSentences" >> beam.CombineGlobally(beam.combiners.CountCombineFn())
        
        # Step 6: Calculate Kanji/Kana Ratio
        orig_sum = counts | "GetOrig" >> beam.Map(lambda x: x[0]) | "SumOrig" >> beam.CombineGlobally(sum)
        recv_sum = counts | "GetRecv" >> beam.Map(lambda x: x[1]) | "SumRecv" >> beam.CombineGlobally(sum)
        
        class ComputeRatioFn(beam.DoFn):
            def process(self, recv, orig):
                if orig == 0:
                    yield 0.0
                else:
                    yield recv / orig
                    
        ratio = recv_sum | "ComputeRatio" >> beam.ParDo(ComputeRatioFn(), orig=beam.pvalue.AsSingleton(orig_sum))
        
        # Step 7: Write result to output file
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
