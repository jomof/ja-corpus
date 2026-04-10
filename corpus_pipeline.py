import argparse
import itertools
import logging
from datasets import load_dataset
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class FetchShardDoFn(beam.DoFn):
    def process(self, shard_index, num_shards, limit):
        logging.info(f"Processing shard {shard_index} of {num_shards} with limit {limit}")
        try:
            # Load dataset in streaming mode
            dataset = load_dataset("mc4", languages=["ja"], streaming=True)
            
            # Manual sharding for IterableDataset since .shard() is missing in this version
            count = 0
            yielded = 0
            for record in dataset["train"]:
                if count % num_shards == shard_index:
                    yield record["text"]
                    yielded += 1
                    if limit > 0 and yielded >= limit:
                        break
                count += 1
                
        except Exception as e:
            logging.error(f"Error processing shard {shard_index}: {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="output.txt", help="Output file")
    parser.add_argument("--num_shards", type=int, default=2, help="Number of shards")
    parser.add_argument("--limit", type=int, default=50, help="Limit records per shard (0 for no limit)")
    
    # Parse known args to separate custom args from Beam options
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Generate shard indices
        shard_indices = p | "CreateShardIndices" >> beam.Create(list(range(known_args.num_shards)))
        
        # Step 2: Fetch data in parallel shards
        texts = shard_indices | "FetchShards" >> beam.ParDo(
            FetchShardDoFn(),
            num_shards=known_args.num_shards,
            limit=known_args.limit
        )
        
        # Step 3: Toy MapReduce (Count characters in each text and sum them)
        char_counts = texts | "CountChars" >> beam.Map(lambda text: len(text))
        total_chars = char_counts | "SumChars" >> beam.CombineGlobally(sum)
        
        # Step 4: Write result to output file
        total_chars | "FormatResult" >> beam.Map(lambda count: f"Total characters: {count}") \
                    | "WriteOutput" >> beam.io.WriteToText(known_args.output, shard_name_template="")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
