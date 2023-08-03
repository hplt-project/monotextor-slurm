use std::io::{BufRead, BufReader};
use std::collections::HashSet;
use std::time::Instant;
use std::fs::File;
use itertools::Itertools;
use rayon::prelude::*;
use gaoya::minhash::{
    calculate_minhash_params,
    MinHashIndex, HashSetContainer,
};
use zstd::stream::read::Decoder;
use serde_json::Result;
use clap::Parser;
use env_logger::Env;
use log::info;

use monotextor_utils::{DocumentText, Tokenization, MinHashProcessor};


#[derive(Parser)]
#[clap(version, about="Index a set of documents in JSONL format. \
                       Then return the queries of each document agains all the index.")]
struct Args{
    #[clap(long, default_value_t=20000,
           help="Number of lines to be processed at a time")]
    batch_size: usize,
    #[clap(long, short, default_value_t=-1,
           help="Band to be indexed. Values from 0 to band_size-1. If none specified, index all.")]
    band_id: isize,
    #[clap(arg_enum, long, short, default_value="whitespace",
           help="Tokenization type.")]
    tokenizer: Tokenization,
    #[clap(short, long, default_value_t=3,
           help="Size of the non-overlapping window for character tokenization.")]
    window_size: usize,

    #[clap(long, default_value_t=1000,
        help="Documents with higher number of duplicates than this amount \
             will be marked to be directly discarded. \
             Not even keeping one of the group as representative.")]
    num_duplicates_threshold: usize,
    #[clap(long, short, default_value_t=0.8, help="Jaccard similarity threshold.")]
    jaccard_threshold: f64,
    #[clap(long, short, default_value_t=260,
           help="Number of permutations, a.k.a number of hashes.")]
    permutations: usize,
    //#[clap(long, required=false, help="Number of bands. If provided, permutations will be ignored.")]
    //num_bands: usize,
    //#[clap(long, required=false, help="Band width. If provided, permutations will be ignored.")]
    //band_width: usize,

    #[clap(long, short, required=false, takes_value=false,
           help="Print MinHash parameters and finish.")]
    dry_run: bool,

    #[clap(help="zstd compressed jsonl files to be indexed.")]
    files: Vec<String>,
}


// Print a list of index queries
fn print_queries(queries: &Vec<HashSet<usize>>, dups_threshold: usize) {
    for q in queries {
        // Very big query results marked to be directly discarded
        if q.len() >= dups_threshold {
            println!("DISCARD");
            continue;
        }
        // Print each element of the query separated by space
        for (i, elem) in q.iter().enumerate() {
            print!("{}", elem);
            if i != q.len() - 1 {
                print!(" ");
            }
        }
        println!("");
    }
}

// Read one file, parse, hash and insert each document in the index
fn index_file(filename: &String, global_id: &mut usize, batch_size: usize,
              index: &mut MinHashIndex<u32, usize, HashSetContainer<usize>>,
              hasher: &MinHashProcessor,
              query: bool, dups_threshold: usize) {

    // read zstd compressed input, iterate in chunks
    let file = File::open(filename)
        .expect(format!("Error opening file '{filename}'").as_str());
    let decoder = Decoder::new(file)
        .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
    let chunks = &BufReader::new(decoder).lines().chunks(batch_size);
    let mut batched_lines = chunks.into_iter();

    // Read and process input in batches
    while let Some(chunk) = batched_lines.next() {
        // read the actual lines, panic if any error
        let batch: Vec<String> = chunk
            .map(|line| line.expect("Error reading line"))
            .collect();

        let signatures: Vec<_> = batch.par_iter()
            .map(|line: &String| {
                let doc: DocumentText = serde_json::from_str(line.as_str())
                    .expect("Error parsing JSON document");
                hasher.create_signature(&doc.text)
            }).collect();

        // Enumerate all the documents, global id's
        let new_id = *global_id + signatures.len();
        let ids: Vec<usize> = (*global_id..new_id).collect();

        if !query {
            // insert into index in parallel
            index.par_bulk_insert(ids, signatures);
        } else {
            let queries = index.par_bulk_query(&signatures);
            print_queries(&queries, dups_threshold);
        }
        *global_id = new_id;
    }
}


fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let now = Instant::now();

    // Create MinHash index and hasher objects
    let (num_bands, band_width) = calculate_minhash_params(
        args.jaccard_threshold, args.permutations
    );
    let hasher = MinHashProcessor::new(num_bands * band_width, args.tokenizer, args.window_size);
    let mut index:
        MinHashIndex<u32, usize, HashSetContainer<usize>> =
    {
        MinHashIndex::new_index(num_bands, band_width, args.jaccard_threshold, args.band_id)
    };
    info!("Num permutations: {}", num_bands*band_width);
    info!("Num bands: {}", num_bands);
    info!("Band width: {}", band_width);
    info!("Indexed band num: {}", args.band_id);
    if args.dry_run {
        // With dry run, print params and exit
        info!("Finished");
        return Ok(())
    }

    info!("Indexing documents");
    // Read, deserialize, hash and index each file
    let mut global_id = 0; // document id
    for file in &args.files {
        index_file(file, &mut global_id, args.batch_size,
                   &mut index, &hasher, false, args.num_duplicates_threshold);
    }
    info!("Indexed {} documents", global_id);

    info!("Querying documents");
    // start reading again, this time we query each document
    println!("{}", global_id);
    let mut global_id = 0;
    for file in &args.files {
        index_file(file, &mut global_id, args.batch_size,
                   &mut index, &hasher, true, args.num_duplicates_threshold);
    }
    info!("Queried {} documents", global_id);
    drop(index);
    drop(hasher);

    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    Ok(())
}
