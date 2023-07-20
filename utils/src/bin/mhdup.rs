use std::io::{BufRead, BufReader};
use std::collections::HashSet;
use std::fs::File;
use itertools::Itertools;
use rayon::prelude::*;
use gaoya::minhash::{
    calculate_minhash_params,
    MinHashIndex, MinHasher32, MinHasher,
    HashSetContainer,
};
use zstd::stream::read::Decoder;
use gaoya::text::whitespace_split;
use fnv::FnvBuildHasher;
use serde_json::Result;
use clap::Parser;

use monotextor_utils::{DocumentText};

#[derive(Parser)]
#[clap(version)]
struct Args{
    #[clap(long, short, default_value_t=2000)]
    batch_size: usize,

    files: Vec<String>,
}


// Read one file, parse, hash and insert each document in the index
fn index_file(filename: &String, global_id: &mut usize, batch_size: usize,
              index: &mut MinHashIndex<u32, usize, HashSetContainer<usize>>,
              hasher: &MinHasher32<FnvBuildHasher>,
              query: bool, dup_ids: &mut HashSet<usize>) {

    // read zstd compressed input, iterate in chunks
    let file = File::open(filename).unwrap();
    let decoder = Decoder::new(file).unwrap();
    let chunks = &BufReader::new(decoder).lines().chunks(batch_size);
    let mut batched_lines = chunks.into_iter();

    // Read and process input in batches
    while let Some(chunk) = batched_lines.next() {
        // read the actual lines, panic if any error
        let batch: Vec<String> = chunk
            .map(|line| line.unwrap())
            .collect();

        // parse each json line into doc in parallel
        let docs: Vec<DocumentText> = batch.par_iter()
            .map(|line: &String| {
                serde_json::from_str(line.as_str()).unwrap()
            }).collect();

        // hash documents in parallel
        let signatures = docs.par_iter()
            .map(|doc| {
                hasher.create_signature(
                    whitespace_split(&doc.text.to_lowercase())
                )
            }).collect();

        // Enumerate all the documents, global id's
        let new_id = *global_id + docs.len() - 1;
        let ids: Vec<usize> = (*global_id..new_id).collect();

        if !query {
            // insert into index in parallel
            index.par_bulk_insert(ids, signatures);
        } else {
            let queries = index.par_bulk_query(&signatures);
            for (i, (q, gid)) in queries.iter().zip(ids).enumerate() {
                if dup_ids.contains(&gid) {
                    println!("{}", docs[i].text.replace("\n", "  "));
                }
                for elem in q {
                    if *elem != gid {
                        dup_ids.insert(*elem);
                    }
                }
            }
        }
        *global_id = new_id;
    }
}


fn main() -> Result<()> {
    let args = Args::parse();
    //let mut writer = io::stdout().lock();

    // Create MinHash index and hasher objects
    let num_hashes = 250;
    let jaccard_threshold = 0.8;
    let (num_bands, band_width) = calculate_minhash_params(jaccard_threshold, num_hashes);
    let hasher = MinHasher32::new(num_bands * band_width);
    let mut index:
        MinHashIndex<u32, usize, HashSetContainer<usize>> =
    {
        MinHashIndex::new_index(num_bands, band_width, jaccard_threshold, -1)
    };
    let mut dup_ids = HashSet::<usize>::new();

    // Read, deserialize, hash and index each file
    let mut global_id = 0; // document id
    for file in &args.files {
        index_file(file, &mut global_id, args.batch_size, &mut index, &hasher, false, &mut dup_ids);
    }

    // start reading again, this time we query each document
    let mut global_id = 0;
    for file in &args.files {
        index_file(file, &mut global_id, args.batch_size, &mut index, &hasher, true, &mut dup_ids);
    }

    Ok(())
}