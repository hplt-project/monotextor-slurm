use std::io::{self, BufRead, BufReader};
use serde::{Deserialize, Serialize};
use itertools::Itertools;
use rayon::prelude::*;
use gaoya::minhash::{
    calculate_minhash_params,
    MinHashIndex, MinHasher32, MinHasher,
    HashSetContainer,
};
use zstd::stream::read::Decoder;
use gaoya::text::whitespace_split;
use serde_json::Result;

#[derive(Debug, Deserialize, Serialize)]
struct DocumentText {
    // Parse documents ignoring all fields but "text"
    text: String,
}

fn main() -> Result<()> {
    // read zstd compressed input, iterate in chunks
    let batch_size = 2000;
    let decoder = Decoder::new(io::stdin().lock()).unwrap();
    let chunks = &BufReader::new(decoder).lines().chunks(batch_size);
    let mut batched_lines = chunks.into_iter();
    //let mut writer = io::stdout().lock();

    // Create MinHash index and hasher objects
    let num_hashes = 250;
    let jaccard_threshold = 0.5;
    let (num_bands, band_width) = calculate_minhash_params(jaccard_threshold, num_hashes);
    let hasher = MinHasher32::new(num_bands * band_width);
    let mut index:
        MinHashIndex<u32, usize, HashSetContainer<usize>> =
    {
        MinHashIndex::new_index(num_bands, band_width, jaccard_threshold, -1)
    };

    let mut id = 0; // document id
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

        // Enumerate all the documents, global id's
        let ids: Vec<usize> = (id..id + docs.len() -1).collect();
        id += docs.len() - 1;

        // hash documents in parallel
        let signatures = docs.par_iter()
            .map(|doc| {
                hasher.create_signature(
                    whitespace_split(&doc.text.to_lowercase())
                )
            }).collect();

        // insert into index in parallel
        index.par_bulk_insert(ids, signatures);
    }

    Ok(())
}
