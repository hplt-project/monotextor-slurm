use std::io::{BufRead, BufReader};
use std::fs::File;
use rayon::prelude::*;
use itertools::Itertools;
use gaoya::unionfind::UnionFind;
use gaoya::minhash::{
    MinHashDeduper,
};
use zstd::stream::read::Decoder;

use crate::{DocumentText, Tokenization, MinHashProcessor};


pub struct Indexer
{
    hasher: MinHashProcessor,
    index: MinHashDeduper<u32>,
    batch_size: usize,
}

impl Indexer {
    pub fn new(num_bands: usize, band_width: usize, tokenizer: Tokenization,
               window_size: usize, jaccard_threshold: f64, band_id: isize,
               batch_size: usize) -> Self {
        Self {
            hasher: MinHashProcessor::new(num_bands * band_width, tokenizer, window_size),
            index: MinHashDeduper::new_index(num_bands, band_width, jaccard_threshold, band_id),
            batch_size: batch_size,
        }
    }


    // Read one file, parse, hash and insert each document in the index
    pub fn index_file(&mut self, filename: &String, global_id: &mut usize) {
        // read zstd compressed input, iterate in chunks
        let file = File::open(filename)
            .expect(format!("Error opening file '{filename}'").as_str());
        let decoder = Decoder::new(file)
            .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
        let chunks = &BufReader::new(decoder).lines().chunks(self.batch_size);
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
                    self.hasher.create_signature(&doc.text)
                }).collect();

            // Enumerate all the documents, global id's
            let new_id = *global_id + signatures.len();
            let ids: Vec<usize> = (*global_id..new_id).collect();

            // insert into index in parallel
            self.index.par_bulk_insert(ids, signatures);
            *global_id = new_id;
        }
    }

    pub fn find_clusters(&self) -> UnionFind {
        self.index.find_clusters()
    }
}
