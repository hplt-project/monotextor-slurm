use std::io::{BufRead, BufReader};
use std::collections::HashSet;
use std::fs::File;
use rayon::prelude::*;
use itertools::Itertools;
use gaoya::minhash::{
    MinHashIndex, HashSetContainer,
};
use zstd::stream::read::Decoder;

use crate::{DocumentText, Tokenization, MinHashProcessor};


pub struct Indexer {
    hasher: MinHashProcessor,
    index: MinHashIndex<u32, usize, HashSetContainer<usize>>,
    blocklist: HashSet<usize>,
    batch_size: usize,
    dups_threshold: usize,
}

impl Indexer {
    pub fn new(num_bands: usize, band_width: usize, tokenizer: Tokenization,
               window_size: usize, jaccard_threshold: f64, band_id: isize,
               batch_size: usize, dups_threshold: usize) -> Self {
        Self {
            hasher: MinHashProcessor::new(num_bands * band_width, tokenizer, window_size),
            index: MinHashIndex::new_index(num_bands, band_width, jaccard_threshold, band_id),
            blocklist: HashSet::new(),
            batch_size: batch_size,
            dups_threshold: dups_threshold,
        }
    }


    // Preemptively add to a blocklist the duplicates repeated more than the threshold
    // remove them from the index
    fn _update_blocklist(&mut self, id: &usize, query: &HashSet<usize>) -> bool {
        if query.len() >= self.dups_threshold || self.blocklist.contains(&id) {
            for j in query {
                self.blocklist.insert(*j);
            }
            if !self.blocklist.contains(&id) {
                let list: Vec<usize> = query.iter().cloned().collect();
                self.index.bulk_remove(&list);
            }
            return true;
        }
        return false
    }


    // Print a list of index queries
    fn print_queries(&mut self, queries: &Vec<HashSet<usize>>) {
        for q in queries {
            // Very big query results marked to be directly discarded
            //TODO tag as DISCARD the docs that are higher than threshold or are in the blocklist
            if q.len() >= self.dups_threshold {
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
    pub fn index_file(&mut self, filename: &String, global_id: &mut usize, query: bool) {
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

            if !query {
                // insert into index in parallel
                self.index.par_bulk_insert(ids, signatures);
            } else {
                let queries = self.index.par_bulk_query(&signatures);
                //TODO remove DISCARD docs from the index and add them to a blocklist
                // also add all similars to the blocklist
                self.print_queries(&queries);
            }
            *global_id = new_id;
        }
    }

}
