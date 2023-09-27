use std::io::{BufRead, BufReader};
use std::sync::mpsc::sync_channel;
use std::fs::File;
use std::thread;
use rayon::prelude::*;
use itertools::Itertools;
use gaoya::unionfind::UnionFind;
use gaoya::minhash::{
    MinHashDeduper,
};
use zstd::stream::read::Decoder;

use crate::minhash_processor::{Tokenization, MinHashProcessor};
use crate::DocumentText;


pub struct Indexer
{
    hasher: MinHashProcessor,
    index: MinHashDeduper<u64>,
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
        // create bounded thread communication channel
        let (sender, receiver) = sync_channel(1);

        // Copy to send it to the thread
        let batch_size = self.batch_size;
        let new_filename = filename.clone();

        // Spawn a thread to do the file reading and decompression
        let read_thread = thread::spawn(move || {
            // read zstd compressed input, iterate in chunks
            let file = File::open(&new_filename)
                .expect(format!("Error opening file '{new_filename}'").as_str());
            let decoder = Decoder::new(file)
                .expect(format!("Uncompressed or corrupted file '{new_filename}'").as_str());
            let chunks = &BufReader::new(decoder).lines().chunks(batch_size);

            for batch_result in chunks {
                let batch: Vec<String> = batch_result
                    .map(|line| line.expect("Error reading line"))
                    .collect();
                sender.send(batch).unwrap();
            }
        });

        // Read batched lines sent from the thread and process them
        while let Ok(batch) = receiver.recv() {
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

        read_thread.join().unwrap();
    }

    pub fn find_clusters(&self) -> UnionFind {
        self.index.find_clusters()
    }
}
