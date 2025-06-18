use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::hash::{Hash, Hasher};
use std::fs::File;
use std::time::Instant;

use ahash::AHasher;
use clap::Parser;
use env_logger::Env;
use log::{debug, info};
use zstd::stream::read::Decoder;

use monotextor_utils::utils::memory_usage;
use monotextor_utils::dedup::NoOpHashBuilder;
use monotextor_utils::DocumentText;

#[derive(Parser)]
#[clap(version, about="Exact deduplication")]
struct Args {
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = AHasher::default();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let now = Instant::now();
    let args = Args::parse();

    info!("Started");
    let mut index = HashSet::<u64, NoOpHashBuilder>::with_hasher(NoOpHashBuilder{});

    for filename in args.files {
        let file = File::open(&filename)
            .expect(format!("Error opening file '{filename}'").as_str());
        let decoder = Decoder::new(file)
            .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
        let reader = BufReader::new(decoder);


        for line_res in reader.lines() {
            let line = line_res.unwrap();
            let doc: DocumentText = serde_json::from_str(&line)
                .expect("Error parsing JSON document");

            let hash = calculate_hash(&doc.text);
            if index.insert(hash) {
                println!("{}", line);
            }
        }
    }

    memory_usage();
    info!("Unique elements: {}", index.len());
    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
}
