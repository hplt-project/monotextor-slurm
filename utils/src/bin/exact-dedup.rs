use std::io::{Write, BufWriter, stdout, BufRead, BufReader};
use std::hash::{Hash, Hasher};
use std::fs::File;
use std::sync::mpsc::sync_channel;
use std::time::Instant;
use std::thread;

use ahash::AHasher;
use clap::Parser;
use env_logger::Env;
use fastbloom_rs::{Membership, FilterBuilder};
use log::{info};
use parse_size::parse_size;
use zstd::stream::read::Decoder;

use monotextor_utils::utils::memory_usage;
use monotextor_utils::DocumentText;

#[derive(Parser)]
#[clap(version, about="Exact deduplication")]
struct Args {
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,

    #[clap(long, short, help="Estimated number of elements",
           value_parser = |s: &str| parse_size(s))]
    num_elements: u64,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = AHasher::default();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    info!("Initializing BloomFilter");
    let now = Instant::now();
    // let mut index = BloomFilter::with_false_pos(0.001)
    //     .seed(&42)
    //     .expected_items(args.num_elements as usize);
    let mut index = FilterBuilder::new(args.num_elements, 0.001)
        .build_bloom_filter();
    info!("BloomFilter initialization took {:.2} s", now.elapsed().as_secs_f32());

    info!("Processing");
    let now = Instant::now();
    let (sender, receiver) = sync_channel(100000);
    let mut writer = BufWriter::with_capacity(100_000, stdout().lock());

    let mut num_docs = 0;
    let mut kept_docs = 0;
    thread::spawn(move || {
        for filename in args.files {
            let file = File::open(&filename)
                .expect(format!("Error opening file '{filename}'").as_str());
            let decoder = Decoder::new(file)
                .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
            let reader = BufReader::new(decoder);

            for line_res in reader.split(b'\n') {
                let line = line_res.unwrap();
                sender.send(line).unwrap();
            }
        }
    });

    while let Ok(line) = receiver.recv() {
        let doc: DocumentText = serde_json::from_slice(&line)
            .expect("Error parsing JSON document");
        num_docs += 1;

        // let hash = calculate_hash(&doc.text);
        // if !index.insert_hash(hash) {
        //     kept_docs += 1;
        //     println!("{}", line);
        // }
        let bytes = &doc.text.as_bytes();
        if !index.contains(bytes) {
            kept_docs += 1;
            index.add(bytes);
            writer.write(&line).unwrap();
            writer.write(b"\n").unwrap();
        }
    }

    memory_usage();
    info!("Finished");
    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Total docs: {}", num_docs);
    info!("Kept docs: {} ({:.1}%)", kept_docs, kept_docs as f64 / num_docs as f64 * 100.0);
    info!("Throughput: {:.1} docs/s", num_docs as f32/now.elapsed().as_secs_f32());
    // info!("Unique elements: {}", index.len());
}
