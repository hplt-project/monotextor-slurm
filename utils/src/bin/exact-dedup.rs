use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Instant;
use std::mem::drop;

use ahash::AHasher;
use clap::Parser;
use env_logger::Env;
use fastbloom_rs::{FilterBuilder, Membership};
use glob::glob;
use log::{error, info, debug};
use parse_size::parse_size;
use zstd::stream::read::Decoder;

use monotextor_utils::split::ZSplit;
use monotextor_utils::utils::memory_usage;
use monotextor_utils::DocumentText;

#[derive(Parser)]
#[clap(version, about = "Exact deduplication")]
struct Args {
    #[clap(help = "Output file prefix")]
    out_prefix: String,
    #[clap(help = "List or glob of zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,

    #[clap(long, short, help="Estimated number of elements",
           value_parser = |s: &str| parse_size(s))]
    num_elements: u64,
    #[clap(
        long,
        short,
        default_value_t = 40,
        help = "Uncompressed size per each output batch in GB"
    )]
    split_size: usize,
    #[clap(long, short = 't')]
    num_threads: u32,
    #[clap(long, short, default_value_t = 10)]
    compression_level: i32,
    #[clap(long, short, help="Buffer size in bytes",
           value_parser = |s: &str| parse_size(s))]
    buffer_size: u64,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = AHasher::default();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut args = Args::parse();
    if args.files.len() < 1 {
        error!("error: files requires at least 1 values");
        std::process::exit(1);
    }

    // If only one element, do glob expansion
    if args.files.len() == 1 {
        info!("Expanding glob");
        let mut files: Vec<String> = glob(&args.files[0])
            .expect("Failed to expand glob")
            .into_iter()
            .map(|p| {
                match p {
                    Ok(path) => path.to_str().unwrap().to_string(),
                    Err(e) => panic!("Could not read file {:?} from glob", e),
                }
            })
            .collect();
        args.files.clear();
        args.files.append(&mut files);
        debug!("Expanded glob to {:?}", args.files);
    }

    info!("Initializing BloomFilter");
    let now = Instant::now();
    // let mut index = BloomFilter::with_false_pos(0.001)
    //     .seed(&42)
    //     .expected_items(args.num_elements as usize);
    let mut index = FilterBuilder::new(args.num_elements, 0.001).build_bloom_filter();
    info!(
        "BloomFilter initialization took {:.2} s",
        now.elapsed().as_secs_f32()
    );

    info!("Processing");
    let now = Instant::now();
    let (sender, receiver) = sync_channel(100000);
    let mut writer = ZSplit::new(
        &args.out_prefix,
        args.split_size * 1_000_000_000,
        args.compression_level,
        args.num_threads,
        args.buffer_size as usize,
    )
    .unwrap();

    let mut num_docs = 0;
    let mut kept_docs = 0;
    thread::spawn(move || {
        for filename in args.files {
            let file =
                File::open(&filename).expect(format!("Error opening file '{filename}'").as_str());
            let decoder = Decoder::new(file)
                .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
            let reader = BufReader::with_capacity(args.buffer_size as usize, decoder);

            for line_res in reader.split(b'\n') {
                let line = line_res.unwrap();
                sender.send(line).unwrap();
            }
        }
    });

    while let Ok(line) = receiver.recv() {
        let doc: DocumentText = serde_json::from_slice(&line).expect("Error parsing JSON document");
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
    writer.flush().unwrap();

    memory_usage();
    drop(index);
    info!("Finished");
    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Total docs: {}", num_docs);
    info!(
        "Kept docs: {} ({:.1}%)",
        kept_docs,
        kept_docs as f64 / num_docs as f64 * 100.0
    );
    info!(
        "Throughput: {:.1} docs/s",
        num_docs as f32 / now.elapsed().as_secs_f32()
    );
    // info!("Unique elements: {}", index.len());
}
