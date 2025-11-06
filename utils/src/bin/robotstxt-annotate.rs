use std::sync::mpsc::sync_channel;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::sync::{Arc};
use std::thread;
use std::time::Instant;
use std::fs;

use clap::Parser;
use env_logger::Env;
use fst::Set;
use log::info;
use memmap2::Mmap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

use monotextor_utils::utils::memory_usage;

#[derive(Parser)]
#[command(version, about="Annotate JSONL documents with langid and/or robotstxt allowance")]
struct Args {
    #[arg(help="Path to robotstxt disallowed info with an FST index")]
    disallowed_index: String,
    #[arg(help="Input jsonl zstd metadata file containing 'u' field for URLs")]
    input_file: String,
    #[arg(help="Output jsonl zstd containing robotstxt annotations")]
    output_file: String,
}

// Define a document struct
#[derive(Serialize, Deserialize)]
struct Document {
    u: String,
}


fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let now = Instant::now();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Started");
    let args = Args::parse();

    let mmap = unsafe { Mmap::map(&fs::File::open(args.disallowed_index)?)? };
    let index = Set::new(mmap)?;
    // remove url http and ww prefix
    let url_prefix_re = Arc::new(Regex::new(r"^(https?://)?(www\.)?(.*)$")?);


    let (sender, receiver) = sync_channel(50000);
    let input_reader = BufReader::new(Decoder::new(File::open(args.input_file)?)?);
    let mut output_writer = BufWriter::new(Encoder::new(File::create(args.output_file)?, 3)?.auto_finish());

    let read_thread = thread::spawn(move || {
        for line_result in input_reader.split(b'\n') {
            let line = line_result.unwrap();
            sender.send(line).unwrap();
        }
    });

    while let Ok(line) = receiver.recv() {
        let doc: Document = serde_json::from_slice(&line)
            .expect("Error parsing JSON document");
        let url = url_prefix_re
            .captures(&doc.u)
            .expect("Could not parse url")
            .get(3).expect("Could not obtain capture group 3 for url").as_str();
        if index.contains(url) {
            output_writer.write(b"{\"allowed\": false}\n")?;
        } else {
            output_writer.write(b"{\"allowed\": true}\n")?;
        }
    }

    read_thread.join().unwrap();
    memory_usage();
    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    Ok(())
}
