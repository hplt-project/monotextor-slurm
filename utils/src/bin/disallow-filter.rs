use std::sync::mpsc::sync_channel;
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::thread;
use std::io;
use std::fs;

use fst::Set;
use serde::{Deserialize, Serialize};
use itertools::Itertools;
use log::info;
use regex::Regex;
use env_logger::Env;
use memmap2::Mmap;
use clap::Parser;


#[derive(Parser)]
#[command(version, about="Annotate JSONL documents with langid and/or robotstxt allowance")]
struct Args {
    #[arg(short, help="Add robotstxt disallowed info with an FST index")]
    disallowed_index: String,
}

// Define a document struct
// this however, it is different from the one in lib.rs
// because documents may have different fields at each stage
#[derive(Serialize, Deserialize)]
struct Document {
    u: String,
}


fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Started");
    let args = Args::parse();

    let mmap = unsafe { Mmap::map(&fs::File::open(args.disallowed_index)?)? };
    let index = Set::new(mmap)?;
    // remove url http and ww prefix
    let url_prefix_re = Arc::new(Regex::new(r"^(https?://)?(www\.)?(.*)$")?);

    let batch_size = 50000;
    let stdin = io::stdin();
    let (sender, receiver) = sync_channel(1);

    let mut num_kept = 0_usize;
    let num_read = Arc::new(Mutex::new(0_usize));
    let counter = Arc::clone(&num_read);

    // do the stdin read and batching in a separated thread
    let read_thread = thread::spawn(move || {
        // Read from stdin in batches and process them
        for batch_result in &stdin.lock().lines().chunks(batch_size) {
            let batch: Vec<String> = batch_result
                .map(|line| line.expect("Error decoding line"))
                .collect();
            let mut count = counter.lock().unwrap();
            *count += batch.len();
            sender.send(batch).unwrap();
        }
    });

    while let Ok(batch) = receiver.recv() {
        // process every batch in parallel
        // parse json document
        // add segment level langid
        let lines: Vec<_> = batch.iter()
            .filter_map(|line: &String| {
                let doc: Document = serde_json::from_str(line.as_str())
                    .expect("Error parsing JSON document");

                // Remove http://www prefix
                let url = url_prefix_re
                    .captures(&doc.u)
                    .expect("Could not parse url")
                    .get(3).expect("Could not obtain capture group 3 for url").as_str();

                // Search in the fst if we have the url
                // this time exact match, as we have full urls in the index
                if index.contains(url) {
                    None
                } else {
                    Some(line)
                }
            }).collect();

        // serialize modified documents and print them to stdout
        for line in lines {
            num_kept += 1;
            println!("{}", line);
        }
    }

    read_thread.join().unwrap();
    let num_read_final = *num_read.lock().unwrap();
    info!("{} documents read", num_read_final);
    let removed = num_read_final - num_kept;
    info!("{} documents removed ({:.2} %)", removed, removed as f32 / (num_read_final as f32) *100.0);
    info!("Finished");
    Ok(())
}
