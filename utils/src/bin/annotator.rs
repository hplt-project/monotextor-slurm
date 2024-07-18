use std::sync::mpsc::sync_channel;
use std::io::BufRead;
use std::sync::Arc;
use std::thread;
use std::io;
use std::fs;

use fst::Set;
use serde::{Deserialize, Serialize};
use itertools::Itertools;
use rayon::prelude::*;
use env_logger::Env;
use memmap2::Mmap;
use regex::Regex;
use clap::Parser;

use heli_otr::identifier::Identifier;
use heli_otr::{load_models, pythonpath};


#[derive(Parser)]
#[clap(version, about="Annotate JSONL documents with langid and/or robotstxt allowance")]
struct Args {
    #[clap(short, help="Path to heli-otr model directory")]
    modelpath: Option<String>,
    #[clap(short, help="Add robotstxt disallowed info with an FST index")]
    disallowed_index: Option<String>,
}

// Define a document struct
// this however, it is different from the one in lib.rs
// because documents may have different fields at each stage
#[derive(Serialize, Deserialize)]
struct Document {
    f: String,
    o: usize,
    s: usize,
    rs: usize,
    u: String,
    c: String,
    ts: String,
    collection: String,
    lang: Vec<String>,
    prob: Vec<f32>,
    text: String,
    seg_langs: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    robotstxt: Option<String>,
}


fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let modelpath = if let Some(modelpath) = args.modelpath {
        modelpath
    } else {
        pythonpath().expect("Could loading heli_otr path, please install it as python module or provide a modelpath.")
    };
    // compile langcode fix regex
    let codefix = Regex::new(r"(\w+_)(\w)(\w+)").unwrap();

    let index_main: Option<_>;
    if let Some(filename) = args.disallowed_index {
        let mmap = unsafe { Mmap::map(&fs::File::open(filename)?)? };
        index_main = Some(Set::new(mmap)?);
    } else {
        index_main = None;
    }
    // remove url http and ww prefix
    let url_prefix_re = Arc::new(Regex::new(r"^(https?://)?(www\.)?(.*)$")?);

    // Load model and create atomic references
    // so only one model is loaded, then shared with each thread
    let (charmodel, wordmodel) = load_models(&modelpath);
    let charmodelref = Arc::new(charmodel);
    let wordmodelref = Arc::new(wordmodel);

    let batch_size = 50000;
    let stdin = io::stdin();
    let (sender, receiver) = sync_channel(1);

    // do the stdin read and batching in a separated thread
    let read_thread = thread::spawn(move || {
        // Read from stdin in batches and process them
        for batch_result in &stdin.lock().lines().chunks(batch_size) {
            let batch: Vec<String> = batch_result
                .map(|line| line.expect("Error decoding line"))
                .collect();
            sender.send(batch).unwrap();
        }
    });

    while let Ok(batch) = receiver.recv() {
        // process every batch in parallel
        // parse json document
        // add segment level langid
        let docs: Vec<_> = batch.par_iter()
            .map(|line: &String| {
                // each thread will create the mutable part of the identifier
                // and share with the main thread the language model, which is immutable
                let mut detector = Identifier::new(
                    Arc::clone(&charmodelref),
                    Arc::clone(&wordmodelref),
                );
                let mut doc: Document = serde_json::from_str(line.as_str())
                    .expect("Error parsing JSON document");

                // identify each segment (splitting by endlines) in the document text
                // add the predictions to seg_langs array in the json
                let _ = doc.seg_langs.insert(Vec::new());
                for line in doc.text.lines() {
                    let mut pred = detector.identify(&line).0.to_string();
                    // Uppercase the first letter of the script suffix in the langcode
                    pred = codefix.replace(&pred, |captures: &regex::Captures| {
                        let mut ser = String::new();
                        ser.push_str(&captures[1]);
                        ser.push_str(&captures[2].to_uppercase());
                        ser.push_str(&captures[3]);
                        ser
                    }).to_string();
                    doc.seg_langs.as_mut().unwrap().push(pred);
                }

                if let Some(index) = &index_main {
                    // Remove http://www prefix
                    let url = url_prefix_re
                        .captures(&doc.u)
                        .expect("Could not parse url")
                        .get(3).expect("Could not obtain capture group 3 for url").as_str();

                    // Search in the fst if we have the url
                    // this time exact match, as we have full urls in the index
                    if index.contains(url) {
                        doc.robotstxt = Some(String::from("disallowed"));
                    } else {
                        doc.robotstxt = Some(String::from("allowed"));
                    }
                }

                doc
            }).collect();

        // serialize modified documents and print them to stdout
        for doc in docs {
            println!("{}", serde_json::to_string(&doc).unwrap());
        }
    }

    read_thread.join().unwrap();
    Ok(())
}
