use std::sync::mpsc::sync_channel;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::io;

use serde::{Deserialize, Serialize};
use itertools::Itertools;
use rayon::prelude::*;
use env_logger::Env;
use clap::Parser;

use heli_otr::identifier::Identifier;
use heli_otr::languagemodel::Model;


#[derive(Parser)]
#[clap(version, about="Add segment level language identification to JSONL documents")]
struct Args {
    #[clap(help="Path to heli-otr model directory")]
    modelpath: String,
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
}

// Load heli-otr models in parallel
fn load_models(modelpath: &str) -> (Model, Model) {
    let grampath = format!("{modelpath}/charmodel.bin");
    let char_handle = thread::spawn(move || {
        let path = Path::new(&grampath);
        Model::from_bin(path)
    });

    let wordpath = format!("{modelpath}/wordmodel.bin");
    let word_handle = thread::spawn(move || {
        let path = Path::new(&wordpath);
        Model::from_bin(path)
    });
    let charmodel = char_handle.join().unwrap();
    let wordmodel = word_handle.join().unwrap();

    (charmodel, wordmodel)
}


fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    // Load model and create atomic references
    // so only one model is loaded, then shared with each thread
    let (charmodel, wordmodel) = load_models(&args.modelpath);
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
                    doc.seg_langs.as_mut().unwrap().push(detector.identify(&line).0.to_string());
                }
                doc
            }).collect();

        // serialize modified documents and print them to stdout
        for doc in docs {
            println!("{}", serde_json::to_string(&doc).unwrap());
        }
    }

    read_thread.join().unwrap();
}
