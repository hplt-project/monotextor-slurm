use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::BufRead;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread;

use aho_corasick::AhoCorasick;
use clap::Parser;
use env_logger::Env;
use fst::Set;
use itertools::Itertools;
use log::info;
use memmap2::Mmap;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};

use heli_otr::identifier::Identifier;
use heli_otr::{load_models, pythonpath};

#[derive(Parser)]
#[command(
    version,
    about = "Annotate JSONL documents with langid and/or robotstxt allowance"
)]
struct Args {
    #[arg(short, help = "Path to heli-otr model directory")]
    modelpath: Option<String>,
    #[arg(short, help = "Add robotstxt disallowed info with an FST index")]
    disallowed_index: Option<String>,
    #[arg(
        short,
        help = "Remove documents that contain any of these list of secrets"
    )]
    secrets_list: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Lang {
    lang: Vec<String>,
    prob: Vec<f32>,
}

// Define a document struct
// this however, it is different from the one in lib.rs
// because documents may have different fields at each stage
#[derive(Serialize, Deserialize)]
struct Document {
    text: String,
    xml: Option<String>,
    md: Option<String>,
    f: String,
    o: usize,
    s: usize,
    rs: usize,
    u: String,
    c: String,
    ts: String,
    de: String,
    id: String,
    #[serde(alias = "openlid-v2")]
    openlid_v2: Lang,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    htmllang: Option<Vec<String>>,
    #[serde(alias = "glotlid-v3")]
    glotlid_v3: Lang,
    #[serde(alias = "openlid-v3")]
    openlid_v3: Lang,
    #[serde(skip_serializing_if = "Option::is_none")]
    seg_langs_openlid_v3: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    crawl_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pii: Option<Vec<(usize, usize)>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc_scores: Option<Vec<f32>>,
    #[serde(rename = "web-register", skip_serializing_if = "Option::is_none")]
    web_register: Option<HashMap<String, f32>>,
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Started");
    let args = Args::parse();
    let modelpath = if let Some(modelpath) = args.modelpath {
        modelpath
    } else {
        pythonpath().expect("Could loading heli_otr path, please install it as python module or provide a modelpath.")
    };

    let index_main: Option<_>;
    if let Some(filename) = args.disallowed_index {
        let mmap = unsafe { Mmap::map(&fs::File::open(filename)?)? };
        index_main = Some(Set::new(mmap)?);
    } else {
        index_main = None;
    }
    // remove url http and ww prefix
    let url_prefix_re = Arc::new(Regex::new(r"^(https?://)?(www\.)?(.*)$")?);

    let secrets_matcher: Option<_>;
    if let Some(filename) = args.secrets_list {
        let file_read =
            io::BufReader::new(fs::File::open(&filename).expect("Secrets list does not exist"));
        let patterns: Vec<_> = file_read.lines().map(|line| line.unwrap()).collect();
        secrets_matcher = Some(AhoCorasick::new(&patterns)?);
    } else {
        secrets_matcher = None;
    }

    // Load model and create atomic references
    // so only one model is loaded, then shared with each thread
    let (charmodel, wordmodel) = load_models(&modelpath);
    let charmodelref = Arc::new(charmodel);
    let wordmodelref = Arc::new(wordmodel);

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
        let docs: Vec<_> = batch
            .par_iter()
            .filter_map(|line: &String| {
                // each thread will create the mutable part of the identifier
                // and share with the main thread the language model, which is immutable
                let mut detector =
                    Identifier::new(Arc::clone(&charmodelref), Arc::clone(&wordmodelref));
                let mut doc: Document =
                    serde_json::from_str(line.as_str()).expect("Error parsing JSON document");

                // Documents that contain secrets are discarded
                if let Some(matcher) = &secrets_matcher {
                    if matcher.is_match(&doc.text) {
                        return None;
                    }
                }

                // identify each segment (splitting by endlines) in the document text
                // add the predictions to seg_langs array in the json
                let _ = doc.seg_langs_openlid_v3.insert(Vec::new());
                for line in doc.text.lines() {
                    let pred = detector.identify(&line).0.to_string();
                    doc.seg_langs_openlid_v3.as_mut().unwrap().push(pred);
                }

                if let Some(index) = &index_main {
                    // Remove http://www prefix
                    let url = url_prefix_re
                        .captures(&doc.u)
                        .expect("Could not parse url")
                        .get(3)
                        .expect("Could not obtain capture group 3 for url")
                        .as_str();

                    // Search in the fst if we have the url
                    // this time exact match, as we have full urls in the index
                    if index.contains(url) {
                        doc.allowed = Some(false);
                    } else {
                        doc.allowed = Some(true);
                    }
                }

                Some(doc)
            })
            .collect();

        // serialize modified documents and print them to stdout
        for doc in docs {
            num_kept += 1;
            println!("{}", serde_json::to_string(&doc).unwrap());
        }
    }

    read_thread.join().unwrap();
    let num_read_final = *num_read.lock().unwrap();
    info!("{} documents read", num_read_final);
    let removed = num_read_final - num_kept;
    info!(
        "{} documents removed ({:.2} %)",
        removed,
        removed as f32 / (num_read_final as f32) * 100.0
    );
    info!("Finished");
    Ok(())
}
