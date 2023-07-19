use std::io::{self, BufRead, Write, Result};
use serde_json;
use clap::Parser;

use monotextor_utils::Document;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    language: String,
}


fn main() -> Result<()> {
    let args = Args::parse();
    let stdin = io::stdin();
    let reader = stdin.lock();
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    let mut document = Document::new();
    let mut prev_url: String = String::new();
    let mut doc_id: u64 = 0;
    document.document_lang = args.language.to_owned();

    for (i, line_result) in reader.lines().enumerate() {
        if let Ok(line) = line_result {
            let parts: Vec<&str> = line.split('\t').collect();
            let url = parts[0];

            // url has changed
            // current line is a different doc, print the previous doc
            if !prev_url.is_empty() && !prev_url.eq(&url) {
                let json_result = serde_json::to_string(&document);
                if let Ok(json) = json_result {
                    writer.write_fmt(format_args!("{}\n", json))?;
                }

                doc_id += 1;
                document.clear();
                document.id = doc_id;
            }

            // Concatenate paragraphs with endline separators
            // to reconstruct the documents
            if !document.text.is_empty() {
                document.text.push_str("\n");
            }
            document.text.push_str(parts[1]);
            // insert url and collection only once per doc
            if document.url.is_empty() {
                document.url = url.to_string();
            }
            if document.collection.is_empty() {
                document.collection = parts[2].to_string();
            }
            document.langs.push(parts[3].to_string());
            // parse scores to float
            let score_result = parts[4].parse::<f32>();
            match score_result {
                Ok(score) => document.scores.push(score),
                Err(_) => panic!("Error parsing '{}' to float in line {}", parts[4], i)
            }

            prev_url = parts[0].to_string();
        }
    }

    // print the last document
    if !document.text.is_empty() {
        let json_result = serde_json::to_string(&document);
        if let Ok(json) = json_result {
            writer.write_fmt(format_args!("{}\n", json))?;
        }
    }

    Ok(())
}
