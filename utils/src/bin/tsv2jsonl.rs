use std::io::{self, BufRead, Write, Result};
use serde_json;
use clap::Parser;

use monotextor_utils::Document;

#[derive(Parser)]
#[clap(author, version, about="Convert TSV format to JSONL documents", long_about = None)]
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

    let mut document = Document::new(args.language.to_owned());
    let mut prev_url: String = String::new();

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

                // For each new document, we simply clear the contents
                // to avoid allocating memory again
                // then re-use the same object
                document.clear();
            }

            prev_url = parts[0].to_string();
            // Add line to the document
            // if an error occurs, panic with the current line number
            if let Err(e) = document.add_line(parts) {
                panic!("Error in line {}: {}", i, e);
            }
        }
    }

    // print the last document
    if !document.is_empty() {
        let json_result = serde_json::to_string(&document);
        if let Ok(json) = json_result {
            writer.write_fmt(format_args!("{}\n", json))?;
        }
    }

    Ok(())
}
