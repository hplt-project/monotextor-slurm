use std::io::{BufRead, BufReader, Result};
use std::time::Instant;
use std::fs::File;
use zstd::stream::read::Decoder;
use clap::Parser;
use env_logger::Env;
use log::{info,debug};
use regex::Regex;

use monotextor_utils::{UnionFind, filter_dups};

#[derive(Parser)]
#[clap(version, about="Deduplicate a set of JSONL documents using clusters array. \
                       Non-duplicates and one document per duplicates cluster will be kept. \
                       Document id's of kept documents will be re-assigned.")]
struct Args{
    #[clap(short, long, required=false, takes_value=false,
           help="Print discarded duplicates, instead of non-discarded.")]
    duplicates: bool,

    #[clap(help="File containg the clusters array/s of duplicates.")]
    clusterfile: String,
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,
}



fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let now = Instant::now();
    let args = Args::parse();
    let filename = args.clusterfile.clone();
    let file = File::open(args.clusterfile)
        .expect(format!("Error opening file '{filename}'").as_str());
    let decoder = Decoder::new(file)
        .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
    let mut reader = BufReader::new(decoder);

    // Read header containing the number of records
    let mut line = String::new();
    reader.read_line(&mut line).expect("Error reading header of clusterfile");
    line.pop();
    let parts: Vec<&str> = line.split(&[' ', '\t']).collect();
    let num_records: usize = parts[0].parse()
        .expect(format!("Could not parse {}", parts[0]).as_str());

    // Create parents array
    let mut uf = UnionFind::new(num_records);

    let regex_header = Regex::new(r#"^[0-9]+$"#).expect("Error creating regex");
    info!("Reading clusterfile of {} documents", num_records);
    for (i, line_result) in reader.lines().enumerate() {
        line = line_result.expect("Error reading line");
        if regex_header.is_match(line.as_str()) {
            continue;
        }

        // parse the line and add doc ids to the set
        let parts = line.split(&[' ', '\t']);
        for (j, p) in parts.enumerate() {
            if p.is_empty() {
                continue
            }
            let id: usize = p.parse().expect(
                format!("Could not parse '{}' in line {}:", p, i).as_str());
            if i == 0 {
                uf.parents[j] = id;
            }
            else {
                if id == uf.parents[j] {
                    continue
                }
                uf.union(j, id);
            }
        }
    }
    drop(line);
    debug!("Parents array: {:?}", uf.parents);

    let regex_id = Regex::new(r#"^\{"id":[0-9]+,"#).expect("Error creating regex");
    let mut num_unique = 0_usize; //number of unique docs
    let mut num_docs = 0_usize; //number of readed docs
    info!("Reading documents and discarding duplicates");
    for f in &args.files {
        filter_dups(f, &mut num_docs, &mut num_unique, &uf.parents, &regex_id, args.duplicates);
    }
    let pct = (num_unique as f32 / num_records as f32) * 100.0;
    info!("Duplicates discarded, {} documents kept ({:.2} %)", num_unique, pct);

    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    if num_docs != num_records {
        panic!("Number of read docs is different than in cluster file: {} vs {}", num_docs, num_records);
    }
    Ok(())
}
