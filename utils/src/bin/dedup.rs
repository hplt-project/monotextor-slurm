use std::io::{BufRead, BufReader, Result};
use std::time::Instant;
use std::fs::File;
use zstd::stream::read::Decoder;
use clap::Parser;
use env_logger::Env;
use log::{info,debug};
use regex::Regex;

use monotextor_utils::queryreader::QueryReader;

#[derive(Parser)]
#[clap(version, about="Deduplicate a set of JSONL documents using index queries. \
                       Non-duplicates and one document per duplicates vluster will be kept. \
                       Document id's of kept documents will be re-assigned.")]
struct Args{
    #[clap(short, long, required=false, takes_value=false,
           help="Print discarded duplicates, instead of non-discarded.")]
    duplicates: bool,

    #[clap(short, long, help="Files containg the queries from the index.")]
    queryfiles: Vec<String>,
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,
}


fn filter_dups(filename: &String, parents: &Vec<usize>, regex_id: &Regex, duplicates: bool){
    let file = File::open(filename).unwrap();
    let decoder = Decoder::new(file).unwrap();
    let reader = BufReader::new(decoder);
    let mut readed = 0_usize;

    for (i, line_result) in reader.lines().enumerate() {
        let mut line = line_result.unwrap();

        // Discard every document that it is not its own parent
        // That way, we keep documents that do not have known duplicates
        // and one from each set of duplicates (the uppermost parent)
        if duplicates {
            if parents[i] != i {
                println!("{}", line);
            }
            continue;
        } else if parents[i] != i {
            continue;
        }
        readed += 1;

        // Re-assign new document id with regex, id always at the beggining, no need to parse the
        // whole json
        line = regex_id.replace(&line, format!("{{\"id\":{},", readed)).to_string();
        println!("{}", line);
    }

}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let now = Instant::now();
    let args = Args::parse();

    debug!("{:?}", args.queryfiles);
    let qr = QueryReader::new(args.queryfiles);

    info!("Reading queries file");
    let uf = qr.read_all();
    debug!("Parents array: {:?}", uf.parents);

    let regex_id = Regex::new(r#"^\{"id":[0-9]+,"#).unwrap();
    info!("Reading documents and discarding duplicates");
    for f in &args.files {
        filter_dups(f, &uf.parents, &regex_id, args.duplicates);
    }

    info!("Elapsed time: {} s", now.elapsed().as_secs());
    info!("Finished");
    Ok(())
}
