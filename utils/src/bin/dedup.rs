use std::io::{BufRead, BufReader, Result};
use std::collections::HashSet;
use std::time::Instant;
use std::fs::File;
use zstd::stream::read::Decoder;
use clap::Parser;
use env_logger::Env;
use log::{info,debug};
use regex::Regex;

use monotextor_utils::UnionFind;

#[derive(Parser)]
#[clap(version, about="Deduplicate a set of JSONL documents using index queries. \
                       Non-duplicates and one document per duplicates vluster will be kept. \
                       Document id's of kept documents will be re-assigned.")]
struct Args{
    #[clap(short, long, required=false, takes_value=false,
           help="Print discarded duplicates, instead of non-discarded.")]
    duplicates: bool,

    #[clap(help="File containg the queries from the index.")]
    queryfile: String,
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,
}


fn filter_dups(filename: &String, parents: &Vec<usize>, regex_id: &Regex, duplicates: bool){
    let file = File::open(filename)
        .expect(format!("Error opening file '{filename}'").as_str());
    let decoder = Decoder::new(file)
        .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
    let reader = BufReader::new(decoder);
    let mut readed = 0_usize;

    for (i, line_result) in reader.lines().enumerate() {
        let mut line = line_result.expect("Error reading line");

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
    let filename = args.queryfile.clone();
    let file = File::open(args.queryfile)
        .expect(format!("Error opening file '{filename}'").as_str());
    let decoder = Decoder::new(file)
        .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
    let mut reader = BufReader::new(decoder);

    // Read header containing the number of records
    let mut line = String::new();
    reader.read_line(&mut line).expect("Error reading header of queryfile");
    line.pop();
    let parts: Vec<&str> = line.split(&[' ', '\t']).collect();
    let num_records: usize = parts[0].parse()
        .expect(format!("Could not parse {}", parts[0]).as_str());

    // Create parents array
    let mut uf = UnionFind::new(num_records);
    // Create set buffer to dedup each query
    // partitioned minhash will provide a list of queries per each doc
    // duplicated doc_ids may be found in each line
    let mut uniq = HashSet::<usize>::with_capacity(100);

    info!("Reading queries file");
    for (i, line_result) in reader.lines().enumerate() {
        line = line_result.expect("Error reading line");
        uniq.clear();

        // parse the line and add doc ids to the set
        let parts = line.split(&[' ', '\t']);
        for p in parts {
            // DISCARD lines, workaround for very repeated duplicates (aka very long queries)
            // in that case, just set any other doc as parent
            // given that they won't be their own parents, they will be discarded
            if p.starts_with("DISCARD") {
                uf.union(0, i);
                continue;
            }
            let id: usize = p.parse().expect(
                format!("Could not parse '{}' in line {}:", p, i).as_str());
            uniq.insert(id);
        }

        // union each doc id in the query to the current doc (line number)
        for j in &uniq {
            if i == *j {
                continue;
            }
            uf.union(i, *j);
        }
    }
    debug!("Parents array: {:?}", uf.parents);

    let regex_id = Regex::new(r#"^\{"id":[0-9]+,"#).expect("Error creating regex");
    info!("Reading documents and discarding duplicates");
    for f in &args.files {
        filter_dups(f, &uf.parents, &regex_id, args.duplicates);
    }

    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    Ok(())
}
