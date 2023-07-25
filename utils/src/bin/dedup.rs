use std::io::{stdin, BufRead, Result};
use std::collections::HashSet;
use log::info;
use env_logger::Env;

use monotextor_utils::UnionFind;


fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut reader = stdin().lock();

    // Read header containing the number of records
    let mut line = String::new();
    let _ = reader.read_line(&mut line);
    line.pop();
    let parts: Vec<&str> = line.split(&[' ', '\t']).collect();
    let num_records: usize = parts[0].parse().expect(format!("Could not parse {}", parts[0]).as_str());

    // Create parents array
    let mut uf = UnionFind::new(num_records);
    // Create set buffer to dedup each query
    // partitioned minhash will provide a list of queries per each doc
    // duplicated doc_ids may be found in each line
    let mut uniq = HashSet::<usize>::new();

    info!("Reading queries file");
    for (i, line_result) in reader.lines().enumerate() {
        line = line_result.unwrap();

        // parse the line and add doc ids to the set
        let parts: Vec<&str> = line.split(&[' ', '\t']).collect();
        for p in parts {
            let id: usize = p.parse().unwrap();
            uniq.insert(id);
        }

        // union each doc id in the query to the current doc (line number)
        for j in &uniq {
            if i == *j {
                continue;
            }
            uf.union(i, *j);
        }

        uniq.clear();
    }

    println!("{:?}", uf.parents);

    info!("Reading documents and discarding duplicates");

    Ok(())
}
