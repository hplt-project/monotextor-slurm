use std::io::{BufRead, BufReader};
use std::fs::File;
use std::collections::HashMap;
use zstd::stream::read::Decoder;
use gaoya::unionfind::UnionFind;
use regex::Regex;
use log::debug;

pub struct DedupFilter {
    pub num_docs: usize,
    pub num_read_docs: usize,
    pub num_unique: usize,
    print_duplicates: bool,
    pub uf: UnionFind,
    cluster_sizes: Option<HashMap<usize, usize>>,
}

impl DedupFilter {
    pub fn new(clusterfile: String, print_duplicates: bool, add_cluster_size: bool) -> Self {
        let uf = Self::read_cluster_file(clusterfile);
        let mut sizes = None;
        if add_cluster_size {
            sizes = Some(Self::compute_cluster_sizes(&uf));
        }
        Self {
            num_docs: uf.length,
            num_read_docs: 0,
            num_unique: 0,
            print_duplicates: print_duplicates,
            uf: uf,
            cluster_sizes: sizes,
        }
    }

    pub fn read_cluster_file(filename: String) -> UnionFind {
        let file = File::open(&filename)
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

        let regex_header = Regex::new(r#"^[0-9]+$"#).expect("Error creating regex");
        let mut uf = UnionFind::new(num_records);

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

        uf
    }

    // Count how many elements has each cluster
    fn compute_cluster_sizes(uf: &UnionFind) -> HashMap<usize,usize> {
        let mut num_clusters = 0;
        for i in 0..uf.parents.len() {
            if i != uf.parents[i] {
                num_clusters += 1;
            }
        }
        let mut sizes: HashMap<usize,usize> = HashMap::with_capacity(num_clusters);

        for i in uf.parents.iter() {
            if let Some(x) = sizes.get_mut(&i) {
                *x += 1;
            } else {
                sizes.insert(*i, 1);
            }
        }

        sizes
    }

    // Read compressed JSONL and discard duplicates according to a UF parents array
    // Re-assign doc id with a unique num reference given
    // If duplicates is true, print only duplicates
    pub fn filter_dups(&mut self, filename: &String){
        let file = File::open(filename)
            .expect(format!("Error opening file '{filename}'").as_str());
        let decoder = Decoder::new(file)
            .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
        let reader = BufReader::new(decoder);

        for line_result in reader.lines() {
            let line = line_result.expect("Error reading line");

            // Discard every document that it is not its own parent
            // That way, we keep documents that do not have known duplicates
            // and one from each set of duplicates (the uppermost parent)
            if self.print_duplicates {
                if self.uf.parents[self.num_read_docs] != self.num_read_docs {
                    println!("{}", line);
                }
                self.num_read_docs += 1;
                continue;
            } else if self.uf.parents[self.num_read_docs] != self.num_read_docs {
                debug!("Discarding document {} in cluster {}",
                       self.num_read_docs,
                       self.uf.parents[self.num_read_docs]);
                self.num_read_docs += 1;
                continue;
            }

            if let Some(cluster_sizes) = &self.cluster_sizes {
                let csize = cluster_sizes
                    .get(&self.uf.parents[self.num_read_docs])
                    .expect(format!(
                            "Could not found cluster size for doc {} in cluster {}",
                            self.num_read_docs,
                            self.uf.parents[self.num_read_docs])
                        .as_str());

                // we do not parse the document, otherwise the speed reduces by a half
                // so just doing a safe check that the end of the string is actually the
                // end of the JSON, therefore we can print the line as is, omit the last char
                // and add the cluster_size field
                if line.as_bytes()[line.len()-1] != b'}' {
                    panic!("Wrong line ending");
                }
                println!("{},\"cluster_size\":{}}}", &line[0..line.len()-1], csize);
            } else {
                println!("{}", line);
            }

            self.num_read_docs += 1;
            self.num_unique += 1;
        }
    }
}
