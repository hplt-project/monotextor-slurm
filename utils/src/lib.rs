use std::io::{BufRead, BufReader, Lines};
use std::process::{id, Command};
use std::str::from_utf8;
use std::fs::File;
use serde::{Deserialize, Serialize};
use gaoya::minhash::{MinHasher32, MinHasher};
use gaoya::text::whitespace_split;
use zstd::stream::read::Decoder;
use log::{info, warn, debug};
use shingles::Shingles;
use fnv::FnvBuildHasher;
use clap::ArgEnum;
use regex::Regex;
use seahash;

pub mod indexer;

pub trait TextField {
    fn get_text(&self) -> String;
}


#[derive(Serialize, Deserialize)]
pub struct Document {
    pub id: u64,
    pub document_lang: String,
    pub scores: Vec<f32>,
    pub langs: Vec<String>,
    pub text: String,
    pub url: String,
    pub collection: String,
}

impl Document {
    pub fn new() -> Self {
        Self {
            // Create with capacity is a little bit faster
            // if we assume always working with large inputs
            scores: Vec::with_capacity(500),
            langs: Vec::with_capacity(500),
            text: String::with_capacity(500000),
            url: String::new(),
            collection: String::new(),
            document_lang: String::new(),
            id: 0,
        }
    }
    pub fn clear(&mut self) {
        // Clear the content of the document
        self.scores.clear();
        self.langs.clear();
        self.text.clear();
        self.url.clear();
        self.collection.clear();
    }
}

impl TextField for Document {
    fn get_text(&self) -> String {
        self.text.clone()
    }
}

#[derive(Deserialize, Serialize)]
pub struct DocumentText {
    // Parse documents ignoring all fields but "text"
    pub text: String,
}

impl TextField for DocumentText {
    fn get_text(&self) -> String {
        self.text.clone()
    }
}


pub struct UnionFind {
    pub parents: Vec<usize>,
    pub length: usize,
}

// Implementation of the Union Find algorithm to obtain all the connected duplicates
impl UnionFind {
    pub fn new(length: usize) -> Self {
        Self {
            parents: (0..length).collect(),
            length: length,
        }
    }

    // find the parent of a node
    // after finding the uppermost parent, we set the direct parent of x, to that parent
    // so we widen the tree and subsequent finds will be much faster (only one jump)
    // doing mutable self because it's called from union, who has mutable self
    pub fn find(&mut self, x: usize) -> usize {
        let mut p = x;
        while self.parents[p] != p {
            p = self.parents[p];
        }
        self.parents[x] = p; // path compression
        return p;
    }

    pub fn union(&mut self, x: usize, y: usize) {
        if x == y {
            return
        }
        let par_x = self.find(x);
        let par_y = self.find(y);
        self.parents[par_y] = par_x;
    }
}

// Read compressed JSONL and discard duplicates according to a UF parents array
// Re-assign doc id with a unique num reference given
// If duplicates is true, print only duplicates
pub fn filter_dups(filename: &String, num_docs: &mut usize, num_unique: &mut usize,
               parents: &Vec<usize>, regex_id: &Regex, duplicates: bool){
    let file = File::open(filename)
        .expect(format!("Error opening file '{filename}'").as_str());
    let decoder = Decoder::new(file)
        .expect(format!("Uncompressed or corrupted file '{filename}'").as_str());
    let reader = BufReader::new(decoder);

    for line_result in reader.lines() {
        let mut line = line_result.expect("Error reading line");

        // Discard every document that it is not its own parent
        // That way, we keep documents that do not have known duplicates
        // and one from each set of duplicates (the uppermost parent)
        if duplicates {
            if parents[*num_docs] != *num_docs {
                println!("{}", line);
            }
            *num_docs += 1;
            continue;
        } else if parents[*num_docs] != *num_docs {
            debug!("Discarding document {num_docs} in cluster {}", parents[*num_docs]);
            *num_docs += 1;
            continue;
        }

        // Re-assign new document id with regex, id always at the beggining, no need to parse the
        // whole json
        line = regex_id.replace(&line, format!("{{\"id\":{},", num_unique)).to_string();
        println!("{}", line);

        *num_docs += 1;
        *num_unique += 1;
    }

}

pub fn memory_usage() {
    let cmd_out = Command::new("sh")
        .arg("-c")
        .arg(format!("cat /proc/{}/status | grep -m 1 VmHWM | grep -o '[0-9]*'", id()))
        .output();
    if let Err(_) = cmd_out {
        warn!("Could not obtain memory usage");
    }else if let Ok(output) = cmd_out {
        let mem = from_utf8(&output.stdout)
            .expect("Error decoding command output")
            .strip_suffix("\n")
            .unwrap()
            .to_string()
            .parse::<u32>().unwrap() as f32 / 1e6;
        info!("Peak memory used: {:.2} GB", mem);
    }
}


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum Tokenization {
    Vectorizer,
    Whitespace,
    Char,
}

//impl Tokenization {
//    pub fn tokenize<'a>(&'a self, text: &'a str) -> Box<dyn Iterator<Item = &str> +'a> {
//        match &self {
//            Tokenization::Whitespace => { whitespace_split_boxed(&text) }
//            Tokenization::Char => { shingle_text_boxed(&text, 1) }
//        }
//    }
//}

pub struct MinHashProcessor {
    minhasher: MinHasher32<FnvBuildHasher>,
    tokenization: Tokenization,
    window_size: usize,
}

impl MinHashProcessor {
    pub fn new(permutations: usize, tokenization: Tokenization, window_size: usize)
            -> MinHashProcessor {
        Self {
            minhasher: MinHasher32::new(permutations),
            tokenization: tokenization,
            window_size: window_size,
        }
    }

    pub fn create_signature(&self, text: &str) -> Vec<u32> {
        match self.tokenization {
            Tokenization::Vectorizer => {
                // Emulate HashingVectorizer index
                let mut indices: Vec<i32> = Vec::with_capacity(100);
                for token in whitespace_split(&text.to_lowercase()) {
                    let hash = seahash::hash_seeded(token.as_bytes(), 1, 1000, 200, 89);
                    let hash = (hash % 1_048_576) as i32;
                    indices.push(hash);
                }
                self.minhasher.create_signature(indices.into_iter())
            }
            Tokenization::Whitespace => {
                self.minhasher.create_signature(
                    whitespace_split(&text.to_lowercase()))
            }
            Tokenization::Char => {
                self.minhasher.create_signature(
                    Shingles::new_with_step(text, self.window_size, self.window_size))
            }
        }
    }
}


// An alternate implementation of unix 'paste' command
// that reads and concatenates the lines of compressed files.
// Store opened file descriptors of compressed files,
// string buffer and specify field separator.
pub struct ZPaste<'a> {
    // quite a long generic specification, is there a simpler way?
    readers: Vec<Lines<BufReader<Decoder<'a, BufReader<File>>>>>,
    buf: String,
    separator: char,
}

impl<'a> ZPaste<'a> {
    pub fn new(names: Vec<String>) -> ZPaste<'a> {
        let mut readers:
            Vec<Lines<BufReader<Decoder<BufReader<File>>>>>
            = Vec::new();

        // Open each file and add to the vec its descriptor
        for n in names {
            let file = File::open(n).unwrap();
            let decoder = Decoder::new(file).unwrap();
            readers.push(BufReader::new(decoder).lines());
        }

        Self {
            readers: readers,
            buf: String::with_capacity(100),
            separator: '\t',
        }
    }
}

impl Iterator for ZPaste<'_> {
    type Item = String;

    // Get one line, concatenation of all the lines in the files
    fn next(&mut self) -> Option<String>{
        self.buf.clear();
        let mut readed = false;
        let size = self.readers.len();

        // Read one line of each file, concat all of them with separator
        for (i, reader) in self.readers.iter_mut().enumerate() {
            if let Some(line) = reader.next() {
                self.buf.push_str(&line.unwrap());
                readed = true;
            }
            if i != (size - 1) {
                // Insert separator also for empty lines
                self.buf.push(self.separator);
            }
        }
        if !readed {
            // If none of the readers provided data, stop
            return None;
        }

        // clone only allocates needed memory, not all String capacity, so we safe
        Some(self.buf.clone())
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_find() {
        let mut uf = UnionFind::new(6);
        uf.union(3,2);
        uf.union(4,2);

        assert_eq!(uf.parents, [0, 1, 3, 4, 4, 5]);
    }

    #[test]
    fn union_find_path_compression() {
        let mut uf = UnionFind::new(6);
        uf.union(3,2);
        uf.union(4,2);

        assert_eq!(uf.find(2), 4);
        assert_eq!(uf.parents, [0, 1, 4, 4, 4, 5]);
    }
}
