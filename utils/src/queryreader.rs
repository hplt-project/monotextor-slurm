use std::io::{BufRead, BufReader, Lines};
use std::collections::HashSet;
use std::fs::File;
use zstd::stream::read::Decoder;
use log::debug;

use crate::UnionFind;

// Struct that reads query files as
// an alternate implementation of unix 'paste' command,
// reading and concatenating the lines of compressed files.
// Store opened file descriptors of compressed files,
// string buffer and specify field separator.
pub struct QueryReader<'a> {
    // quite a long generic specification, is there a simpler way?
    readers: Vec<Lines<BufReader<Decoder<'a, BufReader<File>>>>>,
    readed_num: usize,
    buf: String,
    separator: char,
    uniq: HashSet<usize>,
    uf: UnionFind,
}

impl<'a> QueryReader<'a> {
    pub fn new(names: Vec<String>) -> QueryReader<'a> {
        let mut readers:
            Vec<Lines<BufReader<Decoder<BufReader<File>>>>>
            = Vec::new();

        let mut header = String::with_capacity(20);
        // Open each file and add to the vec its descriptor
        // read each header, keep only last (they should all be the same)
        for n in names {
            let file = File::open(n).unwrap();
            let decoder = Decoder::new(file).unwrap();
            let mut reader = BufReader::new(decoder);
            header.clear();
            reader.read_line(&mut header).unwrap();
            readers.push(reader.lines());
        }

        // Parse header containing the number of records
        header.pop();
        let num_records: usize = header.parse().expect(format!("Could not parse {}", header).as_str());

        Self {
            readers: readers,
            readed_num: 0,
            buf: String::with_capacity(100),
            separator: '\t',
            uniq: HashSet::with_capacity(999),
            uf: UnionFind::new(num_records),
        }
    }

    // Parse a query from a file
    // doing a function instead of method(self)
    // because read_line is doing mutable borrow of self in the loop
    fn read_query(uf: &mut UnionFind, uniq: &mut  HashSet<usize>,
                  line: String, readed_num: usize) -> bool{
        // parse the line and add doc ids to the set
        //TODO is the collect needed? we could just iterate over split
        let parts: Vec<&str> = line.split(&[' ', '\t']).collect();
        for p in parts {
            // DISCARD lines, workaround for very repeated duplicates (aka very long queries)
            // in that case, just set any other doc as parent
            // given that they won't be their own parents, they will be discarded
            if p.starts_with("DISCARD") {
                uf.union(0, readed_num);
                return true;
            }
            let id: usize = p.parse().expect(
                format!("Could not parse '{}' in line {}:", p, readed_num).as_str());
            uniq.insert(id);
        }
        debug!("Readed line {} query: {:?}", readed_num, uniq);

        false
    }

    // Read one line of each file and parse the queries
    // return false if reached EOF for all files
    fn read_line(&mut self) -> bool {
        self.buf.clear();
        self.uniq.clear();
        let mut readed = false;
        let mut discarded = false;

        // Read one line of each file, each line being a query
        for reader in self.readers.iter_mut() {
            if discarded {
                // if current doc is tagged as discarded in any of the query files
                // don't insert anything else, just keep reading the files to avoid offsets
                continue
            }
            if let Some(line_result) = reader.next() {
                // cannot borrow self.readers twice, so invoking as function with all the params
                discarded = Self::read_query(&mut self.uf, &mut self.uniq,
                                             line_result.unwrap(), self.readed_num);
                readed = true;
            }
        }
        if !readed {
            // If none of the readers provided data, stop
            return false;
        }

        true
    }

    // Read all the queries and destroy the object, closing all files and returning parents vector
    pub fn read_all(mut self) -> UnionFind {
        while self.read_line() {
            // union each doc id in the query to the current doc (line number)
            // readed_num = current doc id
            for j in &self.uniq {
                if self.readed_num == *j {
                    continue;
                }
                self.uf.union(self.readed_num, *j);
            }
            self.readed_num += 1;
        }

        self.uf
    }
}

impl Iterator for QueryReader<'_> {
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
