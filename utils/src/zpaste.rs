use std::io::{BufRead, BufReader, Lines};
use std::fs::File;
use zstd::stream::read::Decoder;

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
