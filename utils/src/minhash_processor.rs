use clap::ValueEnum;
use fnv::FnvBuildHasher;
use gaoya::minhash::{MinHasher, MinHasher32};
use gaoya::text::whitespace_split;
use seahash;
use shingles::Shingles;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
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
    pub fn new(
        permutations: usize,
        tokenization: Tokenization,
        window_size: usize,
    ) -> MinHashProcessor {
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
            Tokenization::Whitespace => self
                .minhasher
                .create_signature(whitespace_split(&text.to_lowercase())),
            Tokenization::Char => self.minhasher.create_signature(Shingles::new_with_step(
                text,
                self.window_size,
                self.window_size,
            )),
        }
    }
}
