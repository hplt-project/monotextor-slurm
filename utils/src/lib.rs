use serde::{Deserialize, Serialize};

pub mod minhash_processor;
pub mod indexer;
pub mod zpaste;
pub mod utils;
pub mod dedup;

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
