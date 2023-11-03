use serde::{Deserialize, Serialize};
use regex::Regex;

pub mod minhash_processor;
pub mod indexer;
pub mod zpaste;
pub mod utils;
pub mod dedup;

pub trait TextField {
    fn get_text(&self) -> String;
}

// Function called by serde
// needed to skip fields that do not satisfy trait bounds
fn default() -> Regex { Regex::new("").unwrap() }

#[derive(Serialize, Deserialize)]
pub struct Document {
    id: u64,
    document_lang: String,
    scores: Vec<f32>,
    langs: Vec<String>,
    text: String,
    url: String,
    collection: String,
    correct_lang_ratio: f32,
    average_num_word: f32,
    average_num_char: f32,
    average_score: f32,

    #[serde(skip, default = "default")]
    count_word: Regex,
}

impl Document {
    pub fn new(lang: String) -> Self {
        Self {
            // Create with capacity is a little bit faster
            // if we assume always working with large inputs
            scores: Vec::with_capacity(500),
            langs: Vec::with_capacity(500),
            text: String::with_capacity(500000),
            url: String::new(),
            collection: String::new(),
            document_lang: lang,
            id: 0,
            correct_lang_ratio: -1.0,
            average_score: -1.0,
            average_num_word: -1.0,
            average_num_char: -1.0,

            count_word: Regex::new(r"\s+|$").expect("Error creating regex"),
        }
    }

    // Clear the content of the document to read new document
    // increment id
    pub fn clear(&mut self) {
        self.id += 1;
        self.scores.clear();
        self.langs.clear();
        self.text.clear();
        self.url.clear();
        self.collection.clear();
        self.correct_lang_ratio = -1.0;
        self.average_score = -1.0;
        self.average_num_word = -1.0;
        self.average_num_char = -1.0;
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    // Add a tab separated line to the document
    // Concatenate the text and metadata fields
    // Update stats
    pub fn add_line(&mut self, parts: Vec<&str>) -> Result<(), String>{
        // Concatenate paragraphs with endline separators
        // to reconstruct the documents
        if !self.text.is_empty() {
            self.text.push_str("\n");
        }
        self.text.push_str(parts[1]);
        // insert url and collection only once per doc
        if self.url.is_empty() {
            self.url = parts[0].to_string();
        }
        if self.collection.is_empty() {
            self.collection = parts[2].to_string();
        }
        self.langs.push(parts[3].to_string());
        // parse scores to float
        let score_result = parts[4].parse::<f32>();
        match score_result {
            Ok(score) => self.scores.push(score),
            Err(_) => return Err(format!("Error parsing '{}' to float", parts[4]).to_string())
        }

        Ok(())
    }

    pub fn stats(&mut self) {
        let num_corr_langs = self.langs.iter()
            .fold(0, |acc, e| if e.eq(&self.document_lang) { acc + 1 } else { acc });
        self.correct_lang_ratio = num_corr_langs as f32 / self.langs.len() as f32;

        let num_word = self.count_word.find_iter(&self.text).count();
        self.average_num_word = num_word as f32 / self.langs.len() as f32;
        let num_char = self.text.chars().count();
        self.average_num_char = num_char as f32 / self.langs.len() as f32;
        self.average_score = self.scores.iter().sum::<f32>() / self.scores.len() as f32;
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
