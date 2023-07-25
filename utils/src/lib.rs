use serde::{Deserialize, Serialize};

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
