use serde::{Deserialize, Serialize};

pub mod minhash_processor;
pub mod indexer;
pub mod utils;
pub mod dedup;

#[derive(Deserialize, Serialize)]
pub struct DocumentText {
    // Parse documents ignoring all fields but "text"
    pub text: String,
}
