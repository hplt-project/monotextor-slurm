use serde::{Deserialize, Serialize};

pub mod dedup;
pub mod indexer;
pub mod minhash_processor;
pub mod split;
pub mod utils;

#[derive(Deserialize, Serialize)]
pub struct DocumentText {
    // Parse documents ignoring all fields but "text"
    pub text: String,
}
