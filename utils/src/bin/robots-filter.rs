/* Small program that uses an FST index of URLs to query from robots.txt patterns
 * The robots.txt patterns are expected in tab separated format, where each line
 * is a (Dis)allow entry in a robots.txt file. The first field must be the pattern
 * (domain + path) and the second field must be a 0 or 1, whether if it's disallowed or not.
 * All the entries of a robots.txt must appear contiguous.
 *
 * This assumes the user has taken care of a previous parsing of the robots.txt
 * dumping the entries that considers relevant (entries that match a certain set of
 * user-agents, for example) to the input file of this program.
 */
use std::io::{self,BufRead};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, RwLock};
use std::collections::HashSet;
use std::fs;

use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle, ProgressFinish};
use fst::{Set, IntoStreamer, Streamer};
use patricia_tree::StringPatriciaSet;
use regex_automata::dense;
use rayon::prelude::*;
use env_logger::Env;
use regex::Regex;
use clap::Parser;
use log::{debug, warn};


#[derive(Parser)]
#[command(version, about="Generate a list of disallowed URLs by robots.txt")]
struct Args {
    #[arg(help="FST indexed URLs to search in")]
    indexpath: String,
    #[arg(help="List of allowance URL patterns from robots.txt")]
    allowancefiles: Vec<String>,
}

struct SharedState {
    index: Arc<Set<Vec<u8>>>,
    sender: Sender<String>,
    banned: Arc<RwLock<HashSet<String>>>,
}

impl Clone for SharedState {
    fn clone(&self) -> Self {
        SharedState {
            index: self.index.clone(),
            sender: self.sender.clone(), // Clone the sender for thread safety
            banned: self.banned.clone(),
        }
    }
}

// escape metasequences characters like regex_syntax::escape
// except *, which is converted to .*
// and anchors are removed
pub fn is_meta_character(c: char) -> bool {
    match c {
        '\\' | '.' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{'
        | '}' | '#' | '&' | '-' | '~' => true,
        _ => false,
    }
}
pub fn escape(text: &str) -> String {
    let mut buf = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '^' || c == '$' {
            continue
        } else if c == '*' {
            buf.push('.');
        } else if is_meta_character(c) {
            buf.push('\\');
        }
        buf.push(c);
    }
    buf
}


fn process_file(state: &mut SharedState, filepath: &str) -> Result<(), Box<dyn std::error::Error + 'static>> {
    // check if $ anchor at the end
    let anchor_end = Regex::new(r"[\$\*]$")?;
    // extract domain from url
    let domain_re = Regex::new(r"^(https?://)?(www\.)?([^/]+)?.*$")?;

    // Open iterator over the lines of the dissallowed list
    let file = fs::File::open(filepath)?;
    let lines = io::BufReader::new(file).lines();

    // asumes URLs coming grouped by each robots file
    // so each group should be the same domain
    // wee keep the domain name and the disallowed urls for that domain
    let mut cur_domain = String::with_capacity(500);
    let mut cur_allowance: StringPatriciaSet = StringPatriciaSet::new();
    let mut warned_size = false;

    for (i, line_result) in lines.enumerate() {
        // parse tab serparted lines
        let line = line_result?;
        let parts: Vec<_> = line.split('\t').collect();
        let url = parts[0];
        let allowed = match parts[1] {
            "1" => true,
            "0" => false,
            _ => panic!("{}", format!("Could not parse bool in line {}", i+1).as_str()),
        };

        // Extract domain, check if has changed
        // that means new robots.txt file
        let domain = domain_re
            .captures(&url)
            .expect(format!("Failed parsing domain from url: {line}").as_str())
            .get(3).ok_or("Could not obtain captured group 3")?.as_str();
        if cur_domain != domain {
            // New domain, print the final list of disallowed
            cur_domain.clear();
            cur_domain.push_str(domain);
            debug!("New domain! {cur_domain}");
            for u in cur_allowance.iter() {
                state.sender.send(format!("{}", u))?;
            }
            cur_allowance = StringPatriciaSet::new();
            warned_size = false;
        }
        if state.banned.read().unwrap().contains(&cur_domain) {
            continue;
        }

        // Escape regex characters that are not supported
        let escaped = escape(&url);

        // If the url had $ at the end, do not add the .* suffix
        let query;
        if !anchor_end.is_match(&url) {
            query = format!("{escaped}.*");
        } else {
            query = escaped.to_string();
        }
        debug!("Escaped url: {query}");

        // Build searcher
        let dense_dfa = dense::Builder::new()
            .anchored(true)
            .minimize(false)
            .byte_classes(true)
            .premultiply(true)
            .reverse(false)
            .build(&query).expect(format!("Escaped '{}' url '{}'", query, url).as_str());
        let dfa = match dense_dfa {
            dense::DenseDFA::PremultipliedByteClass(dfa) => dfa,
            _ => unreachable!(),
        };
        // search over the FST
        let mut results = state.index.search(dfa).into_stream();
        let mut num_results = 0;
        // iterate over the results
        // for those allowed, remove the url from the set in case was added by a previous query
        // for those disallowed, add them to the set for the current domain
        while let Some(key) = results.next() {
            num_results += 1;
            if allowed {
                cur_allowance.remove(&String::from_utf8(key.to_vec())?);
            } else {
                // if the allowance list becomes too large, stop adding urls to it
                if cur_allowance.len() > 500_000 {
                    state.sender.send(format!("{}", String::from_utf8(key.to_vec())?))?;
                    if !warned_size {
                        warn!("The in-memory list reached size limit for query '{}' in file '{}'", escaped, filepath);
                        warned_size = true;
                    }
                } else {
                    cur_allowance.insert(String::from_utf8(key.to_vec())?);
                }
            }
        }
        if num_results > 5_000_000 {
            warn!("Very big query results '{}' with '{}'", num_results, url);
            state.banned.write().unwrap().insert(cur_domain.clone());
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let progstyle = ProgressStyle::default_bar();
    let progbar = ProgressBar::new(args.allowancefiles.len() as u64)
        .with_style(progstyle)
        .with_finish(ProgressFinish::AndLeave);

    // let mmap = unsafe { Mmap::map(&fs::File::open(args.indexpath)?)? };
    let index = Arc::new(Set::new(fs::read(args.indexpath)?)?);

    let (sender, receiver) = channel();
    let shared_state = SharedState {
        index: index.clone(),
        sender: sender,
        banned: Arc::new(RwLock::new(HashSet::new())),
    };

    // process each file in parallel, sharing the same index
    let handler = std::thread::spawn(move || {
        args.allowancefiles
            .par_iter()
            .progress_with(progbar)
            .for_each_with(shared_state.clone(), |state, filename| {
                process_file(state, &filename).unwrap();
            });

        // explicitly drop the shared state, so we make sure there are no remaining senders
        // after the par_iter finishes and therefore the receiver does not deadblock
        drop(shared_state);
    });

    while let Ok(line) = receiver.recv() {
        println!("{}", line);
    }
    handler.join().unwrap();

    Ok(())
}
