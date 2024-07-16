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
use std::collections::HashSet;
use std::io::{self,BufRead};
use std::fs;

use fst::{Set, IntoStreamer, Streamer};
use regex_automata::dense;
use env_logger::Env;
use memmap2::Mmap;
use regex::Regex;
use clap::Parser;
use log::debug;


#[derive(Parser)]
#[clap(version, about="Generate a list of disallowed URLs by robots.txt")]
struct Args {
    #[clap(help="FST indexed URLs to search in")]
    indexpath: String,
    #[clap(help="List of allowance URL patterns from robots.txt")]
    allowancepath: String,
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    // Regex for escaping sequences in the allowance patterns
    // robots.txt only supports * and $
    // so the rest of the symbols have to be interpreted as literals
    let escaper = Regex::new(r"[\-\.\+\?\(\)\[\]\{\}\\]")?;
    // converts '*' to '.*'
    let wildcard_convert = Regex::new(r"\*")?;
    // removes unsupported anchors by FST
    let remove_anchors = Regex::new(r"[\^\$]")?;
    // check if $ anchor at the end
    let anchor_end = Regex::new(r"\$$")?;
    // extract domain from url
    let domain_re = Regex::new(r"^(https?://)?(www\.)?([^/]+)?.*$")?;

    let mmap = unsafe { Mmap::map(&fs::File::open(args.indexpath)?)? };
    let index = Set::new(mmap)?;

    // Open iterator over the lines of the dissallowed list
    let file = fs::File::open(args.allowancepath)?;
    let lines = io::BufReader::new(file).lines();

    // asumes URLs coming grouped by each robots file
    // so each group should be the same domain
    // wee keep the domain name and the disallowed urls for that domain
    let mut cur_domain = String::with_capacity(500);
    let mut cur_allowance: HashSet<String> = HashSet::with_capacity(200);

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
            for u in cur_allowance.drain() {
                println!("{}", u);
            }
        }

        // Escape regex characters that are not supported
        let escaped = escaper.replace_all(&url, r"\$0");
        let escaped_wildcard = wildcard_convert.replace_all(&escaped, r".*");
        let removed_anchors = remove_anchors.replace_all(&escaped_wildcard, "");

        // If the url had $ at the end, do not add the .* suffix
        let query;
        if !anchor_end.is_match(&url) {
            query = format!("{removed_anchors}.*");
        } else {
            query = removed_anchors.to_string();
        }
        debug!("Escaped url: {query}");

        // Build searcher
        let dense_dfa = dense::Builder::new()
            .anchored(true)
            .minimize(false)
            .byte_classes(true)
            .premultiply(true)
            .reverse(false)
            .build(&query)?;
        let dfa = match dense_dfa {
            dense::DenseDFA::PremultipliedByteClass(dfa) => dfa,
            _ => unreachable!(),
        };
        // search over the FST
        let mut results = index.search(dfa).into_stream();
        // iterate over the results
        // for those allowed, remove the url from the set in case was added by a previous query
        // for those disallowed, add them to the set for the current domain
        while let Some(key) = results.next() {
            if allowed {
                cur_allowance.remove(&String::from_utf8(key.to_vec())?);
            } else {
                cur_allowance.insert(String::from_utf8(key.to_vec())?);
            }
        }
    }
    Ok(())
}
