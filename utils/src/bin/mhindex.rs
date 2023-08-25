use std::time::Instant;
use gaoya::minhash::calculate_minhash_params;
use serde_json::Result;
use clap::Parser;
use env_logger::Env;
use log::info;

use monotextor_utils::Tokenization;
use monotextor_utils::indexer::Indexer;


#[derive(Parser)]
#[clap(version, about="Index a set of documents in JSONL format. \
                       Then return the queries of each document agains all the index.")]
struct Args{
    #[clap(long, default_value_t=20000,
           help="Number of lines to be processed at a time")]
    batch_size: usize,
    #[clap(long, short, default_value_t=-1,
           help="Band to be indexed. Values from 0 to band_size-1. If none specified, index all.")]
    band_id: isize,
    #[clap(arg_enum, long, short, default_value="whitespace",
           help="Tokenization type.")]
    tokenizer: Tokenization,
    #[clap(short, long, default_value_t=3,
           help="Size of the non-overlapping window for character tokenization.")]
    window_size: usize,

    #[clap(long, default_value_t=1000,
        help="Documents with higher number of duplicates than this amount \
             will be marked to be directly discarded. \
             Not even keeping one of the group as representative.")]
    num_duplicates_threshold: usize,
    #[clap(long, short, default_value_t=0.8, help="Jaccard similarity threshold.")]
    jaccard_threshold: f64,
    #[clap(long, short, default_value_t=260,
           help="Number of permutations, a.k.a number of hashes.")]
    permutations: usize,
    //#[clap(long, required=false, help="Number of bands. If provided, permutations will be ignored.")]
    //num_bands: usize,
    //#[clap(long, required=false, help="Band width. If provided, permutations will be ignored.")]
    //band_width: usize,

    #[clap(long, short, required=false, takes_value=false,
           help="Print MinHash parameters and finish.")]
    dry_run: bool,

    #[clap(help="zstd compressed jsonl files to be indexed.")]
    files: Vec<String>,
}


fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let now = Instant::now();

    // Create MinHash index and hasher objects
    let (num_bands, band_width) = calculate_minhash_params(
        args.jaccard_threshold, args.permutations
    );
    let mut indexer = Indexer::new(num_bands, band_width, args.tokenizer, args.window_size,
                                   args.jaccard_threshold, args.band_id, args.batch_size,
                                   args.num_duplicates_threshold);
    info!("Num permutations: {}", num_bands*band_width);
    info!("Num bands: {}", num_bands);
    info!("Band width: {}", band_width);
    info!("Indexed band num: {}", args.band_id);
    if args.dry_run {
        // With dry run, print params and exit
        info!("Finished");
        return Ok(())
    }

    info!("Indexing documents");
    // Read, deserialize, hash and index each file
    let mut global_id = 0; // document id
    for file in &args.files {
        indexer.index_file(file, &mut global_id, false);
    }
    info!("Indexed {} documents", global_id);

    info!("Querying documents");
    // start reading again, this time we query each document
    println!("{}", global_id);
    let mut global_id = 0;
    for file in &args.files {
        indexer.index_file(file, &mut global_id, true);
    }
    info!("Queried {} documents", global_id);
    drop(indexer);

    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    Ok(())
}
