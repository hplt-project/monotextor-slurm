use std::time::Instant;
use clap::Parser;
use env_logger::Env;
use log::{debug, info};

use monotextor_utils::dedup::DedupFilter;
use monotextor_utils::utils::memory_usage;

#[derive(Parser)]
#[clap(version, about="Deduplicate a set of JSONL documents using clusters array. \
                       Non-duplicates and one document per duplicates cluster will be kept. \
                       Document id's of kept documents will be re-assigned.")]
struct Args{
    #[clap(short, long, required=false, takes_value=false,
           help="Print discarded duplicates, instead of non-discarded.")]
    print_duplicates: bool,
    #[clap(short = 'c', long, required=false, takes_value=false,
           help="Add the size of the cluster to each document metadata")]
    add_cluster_size: bool,

    #[clap(help="File containg the clusters array/s of duplicates.")]
    clusterfile: String,
    #[clap(help="zstd compressed jsonl files to be filtered.")]
    files: Vec<String>,
}



fn main(){
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let now = Instant::now();
    let args = Args::parse();

    info!("Reading clusterfile");
    let mut deduper = DedupFilter::new(
        args.clusterfile,
        args.print_duplicates,
        args.add_cluster_size);
    debug!("Parents array: {:?}", deduper.uf.parents);

    info!("Reading documents and discarding duplicates");
    for f in &args.files {
        deduper.filter_dups(f);
    }
    let pct = (deduper.num_unique as f32 / deduper.num_docs as f32) * 100.0;
    info!("Duplicates discarded, {} documents kept ({:.2} %)", deduper.num_unique, pct);

    memory_usage();
    info!("Elapsed time: {:.2} s", now.elapsed().as_secs_f32());
    info!("Finished");
    if deduper.num_read_docs != deduper.num_docs {
        panic!("Number of read docs is different than in cluster file: {} vs {}",
               deduper.num_read_docs, deduper.num_docs);
    }
}
