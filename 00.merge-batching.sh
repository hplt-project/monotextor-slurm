#!/bin/bash
# Submit merge-batching jobs
# - merge warc2text batches
# - decode base64 text
# - re-batching with one paragraph per line
# to a size appropiate for processing in this pipeline

source .env
source .checks
set -euo pipefail

L=$1
COLL=$2
mkdir -p $SLURM_LOGS_DIR
echo ${COLLECTIONS[$COLL]}/$L

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-$COLL-merge-batching --parsable 00.merge-batching $L $COLL
