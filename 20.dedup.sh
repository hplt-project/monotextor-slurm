#!/bin/bash
# Submit dedup jobs

source .env
source .checks
set -euo pipefail

L=$1
COLL=$2
mkdir -p $SLURM_LOGS_DIR
echo ${COLLECTIONS[$COLL]}/$L

INDEX_ID=$(\
    SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
    sbatch -J $L-$COLL-index --parsable 20.index $L $COLL)
echo "Submitted batch job $INDEX_ID"

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-$COLL-dedup -d afterok:$INDEX_ID 20.dedup $L $COLL
