#!/bin/bash
# Submit merge-batching jobs
# - merge metadata text and lang json
# - split by language folder

source .env
source .checks
set -euo pipefail

COLL=$1
mkdir -p $SLURM_LOGS_DIR
echo ${COLLECTIONS[$COLL]}

jobid=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $COLL-merge-text-meta --parsable 01.merge-text-meta $COLL)
echo Submitted batch job $jobid

jobid=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $COLL-split-lang \
    --parsable -d afterok:$jobid \
    02.split-lang $COLL)
echo Submitted batch job $jobid
