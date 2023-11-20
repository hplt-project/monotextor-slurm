#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail
MAX_JOBS=120
NUM_JOBS=60
TIME_RETRY=10m

L=$1

if [ $# -lt 2 ] || [ "$2" == "all" ]; then
# List all the batches that need to be processed (size of the job array)
    INDEX=1-$(ls -1 $WORKSPACE/dedup/$L/${L}_*.zst | sed -E 's#.*/\w{2,3}_([0-9]+)\.jsonl\.zst#\1#' | sort -n | tail -1)
elif [ $# -gt 2 ] && [ "$INDEX" == "failed" ]; then
# Select only failed jobs (timeout, oom and failed status)
# Create a list of batch id's separated by comma
    JOB=$3
    INDEX=$(
        sacct -j $JOB --parsable -s oom,f,to -n \
        | grep -v '.clean' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
else
    INDEX=$2
fi

echo "Job array of index $INDEX"
read -p "Confirm? [y/n] " -n 1 -r
if [[ ! $REPLY =~ [Yy] ]]; then echo; exit 1; fi
echo

JOB_ID=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=$INDEX \
    -J $L-clean --parsable \
    30.clean $L)
echo Submitted batch job $JOB_ID

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-stats \
    -d afterok:$JOB_ID \
    30.stats $L
