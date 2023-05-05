#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

L=$1
INDEX=$2

if [ "$INDEX" == "all" ]; then
# List all the batches that need to be processed (size of the job array)
    INDEX=1-$(ls -1 $WORKSPACE/$L/batch.*.zst | sed -E 's#.*/batch\.([0-9]+)\.zst#\1#' | sort -n | tail -1)
elif [ "$INDEX" == "failed" ]; then
# Select only failed jobs (timeout, oom and failed status)
    JOB=$3
    INDEX=$(
        sacct -j $JOB --parsable -s oom,f,to -n \
        | grep -v '.batch' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
fi

# Run job array
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=$INDEX \
    -J $L-mono-processing \
    --parsable 10.processing $L
