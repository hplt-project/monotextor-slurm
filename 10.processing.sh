#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

L=$1
INDEX=$2
COLL=$3
echo ${COLLECTIONS[$COLL]}/$L

if [ "$INDEX" == "all" ]; then
# List all the batches that need to be processed (size of the job array)
    INDEX=1-$(ls -1 $WORKSPACE/$COLL/$L/batch.*.zst | sed -E 's#.*/batch\.([0-9]+)\.zst#\1#' | sort -n | tail -1)
elif [ "$INDEX" == "failed" ]; then
# Select only failed jobs (timeout, oom and failed status)
# Create a list of batch id's separated by comma
    JOB=$3
    INDEX=$(
        sacct -j $JOB --parsable -s oom,f,to -n \
        | grep -v '.batch' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
fi

# Run job array of processing
jobid=$(
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=$INDEX \
    -J $L-$COLL-mono-processing \
    --parsable 10.processing $L $COLL
)
echo Submitted $jobid

# Submit job array of jsonl conversion
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch --array=$INDEX -d afterok:$jobid \
    -J $L-$COLL-tsv2jsonl 10.tsv2jsonl $L $COLL
