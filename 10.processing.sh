#!/bin/bash
source .env
source .checks
set -euo pipefail

L=$1
INDEX=$2

if [ "$INDEX" == "all" ]; then
    INDEX=1-$(ls -1 $WORKSPACE/$L/batch.*.zst | sed -E 's#.*/batch\.([0-9]+)\.zst#\1#' | sort -n | tail -1)
elif [ "$INDEX" == "failed" ]; then
    JOB=$3
    INDEX=$(
        sacct -j $JOB --parsable -s oom,f,to -n \
        | grep -v '.batch' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
fi

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=$INDEX \
    -J $L-mono-processing \
    --parsable 10.processing $L
