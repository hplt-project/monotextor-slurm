#!/bin/bash
set -euo pipefail
source .env
source .checks

L=$1

ARRAY_SIZE=$(ls -1 $WORKSPACE/$L/batch.*.zst | sed -E 's#.*/batch\.([0-9]+)\.zst#\1#' | sort -n | tail -1)

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=1-$ARRAY_SIZE \
    -J $L-mono-processing \
    --parsable \
    10.processing $L
