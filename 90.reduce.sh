#!/bin/bash
# Reduce the map procedure
# merging all the batches and converting to the final jsonl format
source .env
source .checks
set -euo pipefail

L=$1

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-reduce 90.reduce $L
