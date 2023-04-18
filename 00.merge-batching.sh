#!/bin/bash
set -euo pipefail
source .env
source .checks

L=$1
COLLECTIONS=${@:2}
mkdir -p $SLURM_LOGS_DIR

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-merge-batching --parsable 00.merge-batching $L $COLLECTIONS
