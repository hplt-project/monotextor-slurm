#!/bin/bash
source .env
source .checks
set -euo pipefail

L=$1
COLLECTIONS=${@:2}
mkdir -p $SLURM_LOGS_DIR

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-merge-batching --parsable 00.merge-batching $L $COLLECTIONS
