#!/bin/bash
source .env
source .checks
set -euo pipefail

L=$1

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-reduce 90.reduce $L
