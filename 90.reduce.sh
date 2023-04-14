#!/bin/bash
set -euo pipefail
source .env

L=$1

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-reduce 90.reduce $L
