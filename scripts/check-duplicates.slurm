#!/bin/bash
#SBATCH --job-name=findups-fix
#SBATCH --partition="small"
#SBATCH --time=72:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1750
#SBATCH --output=logs/%x.out
module load cray-python/3.9.12.1
source .env
set -euo pipefail
module load parallel

L=$1
dir=$2

zstdcat $WORKSPACE/$dir/$L/${L}_$SLURM_ARRAY_TASK_ID.jsonl.zst \
| parallel --pipe -j4 jq ".text" \
| python scripts/exactdups.py
