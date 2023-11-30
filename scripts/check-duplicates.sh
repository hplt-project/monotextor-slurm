#!/bin/bash
set -euo pipefail
source .env
source .checks

L=$1
dir=$2

INDEX=1-$(ls -1 $WORKSPACE/$dir/$L/${L}_*.zst | sed -E 's#.*/\w{2,3}_([0-9]+)\.jsonl\.zst#\1#' | sort -n | tail -1)
sbatch -J $L-findups-$dir --array=$INDEX scripts/check-duplicates.slurm $L $dir
