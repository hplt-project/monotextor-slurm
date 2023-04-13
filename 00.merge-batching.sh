#!/bin/bash
set -euo pipefail
source .env
source .checks

L=$1
COLLECTIONS=${@:2}
mkdir -p $SBATCH_OUTPUT

SBATCH_OUTPUT="$SBATCH_OUTPUT/%x.out" \
sbatch -J merge-batching-$L 00.merge-batching $L $COLLECTIONS
