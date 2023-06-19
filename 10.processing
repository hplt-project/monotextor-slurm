#!/bin/bash
#SBATCH --job-name=processing
#SBATCH --partition="standard"
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=128
#SBATCH --mem=0
#SBATCH --output=logs/%x-%A_%a.out
module load cray-python/3.9.12.1
module load parallel
source .env
set -euo pipefail

L=$1
COLL=$2
INPUT=$WORKSPACE/$COLL/$L/batch.$SLURM_ARRAY_TASK_ID.zst
OUTPUT=$WORKSPACE/$COLL/$L/scored.$SLURM_ARRAY_TASK_ID.zst
MODEL=$MONOCLEANER_MODELS/$L

processing(){
    L=$1
    MODEL=$2
    set -euo pipefail

    monofixer --scol 2 \
        --ignore_normalization --ignore_segmentation \
        --ignore_empty --ignore_duplicates \
        --quiet - - $L \
    | monocleaner --scol 2 \
        --add_lang_ident --detect_script \
        --disable_hardrules --quiet \
        $MODEL - - \
    || {
        echo "Error in pipeline: ${PIPESTATUS[@]}" >&2
        exit 1
    }
}
export -f processing

zstdcat $INPUT \
| parallel --pipe -k \
    --halt now,fail=1 \
    -j180 --block 10M processing $L $MODEL \
| zstdmt -T64 -10 \
> $OUTPUT.tmp_$SLURM_ARRAY_JOB_ID \


mv $OUTPUT.tmp_$SLURM_ARRAY_JOB_ID $OUTPUT
