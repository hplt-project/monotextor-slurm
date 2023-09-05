#!/bin/bash
# Submit dedup jobs

source .env
source .checks
set -euo pipefail

L=$1
COLL=$2
mkdir -p $SLURM_LOGS_DIR
echo ${COLLECTIONS[$COLL]}/$L

# Distributed minhash index
if [[ $# -eq 3 ]] && [[ $3 =~ "dist" ]]; then
    bands=$(mhindex -d 2>&1 | grep 'Num bands' | perl -pe 's/.*Num bands: (\d+)/$1/')
    echo Submitting array job of 1-$bands jobs
    read -p "Confirm? [y/n] " -n 1 -r
    if [[ ! $REPLY =~ [Yy] ]]; then echo; exit 1; fi
    echo

    INDEX_ID=$(\
        SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
        sbatch -J $L-$COLL-index --array=1-$bands \
        --parsable 20.index $L $COLL)
else
    echo Submitting job
    read -p "Confirm? [y/n] " -n 1 -r
    if [[ ! $REPLY =~ [Yy] ]]; then echo; exit 1; fi
    echo

    INDEX_ID=$(\
        SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
        sbatch -J $L-$COLL-index \
        --parsable 20.index $L $COLL)
fi

echo "Submitted batch job $INDEX_ID"

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-$COLL-dedup -d afterok:$INDEX_ID 20.dedup $L $COLL
