#!/bin/bash
# Submit dedup jobs

source .env
source .checks
set -euo pipefail

L=$1
#COLL=$2
mkdir -p $SLURM_LOGS_DIR
for i in ${!COLLECTIONS[@]}; do
    echo ${COLLECTIONS[$i]}/$L
done
echo "Deduping ${!COLLECTIONS[@]}"

if [ -s $WORKSPACE/dedup/$L/clusters.zst ] || [ -s $WORKSPACE/dedup/$L/clusters.17.zst ]; then
    echo "Cluster file already exists, submitting dedup only"
    confirm
    SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
    sbatch -J $L-dedup 20.dedup $L
    exit 0
fi

# Distributed minhash index
if [[ $# -eq 2 ]] && [[ $2 =~ "dist" ]]; then
    bands=$(mhindex -d 2>&1 | grep 'Num bands' | perl -pe 's/.*Num bands: (\d+)/$1/')
    echo Submitting array job of 1-$bands jobs
    confirm

    INDEX_ID=$(\
        SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
        sbatch -J $L-index --array=1-$bands \
        --parsable 20.index $L)
else
    echo Submitting job
    confirm

    INDEX_ID=$(\
        SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
        sbatch -J $L-index \
        --parsable 20.index $L)
fi

echo "Submitted batch job $INDEX_ID"

SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
sbatch -J $L-dedup -d afterok:$INDEX_ID 20.dedup $L
