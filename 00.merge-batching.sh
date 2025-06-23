#!/bin/bash
# Submit merge-batching jobs
# - merge metadata text and lang json
# - split by language folder

source .env
source .checks
set -euo pipefail

COLL=$1
mkdir -p $SLURM_LOGS_DIR
echo ${COLLECTIONS[$COLL]} | tr ' ' '\n'
batches=$(find -L ${COLLECTIONS[$COLL]}* -maxdepth 1 -mindepth 1 -type d)
count=$(echo $batches | wc -w)
echo Num batches $count

case $COLL in
    wide* | survey3 | archivebot)
        mem=7200
    ;;
    *)
        mem=1750
esac

jobid=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.log" \
sbatch -J merge-text-meta-$COLL --mem-per-cpu $mem --parsable 01.merge-text-meta $COLL)
echo Submitted batch job $jobid

jobid=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.log" \
sbatch -J split-lang-$COLL \
    --parsable -d afterok:$jobid \
    02.split-lang $COLL)
echo Submitted batch job $jobid
