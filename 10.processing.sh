#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail
MAX_JOBS=120
NUM_JOBS=60
TIME_RETRY=10m

L=$1
INDEX=$2
COLL=$3
echo ${COLLECTIONS[$COLL]}/$L
if [[ $# -eq 4 ]] && [[ $4 =~ "tsv2json" ]]; then
    ONLY_TSV2JSON="true"
else
    ONLY_TSV2JSON=""
fi

if [ "$INDEX" == "all" ]; then
# List all the batches that need to be processed (size of the job array)
    INDEX=1-$(ls -1 $WORKSPACE/$COLL/$L/batch.*.zst | sed -E 's#.*/batch\.([0-9]+)\.zst#\1#' | sort -n | tail -1)
elif [ "$INDEX" == "failed" ]; then
# Select only failed jobs (timeout, oom and failed status)
# Create a list of batch id's separated by comma
    JOB=$3
    INDEX=$(
        sacct -j $JOB --parsable -s oom,f,to -n \
        | grep -v '.batch' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
fi

IS_RANGE=false
if [[ $INDEX =~ ^[0-9]+-[0-9]+$ ]]; then
    IS_RANGE=true
    MAX_ID=$(echo $INDEX | cut -d'-' -f2)
    MIN_ID=$(echo $INDEX | cut -d'-' -f1)
    INDEX_SIZE=$((MAX_ID - MIN_ID))
fi

echo "Job array of size $INDEX"
#read -p "Confirm? [y/n] " -n 1 -r
#if [[ ! $REPLY =~ [Yy] ]]; then echo; exit 1; fi
#echo


submit-processing (){
    local index=$1
    SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
    sbatch --array=$index \
        -J $L-$COLL-mono-processing \
        --parsable 10.processing $L $COLL
}

submit-retry (){
    local index=$1
    err_msg=$(mktemp); trap "rm $err_msg" EXIT

    local jobid=$(submit-processing $index 2>$err_msg)
    echo "" >&2
    # Keep trying to submit until job limit is over
    # or any other error is obtained
    while grep -q "AssocMaxSubmitJobLimit" $err_msg; do
        # print deleting current line to avoid filling the screen with messages
        printf "\33[2K\rMax job limit: retrying every $TIME_RETRY" >&2
        sleep $TIME_RETRY
        jobid=$(submit-processing $index 2>$err_msg)
    done

    echo "" >&2
    echo $jobid
}

submit (){
    local index=$1
    echo Submitting job array of indexes $index
    DEP=""
    if [[ -z $ONLY_TSV2JSON ]]; then
        # Run job array of processing
        jobid=$(submit-retry $index)
        echo Submitted $jobid
        DEP="-d afterok:$jobid"
    fi

    # Submit job array of jsonl conversion
    SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
    sbatch --array=$index $DEP \
        -J $L-$COLL-tsv2jsonl 10.tsv2jsonl $L $COLL
}


# Submit in parts the job arrays larger than maximum
if [[ "$IS_RANGE" == "true" && $INDEX_SIZE -gt $MAX_JOBS ]]; then
    echo "Array larger than maximum ($MAX_JOBS), dividing into parts of $NUM_JOBS"
    for i in $(seq $MIN_ID $NUM_JOBS $MAX_ID); do
        if [[ $((i+NUM_JOBS-1)) -gt $MAX_ID ]]; then
            submit $i-$MAX_ID
        else
            submit $i-$((i+NUM_JOBS-1))
        fi
    done
else
    submit $INDEX
fi
