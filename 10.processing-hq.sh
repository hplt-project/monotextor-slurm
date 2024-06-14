#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name $COLL --time-limit 5m \
    --workers-per-alloc 1 --max-worker-count 2 --cpus 128 \
    -- -p debug -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/hq-processing-%x.out" -e "$SLURM_LOGS_DIR/hq-worker-processing-%x.err"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"$COLL\") | .id" | head -1)

# Create the task list
entries=$(mktemp); trap "rm $entries" EXIT
#for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
for coll in cc13
do
    for lang in cat_Latn mlt_Latn eng_Latn
    do
        echo "$lang $COLL 1"
    done
done | tee >(cat) >$entries

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --cpus 128 \
    --progress --log=$SLURM_LOGS_DIR/hq-processing.log \
    --max-fails=0 --crash-limit=1 \
    bash 10.processing-hq

# finish que allocation queue
hq alloc remove --force $qid
