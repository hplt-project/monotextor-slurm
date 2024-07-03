#!/bin/bash
source .env
source .checks
set -euo pipefail

WORKERS=2
idle_timeout=30s

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name dedup \
    --workers-per-alloc 1 --max-worker-count $WORKERS --backlog $WORKERS \
    --idle-timeout $idle_timeout --time-limit 30m \
    -- -p debug -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/hq-worker-%x.out" -e "$SLURM_LOGS_DIR/hq-worker-%x.err"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"dedup\") | .id" | head -1)

trap "hq alloc remove --force $qid" INT

# Create the task list
entries=$(mktemp); trap "rm $entries" EXIT
#for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
for coll in cc13 cc16
do
    for lang in `cat ../langs-two | head | grep -v 'eng\|unk'`
    do
        echo "$lang $coll"
    done
done | tee >(cat) >$entries

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --cpus 128 --progress \
    --log=$SLURM_LOGS_DIR/hq-dedup.log \
    --max-fails=10 --crash-limit=5 \
    bash 20.index

#TODO what should we do with taskst that failed in index?
hq submit --each-line $entries \
    --cpus 128 --progress \
    --log=$SLURM_LOGS_DIR/hq-dedup.log \
    --max-fails=10 --crash-limit=5 \
    bash 20.dedup

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $idle_timeout
sleep 15s

# finish que allocation queue
hq alloc remove $qid
