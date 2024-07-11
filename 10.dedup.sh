#!/bin/bash
source .env
source .checks
set -euo pipefail

WORKERS=20
idle_timeout=30s

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name dedup \
    --workers-per-alloc 1 --max-worker-count $WORKERS --backlog $WORKERS \
    --idle-timeout $idle_timeout --time-limit 72h \
    -- -p small -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/hq-worker-%x.out" -e "$SLURM_LOGS_DIR/hq-worker-%x.err"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"dedup\") | .id" | head -1)

trap "hq alloc remove --force $qid" INT

# Create the task list
# sort them by size to start with the biggest
entries=$(mktemp); trap "rm $entries" EXIT
temp=$(mktemp); trap "rm $temp" EXIT
#for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
for coll in wide5 wide10 wide17 cc16 cc21
do
    echo $WORKSPACE/batches/$coll/* | tr ' ' '\n'
done >$temp
du -sh $(cat $temp) | sort -rh | cut -f2 >$entries

echo $(wc -l $entries) tasks
confirm

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --cpus 128 --progress \
    --log=$SLURM_LOGS_DIR/hq-index.log \
    --max-fails=10 --crash-limit=5 \
    bash 10.index

#TODO what should we do with taskst that failed in index?
hq submit --each-line $entries \
    --cpus 128 --progress \
    --log=$SLURM_LOGS_DIR/hq-dedup.log \
    --max-fails=10 --crash-limit=5 \
    bash 10.dedup

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $idle_timeout
sleep 30s

# finish que allocation queue
hq alloc remove $qid
