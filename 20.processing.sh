#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

WORKERS=20
idle_timeout=30s

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name processing \
    --workers-per-alloc 1 --max-worker-count $WORKERS --backlog $WORKERS \
    --idle-timeout $idle_timeout --time-limit 6h \
    -- -p small -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/hq-worker-%x.out" -e "$SLURM_LOGS_DIR/hq-worker-%x.err"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"processing\") | .id" | head -1)

trap "hq alloc remove --force $qid" INT

# Create the task list
entries=$(mktemp); trap "rm $entries" EXIT
for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
do
    for lang in `cat ../langs-two`
    do
        # only create tasks for existing completed dedup files
        num=`find $WORKSPACE/dedup/$lang -type f -name "*batch_*.jsonl.zst"`
        echo "$lang $coll $num"
    done
done >$entries

echo $(wc -l $entries) tasks
confirm

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --nodes 1 --progress \
    --log=$SLURM_LOGS_DIR/hq-processing.log \
    --max-fails=10 --crash-limit=5 \
    bash 20.processing

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $idle_timeout
sleep 15s

# finish que allocation queue
hq alloc remove $qid
