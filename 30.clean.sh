#!/bin/bash
source .env
source .checks
set -euo pipefail

WORKERS=50
idle_timeout=30s

# Create the task list
entries=$(mktemp); trap "rm $entries" EXIT
cat ../langs-two | grep -v unk >$entries

echo $(wc -l $entries) tasks
confirm

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name clean \
    --workers-per-alloc 1 --max-worker-count $WORKERS --backlog $WORKERS \
    --idle-timeout $idle_timeout --time-limit 72h \
    -- -p small -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/hq-worker-%x.out" -e "$SLURM_LOGS_DIR/hq-worker-%x.err"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"clean\") | .id" | head -1)

trap "hq alloc remove --force $qid" INT
trap "hq job cancel all" INT

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --nodes 1 --progress \
    --log=$SLURM_LOGS_DIR/hq-clean.log \
    --max-fails=5 --crash-limit=1 \
    bash 30.clean

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $idle_timeout
sleep 15s

# finish que allocation queue
hq alloc remove $qid
