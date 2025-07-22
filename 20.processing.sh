#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

WORKERS=200
GPU_WORKERS=400
GPU_NODES=2 # num nodes per allocation
# total allocated nodes is GPU_WORKERS/GPU_NODES
idle_timeout=30s

# Create the task list
mkdir -p $WORKSPACE/tasks_list
entries=$WORKSPACE/tasks_list/20.processing
if [ ! -s $entries ];
then
    for lang in `cat langs | grep -v unk`
    do
        # only create tasks for existing completed dedup files
        batches=`find -L $WORKSPACE/collections_merged/$lang -type f -name "*batch_*.jsonl.zst" -exec basename {} \;`
        for batch in $batches;
        do
            echo "$lang $batch"
        done
    done | sort -u >$entries
fi

echo $(wc -l $entries) tasks
confirm

mkdir -p $SLURM_LOGS_DIR/workers
# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
hq alloc add slurm --name processing \
    --workers-per-alloc 1 --max-worker-count $WORKERS --backlog $WORKERS \
    --idle-timeout $idle_timeout --time-limit 72h \
    -- -p small -A $SBATCH_ACCOUNT \
    --cpus-per-task 128 --ntasks 1 --mem-per-cpu 1750 \
    -o "$SLURM_LOGS_DIR/workers/hq-worker-%x.log"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"processing\") | .id" | head -1)

trap "hq job cancel all; hq alloc remove --force $qid" INT

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --nodes 1 --progress \
    --stream=$SLURM_LOGS_DIR/hq-20.processing.logs \
    --max-fails=40 --crash-limit=5 \
    bash 20.processing
set -e

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $idle_timeout
sleep 15s

# finish que allocation queue
hq alloc remove $qid

#### RUN web-registers
# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
# this time allocating GPUs
hq alloc add slurm --name registers \
    --workers-per-alloc $GPU_NODES --max-worker-count $GPU_WORKERS --backlog $GPU_WORKERS \
    --idle-timeout $idle_timeout --time-limit 48h \
    -- -p standard-g -A $SBATCH_ACCOUNT \
    --nodes $GPU_NODES --ntasks-per-node=1 --gpus-per-task=8 \
    -o "$SLURM_LOGS_DIR/workers/hq-worker-%x.log"
# obtain the allocation queue id
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"registers\") | .id" | head -1)

trap "hq job cancel all; hq alloc remove --force $qid" EXIT

hq submit --each-line $entries \
    --nodes 1 --resource "gpus/amd=8" --progress \
    --stream=$SLURM_LOGS_DIR/hq-21.registers.logs \
    --max-fails=10 --crash-limit=5 \
    bash 21.registers

sleep $idle_timeout
sleep 15s
hq alloc remove $qid
