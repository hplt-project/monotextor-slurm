#!/bin/bash
source .env
source .checks
set -euo pipefail

langs=""
export IDLE_TIMEOUT=30s
if [[ $# -eq 2 ]]; then
    langs=$1
fi

# Create the task list
# sort them by size to start with the biggest
mkdir -p $WORKSPACE/tasks_list
entries=$WORKSPACE/tasks_list/10.index
dedup_entries=$WORKSPACE/tasks_list/10.dedup
if [ ! -s $entries ] || [ ! -s $dedup_entries ];
then
    temp=$(mktemp); trap "rm $temp" EXIT
    for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
    do
        for lang_dir in `echo $WORKSPACE/batches/$coll/* | tr ' ' '\n'`
        do
            size=`du -c $lang_dir/batch_*.jsonl.zst | tail -1 | cut -f1`
            # For languages of more than 150GB run distributed index
            if (( $size > 150000000 )); then
                for i in `seq 1 17`; do
                    echo "$lang_dir $i"
                done
            else
                echo $lang_dir
            fi
        done
    done | sort -u >$temp
    cat $temp | shuf --random-source=<(get_seeded_random 42) >$entries

    # create entries for dedup
    cat $temp | cut -d' ' -f1 | uniq >$dedup_entries
fi

echo $(wc -l $entries) tasks
confirm

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
newqueue() {
    local name=$1
    local workers=$2
    local mem=$3
    hq alloc add slurm --name $name \
        --workers-per-alloc 1 --max-worker-count $workers --backlog 10 \
        --idle-timeout $IDLE_TIMEOUT --time-limit 72h \
        -- -p small -A $SBATCH_ACCOUNT \
        --cpus-per-task 128 --ntasks 1 --mem-per-cpu $mem \
        -o "$SLURM_LOGS_DIR/hq-worker-%x.log"
}
queueid() {
    hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"$1\") | .id" | head -1
}

### INDEX
WORKERS=200
queue_name=index
newqueue $queue_name $WORKERS 1750
# obtain the allocation queue id
qid=$(queueid $queue_name)
trap "hq job cancel all; hq alloc remove --force $qid" INT

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $entries \
    --nodes 1 --progress \
    --stream=$SLURM_LOGS_DIR/hq-10.index.logs \
    --max-fails=10 --crash-limit=5 \
    bash 10.index
set -e

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $IDLE_TIMEOUT
sleep 30s
# finish que allocation queue
hq alloc remove --force $qid

### DEDUP
# create a different queue for dedup, needs less resources
queue_name=dedup
newqueue $queue_name $WORKERS 1750
# obtain the allocation queue id
qid=$(queueid $queue_name)
trap - INT
trap "hq job cancel all; hq alloc remove --force $qid" INT

set +e
hq submit --each-line $dedup_entries \
    --nodes 1 --progress \
    --stream=$SLURM_LOGS_DIR/hq-10.dedup.logs \
    --max-fails=10 --crash-limit=5 \
    bash 10.dedup
set -e

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $IDLE_TIMEOUT
sleep 30s

# finish que allocation queue
hq alloc remove $qid
