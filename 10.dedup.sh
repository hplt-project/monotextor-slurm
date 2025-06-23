#!/bin/bash
source .env
source .checks
set -euo pipefail

langs=""
export IDLE_TIMEOUT=30s
if [[ $# -eq 2 ]]; then
    langs=$1
fi

print_task() {
    local lang=$1
    local coll=$2
    if [ "$coll" == "global" ]; then
        size=`du -c $WORKSPACE/batches/*/$lang/batch_*.jsonl.zst | tail -1 | cut -f1`
    else
        size=`du -c $WORKSPACE/batches/$coll/$lang/batch_*.jsonl.zst | tail -1 | cut -f1`
    fi
    # For languages of more than 150GB run distributed minhash
    # each index is the index of the minhash band to run
    if (( $size > 150000000 )); then
        for i in `seq 1 17`; do
            echo "$lang $coll $i"
        done
    else
        echo "$lang $coll"
    fi
}

# Create the task list
# sort them by size to start with the biggest
mkdir -p $WORKSPACE/tasks_list
index_entries=$WORKSPACE/tasks_list/10.index
dedup_entries=$WORKSPACE/tasks_list/10.dedup
merge_entries=$WORKSPACE/tasks_list/11.merge-collections
if [ ! -s $index_entries ] || [ ! -s $dedup_entries ] || [ ! -s $merge_entries ];
then
    temp=$(mktemp); trap "rm $temp" EXIT
    for lang in `cat langs | grep -v unk`
    do
        if echo $lang | grep -qv "eng_Latn\|rus_Cyrl\|cmn_Hans"
        then
            print_task $lang global
        else
            for coll in `echo ${!COLLECTIONS[@]} | tr ' ' '\n' | sort`
            do
                print_task $lang $coll
            done
        fi
    done | sort -u >$temp
    cat $temp | shuf --random-source=<(get_seeded_random 42) >$index_entries

    # create entries for dedup
    cat $temp | cut -d' ' -f1-2 | uniq >$dedup_entries
    # create entries for merge collections
    # making a unique by first column (lang)
    # so if the second column is "global" it won't merge because it's already merged
    # otherwise merge
    cat $dedup_entries | sort -t' ' -k1,1 -u >$merge_entries
fi

echo $(wc -l $index_entries) tasks
confirm

# Create an allocation queue that will allocate a full node for each worker
# each worker will process one task
newqueue() {
    local name=$1
    local workers=$2
    local mem=$3
    hq alloc add slurm --name $name \
        --workers-per-alloc 1 --max-worker-count $workers --backlog $workers \
        --idle-timeout $IDLE_TIMEOUT --time-limit 24h \
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
newqueue $queue_name $WORKERS 3500
# obtain the allocation queue id
qid=$(queueid $queue_name)
trap "hq job cancel all; hq alloc remove --force $qid" INT

set +e # remove strict mode, so if job fails, script does not finish and the queue can be closed afterwards
hq submit --each-line $index_entries \
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
    --max-fails=10 --crash-limit=1 \
    bash 10.dedup
set -e

hq submit --each-line $merge_entries \
    --cpus 64 --progress \
    --stream=$SLURM_LOGS_DIR/hq-11.merge-collections.logs \
    --max-fails=10 --crash-limit=1 \
    bash 11.merge-collections

# Wait until the queue workers are shut down
# sleep a bit more than timeout to avoid running the remove command while workers are still shutting down
sleep $IDLE_TIMEOUT
sleep 30s

# finish que allocation queue
hq alloc remove $qid
