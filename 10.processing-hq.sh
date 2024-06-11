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

COLL=$1
echo ${COLLECTIONS[$COLL]}

hq alloc add slurm --name $COLL --time-limit 5m \
    --workers-per-alloc 1 --max-worker-count 1 \
    -- -p debug -A $SBATCH_ACCOUNT
qid=$(hq alloc list --output-mode json | jq -cr ".[] | select(.name == \"$COLL\") | .id" | head -1)

entries=$(mktemp); trap "rm $entries" EXIT
for lang in cat_Latn mlt_Latn
do
    #hq submit --array 1 --name $COLL-$lang-processing \
    #    --log=$SLURM_LOGS_DIR/$COLL-$lang-processing.log \
    #    ./10.processing-hq $lang $COLL
    echo "$lang $COLL 1"
done >$entries

cat $entries
hq submit --each-line $entries \
    --progress --max-fails 0 \
    --log=hqlog \
    bash 10.processing-hq

hq alloc remove --force $qid
