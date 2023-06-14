#!/bin/bash
source .env
source .checks
#set -euo pipefail

L=$1

colls=${!COLLECTIONS[@]}
#echo $colls | tr ' ' '\t'
for c in $colls; do
    if [[ $c =~ sample ]]; then continue; fi

    dir=$WORKSPACE/$c/$L

    running=$(ls -1 $dir/batch.* | grep tmp | wc -l)
    nbatches=$(ls -1 $dir/batch.* | wc -l)
    if [[ $nbatches -eq 0 ]]; then
        printf "none\t"
        continue
    elif [[ $running -ne 0 ]]; then
        printf "batching\t"
        continue
    fi

    finished=$(ls -1 $dir/scored.*.jsonl.* | grep -c -v tmp_)
    total=$(ls -1 $dir/scored.*.jsonl.* | wc -l)
    if [[ $total -eq 0 ]]; then
        printf "batched"
    elif [[ $finished -ne $nbatches ]]; then
        printf "scoring $finished/$nbatches"
    else
        printf "scored"
    fi
    printf "\t"
done
echo
