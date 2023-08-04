#!/bin/bash
source .env
source .checks
#set -euo pipefail

L=$1

colls=${!COLLECTIONS[@]}
colls=$(echo $colls | tr ' ' '\n' | sort)
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
        continue
    fi

    dedup_tmp=$(ls -1 $dir/dedup.*.jsonl.zst.tmp | wc -l)
    query=$(ls -1 $dir/queries*.zst | wc -l)
    if [ -f $dir/dedup.1.jsonl.zst ] && [ $dedup_tmp -eq 0 ] && [ $query -eq 0 ]
    then
        printf "deduped"
    else
        printf "scored"
    fi
    printf "\t"
done
echo
