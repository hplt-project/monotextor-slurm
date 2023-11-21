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
    scored=$(ls -1 $dir/scored.*.jsonl.* | grep -c -v tmp_)
    scored_tmp=$(ls -1 $dir/scored.*.jsonl.* | grep -c tmp_)
    total=$(ls -1 $dir/scored.*.jsonl.* | wc -l)
    if [[ $nbatches -eq 0 ]] && [[ $scored -eq 0 ]]; then
        printf "none\t"
        continue
    elif [[ $running -ne 0 ]]; then
        printf "batching\t"
        continue
    fi

    if [[ $total -eq 0 ]]; then
        printf "batched\t"
        continue
    elif [[ $nbatches -ne 0 ]] && [[ $scored -ne $nbatches ]]; then
        printf "scoring $scored/$nbatches\t"
        continue
    fi

    dir=$WORKSPACE/dedup/$L

    dedup_tmp=$(ls -1 $dir/*.jsonl.zst.tmp | wc -l)
    if ! [ -f $dir/${L}_1.jsonl.zst ] || ! [ $dedup_tmp -eq 0 ]
    then
        printf "scored\t"
        continue
    fi

    dir=$WORKSPACE/clean/$L

    clean_tmp=$(ls -1 $dir/*.jsonl.zst.tmp | wc -l)
    stats=$(ls -1 $dir/${L}_stats | wc -l)
    if [ -f $dir/${L}_1.jsonl.zst ] && [ $clean_tmp -eq 0 ] && [ $stats -eq 1 ]
    then
        printf "cleaned\tcleaned\tcleaned\tcleaned\n"
        exit 0
    else
        printf "deduped\tdeduped\tdeduped\tdeduped\n"
        exit 0
    fi

    printf "\t"
done
echo
