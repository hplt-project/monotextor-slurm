#!/bin/bash
source .env
source .checks
#set -euo pipefail

L=$1

colls=${!COLLECTIONS[@]}
colls=$(echo $colls | tr ' ' '\n' | sort)
for c in $colls; do
    if [[ $c =~ sample ]]; then continue; fi

    dir=$WORKSPACE/batches/$c/$L
    nbatches=$(ls -1 $dir/batch_*.jsonl.zst | wc -l)
    if [[ $nbatches -eq 0 ]]; then
        printf "none\t"
        continue
    fi

    dir=$WORKSPACE/dedup/$c/$L
    dedup=$(ls  -1 $dir/batch_*.jsonl.zst | wc -l)
    dedup_tmp=$(ls  -1 $dir/batch_*.jsonl.zst.tmp | wc -l)
    if [[ $dedup -eq 0 ]] || [[ $dedup_tmp -ne 0 ]]; then
        printf "batched\t"
        continue
    fi

    dir=$WORKSPACE/annotated/$c/$L
    scored=$(ls  -1 $dir/batch_*.jsonl.zst | wc -l)
    scored_tmp=$(ls  -1 $dir/batch_*.jsonl.zst.tmp | wc -l)
    if [[ $scored -eq 0 ]] || [[ $scored_tmp -ne 0 ]]; then
        printf "deduped\t"
        continue
    else
        printf "annotated\t"
    fi
done 2>/dev/null | cut -f-21
