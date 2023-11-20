#!/bin/bash
module load cray-python

DIR=~/scratch_465000498/zaragoza/monotextor-processing/wide16/ca
QUERY_FILE=$DIR/queries.zst
FILES=$DIR/scored.*.jsonl.zst
tmp=$(mktemp); trap "rm $tmp" EXIT

#Q=$(zstdcat $QUERY_FILE | grep -v DISCARD | awk -F' ' 'NF>1' | shuf -n1 | tr ' ' '\n' | shuf -n5)
Q=$(zstdcat $QUERY_FILE | grep -n DISCARD | cut -d':' -f1 | shuf -n200)

zstdcat $FILES | ./printlines.py $Q >$tmp

cat $tmp | jq -r '.url' | less

cat $tmp \
| while read -r doc; do
    echo "$doc" | jq -r '.text' \
    | cat <(echo "$doc" | jq -r '.url') - \
    | less
done
