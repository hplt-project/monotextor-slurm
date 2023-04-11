#!/bin/bash

L=$1
INPUT=$2
OUTPUT=$3
MODEL=models/$L

zstdcat $INPUT \
    | monofixer --scol 2 \
        --ignore_normalization --ignore_segmentation \
        --ignore_empty --ignore_duplicates \
        -q - - $L \
    | monocleaner --scol 2 \
        --add_lang_ident --detect_script \
        --disable_hardrules \
        -q $MODEL - - \
    | zstd > $OUTPUT
