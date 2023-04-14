#!/bin/bash
set -euo pipefail
source .env

L=$1

if [ "$L" == "all" ]; then
    rm -I $WORKSPACE/*/*tmp_*
else
    rm -I $WORKSPACE/$L/*tmp_*
fi
