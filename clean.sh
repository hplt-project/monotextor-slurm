#!/bin/bash
set -euo pipefail
source .env

L=$1

if [ "$L" == "all" ]; then
    rm --interactive=once $WORKSPACE/*/*tmp_*
else
    rm --interactive=once $WORKSPACE/$L/*tmp_*
fi
