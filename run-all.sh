#!/bin/bash
set -euo pipefail
source .env
source .checks

L=$1

#JOBID=$(./00.merge-batching.sh $L)
#echo "Submitted merge-batching job $JOBID"
#JOBID=$(./10.processing.sh $L -d afterok:$JOBID)
#echo "Submitted mono-processing job $JOBID"
#./90.reduce.sh $L -d afterok:$JOBID
