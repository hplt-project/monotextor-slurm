#!/bin/bash
# Run job array where each job processes one batch of paragraphs
# each processing job runs monofixer + monocleaner
# this is equivalent to the map step in a map-reduce
source .env
source .checks
set -euo pipefail

usage () {
    echo "Usage: `basename $0` [options] <lang>"
    echo "Options:"
    echo "    -d                Discard documents instead of tagging"
    echo "    -e                Process external contributions"
    echo "    -f FAILED_JOBID   Re-run failed jobs of previous job array"
    echo "    -i INDEX          Job array index instead of run 'all'"
    echo "    -h                Shows this message"
}


MAX_JOBS=120
NUM_JOBS=60
TIME_RETRY=10m

input_dir="dedup"
DISCARD=false
EXTERNAL=false
FAILED=""
INDEX=""
while getopts "df:i:eh" options
do
    case "${options}" in
        d) DISCARD=true;;
        f) FAILED=$OPTARG;;
        i) INDEX=$OPTARG;;
        e) EXTERNAL=true;;
        h) usage
            exit 0;;
        \?) usage >&2
            exit 1;;
    esac
done

L=${@:$OPTIND:1}

if [ "$INDEX" = "" ]; then
# List all the batches that need to be processed (size of the job array)
    if [ "$EXTERNAL" = true ]; then
        external="external"
        input_dir=external
    fi
    INDEX=1-$(ls -1 $WORKSPACE/$input_dir/$L/${L}_*.zst | sed -E 's#.*/\w{2,3}_([0-9]+)\.jsonl\.zst#\1#' | sort -n | tail -1)
else
    INDEX=$2
fi

if ! [ "$FAILED" = "" ]; then
    # Select only failed jobs (timeout, oom and failed status)
    # Create a list of batch id's separated by comma
    INDEX=$(\
        sacct -j $FAILED --parsable -s oom,f,to -n \
        | grep -v '.batch' \
        | sed -E 's/[0-9]+_([0-9]+)\|.*/\1/g' \
        | paste -sd','
    )
fi

if [ "$DISCARD" = true ]; then
    echo "Discarding documents instead of tagging"
fi
echo "Job array of index $INDEX for $L"
read -p "Confirm? [y/n] " -n 1 -r
if [[ ! $REPLY =~ [Yy] ]]; then echo; exit 1; fi
echo

# Send parameters to the job scripts through the env
export EXTERNAL
export DISCARD
JOBNAME="$L"
if [ "$EXTERNAL" = true ]; then
    JOBNAME="external-$L"
fi

JOB_ID=$(\
SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x-%A_%a.out" \
sbatch --array=$INDEX \
    -J $JOBNAME-clean --parsable \
    30.clean $L)
echo Submitted batch job $JOB_ID

if [ "$DISCARD" = false ]; then
    SBATCH_OUTPUT="$SLURM_LOGS_DIR/%x.out" \
    sbatch -J $JOBNAME-stats \
        -d afterok:$JOB_ID \
        30.stats $L
fi
