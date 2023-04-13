#!/bin/bash
set -euo pipefail
source .env

L=$1
COLLECTIONS=${@:2}

sbatch -J merge-batching-$L 00.merge-batching $L $COLLECTIONS
