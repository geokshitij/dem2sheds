#!/bin/bash

# -------------------------------------------------------------------
# Script: 03_submit_job.sh
# Purpose: Calculates the number of chunks and submits the job array
#          to Slurm with the correct parameters.
# -------------------------------------------------------------------

set -e

CHUNK_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems/huc_chunks"

echo "Finding chunk files in ${CHUNK_DIR}..."

# 1. Calculate the number of chunks
NUM_CHUNKS=$(find "${CHUNK_DIR}" -type f | wc -l)

if [ "${NUM_CHUNKS}" -eq 0 ]; then
    echo "[ERROR] No chunk files found. Did you run 02_create_chunks.sh?"
    exit 1
fi

# 2. Calculate the array index range (0 to N-1)
ARRAY_RANGE="0-$((NUM_CHUNKS - 1))"

echo "Found ${NUM_CHUNKS} chunks. Submitting job array with range: ${ARRAY_RANGE}"

# 3. Submit the job, passing the array range on the command line
sbatch --array="${ARRAY_RANGE}%100" process_chunk.sbatch

echo "✅ Job submitted."
