#!/usr/bin/env bash

# -------------------------------------------------------------------
# Script: 02_create_chunks.sh
# Purpose: Splits the master HUC12 list into smaller chunk files
#          for batch processing.
# -------------------------------------------------------------------

set -e

# === CONFIGURATION ===
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
HUC_LIST_FILE="${CLIPPED_DEM_DIR}/huc12_list.txt"
CHUNK_DIR="${CLIPPED_DEM_DIR}/huc_chunks"
HUCS_PER_CHUNK=100

# === SETUP ===
if [ ! -f "${HUC_LIST_FILE}" ]; then
    echo "[ERROR] Master HUC list not found at: ${HUC_LIST_FILE}"
    exit 1
fi

echo "Cleaning old chunks and creating new chunk directory..."
rm -rf "${CHUNK_DIR}"
mkdir -p "${CHUNK_DIR}"

echo "Splitting master list into chunks of ${HUCS_PER_CHUNK} HUCs each..."

# THE FIX IS HERE: Removed '--numeric-suffixes=1'.
# The default behavior is to start numbering from 0000.
split -l "${HUCS_PER_CHUNK}" -a 4 --numeric-suffixes --additional-suffix=.txt \
    "${HUC_LIST_FILE}" "${CHUNK_DIR}/chunk_"

NUM_CHUNKS=$(find "${CHUNK_DIR}" -type f | wc -l)

echo "-------------------------------------------------------------"
echo "✅ Split HUC list into ${NUM_CHUNKS} chunk files (named chunk_0000.txt, etc.)."
echo "Chunks are located in: ${CHUNK_DIR}"
echo "You can now submit the job with 'sbatch submit_clip_by_chunk.sbatch'"
echo "-------------------------------------------------------------"