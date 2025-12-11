#!/usr/bin/env bash

# -------------------------------------------------------------------
# Script: 03_verify_output.sh
# Purpose: Verifies the output of the Slurm job array.
# -------------------------------------------------------------------

set -e

# === CONFIGURATION ===
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
HUC_LIST_FILE="${CLIPPED_DEM_DIR}/huc12_list.txt"

echo "Verifying output..."

# Check if the list file exists
if [ ! -f "${HUC_LIST_FILE}" ]; then
    echo "[ERROR] HUC list file not found at ${HUC_LIST_FILE}. Cannot verify."
    exit 1
fi

NUM_HUCS=$(wc -l < "${HUC_LIST_FILE}")
NUM_OUTPUTS=$(find "${CLIPPED_DEM_DIR}" -name "*.tif" | wc -l)

echo "-------------------------------------------------------------"
echo "Verification Results:"
echo "Expected DEMs: ${NUM_HUCS}"
echo "Generated DEMs:  ${NUM_OUTPUTS}"
echo "-------------------------------------------------------------"

if [ "${NUM_OUTPUTS}" -lt "${NUM_HUCS}" ]; then
    echo "[WARN] Not all DEMs were generated."
    echo "You can re-run the 'sbatch submit_clip.sbatch' command."
    echo "It will automatically skip the ones that are already complete."
else
    echo "✅ Success! All expected DEMs have been generated."
fi
