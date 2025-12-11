#!/usr/bin/env bash

# -------------------------------------------------------------------
# Script: 01_setup_and_generate_list.sh
# Purpose: Prepares directories and generates the master list of HUC12 IDs.
#          Run this script ONCE from the command line.
# -------------------------------------------------------------------

set -e # Exit immediately if a command exits with a non-zero status.

# === CONFIGURATION ===
PROCESSED_DIR="/scratch/kdahal3/DEM_CONUS/processed"
MERGED_HUC_FILE="${PROCESSED_DIR}/WBD_CONUS_HUC12.gpkg"
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
HUC_LAYER_NAME="WBDHU12"
HUC_ID_FIELD="huc12"

# This is the master list that the Slurm job array will read from.
HUC_LIST_FILE="${CLIPPED_DEM_DIR}/huc12_list.txt"

# === SETUP ===
echo "Creating output directory: ${CLIPPED_DEM_DIR}"
mkdir -p "${CLIPPED_DEM_DIR}"

echo "-------------------------------------------------------------"
echo "Generating list of all HUC12 IDs..."
echo "-------------------------------------------------------------"

# === GENERATE HUC LIST ===
# This is the same command from your original script.
ogrinfo -ro -q -sql "SELECT ${HUC_ID_FIELD} FROM ${HUC_LAYER_NAME}" "${MERGED_HUC_FILE}" | \
grep "${HUC_ID_FIELD} (String)" | awk -F' = ' '{print $2}' > "${HUC_LIST_FILE}"

NUM_HUCS=$(wc -l < "${HUC_LIST_FILE}")

if [ "${NUM_HUCS}" -eq 0 ]; then
    echo "[ERROR] HUC list is empty. Check your ogrinfo command and file paths."
    exit 1
fi

echo "✅ Found ${NUM_HUCS} HUC12 basins to process."
echo "List saved to: ${HUC_LIST_FILE}"
echo ""
echo "-------------------------------------------------------------"
echo "Setup is complete. You can now submit the job array."
echo "Next steps:"
echo "1. Create a log directory: mkdir -p logs"
echo "2. Submit the job: sbatch submit_clip_array.sbatch"
echo "-------------------------------------------------------------"