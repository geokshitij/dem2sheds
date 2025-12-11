#!/usr/bin/env bash

# -------------------------------------------------------------------
# Script: clip_dem_by_huc12.sh (Revised)
# Purpose: Clips a master DEM VRT to each HUC12 boundary in parallel.
# -------------------------------------------------------------------

set -e # Exit immediately if a command exits with a non-zero status.

# === CONFIGURATION ===
PROCESSED_DIR="/scratch/kdahal3/DEM_CONUS/processed"
MERGED_HUC_FILE="${PROCESSED_DIR}/WBD_CONUS_HUC12.gpkg"
DEM_VRT_FILE="${PROCESSED_DIR}/CONUS_DEM_30m.vrt"
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
THREADS=126
HUC_LAYER_NAME="WBDHU12"
HUC_ID_FIELD="huc12"

# === SETUP ===
mkdir -p "${CLIPPED_DEM_DIR}"
TMPDIR="${CLIPPED_DEM_DIR}/tmp"
mkdir -p "${TMPDIR}"

echo "-------------------------------------------------------------"
echo "Clipping DEM for each HUC12 basin"
echo "-------------------------------------------------------------"

# === STEP 1: GET LIST OF ALL HUC12 IDs ===
HUC_LIST_FILE="${TMPDIR}/huc12_list.txt"
echo "[1/4] Generating list of all HUC12 IDs..."
ogrinfo -ro -q -sql "SELECT ${HUC_ID_FIELD} FROM ${HUC_LAYER_NAME}" "${MERGED_HUC_FILE}" | \
grep "${HUC_ID_FIELD} (String)" | awk -F' = ' '{print $2}' > "${HUC_LIST_FILE}"

NUM_HUCS=$(wc -l < "${HUC_LIST_FILE}")
echo "[INFO] Found ${NUM_HUCS} HUC12 basins to process."

# === STEP 2: PERFORM A DRY RUN FOR A SINGLE HUC ===
echo "[2/4] Performing a dry run to verify the command..."
TEST_HUC_ID=$(head -n 1 "${HUC_LIST_FILE}")
if [ -z "${TEST_HUC_ID}" ]; then
    echo "[ERROR] HUC list is empty. Cannot perform dry run."
    exit 1
fi

echo "A test command will be generated for the first HUC: ${TEST_HUC_ID}"
echo "The command that will be run in parallel is:"
echo "-------------------------------------------------------------"
# Note the 'echo' at the beginning. This just prints the command.
echo gdalwarp \
    -q \
    -cutline "'${MERGED_HUC_FILE}'" \
    -cl "'${HUC_LAYER_NAME}'" \
    -cwhere "'${HUC_ID_FIELD} = '\''${TEST_HUC_ID}'\''" \
    -crop_to_cutline \
    -dstnodata -9999 \
    -co "COMPRESS=LZW" -co "PREDICTOR=2" \
    "'${DEM_VRT_FILE}'" \
    "'${CLIPPED_DEM_DIR}/${TEST_HUC_ID}.tif'"
echo "-------------------------------------------------------------"
read -p "Does this command look correct? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting script."
    exit 1
fi

# === STEP 3: DEFINE AND RUN THE PARALLEL PROCESSING ===
echo "[3/4] Starting parallel processing (${THREADS} jobs)..."

# Define the function to be used by GNU Parallel
process_huc12() {
    local HUC12_ID="$1"
    # Re-state variables inside function for clarity
    local MERGED_HUC_FILE="/scratch/kdahal3/DEM_CONUS/processed/WBD_CONUS_HUC12.gpkg"
    local DEM_VRT_FILE="/scratch/kdahal3/DEM_CONUS/processed/CONUS_DEM_30m.vrt"
    local CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
    local HUC_LAYER_NAME="WBDHU12"
    local HUC_ID_FIELD="huc12"
    local OUT_RASTER="${CLIPPED_DEM_DIR}/${HUC12_ID}.tif"

    # Skip if the output file already exists to allow resuming
    [ -f "${OUT_RASTER}" ] && return

    gdalwarp \
        -q \
        -cutline "${MERGED_HUC_FILE}" \
        -cl "${HUC_LAYER_NAME}" \
        -cwhere "${HUC_ID_FIELD} = '${HUC12_ID}'" \
        -crop_to_cutline \
        -dstnodata -9999 \
        -co "COMPRESS=LZW" -co "PREDICTOR=2" \
        "${DEM_VRT_FILE}" \
        "${OUT_RASTER}"
}

# Export the function so it's available to parallel's sub-shells
export -f process_huc12

# Run the job
cat "${HUC_LIST_FILE}" | parallel -j ${THREADS} --eta --bar "process_huc12 {}"

# === STEP 4: VERIFY OUTPUT ===
echo "[4/4] Verifying output..."
NUM_OUTPUTS=$(find "${CLIPPED_DEM_DIR}" -name "*.tif" | wc -l)
echo "[INFO] Produced ${NUM_OUTPUTS} / ${NUM_HUCS} clipped DEMs."

if [ "${NUM_OUTPUTS}" -lt "${NUM_HUCS}" ]; then
    echo "[WARN] Not all DEMs were generated. You can re-run the script to process the missing ones."
fi

echo "-------------------------------------------------------------"
echo "✅ All HUC12 basins processed successfully."
echo "Output located in: ${CLIPPED_DEM_DIR}"
echo "-------------------------------------------------------------"