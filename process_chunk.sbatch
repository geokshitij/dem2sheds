#!/bin/bash

#SBATCH --job-name=clip_dem_batch
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:10:00
#SBATCH --output=logs/clip_batch_%A_%a.out
#SBATCH --error=logs/clip_batch_%A_%a.err

# --- NO #SBATCH --array DIRECTIVE HERE ---
# The array range will be provided by the submitter script.

set -e

# === CONFIGURATION ===
PROCESSED_DIR="/scratch/kdahal3/DEM_CONUS/processed"
MERGED_HUC_FILE="${PROCESSED_DIR}/WBD_CONUS_HUC12.gpkg"
DEM_VRT_FILE="${PROCESSED_DIR}/CONUS_DEM_30m.vrt"
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"
CHUNK_DIR="${CLIPPED_DEM_DIR}/huc_chunks"
HUC_LAYER_NAME="WBDHU12"
HUC_ID_FIELD="huc12"

# This script still uses SLURM_ARRAY_TASK_ID, which is set by sbatch
CHUNK_FILE="${CHUNK_DIR}/chunk_$(printf "%04d" ${SLURM_ARRAY_TASK_ID}).txt"

if [ ! -f "${CHUNK_FILE}" ]; then
    echo "[ERROR] Task ${SLURM_ARRAY_TASK_ID} could not find its chunk file: ${CHUNK_FILE}"
    exit 1
fi

echo "=========================================================="
echo "Starting Slurm Batch Task: ${SLURM_ARRAY_TASK_ID}"
echo "Processing HUCs from chunk file: ${CHUNK_FILE}"
echo "Time: $(date)"
echo "=========================================================="

PROCESSED_COUNT=0
while read -r HUC12_ID; do
    [ -z "${HUC12_ID}" ] && continue
    OUT_RASTER="${CLIPPED_DEM_DIR}/${HUC12_ID}.tif"
    [ -f "${OUT_RASTER}" ] && continue

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

    PROCESSED_COUNT=$((PROCESSED_COUNT + 1))
done < "${CHUNK_FILE}"

echo "----------------------------------------------------------"
echo "✅ Task ${SLURM_ARRAY_TASK_ID} complete."
echo "Processed ${PROCESSED_COUNT} new HUCs from this chunk."
echo "Finished at: $(date)"
echo "----------------------------------------------------------"