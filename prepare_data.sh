#!/usr/bin/env bash

# -------------------------------------------------------------------
# Script: prepare_data.sh (Improved)
# Purpose: Pre-processes DEM tiles and HUC shapefiles for clipping.
#
# 1. Merges all HUC12 shapefiles into a single GeoPackage.
# 2. Creates critical attribute and spatial indexes on the GeoPackage for fast queries.
# 3. Creates a Virtual Raster (VRT) from all DEM tiles.
# -------------------------------------------------------------------

set -e # Exit immediately if a command exits with a non-zero status.

# === CONFIGURATION ===
DEM_DIR="/scratch/kdahal3/DEM_CONUS_30m"
HUC_UNZIPPED_DIR="/scratch/kdahal3/DEM_CONUS/WBD_unzipped"
OUTPUT_DIR="/scratch/kdahal3/DEM_CONUS/processed"
MERGED_HUC_FILE="${OUTPUT_DIR}/WBD_CONUS_HUC12.gpkg"
DEM_VRT_FILE="${OUTPUT_DIR}/CONUS_DEM_30m.vrt"
DEM_LIST_FILE="${OUTPUT_DIR}/dem_file_list.txt"

# === SETUP ===
mkdir -p "${OUTPUT_DIR}"
echo "-------------------------------------------------------------"
echo "Preparing data for processing..."
echo "Output directory: ${OUTPUT_DIR}"
echo "-------------------------------------------------------------"

# === PART 1: MERGE HUC12 SHAPEFILES ===
echo "[1/5] Merging HUC12 shapefiles..."

if [ -f "$MERGED_HUC_FILE" ]; then
    echo "[INFO] Merged HUC file already exists. Skipping merge."
else
    HUC12_SHAPEFILES=$(find "${HUC_UNZIPPED_DIR}" -path "*/Shape/WBDHU12.shp")
    FIRST_SHP=$(echo "$HUC12_SHAPEFILES" | head -n 1)
    
    echo "  - Creating base file from: $FIRST_SHP"
    ogr2ogr -f "GPKG" -nlt PROMOTE_TO_MULTI "${MERGED_HUC_FILE}" "${FIRST_SHP}"

    echo "$HUC12_SHAPEFILES" | tail -n +2 | while read -r shp; do
        printf "  - Appending: %s\n" "$shp"
        ogr2ogr -f "GPKG" -append -update "${MERGED_HUC_FILE}" "${shp}"
    done
    echo "[SUCCESS] Merged all HUC12 shapefiles into ${MERGED_HUC_FILE}"
fi

# === PART 2: CREATE INDEXES ON GEOPACKAGE (CRITICAL FOR PERFORMANCE) ===
echo "[2/5] Creating indexes on HUC GeoPackage..."
# This step is essential. Without indexes, querying a single HUC from the
# large file is extremely slow (minutes), causing the entire workflow to hang.

echo "  - Creating attribute index on 'huc12' column..."
ogrinfo -sql "CREATE INDEX IF NOT EXISTS idx_wbdhu12_huc12 ON WBDHU12(huc12)" "${MERGED_HUC_FILE}"

echo "  - Creating spatial index on geometry column..."
ogrinfo -sql "SELECT CreateSpatialIndex('WBDHU12', 'geom')" "${MERGED_HUC_FILE}"

echo "[SUCCESS] Indexes created successfully."

# === PART 3: GENERATE LIST OF DEM FILES ===
echo "[3/5] Generating list of primary DEM files..."
find "${DEM_DIR}" -name "*_DEM.tif" > "${DEM_LIST_FILE}"
NUM_DEMS=$(wc -l < "${DEM_LIST_FILE}")

if [ "$NUM_DEMS" -eq 0 ]; then
  echo "[ERROR] No DEM files found matching the pattern '*_DEM.tif'."
  exit 1
fi
echo "[SUCCESS] Found ${NUM_DEMS} primary DEM files."

# === PART 4: VERIFY THE FILE LIST ===
echo "[4/5] Verifying DEM file list. Here are the first 5 files found:"
echo "-------------------------------------------------------------"
head -n 5 "${DEM_LIST_FILE}"
echo "-------------------------------------------------------------"
read -p "Does this list look correct? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting script."
    exit 1
fi

# === PART 5: BUILD DEM VIRTUAL RASTER (VRT) ===
echo "[5/5] Building Virtual Raster (VRT) from the file list..."
if [ -f "$DEM_VRT_FILE" ]; then
    echo "[INFO] DEM VRT file already exists. Overwriting."
fi
gdalbuildvrt -overwrite -input_file_list "${DEM_LIST_FILE}" "${DEM_VRT_FILE}"

if [ -f "$DEM_VRT_FILE" ]; then
    echo "[SUCCESS] Created DEM Virtual Raster at ${DEM_VRT_FILE}"
else
    echo "[ERROR] gdalbuildvrt failed to create the VRT file."
    exit 1
fi

echo "-------------------------------------------------------------"
echo "✅ Data preparation complete and optimized for performance."
echo "-------------------------------------------------------------"