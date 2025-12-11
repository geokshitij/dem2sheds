#!/usr/bin/env bash
# -------------------------------------------------------------
# Script: download_conus_copdem30.sh
# Purpose: Download Copernicus DEM GLO-30 (30m) tiles for CONUS
# Source: https://registry.opendata.aws/copernicus-dem
# Managed by: Sinergise
# License: Copernicus Open Data License
# -------------------------------------------------------------

# === CONFIGURATION ===
OUTDIR="/scratch/kdahal3/DEM_CONUS_30m"   # output directory
THREADS=126                                # number of parallel downloads
BUCKET="s3://copernicus-dem-30m"
TMPDIR="${OUTDIR}/tmp"

# Approximate bounding box for CONUS
LAT_MIN=24
LAT_MAX=50
LON_MIN=-125
LON_MAX=-66

# === SETUP ===
mkdir -p "${OUTDIR}" "${TMPDIR}"
echo "-------------------------------------------------------------"
echo "Downloading Copernicus GLO-30 DEM for CONUS"
echo "Threads: ${THREADS}"
echo "Output Directory: ${OUTDIR}"
echo "-------------------------------------------------------------"
sleep 2

# === STEP 1: LIST ALL TILES ===
echo "[1/4] Listing all available tiles from S3..."
aws s3 ls --no-sign-request ${BUCKET}/ --recursive | grep '.tif' > "${TMPDIR}/all_tiles.txt"

# === STEP 2: FILTER TILES BY LAT/LON ===
echo "[2/4] Filtering tiles within CONUS bounds..."
awk -v latmin=$LAT_MIN -v latmax=$LAT_MAX -v lonmin=$LON_MIN -v lonmax=$LON_MAX '
{
  match($4, /N([0-9]{2})_00_W([0-9]{3})_00|N([0-9]{2})_00_E([0-9]{3})_00|S([0-9]{2})_00_W([0-9]{3})_00|S([0-9]{2})_00_E([0-9]{3})_00/, a)
  lat = (a[1] != "") ? a[1] : (a[3] != "") ? a[3] : (a[5] != "") ? -a[5] : -a[7]
  lon = (a[2] != "") ? -a[2] : (a[4] != "") ? a[4] : (a[6] != "") ? -a[6] : a[8]
  if (lat >= latmin && lat <= latmax && lon >= lonmin && lon <= lonmax)
    print $4
}' "${TMPDIR}/all_tiles.txt" > "${TMPDIR}/conus_tiles.txt"

NUM_TILES=$(wc -l < "${TMPDIR}/conus_tiles.txt")
echo "[INFO] Found ${NUM_TILES} tiles covering CONUS."

if [ "$NUM_TILES" -eq 0 ]; then
  echo "[ERROR] No tiles found. Check bounds or bucket path."
  exit 1
fi

# === STEP 3: PARALLEL DOWNLOAD ===
echo "[3/4] Downloading tiles in parallel (${THREADS} threads)..."
cat "${TMPDIR}/conus_tiles.txt" | \
parallel -j ${THREADS} --eta \
"aws s3 cp --no-sign-request ${BUCKET}/{} ${OUTDIR}/{}"

echo "[INFO] Downloads complete."

# === STEP 4: VERIFY SAMPLE TILE ===
SAMPLE=$(head -n 1 "${TMPDIR}/conus_tiles.txt")
echo "[4/4] Checking sample file metadata:"
gdalinfo "${OUTDIR}/${SAMPLE}" | head -n 10 || echo "[WARN] GDAL not installed or check skipped."

echo "-------------------------------------------------------------"
echo "✅ Copernicus GLO-30 DEM for CONUS successfully downloaded."
echo "Output: ${OUTDIR}"
echo "Tiles: ${NUM_TILES}"
echo "-------------------------------------------------------------"
