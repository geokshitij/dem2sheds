#!/usr/bin/env bash
#
# submit_clip_array.sh
#
# Single-script workflow:
#  - Create HUC list from GeoPackage
#  - Estimate per-file runtime from historical run (default: 128 cores * 5 hours for 103000 files)
#  - Compute chunk sizes for target job length (default 20 minutes)
#  - Split HUC list into chunk files
#  - Create SLURM array sbatch script that processes each chunk
#  - Submit array with concurrency limit
#
# Usage:
#   ./submit_clip_array.sh [--minutes=20] [--cpus-per-task=4] [--concurrency=50] [--array-name=clip_huc12] [--dry-run]
#
# Example:
#   ./submit_clip_array.sh --minutes=15 --cpus-per-task=4 --concurrency=50
#

set -euo pipefail

#######  USER-CONFIGURABLE SETTINGS (change here or via CLI args) #######
# Data paths (edit if needed)
PROCESSED_DIR="/scratch/kdahal3/DEM_CONUS/processed"
MERGED_HUC_FILE="${PROCESSED_DIR}/WBD_CONUS_HUC12.gpkg"
HUC_LAYER_NAME="WBDHU12"
HUC_ID_FIELD="huc12"
DEM_VRT_FILE="${PROCESSED_DIR}/CONUS_DEM_30m.vrt"
CLIPPED_DEM_DIR="/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems"

# Prior run stats (used for runtime estimate)
# You told me: used to take 128 cores and 5 hours to run all 103K files.
PRIOR_CORES=128
PRIOR_HOURS=5
PRIOR_TOTAL_FILES=103000

# Defaults for splitting / submission
TARGET_MINUTES_DEFAULT=20     # aim for jobs ~10-20m; default 20
CPUS_PER_TASK_DEFAULT=4
CONCURRENCY_DEFAULT=50       # limit of concurrent array tasks (tune for I/O)
SBATCH_TIME_DEFAULT="00:25:00" # walltime for each array task (must be >= TARGET_MINUTES; set automatically below)
SBATCH_CPUS_PER_TASK_DEFAULT=4
SBATCH_MEM="12G"
SBATCH_PARTITION=""  # leave empty for default; set if needed e.g. "compute"
SBATCH_EXTRA="#SBATCH --mail-type=END,FAIL" # extra sbatch options, can be overridden via CLI
JOB_NAME_DEFAULT="clip_huc12"
SLURM_LOG_DIR="${CLIPPED_DEM_DIR}/slurm_logs"
#########################################################################

# CLI arg parsing (simple)
TARGET_MINUTES=${TARGET_MINUTES_DEFAULT}
CPUS_PER_TASK=${CPUS_PER_TASK_DEFAULT}
CONCURRENCY=${CONCURRENCY_DEFAULT}
JOB_NAME=${JOB_NAME_DEFAULT}
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --minutes=*) TARGET_MINUTES="${1#*=}" ;;
    --cpus-per-task=*) CPUS_PER_TASK="${1#*=}" ;;
    --concurrency=*) CONCURRENCY="${1#*=}" ;;
    --job-name=*) JOB_NAME="${1#*=}" ;;
    --dry-run) DRY_RUN=1 ;;
    --help|-h) echo "Usage: $0 [--minutes=N] [--cpus-per-task=N] [--concurrency=N] [--job-name=NAME] [--dry-run]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
  shift
done

# Simple safety checks
if [ ! -f "${MERGED_HUC_FILE}" ]; then
  echo "[ERROR] MERGED_HUC_FILE not found: ${MERGED_HUC_FILE}"
  exit 1
fi
if [ ! -f "${DEM_VRT_FILE}" ]; then
  echo "[ERROR] DEM_VRT_FILE not found: ${DEM_VRT_FILE}"
  exit 1
fi

mkdir -p "${CLIPPED_DEM_DIR}" "${SLURM_LOG_DIR}"

TMPDIR="${CLIPPED_DEM_DIR}/tmp"
CHUNK_DIR="${TMPDIR}/chunks"
STATUS_DIR="${CLIPPED_DEM_DIR}/status"
LOCK_DIR="${CLIPPED_DEM_DIR}/locks"
mkdir -p "${TMPDIR}" "${CHUNK_DIR}" "${STATUS_DIR}" "${LOCK_DIR}"

HUC_LIST="${TMPDIR}/huc12_list.txt"

echo "-------------------------------------------------------------"
echo "Submit clip HUC12 array workflow"
echo "  MERGED_HUC_FILE: ${MERGED_HUC_FILE}"
echo "  DEM_VRT_FILE:    ${DEM_VRT_FILE}"
echo "  OUTPUT_DIR:      ${CLIPPED_DEM_DIR}"
echo "  CHUNK_DIR:       ${CHUNK_DIR}"
echo "  TARGET_MINUTES:  ${TARGET_MINUTES}"
echo "  CPUS_PER_TASK:   ${CPUS_PER_TASK}"
echo "  CONCURRENCY:     ${CONCURRENCY}"
echo "-------------------------------------------------------------"

# Step 1: generate HUC list
echo "[1/6] Generating HUC list..."
# Use ogrinfo output parsing (same method you used)
ogrinfo -ro -q -sql "SELECT ${HUC_ID_FIELD} FROM ${HUC_LAYER_NAME}" "${MERGED_HUC_FILE}" | \
  grep "${HUC_ID_FIELD} (String)" | awk -F' = ' '{print $2}' > "${HUC_LIST}"

NUM_HUCS=$(wc -l < "${HUC_LIST}" | tr -d ' ')
echo "[INFO] Found ${NUM_HUCS} HUC IDs."

if [ "${NUM_HUCS}" -eq 0 ]; then
  echo "[ERROR] HUC list empty. Aborting."
  exit 1
fi

# Step 2: estimate average seconds per file from prior stats (safe floating math via awk)
AVG_SEC_PER_FILE=$(awk -v cores="${PRIOR_CORES}" -v hours="${PRIOR_HOURS}" -v total="${PRIOR_TOTAL_FILES}" 'BEGIN{print (cores*hours*3600)/total}')
# compute target seconds (min -> sec)
TARGET_SECONDS=$(( TARGET_MINUTES * 60 ))
# expected files per core in target duration
# files_per_job = cpus * (target_seconds / avg_sec_per_file)
FILES_PER_JOB_FLOAT=$(awk -v cpus="${CPUS_PER_TASK}" -v tsec="${TARGET_SECONDS}" -v avg="${AVG_SEC_PER_FILE}" 'BEGIN{printf "%f", (cpus * tsec / avg)}')
# ceil to integer, with minimum 1
FILES_PER_JOB=$(awk -v f="${FILES_PER_JOB_FLOAT}" 'BEGIN{f2=int(f); if(f>f2) f2++; if(f2<1) f2=1; print f2}')
# compute number of chunks
NUM_CHUNKS=$(( (NUM_HUCS + FILES_PER_JOB - 1) / FILES_PER_JOB ))

echo "[2/6] Runtime estimate and chunking"
echo "  Historical estimate: ${PRIOR_CORES} cores * ${PRIOR_HOURS} h -> ${PRIOR_TOTAL_FILES} files"
echo "  Avg seconds / file (estimated) = ${AVG_SEC_PER_FILE}"
echo "  Target job length = ${TARGET_MINUTES} minutes (${TARGET_SECONDS} sec)"
echo "  CPUS_PER_TASK = ${CPUS_PER_TASK}"
echo "  Calculated files_per_job (ceil) = ${FILES_PER_JOB} (approx ${FILES_PER_JOB_FLOAT})"
echo "  Will create ${NUM_CHUNKS} chunks (array size)"
echo

# Step 3: split into chunk files (remove old chunks first)
echo "[3/6] Creating chunk files in ${CHUNK_DIR}..."
rm -f "${CHUNK_DIR}"/chunk_*.txt
if [ "${FILES_PER_JOB}" -ge "${NUM_HUCS}" ]; then
  # everything in one chunk
  cp "${HUC_LIST}" "${CHUNK_DIR}/chunk_00.txt"
  NUM_CHUNKS=1
else
  # use split with numeric suffixes; choose width based on NUM_CHUNKS
  WIDTH=$(printf "%02d" "${NUM_CHUNKS}" | wc -c) # approximate width
  # safer: create using split -l
  split -d -l "${FILES_PER_JOB}" --additional-suffix=.txt "${HUC_LIST}" "${CHUNK_DIR}/chunk_"
  # rename to have zero-padded 2-digit suffixes if needed (split produces chunk_00, chunk_01 ... already)
fi
# Count chunks created
ACTUAL_CHUNKS=$(ls -1 "${CHUNK_DIR}"/chunk_*.txt 2>/dev/null | wc -l | tr -d ' ')
if [ "${ACTUAL_CHUNKS}" -eq 0 ]; then
  echo "[ERROR] No chunk files created. Aborting."
  exit 1
fi
echo "[INFO] Chunk files created: ${ACTUAL_CHUNKS}"

# Adjust NUM_CHUNKS to actual
NUM_CHUNKS=${ACTUAL_CHUNKS}

# Step 4: write sbatch array script
SBATCH_SCRIPT="${TMPDIR}/slurm_clip_array.sbatch.sh"
echo "[4/6] Writing sbatch script to ${SBATCH_SCRIPT}..."

# pick a sbatch time slightly larger than TARGET_MINUTES (add safety margin)
# compute SBATCH_TIME automatically: TARGET_MINUTES + 6 minutes margin
MINS_MARGIN=6
SBATCH_MINS=$(( TARGET_MINUTES + MINS_MARGIN ))
# format HH:MM:SS
H=$(( SBATCH_MINS / 60 ))
M=$(( SBATCH_MINS % 60 ))
SBATCH_TIME=$(printf "%02d:%02d:00" "${H}" "${M}")

cat > "${SBATCH_SCRIPT}" <<'SBATCH_EOF'
#!/usr/bin/env bash
#SBATCH --job-name=__JOB_NAME__
#SBATCH --output=__LOG_DIR__/clip_%A_%a.out
#SBATCH --error=__LOG_DIR__/clip_%A_%a.err
#SBATCH --time=__SBATCH_TIME__
#SBATCH --cpus-per-task=__CPUS_PER_TASK__
#SBATCH --mem=__SBATCH_MEM__
__SBATCH_PART__
__SBATCH_EXTRA__
#SBATCH --array=1-__NUM_CHUNKS__%__CONCURRENCY__

set -euo pipefail

# CONFIG (inherited via substitution)
PROCESSED_DIR="__PROCESSED_DIR__"
MERGED_HUC_FILE="__MERGED_HUC_FILE__"
HUC_LAYER_NAME="__HUC_LAYER_NAME__"
HUC_ID_FIELD="__HUC_ID_FIELD__"
DEM_VRT_FILE="__DEM_VRT_FILE__"
CLIPPED_DEM_DIR="__CLIPPED_DEM_DIR__"
TMPDIR="__TMPDIR__"
CHUNK_DIR="__CHUNK_DIR__"
STATUS_DIR="__STATUS_DIR__"
LOCK_DIR="__LOCK_DIR__"

# ensure directories exist
mkdir -p "${CLIPPED_DEM_DIR}" "${TMPDIR}" "${CHUNK_DIR}" "${STATUS_DIR}" "${LOCK_DIR}"

TASK_ID=${SLURM_ARRAY_TASK_ID:-1}
# chunk files are zero-based chunk_00... so task_id - 1 -> file index
IDX=$((TASK_ID-1))
# determine zero-padded suffix width from available files
CHUNK_FILE=$(ls -1 "${CHUNK_DIR}"/chunk_*.txt | sed -n "$((IDX+1))p")
if [ -z "${CHUNK_FILE}" ]; then
  echo "[WARN] Chunk file for index ${IDX} not found; exiting."
  exit 0
fi

echo "[INFO] SLURM task ${TASK_ID} on $(hostname) processing ${CHUNK_FILE}"
echo "[INFO] cpus: ${SLURM_CPUS_ON_NODE:-$SLURM_CPUS_PER_TASK}"

# small randomized sleep to desynchronize startup and reduce I/O spikes
sleep $((RANDOM % 6))

process_huc12() {
  local HUC12_ID="$1"
  local OUT_RASTER="${CLIPPED_DEM_DIR}/${HUC12_ID}.tif"
  local DONE_MARKER="${STATUS_DIR}/${HUC12_ID}.done"
  local LOCK_PATH="${LOCK_DIR}/${HUC12_ID}.lock"
  local TMP_OUT="${TMPDIR}/${HUC12_ID}.tif.$$"

  # skip if done + output exists
  if [ -f "${DONE_MARKER}" ] && [ -f "${OUT_RASTER}" ]; then
    echo "[SKIP] ${HUC12_ID} already done"
    return 0
  fi

  # try to acquire lock (atomic mkdir)
  if ! mkdir "${LOCK_PATH}" 2>/dev/null; then
    echo "[LOCKED] ${HUC12_ID} locked by another process, skipping"
    return 0
  fi
  trap 'rm -rf "${LOCK_PATH}"' RETURN

  # remove stale tmp if any
  rm -f "${TMP_OUT}"

  if gdalwarp -q \
      -cutline "${MERGED_HUC_FILE}" \
      -cl "${HUC_LAYER_NAME}" \
      -cwhere "${HUC_ID_FIELD} = '${HUC12_ID}'" \
      -crop_to_cutline \
      -dstnodata -9999 \
      -co "COMPRESS=LZW" -co "PREDICTOR=2" \
      "${DEM_VRT_FILE}" "${TMP_OUT}"; then

      mv -f "${TMP_OUT}" "${OUT_RASTER}"
      touch "${DONE_MARKER}"
      echo "[DONE] ${HUC12_ID}"
      return 0
  else
      echo "[ERROR] gdalwarp failed for ${HUC12_ID}"
      rm -f "${TMP_OUT}"
      return 1
  fi
}

export -f process_huc12

# prefer GNU parallel if available; otherwise run a simple background-limited loop
CPU_COUNT=${SLURM_CPUS_PER_TASK:-1}
if command -v parallel >/dev/null 2>&1; then
  cat "${CHUNK_FILE}" | parallel -j "${CPU_COUNT}" --halt soon,fail=1 process_huc12 {}
else
  i=0
  for H in $(cat "${CHUNK_FILE}"); do
    process_huc12 "${H}" &
    ((i++))
    if [ "${i}" -ge "${CPU_COUNT}" ]; then
      wait
      i=0
    fi
  done
  wait
fi

echo "[INFO] Task ${TASK_ID} finished."
SBATCH_EOF

# substitute variables into sbatch script
# prepare SBATCH_PART if partition set
if [ -n "${SBATCH_PARTITION}" ]; then
  SBATCH_PART="#SBATCH --partition=${SBATCH_PARTITION}"
else
  SBATCH_PART=""
fi

# now perform substitutions
sed -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__LOG_DIR__|${SLURM_LOG_DIR}|g" \
    -e "s|__SBATCH_TIME__|${SBATCH_TIME}|g" \
    -e "s|__CPUS_PER_TASK__|${CPUS_PER_TASK}|g" \
    -e "s|__SBATCH_MEM__|${SBATCH_MEM}|g" \
    -e "s|__SBATCH_PART__|${SBATCH_PART}|g" \
    -e "s|__SBATCH_EXTRA__|${SBATCH_EXTRA}|g" \
    -e "s|__NUM_CHUNKS__|${NUM_CHUNKS}|g" \
    -e "s|__CONCURRENCY__|${CONCURRENCY}|g" \
    -e "s|__PROCESSED_DIR__|${PROCESSED_DIR}|g" \
    -e "s|__MERGED_HUC_FILE__|${MERGED_HUC_FILE}|g" \
    -e "s|__HUC_LAYER_NAME__|${HUC_LAYER_NAME}|g" \
    -e "s|__HUC_ID_FIELD__|${HUC_ID_FIELD}|g" \
    -e "s|__DEM_VRT_FILE__|${DEM_VRT_FILE}|g" \
    -e "s|__CLIPPED_DEM_DIR__|${CLIPPED_DEM_DIR}|g" \
    -e "s|__TMPDIR__|${TMPDIR}|g" \
    -e "s|__CHUNK_DIR__|${CHUNK_DIR}|g" \
    -e "s|__STATUS_DIR__|${STATUS_DIR}|g" \
    -e "s|__LOCK_DIR__|${LOCK_DIR}|g" \
    "${SBATCH_SCRIPT}" > "${SBATCH_SCRIPT}.final"
chmod +x "${SBATCH_SCRIPT}.final"

# Step 5: final report & dry-run option
echo "[5/6] Summary:"
echo "  Total HUCs:          ${NUM_HUCS}"
echo "  Files per job (est): ${FILES_PER_JOB}"
echo "  Array size:          ${NUM_CHUNKS}"
echo "  SBATCH script:       ${SBATCH_SCRIPT}.final"
echo "  Logs dir:            ${SLURM_LOG_DIR}"
echo "  Status dir:          ${STATUS_DIR}"
echo "  Lock dir:            ${LOCK_DIR}"
echo
echo "  sbatch time per task: ${SBATCH_TIME}"
echo

if [ "${DRY_RUN}" -eq 1 ]; then
  echo "[DRY RUN] Not submitting array. Exiting."
  exit 0
fi

# Step 6: submit sbatch array
echo "[6/6] Submitting SLURM array..."
SUBMIT_CMD="sbatch ${SBATCH_SCRIPT}.final"
echo "[CMD] ${SUBMIT_CMD}"
${SUBMIT_CMD}

echo "Submitted. Use 'squeue -u \$USER' to monitor, and inspect ${SLURM_LOG_DIR} for logs."
echo "You can re-run this script (or just re-submit the sbatch) — completed HUCs are skipped via .done markers."
echo "-------------------------------------------------------------"

