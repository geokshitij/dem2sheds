## dem2sheds

Workflow for building a CONUS-wide collection of watershed DEMs:
- Download Copernicus GLO-30 tiles that cover CONUS.
- Download and unpack Watershed Boundary Dataset (WBD) HU2 shapefiles.
- Merge all HUC12 polygons, index them, and build a DEM VRT.
- Clip the DEM to every HUC12 polygon (either on a single node with GNU Parallel or via Slurm array jobs).

### Data sources
- DEM: Copernicus DEM GLO-30 (30m) from the public S3 bucket `s3://copernicus-dem-30m`.
- Watersheds: USGS WBD HU2 shapefiles from `https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/HU2/Shape`.

### Prerequisites
- AWS CLI (no credentials needed for the public bucket)
- GDAL tools (`gdalwarp`, `gdalbuildvrt`, `ogr2ogr`, `ogrinfo`)
- GNU Parallel
- unzip, wget, awk, findutils
- Slurm (only for the array-based workflows)

### Directory assumptions (edit in scripts if needed)
```
/scratch/kdahal3/DEM_CONUS_30m          # raw DEM tiles
/scratch/kdahal3/DEM_CONUS/WBD_unzipped # extracted WBD HU2 zips
/scratch/kdahal3/DEM_CONUS/processed    # merged HUC GPKG + DEM VRT + file lists
/scratch/kdahal3/DEM_CONUS/clipped_huc12_dems # per-HUC12 clipped DEMs + chunks/logs
```

### Quick start
1) Download datasets  
```bash
# DEM tiles (~30m GLO-30)
./download.sh

# WBD HU2 zips, then extract them
./download_wbd_zips.sh
./extract_wbd.sh
```

2) Prepare merged inputs (GeoPackage + VRT)  
```bash
# Merges all WBDHU12 shapefiles, builds attribute/spatial indexes,
# creates a DEM file list, and builds a CONUS DEM VRT.
./prepare_data.sh
```

3) Clip DEMs by HUC12 (two options)
- **Single-node parallel:** Uses GNU Parallel; good for a single machine with many cores.  
  ```bash
  ./clip_dem_by_huc12.sh
  ```
- **Slurm array (chunked):** Breaks the HUC list into chunks and runs an array job.  
  ```bash
  # One-time list -> chunks -> submit -> verify
  ./01_prepare_huc_list.sh
  ./02_create_chunks.sh
  ./03_submit_job.sh        # sbatch array submission
  ./03_verify_output.sh
  ```
- **Auto-chunk + submit (Slurm):** One script that sizes chunks from a runtime target and submits the array.  
  ```bash
  ./submit_clip_array.sh --minutes=20 --cpus-per-task=4 --concurrency=50
  # Use --dry-run to inspect without submitting.
  ```

### Script overview
- `download.sh` — Lists CONUS tiles from the Copernicus bucket, filters by lat/lon, and downloads in parallel; verifies a sample with `gdalinfo`.
- `download_wbd_zips.sh` / `extract_wbd.sh` — Fetch HU2 WBD zips (01–22) and unzip to `WBD_unzipped/`.
- `prepare_data.sh` — Merges all `WBDHU12.shp` files into `WBD_CONUS_HUC12.gpkg`, creates an attribute + spatial index, writes a DEM file list, and builds `CONUS_DEM_30m.vrt`.
- `clip_dem_by_huc12.sh` — Reads HUC IDs from the GeoPackage and runs `gdalwarp` per HUC in parallel; resumable (skips existing outputs).
- `01_prepare_huc_list.sh` → `02_create_chunks.sh` → `03_submit_job.sh` → `03_verify_output.sh` — Classic Slurm chunked workflow (chunk files named `chunk_0000.txt`, etc.; `process_chunk.sbatch` does the per-chunk clipping).
- `submit_clip_array.sh` — Single-entry Slurm workflow: builds the HUC list, estimates chunk sizes from prior runtime stats, writes a temporary sbatch script, and submits an array with concurrency limits and locking to avoid races.

### Notes and tips
- Change path variables at the top of each script if your storage layout differs.
- Scripts are resumable: clipping steps skip existing GeoTIFFs; Slurm variants also use `.done` markers/locks to avoid duplicate work.
- Increase or decrease `THREADS`, `CPUS_PER_TASK`, and `CONCURRENCY` to match your I/O and scheduler limits.
- Verify inputs: `prepare_data.sh` pauses to show sample DEM filenames before building the VRT.
- Outputs: per-HUC12 DEMs are written to `clipped_huc12_dems/{HUC12}.tif`; logs for Slurm runs live under `clipped_huc12_dems/slurm_logs`.
