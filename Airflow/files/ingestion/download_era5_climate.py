"""
ERA5-Land Climate Data Ingestion Script

Downloads ERA5-Land monthly climate data (1950-2024) as NetCDF file.
This is a raw data ingestion step - no filtering or aggregation is performed.

Prerequisites:
- cdsapi package installed
- ~/.cdsapirc file configured with CDS API credentials
  Format:
    url: https://cds.climate.copernicus.eu/api/v2
    key: <YOUR_UID>:<YOUR_API_KEY>

Output:
- Stores raw NetCDF file in HDFS-compatible path for downstream processing
"""

import cdsapi
import os
import sys

# ===============================
# CONFIG
# ===============================

# When running in Docker, this path should be accessible to HDFS/Spark
RAW_DIR = "/opt/airflow/files/data/climate"
OUTPUT_FILE = "era5_land_monthly.nc"
OUTPUT_PATH = os.path.join(RAW_DIR, OUTPUT_FILE)

# Ensure raw directory exists
os.makedirs(RAW_DIR, exist_ok=True)

# ===============================
# CHECK IF FILE EXISTS
# ===============================

if os.path.exists(OUTPUT_PATH):
    file_size = os.path.getsize(OUTPUT_PATH)
    print(f"[INFO] ERA5-Land NetCDF already exists: {OUTPUT_PATH}")
    print(f"[INFO] File size: {file_size / (1024**3):.2f} GB")
    print("[INFO] Skipping download. Delete file to re-download.")
    sys.exit(0)

# ===============================
# CDS CLIENT
# ===============================

print("[INFO] Initializing CDS API client...")
try:
    c = cdsapi.Client()
except Exception as e:
    print(f"[ERROR] Failed to initialize CDS API client: {e}")
    print("[ERROR] Ensure ~/.cdsapirc is configured with valid credentials.")
    sys.exit(1)

# ===============================
# DOWNLOAD ERA5-LAND
# ===============================

print("[INFO] Starting ERA5-Land data download...")
print(f"[INFO] Target path: {OUTPUT_PATH}")
print("[INFO] Variables: 2m_temperature, total_precipitation, surface_net_solar_radiation,")
print("[INFO]            volumetric_soil_water_layer_1, 10m wind components")
print("[INFO] Time range: 1950-2024 (monthly means)")
print("[INFO] This may take several minutes to hours depending on data size...")

try:
    c.retrieve(
        "reanalysis-era5-land-monthly-means",
        {
            "product_type": "monthly_averaged_reanalysis",
            "variable": [
                "2m_temperature",
                "total_precipitation",
                "surface_net_solar_radiation",
                "volumetric_soil_water_layer_1",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
            ],
            "year": [str(y) for y in range(1950, 2025)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "time": "00:00",
            "format": "netcdf",
        },
        OUTPUT_PATH,
    )
    
    file_size = os.path.getsize(OUTPUT_PATH)
    print(f"\n[OK] ERA5-Land NetCDF downloaded successfully!")
    print(f"[OK] Location: {OUTPUT_PATH}")
    print(f"[OK] Size: {file_size / (1024**3):.2f} GB")
    
except Exception as e:
    print(f"\n[ERROR] Download failed: {e}")
    # Clean up partial download
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)
        print("[INFO] Cleaned up partial download.")
    sys.exit(1)
