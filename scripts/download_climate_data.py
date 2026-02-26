"""
ERA5-Land Europe-Wide Monthly Climate Data Download

Downloads the UNIFIED ERA5-Land batch dataset for all analytical queries.
This is a SINGLE download covering all of Europe (72°N to 34°N, 25°W to 45°E)
with 12 climate variables at 0.1° resolution, 1950-2024.

Expected size: ~3-5 GB (compressed NetCDF)
Expected download time: 1-4 hours depending on CDS queue

This ONE file supports all 10 batch queries and 5 streaming+batch queries.
"""

import cdsapi
import os
import sys
import time

# ===============================
# CONFIG
# ===============================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

RAW_DIR = os.path.join(PROJECT_ROOT, "Airflow", "files", "data", "climate")
OUTPUT_FILE = "era5_europe_monthly_1950_2024.nc"
OUTPUT_PATH = os.path.join(RAW_DIR, OUTPUT_FILE)

os.makedirs(RAW_DIR, exist_ok=True)

START_YEAR = 1950
END_YEAR = 2024

# Europe-wide bounding box [North, West, South, East]
# Covers: Scandinavia, UK/Ireland, Iberia, Italy, Balkans, Eastern Europe
AREA = [72.0, -25.0, 34.0, 45.0]

# All 12 variables needed for the 15 analytical queries
VARIABLES = [
    # Temperature & thermal (Q1, Q2, Q4, Q7, Q8, Q9, Q10, S1, S4, S5)
    "2m_temperature",
    "skin_temperature",
    # Hydrological (Q3, Q4, Q5, Q9, S3)
    "total_precipitation",
    "evaporation",
    "volumetric_soil_water_layer_1",
    # Wind (Q6, Q10, S2, S5)
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    # Radiation (Q7, Q10)
    "surface_solar_radiation_downwards",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    # Snow & pressure (Q3, Q9, S5)
    "snow_depth",
    "surface_pressure",
]

# ===============================
# CHECK EXISTING FILE
# ===============================

if os.path.exists(OUTPUT_PATH):
    file_size = os.path.getsize(OUTPUT_PATH)
    file_size_gb = file_size / (1024**3)
    print(f"[INFO] ERA5 Europe NetCDF already exists: {OUTPUT_PATH}")
    print(f"[INFO] File size: {file_size_gb:.2f} GB")
    if file_size_gb > 1.0:
        print("[INFO] File looks complete. Skipping download.")
        print("[INFO] Delete the file to force re-download.")
        sys.exit(0)
    else:
        print("[WARN] File seems too small (< 1 GB). Re-downloading...")
        os.remove(OUTPUT_PATH)

# ===============================
# CDS CLIENT
# ===============================

print("[INFO] Initializing CDS API client...")
try:
    c = cdsapi.Client()
except Exception as e:
    print(f"[ERROR] CDS API client init failed: {e}")
    print("[ERROR] Check ~/.cdsapirc credentials.")
    sys.exit(1)

# ===============================
# DOWNLOAD REQUEST
# ===============================

print("=" * 60)
print("ERA5-Land Europe Monthly Download")
print("=" * 60)
print(f"  Output:     {OUTPUT_PATH}")
print(f"  Variables:  {len(VARIABLES)}")
print(f"  Years:      {START_YEAR}-{END_YEAR} ({END_YEAR - START_YEAR + 1} years)")
print(f"  Months:     01-12")
print(f"  Area:       N={AREA[0]}, W={AREA[1]}, S={AREA[2]}, E={AREA[3]}")
print(f"  Grid:       ~0.1° (~380 x 700 = 266,000 grid points)")
print(f"  Est. size:  3-5 GB")
print(f"  Est. time:  1-4 hours")
print("=" * 60)

start_time = time.time()

try:
    c.retrieve(
        "reanalysis-era5-land-monthly-means",
        {
            "product_type": "monthly_averaged_reanalysis",
            "variable": VARIABLES,
            "year": [str(y) for y in range(START_YEAR, END_YEAR + 1)],
            "month": [f"{m:02d}" for m in range(1, 13)],
            "time": "00:00",
            "area": AREA,
            "format": "netcdf",
        },
        OUTPUT_PATH,
    )

    elapsed = time.time() - start_time
    file_size = os.path.getsize(OUTPUT_PATH)
    file_size_gb = file_size / (1024**3)

    print()
    print("=" * 60)
    print(f"[DONE] Download completed in {elapsed / 60:.1f} minutes")
    print(f"[DONE] Saved to: {OUTPUT_PATH}")
    print(f"[DONE] File size: {file_size_gb:.2f} GB")
    print("=" * 60)

except Exception as e:
    elapsed = time.time() - start_time
    print(f"\n[ERROR] Download failed after {elapsed / 60:.1f} minutes: {e}")
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)
        print("[INFO] Cleaned up partial download.")
    sys.exit(1)
