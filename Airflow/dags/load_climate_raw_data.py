"""
Airflow DAG: Load Climate Raw Data

This DAG handles the ingestion of ERA5-Land climate data into the raw zone.
It downloads monthly climate data (1950-2024) as NetCDF format from 
Copernicus Climate Data Store (CDS).

Prerequisites:
1. CDS API account and credentials
2. ~/.cdsapirc file mounted or created with format:
   url: https://cds.climate.copernicus.eu/api/v2
   key: <YOUR_UID>:<YOUR_API_KEY>

Data Flow:
- Source: Copernicus Climate Data Store (CDS API)
- Destination: /opt/airflow/files/data/climate/era5_land_monthly.nc
- Format: NetCDF (.nc)
- Size: ~5-10 GB (depends on variables and time range)

The downloaded file is stored in the shared volume accessible to:
- Airflow tasks
- Spark transformation jobs
- HDFS (via volume mapping)

This is a RAW ingestion step - no processing, filtering, or aggregation.
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.standard.operators.python import PythonOperator
import subprocess
import os


@dag(
    dag_id="load_climate_raw_data",
    description="DAG for loading ERA5-Land climate data to raw zone.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
    tags=["climate", "ingestion", "raw", "era5"],
)
def load_climate_raw_data():
    """
    Climate data ingestion DAG.
    
    Tasks:
    1. check_cdsapi_config: Verify CDS API credentials are configured
    2. download_era5_data: Execute Python script to download ERA5-Land NetCDF
    3. verify_download: Confirm the NetCDF file was created successfully
    """

    @task
    def check_cdsapi_config():
        """
        Verify that CDS API credentials are configured.
        
        Checks for ~/.cdsapirc file which should contain:
        url: https://cds.climate.copernicus.eu/api/v2
        key: <YOUR_UID>:<YOUR_API_KEY>
        """
        cdsapirc_path = os.path.expanduser("~/.cdsapirc")
        
        if not os.path.exists(cdsapirc_path):
            raise FileNotFoundError(
                f"CDS API config not found at {cdsapirc_path}. "
                "Please create this file with your CDS API credentials. "
                "Visit: https://cds.climate.copernicus.eu/api-how-to"
            )
        
        # Verify file is readable
        with open(cdsapirc_path, "r") as f:
            content = f.read()
            if "url:" not in content or "key:" not in content:
                raise ValueError(
                    f"Invalid CDS API config format in {cdsapirc_path}. "
                    "Expected 'url:' and 'key:' fields."
                )
        
        print(f"✓ CDS API config verified at {cdsapirc_path}")
        return cdsapirc_path

    @task
    def download_era5_data():
        """
        Execute the ERA5-Land download script.
        
        The script:
        - Checks if file already exists (idempotent)
        - Downloads ERA5-Land monthly data (1950-2024)
        - Stores NetCDF in /opt/airflow/files/data/climate/
        - Handles errors and partial downloads
        """
        script_path = "/opt/airflow/files/ingestion/download_era5_climate.py"
        
        print(f"Executing climate data ingestion script: {script_path}")
        
        # Run the download script using Python
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            check=False
        )
        
        # Print stdout and stderr
        print("--- Script Output ---")
        print(result.stdout)
        if result.stderr:
            print("--- Script Errors ---")
            print(result.stderr)
        
        # Check return code
        if result.returncode != 0:
            raise RuntimeError(
                f"ERA5 download script failed with exit code {result.returncode}. "
                f"Check logs above for details."
            )
        
        print("✓ ERA5-Land data download completed successfully")
        return script_path

    @task
    def verify_download(script_path):
        """
        Verify that the NetCDF file was created and is valid.
        
        Checks:
        - File exists
        - File size is reasonable (> 100 MB)
        - File is readable
        """
        netcdf_path = "/opt/airflow/files/data/climate/era5_land_monthly.nc"
        
        if not os.path.exists(netcdf_path):
            raise FileNotFoundError(
                f"NetCDF file not found at {netcdf_path}. "
                "Download may have failed."
            )
        
        file_size = os.path.getsize(netcdf_path)
        file_size_gb = file_size / (1024**3)
        
        # Sanity check: ERA5-Land monthly data should be reasonably sized
        if file_size < 100 * 1024 * 1024:  # Less than 100 MB is suspicious
            raise ValueError(
                f"NetCDF file size ({file_size_gb:.2f} GB) is suspiciously small. "
                "Download may be incomplete."
            )
        
        print(f"✓ NetCDF file verified:")
        print(f"  Path: {netcdf_path}")
        print(f"  Size: {file_size_gb:.2f} GB")
        print(f"  Status: Ready for transformation")
        
        return netcdf_path

    # Define task dependencies
    cdsapi_config = check_cdsapi_config()
    download_result = download_era5_data()
    verification = verify_download(download_result)
    
    # Task flow: check config -> download -> verify
    cdsapi_config >> download_result >> verification


# Instantiate the DAG
load_climate_raw_data()
