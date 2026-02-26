from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook


@dag(
    dag_id="load_raw_data",
    description="DAG loading raw climate data to HDFS (Kaggle CSV + ERA5 NetCDF).",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
    tags=["climate", "ingestion", "raw"],
)
def load_raw_data():

    @task
    def load_kaggle_csv():
        """Load Kaggle Global Land Temperatures CSV to HDFS raw zone."""
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")

        local_source = "/opt/airflow/files/data/GlobalLandTemperaturesByCity.csv"
        hdfs_destination = "/data/raw/climate/temperatures/GlobalLandTemperaturesByCity.csv"
        
        print(f"Loading Kaggle CSV from {local_source} to {hdfs_destination}")
        hdfs_hook.load_file(
            source=local_source, destination=hdfs_destination, overwrite=True
        )

        return hdfs_destination

    @task
    def load_era5_netcdf():
        """Load ERA5-Land Europe-wide NetCDF file to HDFS raw zone."""
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")

        local_source = "/opt/airflow/files/data/climate/era5_europe_monthly_1950_2024.nc"
        hdfs_destination = "/data/raw/climate/era5/era5_europe_monthly_1950_2024.nc"
        
        print(f"Loading ERA5 NetCDF from {local_source} to {hdfs_destination}")
        hdfs_hook.load_file(
            source=local_source, destination=hdfs_destination, overwrite=True
        )

        return hdfs_destination

    @task
    def check_hdfs_files(file_paths: list):
        """Verify that all files were successfully loaded to HDFS."""
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")

        for file_path in file_paths:
            if hdfs_hook.check_for_path(hdfs_path=file_path):
                print(f"✓ File {file_path} found in HDFS.")
            else:
                print(f"✗ File {file_path} not found in HDFS.")
                raise FileNotFoundError(f"File {file_path} not found in HDFS")

    # Task dependencies
    csv_path = load_kaggle_csv()
    era5_path = load_era5_netcdf()
    check_hdfs_files([csv_path, era5_path])


load_raw_data()
