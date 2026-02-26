from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="transform_raw_data",
    description="DAG transforming raw climate data (Kaggle CSV + ERA5 NetCDF) to Parquet.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    catchup=False,
    tags=["climate", "transformation", "parquet"],
)
def transform_raw_data():

    HDFS_DEFAULT_FS = "{{ var.value.HDFS_DEFAULT_FS }}"

    transform_kaggle_temperatures = SparkSubmitOperator(
        task_id="transform_kaggle_temperatures",
        application="/opt/airflow/files/spark/transformation_jobs/transform_kaggle_temperatures.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf={
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
        },
        env_vars={"PYSPARK_PYTHON": "python3", "PYSPARK_DRIVER_PYTHON": "python3"},
        application_args=[
            f"{HDFS_DEFAULT_FS}/data/raw/climate/temperatures/GlobalLandTemperaturesByCity.csv",
            f"{HDFS_DEFAULT_FS}/data/transformed/climate/temperatures",
        ],
    )

    # ERA5 uses local[*] because netCDF4 runs on the driver only
    # and createDataFrame needs matching Python version on workers.
    transform_era5_netcdf = BashOperator(
        task_id="transform_era5_netcdf",
        bash_command=(
            "$SPARK_HOME/bin/spark-submit "
            "--master 'local[*]' "
            "--driver-memory 3g "
            "/opt/airflow/files/spark/transformation_jobs/transform_era5_netcdf.py "
            "/opt/airflow/files/data/climate/era5_europe_monthly_1950_2024.nc "
            "hdfs://namenode:9000/data/transformed/climate/era5"
        ),
        env={
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
            "SPARK_HOME": "/opt/spark-3.0.1-bin-hadoop3.2",
            "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
            "PATH": "/usr/local/bin:/usr/bin:/bin:/opt/spark-3.0.1-bin-hadoop3.2/bin",
        },
    )

    # Both transformations can run in parallel
    [transform_kaggle_temperatures, transform_era5_netcdf]


transform_raw_data()
