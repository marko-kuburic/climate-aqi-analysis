from datetime import datetime

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


MONGO_JARS_CONF = {
    "spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
    "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
    "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
    "/opt/spark/jars/bson-4.8.2.jar",
    "spark.executor.extraClassPath": "/opt/spark/jars/*",
    "spark.driver.extraClassPath": "/opt/spark/jars/*",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "2",
}


@dag(
    dag_id="analytical_queries",
    description="DAG running 10 complex analytical queries on European climate data.",
    start_date=datetime(2025, 12, 28),
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    tags=["climate", "analytical", "batch"],
)
def analytical_queries():

    HDFS = "{{ var.value.HDFS_DEFAULT_FS }}"
    MONGO_URI = "{{ var.value.MONGO_URI }}"
    ERA5 = f"{HDFS}/data/transformed/climate/era5"
    TEMPS = f"{HDFS}/data/transformed/climate/temperatures"
    DB = "analytical"

    # ── Q1  Continental Warming Gradient ─────────────────────────────────
    q01 = SparkSubmitOperator(
        task_id="q01_warming_gradient",
        application="/opt/airflow/files/spark/analytical_jobs/q01_warming_gradient.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q2  ERA5 vs Berkeley Earth Cross-Validation ──────────────────────
    q02 = SparkSubmitOperator(
        task_id="q02_cross_validation",
        application="/opt/airflow/files/spark/analytical_jobs/q02_cross_validation.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, TEMPS, MONGO_URI, DB],
    )

    # ── Q3  Snow Cover Decline ───────────────────────────────────────────
    q03 = SparkSubmitOperator(
        task_id="q03_snow_decline",
        application="/opt/airflow/files/spark/analytical_jobs/q03_snow_decline.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q4  Compound Drought Index ───────────────────────────────────────
    q04 = SparkSubmitOperator(
        task_id="q04_drought_index",
        application="/opt/airflow/files/spark/analytical_jobs/q04_drought_index.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q5  Precipitation Regime Shift ───────────────────────────────────
    q05 = SparkSubmitOperator(
        task_id="q05_precipitation_regime",
        application="/opt/airflow/files/spark/analytical_jobs/q05_precipitation_regime.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q6  Wind Energy Atlas ────────────────────────────────────────────
    q06 = SparkSubmitOperator(
        task_id="q06_wind_energy",
        application="/opt/airflow/files/spark/analytical_jobs/q06_wind_energy.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q7  Solar Radiation ──────────────────────────────────────────────
    q07 = SparkSubmitOperator(
        task_id="q07_solar_radiation",
        application="/opt/airflow/files/spark/analytical_jobs/q07_solar_radiation.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # ── Q8  Urban Heat Island ────────────────────────────────────────────
    q08 = SparkSubmitOperator(
        task_id="q08_urban_heat_island",
        application="/opt/airflow/files/spark/analytical_jobs/q08_urban_heat_island.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, TEMPS, MONGO_URI, DB],
    )

    # ── Q9  Climate Extremes Acceleration ────────────────────────────────
    q09 = SparkSubmitOperator(
        task_id="q09_climate_extremes",
        application="/opt/airflow/files/spark/analytical_jobs/q09_climate_extremes.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, TEMPS, MONGO_URI, DB],
    )

    # ── Q10  Surface Energy Balance ──────────────────────────────────────
    q10 = SparkSubmitOperator(
        task_id="q10_energy_balance",
        application="/opt/airflow/files/spark/analytical_jobs/q10_energy_balance.py",
        conn_id="SPARK_CONNECTION",
        verbose=False,
        conf=MONGO_JARS_CONF,
        application_args=[ERA5, MONGO_URI, DB],
    )

    # All queries are independent — run in parallel
    [q01, q02, q03, q04, q05, q06, q07, q08, q09, q10]


analytical_queries()
