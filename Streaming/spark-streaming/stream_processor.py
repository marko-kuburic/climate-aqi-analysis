#!/usr/bin/env python3
"""
Spark Structured Streaming — Unified Stream Processor

Reads air quality events from Kafka, performs 5 streaming queries with
windowed aggregations and stream-batch joins, writes results to
Elasticsearch and HDFS.

Streaming Queries:
  S1: AQI-Climate Correlation (join with ERA5 batch data)
  S2: Wind Stagnation Alert (low-wind + high-AQI detection)
  S3: Precipitation Washout Effect (rain → AQI drop detection)
  S4: Historical Warming Context (join with Berkeley Earth)
  S5: Seasonal Anomaly Detection (rolling z-score)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.window import Window

# ═══════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "air-quality-stream"
ES_NODES = "elasticsearch"
ES_PORT = "9200"
HDFS_BASE = "hdfs://namenode:9000"
ERA5_PATH = f"{HDFS_BASE}/data/transformed/climate/era5"
TEMPS_PATH = f"{HDFS_BASE}/data/transformed/climate/temperatures"
CHECKPOINT_BASE = f"{HDFS_BASE}/data/streaming/checkpoints"

# Schema for Kafka JSON messages (matches AQICN producer output)
AQI_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("aqi_category", StringType(), True),
    StructField("dominant_pollutant", StringType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("measurement_time", StringType(), True),
    StructField("station_name", StringType(), True),
])


ES_URL = f"http://{ES_NODES}:{ES_PORT}"


def _write_batch_to_es(df, epoch_id, index_name):
    """Write a micro-batch DataFrame to Elasticsearch via REST API."""
    import json
    from urllib.request import Request, urlopen
    from urllib.error import URLError

    rows = df.toJSON().collect()
    if not rows:
        return

    # Bulk index via ES _bulk API
    bulk_body = ""
    for row_json in rows:
        meta = json.dumps({"index": {"_index": index_name}})
        bulk_body += meta + "\n" + row_json + "\n"

    try:
        req = Request(
            f"{ES_URL}/_bulk",
            data=bulk_body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            method="POST"
        )
        resp = urlopen(req, timeout=30)
        resp.read()
    except (URLError, OSError) as e:
        print(f"⚠ ES bulk write to {index_name} failed (epoch {epoch_id}): {e}")


def create_spark_session():
    """Create Spark session with Kafka package."""
    return (
        SparkSession.builder
        .appName("AirQuality_StreamProcessor")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    """Read and parse the Kafka stream into a typed DataFrame."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(
            F.from_json(F.col("json_str"), AQI_SCHEMA).alias("data"),
            F.col("kafka_ts")
        )
        .select("data.*", "kafka_ts")
        .withColumn("event_time",
                    F.coalesce(
                        F.to_timestamp("timestamp"),
                        F.col("kafka_ts")))
        .withColumn("event_month", F.month("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
    )
    return parsed


def load_batch_data(spark):
    """Load static batch datasets for stream-batch joins."""
    try:
        era5 = spark.read.parquet(ERA5_PATH)
        print(f"✓ ERA5 batch data loaded: {era5.count():,} rows")
    except Exception as e:
        print(f"⚠ Could not load ERA5 data: {e}")
        era5 = None

    try:
        temps = spark.read.parquet(TEMPS_PATH)
        print(f"✓ Berkeley Earth batch data loaded: {temps.count():,} rows")
    except Exception as e:
        print(f"⚠ Could not load Berkeley Earth data: {e}")
        temps = None

    return era5, temps


# ═══════════════════════════════════════════════════════════════════════
# S1: AQI-Climate Correlation
# Joins real-time AQI with ERA5 historical climate for the same
# month at the nearest grid cell. Computes whether AQI events
# correlate with historical temperature anomalies.
# ═══════════════════════════════════════════════════════════════════════
def s1_aqi_climate_correlation(stream_df, era5_df):
    """Stream-batch join: real-time AQI ↔ ERA5 climatology."""
    if era5_df is None:
        print("⚠ S1 skipped: no ERA5 data")
        return None

    # Build monthly climatology for Belgrade grid cell
    belgrade_climate = (
        era5_df
        .filter(
            (F.col("latitude").between(44.5, 45.0)) &
            (F.col("longitude").between(20.0, 21.0))
        )
        .groupBy("month")
        .agg(
            F.avg("t2m_celsius").alias("era5_clim_temp"),
            F.avg("tp_mm").alias("era5_clim_precip"),
            F.avg("wind_speed_ms").alias("era5_clim_wind"),
            F.avg("sp_hpa").alias("era5_clim_pressure"),
            F.stddev("t2m_celsius").alias("era5_temp_std"),
        )
        .withColumnRenamed("month", "clim_month")
    )

    # 10-minute tumbling window aggregation + join with climatology
    windowed = (
        stream_df
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            F.window("event_time", "10 minutes"),
            "city"
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.avg("pm2_5").alias("avg_pm25"),
            F.avg("temperature").alias("stream_temp"),
            F.avg("humidity").alias("stream_humidity"),
            F.avg("wind_speed").alias("stream_wind"),
            F.avg("pressure").alias("stream_pressure"),
            F.count("*").alias("event_count")
        )
        .withColumn("event_month", F.month(F.col("window.start")))
    )

    # Stream-batch join on month
    joined = (
        windowed
        .join(belgrade_climate,
              windowed["event_month"] == belgrade_climate["clim_month"],
              "left")
        .withColumn("temp_anomaly_vs_era5",
                    F.col("stream_temp") - F.col("era5_clim_temp"))
        .withColumn("temp_z_score",
                    F.when(F.col("era5_temp_std") > 0,
                           (F.col("stream_temp") - F.col("era5_clim_temp")) / F.col("era5_temp_std"))
                     .otherwise(0))
        .withColumn("wind_deficit",
                    F.col("era5_clim_wind") - F.coalesce(F.col("stream_wind"), F.lit(0)))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "avg_aqi", "avg_pm25",
            "stream_temp", "era5_clim_temp", "temp_anomaly_vs_era5", "temp_z_score",
            "stream_wind", "era5_clim_wind", "wind_deficit",
            "stream_pressure", "stream_humidity", "event_count"
        )
    )

    query = (
        joined.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: _write_batch_to_es(df, eid, "s1-aqi-climate-correlation"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/s1")
        .start()
    )
    print("✓ S1 (AQI-Climate Correlation) stream started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# S2: Wind Stagnation Alert
# Detects low-wind conditions + high AQI (air stagnation events)
# Uses 30-min sliding window with 5-min slide.
# ═══════════════════════════════════════════════════════════════════════
def s2_wind_stagnation(stream_df):
    """Detect air stagnation: low wind + high AQI."""
    windowed = (
        stream_df
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            F.window("event_time", "30 minutes", "5 minutes"),
            "city"
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.avg("wind_speed").alias("avg_wind_speed"),
            F.max("aqi").alias("max_aqi"),
            F.avg("pm2_5").alias("avg_pm25"),
            F.avg("pm10").alias("avg_pm10"),
            F.avg("humidity").alias("avg_humidity"),
            F.count("*").alias("event_count")
        )
        .withColumn("is_stagnation",
                    F.when(
                        (F.col("avg_wind_speed") < 2.0) & (F.col("avg_aqi") > 100),
                        F.lit(True)
                    ).otherwise(F.lit(False)))
        .withColumn("stagnation_severity",
                    F.when(F.col("avg_aqi") > 200, "CRITICAL")
                     .when(F.col("avg_aqi") > 150, "HIGH")
                     .when(F.col("avg_aqi") > 100, "MODERATE")
                     .otherwise("LOW"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "avg_aqi", "max_aqi", "avg_wind_speed",
            "avg_pm25", "avg_pm10", "avg_humidity",
            "is_stagnation", "stagnation_severity", "event_count"
        )
    )

    query = (
        windowed.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: _write_batch_to_es(df, eid, "s2-wind-stagnation"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/s2")
        .start()
    )
    print("✓ S2 (Wind Stagnation Alert) stream started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# S3: Precipitation Washout Effect
# Detects AQI drops following pressure changes that indicate rain.
# Uses 1-hour tumbling windows.
# ═══════════════════════════════════════════════════════════════════════
def s3_precipitation_washout(stream_df, era5_df):
    """Detect potential washout: pressure drop + AQI improvement."""
    if era5_df is None:
        print("⚠ S3 skipped: no ERA5 data")
        return None

    # Monthly avg precipitation for Belgrade from ERA5
    belgrade_precip = (
        era5_df
        .filter(
            (F.col("latitude").between(44.5, 45.0)) &
            (F.col("longitude").between(20.0, 21.0))
        )
        .groupBy("month")
        .agg(
            F.avg("tp_mm").alias("era5_avg_precip_mm"),
            F.avg("sp_hpa").alias("era5_avg_pressure_hpa"),
        )
        .withColumnRenamed("month", "clim_month")
    )

    windowed = (
        stream_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "1 hour"),
            "city"
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.first("aqi").alias("first_aqi"),
            F.last("aqi").alias("last_aqi"),
            F.avg("pressure").alias("avg_pressure"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("pm2_5").alias("avg_pm25"),
            F.count("*").alias("event_count")
        )
        .withColumn("aqi_change", F.col("last_aqi") - F.col("first_aqi"))
        .withColumn("event_month", F.month(F.col("window.start")))
    )

    # Join with ERA5 monthly precipitation norms
    joined = (
        windowed
        .join(belgrade_precip,
              windowed["event_month"] == belgrade_precip["clim_month"],
              "left")
        .withColumn("pressure_vs_normal",
                    F.col("avg_pressure") - F.col("era5_avg_pressure_hpa"))
        .withColumn("likely_rain",
                    F.when(
                        (F.col("avg_humidity") > 80) & (F.col("pressure_vs_normal") < -5),
                        F.lit(True)
                    ).otherwise(F.lit(False)))
        .withColumn("washout_detected",
                    F.when(
                        (F.col("likely_rain") == True) & (F.col("aqi_change") < -20),
                        F.lit(True)
                    ).otherwise(F.lit(False)))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "avg_aqi", "first_aqi", "last_aqi", "aqi_change",
            "avg_pressure", "era5_avg_pressure_hpa", "pressure_vs_normal",
            "avg_humidity", "likely_rain", "washout_detected",
            "era5_avg_precip_mm", "event_count"
        )
    )

    query = (
        joined.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: _write_batch_to_es(df, eid, "s3-precipitation-washout"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/s3")
        .start()
    )
    print("✓ S3 (Precipitation Washout) stream started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# S4: Historical Warming Context
# Joins real-time temperature with Berkeley Earth historical monthly
# average for Belgrade. Shows how current temp compares to 1900-2013.
# ═══════════════════════════════════════════════════════════════════════
def s4_warming_context(stream_df, temps_df):
    """Stream-batch join: real-time temp ↔ Berkeley Earth history."""
    if temps_df is None:
        print("⚠ S4 skipped: no Berkeley Earth data")
        return None

    # Monthly average for Belgrade across all decades
    belgrade_history = (
        temps_df
        .filter(F.lower(F.col("city")) == "belgrade")
        .groupBy("month")
        .agg(
            F.avg("avg_temperature").alias("historical_avg_temp"),
            F.min("avg_temperature").alias("historical_min_temp"),
            F.max("avg_temperature").alias("historical_max_temp"),
            F.stddev("avg_temperature").alias("historical_temp_std"),
            F.count("*").alias("years_of_data"),
        )
        .withColumnRenamed("month", "hist_month")
    )

    # Also compute per-decade averages for trend context
    belgrade_decades = (
        temps_df
        .filter(F.lower(F.col("city")) == "belgrade")
        .groupBy("month", "decade")
        .agg(F.avg("avg_temperature").alias("decade_avg_temp"))
    )

    # Latest decade average
    latest_decade = (
        belgrade_decades
        .filter(F.col("decade") == 2010)
        .select(
            F.col("month").alias("ld_month"),
            F.col("decade_avg_temp").alias("latest_decade_temp")
        )
    )

    windowed = (
        stream_df
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            F.window("event_time", "15 minutes"),
            "city"
        )
        .agg(
            F.avg("temperature").alias("current_temp"),
            F.avg("aqi").alias("current_aqi"),
            F.count("*").alias("event_count")
        )
        .withColumn("event_month", F.month(F.col("window.start")))
    )

    joined = (
        windowed
        .join(belgrade_history,
              windowed["event_month"] == belgrade_history["hist_month"], "left")
        .join(latest_decade,
              windowed["event_month"] == latest_decade["ld_month"], "left")
        .withColumn("anomaly_vs_all_time",
                    F.col("current_temp") - F.col("historical_avg_temp"))
        .withColumn("anomaly_vs_2010s",
                    F.col("current_temp") - F.col("latest_decade_temp"))
        .withColumn("temp_z_score",
                    F.when(F.col("historical_temp_std") > 0,
                           (F.col("current_temp") - F.col("historical_avg_temp")) /
                           F.col("historical_temp_std"))
                     .otherwise(0))
        .withColumn("extreme_classification",
                    F.when(F.abs(F.col("temp_z_score")) > 3, "EXTREME")
                     .when(F.abs(F.col("temp_z_score")) > 2, "VERY_UNUSUAL")
                     .when(F.abs(F.col("temp_z_score")) > 1, "UNUSUAL")
                     .otherwise("NORMAL"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "current_temp", "current_aqi",
            "historical_avg_temp", "historical_min_temp", "historical_max_temp",
            "latest_decade_temp",
            "anomaly_vs_all_time", "anomaly_vs_2010s", "temp_z_score",
            "extreme_classification", "years_of_data", "event_count"
        )
    )

    query = (
        joined.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: _write_batch_to_es(df, eid, "s4-warming-context"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/s4")
        .start()
    )
    print("✓ S4 (Historical Warming Context) stream started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# S5: Seasonal AQI Anomaly Detection
# Tracks running AQI statistics per time-of-day and flags anomalies.
# Uses sliding windows for rolling average and z-score computation.
# ═══════════════════════════════════════════════════════════════════════
def s5_seasonal_anomaly(stream_df):
    """Rolling z-score anomaly detection on AQI stream."""
    # 2-hour sliding window with 10-minute slides
    windowed = (
        stream_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "2 hours", "10 minutes"),
            "city"
        )
        .agg(
            F.avg("aqi").alias("rolling_avg_aqi"),
            F.stddev("aqi").alias("rolling_std_aqi"),
            F.min("aqi").alias("rolling_min_aqi"),
            F.max("aqi").alias("rolling_max_aqi"),
            F.avg("pm2_5").alias("rolling_avg_pm25"),
            F.avg("temperature").alias("rolling_avg_temp"),
            F.avg("wind_speed").alias("rolling_avg_wind"),
            F.last("aqi").alias("latest_aqi"),
            F.last("dominant_pollutant").alias("latest_pollutant"),
            F.count("*").alias("event_count")
        )
        # Compute z-score of latest AQI vs rolling window
        .withColumn("aqi_z_score",
                    F.when(
                        (F.col("rolling_std_aqi").isNotNull()) &
                        (F.col("rolling_std_aqi") > 0),
                        (F.col("latest_aqi") - F.col("rolling_avg_aqi")) /
                        F.col("rolling_std_aqi")
                    ).otherwise(0))
        .withColumn("is_anomaly",
                    F.when(F.abs(F.col("aqi_z_score")) > 2.0, True)
                     .otherwise(False))
        .withColumn("anomaly_direction",
                    F.when(F.col("aqi_z_score") > 2.0, "SPIKE")
                     .when(F.col("aqi_z_score") < -2.0, "DROP")
                     .otherwise("NORMAL"))
        .withColumn("aqi_volatility",
                    F.col("rolling_max_aqi") - F.col("rolling_min_aqi"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "latest_aqi", "rolling_avg_aqi", "rolling_std_aqi",
            "rolling_min_aqi", "rolling_max_aqi", "aqi_z_score",
            "is_anomaly", "anomaly_direction", "aqi_volatility",
            "rolling_avg_pm25", "rolling_avg_temp", "rolling_avg_wind",
            "latest_pollutant", "event_count"
        )
    )

    query = (
        windowed.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: _write_batch_to_es(df, eid, "s5-seasonal-anomaly"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/s5")
        .start()
    )
    print("✓ S5 (Seasonal Anomaly Detection) stream started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# Also write raw stream to HDFS for batch reprocessing
# ═══════════════════════════════════════════════════════════════════════
def raw_to_hdfs(stream_df):
    """Write raw AQI stream to HDFS as Parquet for batch access."""
    query = (
        stream_df
        .withWatermark("event_time", "5 minutes")
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{HDFS_BASE}/data/raw/streaming/air_quality")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw_hdfs")
        .partitionBy("city")
        .start()
    )
    print("✓ Raw stream → HDFS (Parquet) sink started")
    return query


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("Spark Structured Streaming — Air Quality Stream Processor")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load batch data for stream-batch joins
    era5_df, temps_df = load_batch_data(spark)

    # Parse Kafka stream
    stream_df = read_kafka_stream(spark)

    # Start all 5 streaming queries + raw HDFS sink
    queries = []

    q1 = s1_aqi_climate_correlation(stream_df, era5_df)
    if q1: queries.append(q1)

    q2 = s2_wind_stagnation(stream_df)
    queries.append(q2)

    q3 = s3_precipitation_washout(stream_df, era5_df)
    if q3: queries.append(q3)

    q4 = s4_warming_context(stream_df, temps_df)
    if q4: queries.append(q4)

    q5 = s5_seasonal_anomaly(stream_df)
    queries.append(q5)

    hdfs_q = raw_to_hdfs(stream_df)
    queries.append(hdfs_q)

    print(f"\n✓ {len(queries)} streaming queries running. Waiting for termination...")

    # Block until any query terminates
    spark.streams.awaitAnyTermination()
