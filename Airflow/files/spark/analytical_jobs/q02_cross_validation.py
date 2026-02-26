"""
Q2: ERA5 vs Berkeley Earth Cross-Validation Across 50+ European Cities
Validates ERA5 reanalysis against Berkeley Earth station observations.
Uses window functions for ranking cities by bias.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    kaggle_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]

    spark = (
        SparkSession.builder
        .appName("Q2_cross_validation")
        .getOrCreate()
    )

    print("=" * 70)
    print("Q2: ERA5 vs Berkeley Earth Cross-Validation")
    print("=" * 70)

    df_era5 = spark.read.parquet(era5_path)
    df_kaggle = spark.read.parquet(kaggle_path)

    print(f"ERA5 records:   {df_era5.count():,}")
    print(f"Kaggle records: {df_kaggle.count():,}")

    # Get unique Kaggle cities with their coordinates
    df_cities = (
        df_kaggle
        .select("city", "country", "latitude", "longitude")
        .distinct()
    )
    print(f"Unique European cities in Kaggle: {df_cities.count()}")

    # For each Kaggle city, find nearest ERA5 grid cell
    # Round Kaggle lat/lon to nearest 0.1° to match ERA5 grid
    df_kaggle_rounded = (
        df_kaggle
        .withColumn("era5_lat", F.round(F.col("latitude"), 1))
        .withColumn("era5_lon", F.round(F.col("longitude"), 1))
    )

    # Compute monthly averages per ERA5 grid cell
    df_era5_monthly = (
        df_era5
        .withColumn("era5_lat", F.round(F.col("latitude"), 1))
        .withColumn("era5_lon", F.round(F.col("longitude"), 1))
        .groupBy("year", "month", "era5_lat", "era5_lon")
        .agg(F.avg("t2m_celsius").alias("era5_temp_celsius"))
    )

    # Join Kaggle with ERA5 on year, month, and nearest grid cell
    # Overlap period: 1950-2013
    df_joined = (
        df_kaggle_rounded
        .filter((F.col("year") >= 1950) & (F.col("year") <= 2013))
        .join(
            df_era5_monthly,
            on=["year", "month", "era5_lat", "era5_lon"],
            how="inner"
        )
        .withColumn("bias", F.col("era5_temp_celsius") - F.col("avg_temperature"))
        .withColumn("abs_error", F.abs(F.col("bias")))
        .withColumn("squared_error", F.col("bias") ** 2)
    )

    print(f"Joined records: {df_joined.count():,}")

    # Per-city validation statistics
    df_city_stats = (
        df_joined
        .groupBy("city", "country", "latitude", "longitude")
        .agg(
            F.count("*").alias("n_observations"),
            F.avg("bias").alias("mean_bias"),
            F.avg("abs_error").alias("mae"),
            F.sqrt(F.avg("squared_error")).alias("rmse"),
            F.corr("avg_temperature", "era5_temp_celsius").alias("pearson_r"),
            F.avg("avg_temperature_uncertainty").alias("mean_uncertainty"),
            F.stddev("bias").alias("bias_stddev")
        )
    )

    # Rank cities by RMSE using window function
    w = Window.orderBy("rmse")
    df_city_stats = (
        df_city_stats
        .withColumn("rmse_rank", F.row_number().over(w))
        .withColumn("accuracy_category",
                    F.when(F.col("rmse") < 1.0, "Excellent")
                     .when(F.col("rmse") < 2.0, "Good")
                     .when(F.col("rmse") < 3.0, "Fair")
                     .otherwise("Poor"))
    )

    print("\nTop 20 cities by ERA5 accuracy (lowest RMSE):")
    df_city_stats.orderBy("rmse").show(20, truncate=False)

    print("\nBottom 10 cities (highest RMSE):")
    df_city_stats.orderBy(F.desc("rmse")).show(10, truncate=False)

    # Temporal bias evolution (does bias change over decades?)
    df_temporal_bias = (
        df_joined
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "city")
        .agg(
            F.avg("bias").alias("decadal_mean_bias"),
            F.count("*").alias("observations")
        )
    )

    # Write results to MongoDB
    df_city_stats.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q2_cross_validation") \
        .save()

    df_temporal_bias.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("append") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q2_temporal_bias") \
        .save()

    print(f"✓ Q2 results written to MongoDB: q2_cross_validation, q2_temporal_bias")
    spark.stop()
