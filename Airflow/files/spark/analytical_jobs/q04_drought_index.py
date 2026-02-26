"""
Q4: Compound Drought Severity Index — Mediterranean vs Continental Europe
Multi-variable drought index using z-scores of precipitation and soil moisture.
Window functions for running averages and percentile ranking.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q4_drought_index").getOrCreate()

    print("=" * 70)
    print("Q4: Compound Drought Index — Mediterranean vs Continental")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    # Define climate zones
    df = df.withColumn(
        "climate_zone",
        F.when(F.col("latitude") < 44, "Mediterranean")
         .when(F.col("latitude") < 55, "Continental")
         .otherwise(None)
    ).filter(F.col("climate_zone").isNotNull())

    # Compute 1961-1990 baseline statistics per grid cell per month
    df_baseline = (
        df.filter((F.col("year") >= 1961) & (F.col("year") <= 1990))
        .groupBy("latitude", "longitude", "month")
        .agg(
            F.avg("tp_mm").alias("tp_baseline_mean"),
            F.stddev("tp_mm").alias("tp_baseline_std"),
            F.avg("swvl1").alias("swvl1_baseline_mean"),
            F.stddev("swvl1").alias("swvl1_baseline_std"),
        )
    )

    # Join with full data to compute z-scores
    df_z = (
        df.join(df_baseline, on=["latitude", "longitude", "month"], how="inner")
        .withColumn("tp_z",
                    F.when(F.col("tp_baseline_std") > 0,
                           (F.col("tp_mm") - F.col("tp_baseline_mean")) / F.col("tp_baseline_std"))
                     .otherwise(0))
        .withColumn("swvl1_z",
                    F.when(F.col("swvl1_baseline_std") > 0,
                           (F.col("swvl1") - F.col("swvl1_baseline_mean")) / F.col("swvl1_baseline_std"))
                     .otherwise(0))
        # Composite drought index (average of z-scores; negative = drought)
        .withColumn("drought_index",
                    (F.col("tp_z") + F.col("swvl1_z")) / 2.0)
        .withColumn("is_drought", F.when(F.col("drought_index") < -1.5, 1).otherwise(0))
    )

    # Drought frequency per decade per climate zone
    df_decadal = (
        df_z
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("climate_zone", "decade")
        .agg(
            F.avg("drought_index").alias("avg_drought_index"),
            F.avg("is_drought").alias("drought_frequency"),
            F.min("drought_index").alias("worst_drought_severity"),
            F.count("*").alias("total_grid_months")
        )
    )

    # LAG for trend detection
    w = Window.partitionBy("climate_zone").orderBy("decade")
    df_trends = (
        df_decadal
        .withColumn("prev_frequency", F.lag("drought_frequency").over(w))
        .withColumn("frequency_change",
                    F.col("drought_frequency") - F.col("prev_frequency"))
        .withColumn("drought_rank",
                    F.dense_rank().over(
                        Window.partitionBy("climate_zone")
                        .orderBy(F.desc("drought_frequency"))))
        .orderBy("climate_zone", "decade")
    )

    print("\nDrought trends per climate zone:")
    df_trends.show(50, truncate=False)

    # Find worst drought years (zone-wide)
    df_worst = (
        df_z
        .groupBy("climate_zone", "year", "month")
        .agg(
            F.avg("drought_index").alias("zone_avg_drought"),
            F.avg("is_drought").alias("zone_drought_fraction")
        )
        .withColumn("severity_rank",
                    F.row_number().over(
                        Window.partitionBy("climate_zone")
                        .orderBy("zone_avg_drought")))
        .filter(F.col("severity_rank") <= 10)
        .orderBy("climate_zone", "severity_rank")
    )

    print("\nTop 10 worst drought months per zone:")
    df_worst.show(20, truncate=False)

    # Write to MongoDB
    df_trends.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q4_drought_index") \
        .save()

    print("✓ Q4 results written to MongoDB: q4_drought_index")
    spark.stop()
