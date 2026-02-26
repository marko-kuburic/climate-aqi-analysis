"""
Q5: Precipitation Regime Shift — Seasonal Redistribution Across Europe
Analyzes whether European rainfall is shifting between seasons.
Uses window functions for running seasonal share calculations and trend detection.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q5_precipitation_regime").getOrCreate()

    print("=" * 70)
    print("Q5: Precipitation Regime Shift — Seasonal Redistribution")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    # Seasonal precipitation per grid cell per year
    df_seasonal = (
        df
        .groupBy("year", "season", "lat_band")
        .agg(
            F.sum("tp_mm").alias("seasonal_precip_mm"),
            F.avg("swvl1").alias("avg_soil_moisture")
        )
    )

    # Annual total per lat_band
    df_annual = (
        df
        .groupBy("year", "lat_band")
        .agg(F.sum("tp_mm").alias("annual_precip_mm"))
    )

    # Join to compute seasonal share
    df_share = (
        df_seasonal
        .join(df_annual, on=["year", "lat_band"], how="inner")
        .withColumn("seasonal_share_pct",
                    F.col("seasonal_precip_mm") / F.col("annual_precip_mm") * 100)
    )

    # Compare periods: 1950-1975 vs 2000-2024
    df_early = (
        df_share.filter((F.col("year") >= 1950) & (F.col("year") <= 1975))
        .groupBy("lat_band", "season")
        .agg(
            F.avg("seasonal_share_pct").alias("early_share_pct"),
            F.avg("seasonal_precip_mm").alias("early_precip_mm")
        )
    )

    df_late = (
        df_share.filter((F.col("year") >= 2000) & (F.col("year") <= 2024))
        .groupBy("lat_band", "season")
        .agg(
            F.avg("seasonal_share_pct").alias("late_share_pct"),
            F.avg("seasonal_precip_mm").alias("late_precip_mm")
        )
    )

    df_comparison = (
        df_early.join(df_late, on=["lat_band", "season"], how="inner")
        .withColumn("share_shift_pct", F.col("late_share_pct") - F.col("early_share_pct"))
        .withColumn("precip_change_mm", F.col("late_precip_mm") - F.col("early_precip_mm"))
        .orderBy("lat_band", "season")
    )

    print("\nSeasonal precipitation share shift (early vs late period):")
    df_comparison.show(25, truncate=False)

    # Decadal evolution with window function
    df_decadal = (
        df_share
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "lat_band", "season")
        .agg(
            F.avg("seasonal_share_pct").alias("avg_share_pct"),
            F.avg("seasonal_precip_mm").alias("avg_precip_mm"),
            F.avg("avg_soil_moisture").alias("avg_soil_moisture")
        )
    )

    w = Window.partitionBy("lat_band", "season").orderBy("decade")
    df_decadal_trend = (
        df_decadal
        .withColumn("prev_share", F.lag("avg_share_pct").over(w))
        .withColumn("share_change", F.col("avg_share_pct") - F.col("prev_share"))
        .withColumn("running_avg_share",
                    F.avg("avg_share_pct").over(
                        w.rowsBetween(-2, 0)))  # 3-decade running average
        .orderBy("lat_band", "season", "decade")
    )

    print("\nDecadal precipitation share evolution:")
    df_decadal_trend.show(60, truncate=False)

    # Write to MongoDB
    df_comparison.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q5_precip_regime") \
        .save()

    df_decadal_trend.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("append") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q5_precip_decadal") \
        .save()

    print("✓ Q5 results written to MongoDB: q5_precip_regime, q5_precip_decadal")
    spark.stop()
