"""
Q9: Climate Extremes Acceleration — Heatwaves and Cold Spells Across Europe
Counts monthly temperature/precipitation extremes per decade.
Uses PERCENT_RANK, ROW_NUMBER, and LAG window functions.
Cross-references with Berkeley Earth pre-1950 data for historical context.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    kaggle_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]

    spark = SparkSession.builder.appName("Q9_climate_extremes").getOrCreate()

    print("=" * 70)
    print("Q9: Climate Extremes Acceleration")
    print("=" * 70)

    df = spark.read.parquet(era5_path)
    df_kaggle = spark.read.parquet(kaggle_path)

    # --- ERA5 extremes (1950-2024) ---
    # Compute 1961-1990 baseline percentiles per grid cell per month
    df_baseline = (
        df.filter((F.col("year") >= 1961) & (F.col("year") <= 1990))
        .groupBy("latitude", "longitude", "month")
        .agg(
            F.expr("percentile_approx(t2m_celsius, 0.05)").alias("t2m_p05"),
            F.expr("percentile_approx(t2m_celsius, 0.95)").alias("t2m_p95"),
            F.expr("percentile_approx(tp_mm, 0.05)").alias("tp_p05"),
            F.expr("percentile_approx(tp_mm, 0.95)").alias("tp_p95"),
        )
    )

    # Classify each month as extreme or not
    df_extremes = (
        df.join(df_baseline, on=["latitude", "longitude", "month"], how="inner")
        .withColumn("hot_extreme", F.when(F.col("t2m_celsius") > F.col("t2m_p95"), 1).otherwise(0))
        .withColumn("cold_extreme", F.when(F.col("t2m_celsius") < F.col("t2m_p05"), 1).otherwise(0))
        .withColumn("wet_extreme", F.when(F.col("tp_mm") > F.col("tp_p95"), 1).otherwise(0))
        .withColumn("dry_extreme", F.when(F.col("tp_mm") < F.col("tp_p05"), 1).otherwise(0))
    )

    # Decadal extreme frequency per lat_band
    df_decadal = (
        df_extremes
        .groupBy("decade", "lat_band")
        .agg(
            F.avg("hot_extreme").alias("hot_extreme_freq"),
            F.avg("cold_extreme").alias("cold_extreme_freq"),
            F.avg("wet_extreme").alias("wet_extreme_freq"),
            F.avg("dry_extreme").alias("dry_extreme_freq"),
            F.count("*").alias("total_observations")
        )
    )

    # Trend with LAG
    w = Window.partitionBy("lat_band").orderBy("decade")
    df_trend = (
        df_decadal
        .withColumn("prev_hot_freq", F.lag("hot_extreme_freq").over(w))
        .withColumn("hot_freq_change", F.col("hot_extreme_freq") - F.col("prev_hot_freq"))
        .withColumn("prev_cold_freq", F.lag("cold_extreme_freq").over(w))
        .withColumn("cold_freq_change", F.col("cold_extreme_freq") - F.col("prev_cold_freq"))
        .orderBy("lat_band", "decade")
    )

    print("\nExtreme frequency trends per lat_band:")
    df_trend.show(50, truncate=False)

    # --- Top 20 most extreme ERA5 months (Europe-wide) ---
    df_top_hot = (
        df
        .groupBy("year", "month", "lat_band")
        .agg(F.avg("t2m_celsius").alias("zone_avg_temp"))
        .withColumn("hot_rank",
                    F.row_number().over(Window.orderBy(F.desc("zone_avg_temp"))))
        .filter(F.col("hot_rank") <= 20)
    )

    print("\nTop 20 hottest months in Europe:")
    df_top_hot.orderBy("hot_rank").show(20, truncate=False)

    # --- Berkeley Earth: pre-1950 vs post-2000 extremes ---
    # Per city: compare frequency of extreme hot months
    w_kaggle = Window.partitionBy("city", "month")
    df_kaggle_base = (
        df_kaggle
        .withColumn("city_month_p95",
                    F.expr("percentile_approx(avg_temperature, 0.95)").over(w_kaggle))
        .withColumn("is_extreme_hot",
                    F.when(F.col("avg_temperature") > F.col("city_month_p95"), 1).otherwise(0))
    )

    df_kaggle_compare = (
        df_kaggle_base
        .withColumn("era",
                    F.when(F.col("year") < 1900, "Pre-1900")
                     .when(F.col("year") < 1950, "1900-1950")
                     .when(F.col("year") < 2000, "1950-2000")
                     .otherwise("2000+"))
        .groupBy("era")
        .agg(
            F.avg("is_extreme_hot").alias("extreme_hot_frequency"),
            F.count("*").alias("observations")
        )
        .orderBy("era")
    )

    print("\nBerkeley Earth: Extreme hot frequency by era:")
    df_kaggle_compare.show()

    # Write to MongoDB
    df_trend.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q9_climate_extremes") \
        .save()

    print("✓ Q9 results written to MongoDB: q9_climate_extremes")
    spark.stop()
