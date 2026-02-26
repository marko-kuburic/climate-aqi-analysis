"""
Q6: European Wind Energy Atlas — Spatial and Temporal Variability
Maps wind energy potential across Europe, ranks best grid cells.
Uses DENSE_RANK and NTILE window functions for spatial ranking.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q6_wind_energy_atlas").getOrCreate()

    print("=" * 70)
    print("Q6: European Wind Energy Atlas")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    RHO = 1.225  # air density kg/m³

    # Compute wind power density: 0.5 * rho * v^3
    df = df.withColumn(
        "wind_power_density",
        0.5 * RHO * F.pow(F.col("wind_speed_ms"), 3)
    )

    # Annual mean per grid cell
    df_annual = (
        df
        .groupBy("year", "latitude", "longitude")
        .agg(
            F.avg("wind_speed_ms").alias("annual_avg_wind_speed"),
            F.avg("wind_power_density").alias("annual_avg_wpd")
        )
    )

    # Long-term statistics per grid cell
    df_grid_stats = (
        df_annual
        .groupBy("latitude", "longitude")
        .agg(
            F.avg("annual_avg_wind_speed").alias("mean_wind_speed"),
            F.stddev("annual_avg_wind_speed").alias("wind_speed_stddev"),
            F.avg("annual_avg_wpd").alias("mean_wpd"),
            F.count("*").alias("years_count")
        )
        .withColumn("coefficient_of_variation",
                    F.col("wind_speed_stddev") / F.col("mean_wind_speed"))
    )

    # Rank grid cells by WPD using DENSE_RANK
    w_wpd = Window.orderBy(F.desc("mean_wpd"))
    df_ranked = (
        df_grid_stats
        .withColumn("wpd_rank", F.dense_rank().over(w_wpd))
        # Classify into percentile groups using NTILE
        .withColumn("wpd_percentile", F.ntile(100).over(w_wpd))
        .withColumn("reliability_class",
                    F.when(F.col("coefficient_of_variation") < 0.10, "Very Reliable")
                     .when(F.col("coefficient_of_variation") < 0.15, "Reliable")
                     .when(F.col("coefficient_of_variation") < 0.20, "Moderate")
                     .otherwise("Variable"))
    )

    print("\nTop 30 grid cells by wind power density:")
    df_ranked.filter(F.col("wpd_rank") <= 30).orderBy("wpd_rank").show(30, truncate=False)

    # Seasonal wind patterns per latitude band
    df_seasonal = (
        df
        .groupBy("lat_band", "season")
        .agg(
            F.avg("wind_speed_ms").alias("avg_wind_speed"),
            F.avg("wind_power_density").alias("avg_wpd")
        )
        .orderBy("lat_band", "season")
    )

    print("\nSeasonal wind speed by latitude band:")
    df_seasonal.show(25, truncate=False)

    # Decadal trend per lat_band
    df_decadal = (
        df
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "lat_band")
        .agg(F.avg("wind_speed_ms").alias("avg_wind_speed"))
    )

    w_dec = Window.partitionBy("lat_band").orderBy("decade")
    df_decadal = df_decadal.withColumn(
        "prev_wind", F.lag("avg_wind_speed").over(w_dec)
    ).withColumn(
        "wind_change", F.col("avg_wind_speed") - F.col("prev_wind")
    )

    print("\nDecadal wind speed trend per lat_band:")
    df_decadal.show(50, truncate=False)

    # Write to MongoDB
    df_ranked.filter(F.col("wpd_rank") <= 100) \
        .withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q6_wind_atlas") \
        .save()

    print("✓ Q6 results written to MongoDB: q6_wind_atlas")
    spark.stop()
