"""
Q3: European Snow Cover Decline — Alps vs Scandinavia vs Carpathians
Compares snow season trends across 3 mountain/northern regions.
Uses LAG window for decade-over-decade change, RANK for worst winters.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

# Region definitions
REGIONS = {
    "Alps":        {"lat_min": 44.0, "lat_max": 48.0, "lon_min":  5.0, "lon_max": 16.0},
    "Scandinavia": {"lat_min": 58.0, "lat_max": 70.0, "lon_min":  5.0, "lon_max": 30.0},
    "Carpathians": {"lat_min": 44.0, "lat_max": 49.0, "lon_min": 17.0, "lon_max": 27.0},
}

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q3_snow_cover_decline").getOrCreate()

    print("=" * 70)
    print("Q3: Snow Cover Decline — Alps vs Scandinavia vs Carpathians")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    # Assign region labels
    region_expr = F.lit(None).cast("string")
    for name, bbox in REGIONS.items():
        region_expr = F.when(
            (F.col("latitude") >= bbox["lat_min"]) & (F.col("latitude") <= bbox["lat_max"]) &
            (F.col("longitude") >= bbox["lon_min"]) & (F.col("longitude") <= bbox["lon_max"]),
            F.lit(name)
        ).otherwise(region_expr)

    df_regions = df.withColumn("region", region_expr).filter(F.col("region").isNotNull())

    # Winter months: Oct-Apr. Define winter year as the year of January
    # (e.g., winter 2000 = Oct 1999 – Apr 2000)
    df_winter = (
        df_regions
        .filter(F.col("month").isin([10, 11, 12, 1, 2, 3, 4]))
        .withColumn("winter_year",
                    F.when(F.col("month") >= 10, F.col("year") + 1)
                     .otherwise(F.col("year")))
        .withColumn("has_snow", F.when(F.col("sde") > 0.01, 1).otherwise(0))
    )

    # Per region per winter: snow months, avg snow depth, avg temp
    df_winter_stats = (
        df_winter
        .groupBy("region", "winter_year")
        .agg(
            F.avg("has_snow").alias("snow_fraction"),
            F.avg("sde").alias("avg_snow_depth_m"),
            F.avg("t2m_celsius").alias("avg_winter_temp"),
            F.count("*").alias("grid_cell_months")
        )
        .withColumn("decade", (F.floor(F.col("winter_year") / 10) * 10).cast("int"))
    )

    # Decadal aggregation
    df_decadal = (
        df_winter_stats
        .groupBy("region", "decade")
        .agg(
            F.avg("snow_fraction").alias("avg_snow_fraction"),
            F.avg("avg_snow_depth_m").alias("avg_snow_depth"),
            F.avg("avg_winter_temp").alias("avg_winter_temp"),
            F.count("*").alias("winters_count")
        )
    )

    # LAG for decade-over-decade change
    w = Window.partitionBy("region").orderBy("decade")
    df_trends = (
        df_decadal
        .withColumn("prev_snow_fraction", F.lag("avg_snow_fraction").over(w))
        .withColumn("snow_fraction_change",
                    F.col("avg_snow_fraction") - F.col("prev_snow_fraction"))
        .withColumn("prev_temp", F.lag("avg_winter_temp").over(w))
        .withColumn("temp_change", F.col("avg_winter_temp") - F.col("prev_temp"))
        .orderBy("region", "decade")
    )

    print("\nDecadal snow trends per region:")
    df_trends.show(50, truncate=False)

    # Rank lowest-snow winters per region
    w_rank = Window.partitionBy("region").orderBy("avg_snow_depth_m")
    df_worst = (
        df_winter_stats
        .withColumn("rank", F.row_number().over(w_rank))
        .filter(F.col("rank") <= 5)
        .orderBy("region", "rank")
    )

    print("\nTop 5 lowest-snow winters per region:")
    df_worst.show(20, truncate=False)

    # Correlation: winter temp vs snow depth per region
    df_corr = (
        df_winter_stats
        .groupBy("region")
        .agg(F.corr("avg_winter_temp", "avg_snow_depth_m").alias("temp_snow_correlation"))
    )
    print("\nTemperature-Snow correlation per region:")
    df_corr.show()

    # Write to MongoDB
    df_trends.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q3_snow_decline") \
        .save()

    print("✓ Q3 results written to MongoDB: q3_snow_decline")
    spark.stop()
