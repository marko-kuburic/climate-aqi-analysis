"""
Q1: Continental Warming Gradient — North vs South Europe (1950–2024)
Computes warming rate (°C/decade) per latitude band to verify Arctic
amplification across Europe. Uses window functions (LAG) for trend computation.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = (
        SparkSession.builder
        .appName("Q1_continental_warming_gradient")
        .getOrCreate()
    )

    print("=" * 70)
    print("Q1: Continental Warming Gradient — North vs South Europe")
    print("=" * 70)

    df = spark.read.parquet(era5_path)
    print(f"Total ERA5 records: {df.count():,}")

    # Annual mean temperature per latitude band
    df_annual = (
        df.groupBy("year", "lat_band")
        .agg(
            F.avg("t2m_celsius").alias("avg_temp_celsius"),
            F.count("*").alias("grid_cell_months")
        )
    )

    # Decadal averages per latitude band
    df_decadal = (
        df_annual
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "lat_band")
        .agg(
            F.avg("avg_temp_celsius").alias("decadal_avg_temp"),
            F.stddev("avg_temp_celsius").alias("decadal_stddev_temp"),
            F.count("*").alias("years_in_decade")
        )
    )

    # Compute decade-over-decade warming using LAG window function
    w = Window.partitionBy("lat_band").orderBy("decade")

    df_trends = (
        df_decadal
        .withColumn("prev_decade_temp", F.lag("decadal_avg_temp").over(w))
        .withColumn("warming_rate_per_decade",
                    F.col("decadal_avg_temp") - F.col("prev_decade_temp"))
        .withColumn("cumulative_warming",
                    F.col("decadal_avg_temp") - F.first("decadal_avg_temp").over(
                        Window.partitionBy("lat_band").orderBy("decade")
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .withColumn("decade_rank",
                    F.row_number().over(w))
        .orderBy("lat_band", "decade")
    )

    print("\nDecadal warming trends per latitude band:")
    df_trends.show(100, truncate=False)

    # Summary: total warming per band (latest decade − earliest decade)
    w_summary = Window.partitionBy("lat_band").orderBy("decade")
    df_summary = (
        df_decadal
        .withColumn("first_temp", F.first("decadal_avg_temp").over(
            w_summary.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .withColumn("last_temp", F.last("decadal_avg_temp").over(
            w_summary.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .select("lat_band", "first_temp", "last_temp")
        .distinct()
        .withColumn("total_warming", F.col("last_temp") - F.col("first_temp"))
        .orderBy("lat_band")
    )

    print("\nTotal warming per latitude band (1950s → 2020s):")
    df_summary.show()

    # Write to MongoDB
    df_result = df_trends.withColumn("analysis_timestamp", F.current_timestamp())
    df_result.write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q1_warming_gradient") \
        .save()

    print("✓ Q1 results written to MongoDB: q1_warming_gradient")
    spark.stop()
