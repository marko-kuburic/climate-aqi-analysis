"""
Q7: Solar Radiation Trends — Dimming, Brightening, and the Mediterranean Solar Belt
Detects global dimming (1950-1980) and brightening (1990+) phases.
Uses LEAD/LAG for transition detection and running averages.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q7_solar_radiation").getOrCreate()

    print("=" * 70)
    print("Q7: Solar Radiation Trends — Dimming & Brightening")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    # Annual mean solar radiation per latitude band
    df_annual = (
        df
        .groupBy("year", "lat_band")
        .agg(
            F.avg("ssrd_wm2").alias("avg_ssrd"),
            F.avg("ssr_wm2").alias("avg_ssr"),
            F.count("*").alias("grid_months")
        )
    )

    # Compute surface albedo proxy: ssr / ssrd
    df_annual = df_annual.withColumn(
        "surface_absorptivity",
        F.when(F.col("avg_ssrd") > 0, F.col("avg_ssr") / F.col("avg_ssrd")).otherwise(None)
    )

    # Running averages and trend detection using window
    w = Window.partitionBy("lat_band").orderBy("year")
    df_trends = (
        df_annual
        .withColumn("prev_year_ssrd", F.lag("avg_ssrd", 1).over(w))
        .withColumn("ssrd_change", F.col("avg_ssrd") - F.col("prev_year_ssrd"))
        # 5-year running average for smooth trend
        .withColumn("running_avg_ssrd_5yr",
                    F.avg("avg_ssrd").over(w.rowsBetween(-2, 2)))
        # 10-year running average
        .withColumn("running_avg_ssrd_10yr",
                    F.avg("avg_ssrd").over(w.rowsBetween(-5, 4)))
        # Phase classification
        .withColumn("climate_period",
                    F.when(F.col("year") < 1980, "Dimming")
                     .when(F.col("year") < 2000, "Transition")
                     .otherwise("Brightening"))
    )

    # Period averages per lat_band
    df_periods = (
        df_trends
        .groupBy("lat_band", "climate_period")
        .agg(
            F.avg("avg_ssrd").alias("period_avg_ssrd"),
            F.avg("avg_ssr").alias("period_avg_ssr"),
            F.avg("surface_absorptivity").alias("period_avg_absorptivity"),
            F.count("*").alias("years_in_period")
        )
        .orderBy("lat_band", "climate_period")
    )

    print("\nSolar radiation by period and latitude band:")
    df_periods.show(20, truncate=False)

    # Summer (JJA) specific analysis for solar energy
    df_summer = (
        df.filter(F.col("season") == "Summer")
        .groupBy("year", "lat_band")
        .agg(F.avg("ssrd_wm2").alias("summer_ssrd"))
    )

    w_summer = Window.partitionBy("lat_band").orderBy("year")
    df_summer = df_summer.withColumn(
        "summer_ssrd_10yr_avg",
        F.avg("summer_ssrd").over(w_summer.rowsBetween(-5, 4))
    )

    # Mediterranean vs Northern comparison
    df_comparison = (
        df_summer
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "lat_band")
        .agg(F.avg("summer_ssrd").alias("decadal_summer_ssrd"))
    )

    w_comp = Window.partitionBy("lat_band").orderBy("decade")
    df_comparison = df_comparison.withColumn(
        "prev_ssrd", F.lag("decadal_summer_ssrd").over(w_comp)
    ).withColumn(
        "ssrd_change", F.col("decadal_summer_ssrd") - F.col("prev_ssrd")
    )

    print("\nDecadal summer solar radiation per lat_band:")
    df_comparison.show(50, truncate=False)

    # Write to MongoDB
    df_periods.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q7_solar_radiation") \
        .save()

    print("✓ Q7 results written to MongoDB: q7_solar_radiation")
    spark.stop()
