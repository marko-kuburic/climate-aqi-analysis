"""
Q10: Surface Energy Balance — Climate Feedback Hotspots Across Europe
Analyzes radiation budget and evaporative cooling trends.
Uses window functions for Bowen ratio trends, running means, and rank.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    era5_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]

    spark = SparkSession.builder.appName("Q10_energy_balance").getOrCreate()

    print("=" * 70)
    print("Q10: Surface Energy Balance — Feedback Hotspots")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    Lv = 2.45e6  # Latent heat of vaporization J/kg
    SECONDS_PER_MONTH = 30.44 * 24 * 3600

    # Compute energy balance components per grid cell per month
    df_energy = (
        df
        .withColumn("net_radiation_wm2", F.col("ssr_wm2") + F.col("str_wm2"))
        # Latent heat flux (using precipitation as evaporation proxy; tp_mm in mm → kg/m²)
        .withColumn("latent_heat_wm2",
                    (F.col("tp_mm") / 1000.0) * Lv / SECONDS_PER_MONTH)
        # Residual ≈ sensible heat + ground heat
        .withColumn("residual_wm2",
                    F.col("net_radiation_wm2") - F.col("latent_heat_wm2"))
        # Bowen ratio = sensible / latent (use residual as sensible proxy)
        .withColumn("bowen_ratio",
                    F.when(F.col("latent_heat_wm2") > 0,
                           F.col("residual_wm2") / F.col("latent_heat_wm2"))
                     .otherwise(None))
        # Evaporative fraction = latent / net_radiation
        .withColumn("evaporative_fraction",
                    F.when(F.col("net_radiation_wm2") > 0,
                           F.col("latent_heat_wm2") / F.col("net_radiation_wm2"))
                     .otherwise(None))
    )

    # Summer energy balance per lat_band per decade
    df_summer = (
        df_energy
        .filter(F.col("season") == "Summer")
        .groupBy("decade", "lat_band")
        .agg(
            F.avg("net_radiation_wm2").alias("avg_net_rad"),
            F.avg("latent_heat_wm2").alias("avg_latent_heat"),
            F.avg("residual_wm2").alias("avg_residual"),
            F.avg("bowen_ratio").alias("avg_bowen_ratio"),
            F.avg("evaporative_fraction").alias("avg_evap_fraction"),
            F.avg("t2m_celsius").alias("avg_summer_temp"),
            F.avg("wind_speed_ms").alias("avg_wind_speed"),
            F.count("*").alias("observations")
        )
    )

    # Trend with LAG
    w = Window.partitionBy("lat_band").orderBy("decade")
    df_trend = (
        df_summer
        .withColumn("prev_bowen", F.lag("avg_bowen_ratio").over(w))
        .withColumn("bowen_change", F.col("avg_bowen_ratio") - F.col("prev_bowen"))
        .withColumn("prev_evap_frac", F.lag("avg_evap_fraction").over(w))
        .withColumn("evap_frac_change", F.col("avg_evap_fraction") - F.col("prev_evap_frac"))
        .withColumn("prev_temp", F.lag("avg_summer_temp").over(w))
        .withColumn("temp_change", F.col("avg_summer_temp") - F.col("prev_temp"))
        # Running 3-decade average of Bowen ratio
        .withColumn("bowen_running_avg",
                    F.avg("avg_bowen_ratio").over(w.rowsBetween(-1, 1)))
        .orderBy("lat_band", "decade")
    )

    print("\nSummer energy balance trends per lat_band:")
    df_trend.show(50, truncate=False)

    # Correlation: evaporative fraction decline ↔ warming
    df_corr = (
        df_energy
        .filter(F.col("season") == "Summer")
        .groupBy("lat_band")
        .agg(
            F.corr("evaporative_fraction", "t2m_celsius").alias("evap_frac_temp_corr"),
            F.corr("bowen_ratio", "t2m_celsius").alias("bowen_temp_corr"),
            F.corr("wind_speed_ms", "t2m_celsius").alias("wind_temp_corr"),
        )
    )
    print("\nCorrelations (summer):")
    df_corr.show()

    # Rank lat_bands by drying trend (Bowen ratio increase)
    df_drying = (
        df_summer
        .filter(F.col("decade").isin([1960, 2020]))
        .groupBy("lat_band")
        .pivot("decade", [1960, 2020])
        .agg(F.first("avg_bowen_ratio"))
    )
    # Use Spark's col references for pivoted names
    if "1960" in df_drying.columns and "2020" in df_drying.columns:
        df_drying = (
            df_drying
            .withColumn("bowen_shift", F.col("2020") - F.col("1960"))
            .withColumn("drying_rank",
                        F.dense_rank().over(Window.orderBy(F.desc("bowen_shift"))))
        )
        print("\nDrying rank by lat_band (Bowen ratio shift 1960→2020):")
        df_drying.show()

    # Write to MongoDB
    df_trend.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q10_energy_balance") \
        .save()

    print("✓ Q10 results written to MongoDB: q10_energy_balance")
    spark.stop()
