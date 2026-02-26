"""
Q8: Urban Heat Island Fingerprint Across 30 Major European Cities
Compares skin temperature at city grid cells vs surrounding rural cells.
Uses window functions for per-city ranking and percentile analysis.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

# 30 major European cities with approximate coordinates
CITIES = [
    ("London", 51.5, -0.1), ("Paris", 48.9, 2.3), ("Berlin", 52.5, 13.4),
    ("Madrid", 40.4, -3.7), ("Rome", 41.9, 12.5), ("Athens", 37.9, 23.7),
    ("Warsaw", 52.2, 21.0), ("Stockholm", 59.3, 18.1), ("Vienna", 48.2, 16.4),
    ("Budapest", 47.5, 19.1), ("Bucharest", 44.4, 26.1), ("Belgrade", 44.8, 20.5),
    ("Prague", 50.1, 14.4), ("Copenhagen", 55.7, 12.6), ("Dublin", 53.3, -6.3),
    ("Helsinki", 60.2, 24.9), ("Lisbon", 38.7, -9.1), ("Amsterdam", 52.4, 4.9),
    ("Brussels", 50.8, 4.4), ("Zagreb", 45.8, 16.0), ("Sofia", 42.7, 23.3),
    ("Bratislava", 48.1, 17.1), ("Ljubljana", 46.1, 14.5), ("Sarajevo", 43.9, 18.4),
    ("Oslo", 59.9, 10.8), ("Vilnius", 54.7, 25.3), ("Riga", 56.9, 24.1),
    ("Tallinn", 59.4, 24.7), ("Munich", 48.1, 11.6), ("Milan", 45.5, 9.2),
]

if __name__ == "__main__":
    era5_path = sys.argv[1]
    kaggle_path = sys.argv[2]
    mongo_uri = sys.argv[3]
    mongo_db = sys.argv[4]

    spark = SparkSession.builder.appName("Q8_urban_heat_island").getOrCreate()

    print("=" * 70)
    print("Q8: Urban Heat Island — 30 European Cities")
    print("=" * 70)

    df = spark.read.parquet(era5_path)

    # Build city reference via SQL to avoid Python serialization on executors
    city_sql = " UNION ALL ".join([
        "SELECT '{}' AS city_name, CAST({} AS DOUBLE) AS city_lat, "
        "CAST({} AS DOUBLE) AS city_lon".format(n, la, lo)
        for n, la, lo in CITIES
    ])
    df_cities = spark.sql(city_sql).cache()

    # Pre-filter ERA5: keep only grid cells within 0.5° of ANY city
    # Build a SQL OR filter for efficiency (no crossJoin needed)
    city_filter = None
    for name, lat, lon in CITIES:
        cond = (
            (F.col("latitude").between(lat - 0.5, lat + 0.5)) &
            (F.col("longitude").between(lon - 0.5, lon + 0.5))
        )
        city_filter = cond if city_filter is None else (city_filter | cond)

    df_near = df.filter(city_filter).cache()
    print(f"Grid cells near cities: {df_near.count():,}")

    # Now broadcast-join the small cities DF with pre-filtered data
    df_tagged = (
        df_near.join(
            F.broadcast(df_cities),
            (F.abs(F.col("latitude") - F.col("city_lat")) < 0.35) &
            (F.abs(F.col("longitude") - F.col("city_lon")) < 0.35),
            how="inner"
        )
        .withColumn("is_city_cell",
                    (F.abs(F.col("latitude") - F.col("city_lat")) < 0.15) &
                    (F.abs(F.col("longitude") - F.col("city_lon")) < 0.15))
    )

    # City cell temperatures
    df_city_temp = (
        df_tagged.filter(F.col("is_city_cell"))
        .select("city_name", "year", "month", "season", "decade",
                F.col("skt_celsius").alias("city_skt"),
                F.col("t2m_celsius").alias("city_t2m"))
    )

    # Rural average temperatures (surrounding cells, not the city cell itself)
    df_rural_all = (
        df_tagged.filter(~F.col("is_city_cell"))
        .groupBy("city_name", "year", "month")
        .agg(F.avg("skt_celsius").alias("rural_avg_skt"))
    )

    # Join city and rural
    df_uhi = (
        df_city_temp
        .join(df_rural_all, on=["city_name", "year", "month"], how="inner")
        .withColumn("uhi_intensity", F.col("city_skt") - F.col("rural_avg_skt"))
        .withColumn("skin_air_diff", F.col("city_skt") - F.col("city_t2m"))
    )

    # Summer UHI per city per decade
    df_summer_uhi = (
        df_uhi
        .filter(F.col("season") == "Summer")
        .groupBy("city_name", "decade")
        .agg(
            F.avg("uhi_intensity").alias("avg_summer_uhi"),
            F.avg("skin_air_diff").alias("avg_skin_air_diff"),
            F.max("uhi_intensity").alias("max_uhi"),
            F.count("*").alias("summer_months")
        )
    )

    # Rank cities by UHI using window function
    w_rank = Window.partitionBy("decade").orderBy(F.desc("avg_summer_uhi"))
    df_ranked = (
        df_summer_uhi
        .withColumn("uhi_rank", F.row_number().over(w_rank))
    )

    print("\nSummer UHI rankings (latest decade):")
    df_ranked.filter(F.col("decade") == 2020).orderBy("uhi_rank").show(30, truncate=False)

    # UHI trend per city
    w_trend = Window.partitionBy("city_name").orderBy("decade")
    df_trend = (
        df_summer_uhi
        .withColumn("prev_uhi", F.lag("avg_summer_uhi").over(w_trend))
        .withColumn("uhi_change", F.col("avg_summer_uhi") - F.col("prev_uhi"))
    )

    print("\nUHI trends (most recent decades):")
    df_trend.filter(F.col("decade") >= 2000).orderBy("city_name", "decade").show(60, truncate=False)

    # Write to MongoDB
    df_ranked.withColumn("analysis_timestamp", F.current_timestamp()) \
        .write.format("mongodb").mode("overwrite") \
        .option("connection.uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "q8_urban_heat_island") \
        .save()

    print("✓ Q8 results written to MongoDB: q8_urban_heat_island")
    spark.stop()
