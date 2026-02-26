"""
Spark transformation: Berkeley Earth GlobalLandTemperaturesByCity → Parquet
Filters for European cities within the ERA5 bounding box (34°N–72°N, 25°W–45°E),
cleans data, adds derived temporal columns.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
import sys

# European bounding box matching ERA5 download
LAT_SOUTH, LAT_NORTH = 34.0, 72.0
LON_WEST, LON_EAST = -25.0, 45.0

if __name__ == "__main__":
    input_csv_path = sys.argv[1]
    output_parquet_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("transform_kaggle_temperatures_europe")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )

    print("=" * 70)
    print("Berkeley Earth European Temperatures → Parquet")
    print("=" * 70)
    print(f"  Input:  {input_csv_path}")
    print(f"  Output: {output_parquet_path}")

    df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
    total = df.count()
    print(f"\n1. Total rows in CSV: {total:,}")

    # Parse latitude/longitude strings like "44.82N", "20.47E"
    df = (
        df
        .withColumn(
            "lat_num",
            F.when(F.col("Latitude").endswith("N"),
                   F.regexp_extract("Latitude", r"([\d.]+)", 1).cast(DoubleType()))
             .otherwise(
                   -F.regexp_extract("Latitude", r"([\d.]+)", 1).cast(DoubleType()))
        )
        .withColumn(
            "lon_num",
            F.when(F.col("Longitude").endswith("E"),
                   F.regexp_extract("Longitude", r"([\d.]+)", 1).cast(DoubleType()))
             .otherwise(
                   -F.regexp_extract("Longitude", r"([\d.]+)", 1).cast(DoubleType()))
        )
    )

    # Filter for European bounding box
    df_europe = df.filter(
        (F.col("lat_num") >= LAT_SOUTH) & (F.col("lat_num") <= LAT_NORTH) &
        (F.col("lon_num") >= LON_WEST) & (F.col("lon_num") <= LON_EAST)
    )
    print(f"2. Rows within Europe bounding box: {df_europe.count():,}")

    # Show city count
    city_count = df_europe.select("City").distinct().count()
    print(f"   Unique European cities: {city_count}")

    # Clean and transform
    df_transformed = (
        df_europe
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
        .withColumnRenamed("AverageTemperatureUncertainty", "avg_temperature_uncertainty")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Country", "country")
        .drop("Latitude", "Longitude")
        .withColumnRenamed("lat_num", "latitude")
        .withColumnRenamed("lon_num", "longitude")
        .withColumn("date", F.to_date("date"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("decade", (F.floor(F.year("date") / 10) * 10).cast("int"))
        .withColumn(
            "season",
            F.when((F.col("month") >= 3) & (F.col("month") <= 5), "Spring")
             .when((F.col("month") >= 6) & (F.col("month") <= 8), "Summer")
             .when((F.col("month") >= 9) & (F.col("month") <= 11), "Autumn")
             .otherwise("Winter")
        )
        .filter(F.col("avg_temperature").isNotNull())
        .withColumn("avg_temperature", F.col("avg_temperature").cast(DoubleType()))
        .withColumn("avg_temperature_uncertainty",
                    F.col("avg_temperature_uncertainty").cast(DoubleType()))
    )

    row_count = df_transformed.count()
    print(f"3. Final row count: {row_count:,}")
    df_transformed.printSchema()
    df_transformed.show(10)

    print(f"\n4. Writing Parquet to: {output_parquet_path}")
    df_transformed.write.mode("overwrite").partitionBy("year").parquet(output_parquet_path)

    print(f"\n{'=' * 70}")
    print(f"✓ Berkeley Earth transformation completed: {row_count:,} rows, {city_count} cities")
    print(f"{'=' * 70}")
    spark.stop()
