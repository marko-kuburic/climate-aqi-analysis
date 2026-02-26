"""
Spark transformation: ERA5-Land Europe NetCDF → Parquet
Reads the unified ERA5 Europe NetCDF, converts to Parquet with unit
conversions and derived fields. Output partitioned by year for efficient queries.

Uses chunked numpy processing to avoid OOM on 240M+ cell dataset.
Spatial sub-sampling (every 2nd point → 0.2° grid) keeps data manageable.

Unit conversions applied:
  - Temperature: Kelvin → Celsius (−273.15)
  - Precipitation: meters → mm (×1000)
  - Radiation: J/m² → W/m² (÷ seconds in month, approximated)
  - Wind speed: derived from u10, v10 → sqrt(u² + v²)
  - Surface pressure: Pa → hPa (÷ 100)
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, FloatType, IntegerType, StringType
)
import sys

# Spatial sub-sample factor: every Nth grid point (1=full, 2=half, 5=fifth)
SPATIAL_STEP = 2

SCHEMA = StructType([
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("t2m", FloatType()),
    StructField("skt", FloatType()),
    StructField("tp", FloatType()),
    StructField("swvl1", FloatType()),
    StructField("u10", FloatType()),
    StructField("v10", FloatType()),
    StructField("ssrd", FloatType()),
    StructField("ssr", FloatType()),
    StructField("str_rad", FloatType()),
    StructField("sde", FloatType()),
    StructField("sp", FloatType()),
])

ERA5_VARS = ["t2m", "skt", "tp", "swvl1", "u10", "v10",
             "ssrd", "ssr", "str", "sde", "sp"]


def process_year(ds, times, time_indices, lats, lons, year):
    """Process all months of one year using vectorised numpy. Returns list of Row tuples."""
    import numpy as np

    n_lat = len(lats)
    n_lon = len(lons)
    n_cells = n_lat * n_lon
    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing="ij")
    flat_lat = lat_grid.ravel().astype(np.float32)
    flat_lon = lon_grid.ravel().astype(np.float32)

    rows = []
    for t_idx in time_indices:
        t_val = times[t_idx]
        month = int(t_val.month)
        yr = int(t_val.year)

        year_list = [yr] * n_cells
        month_list = [month] * n_cells

        var_lists = {}
        for vname in ERA5_VARS:
            if vname in ds.variables:
                arr = ds.variables[vname][t_idx, ::SPATIAL_STEP, ::SPATIAL_STEP]
                var_lists[vname] = arr.astype(np.float32).ravel().tolist()
            else:
                var_lists[vname] = [None] * n_cells

        step_rows = list(zip(
            year_list, month_list,
            flat_lat.tolist(), flat_lon.tolist(),
            var_lists["t2m"], var_lists["skt"], var_lists["tp"],
            var_lists["swvl1"], var_lists["u10"], var_lists["v10"],
            var_lists["ssrd"], var_lists["ssr"], var_lists["str"],
            var_lists["sde"], var_lists["sp"],
        ))
        rows.extend(step_rows)
    return rows


if __name__ == "__main__":
    nc_input_path = sys.argv[1]
    parquet_output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("transform_era5_netcdf_europe")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )

    print("=" * 70)
    print("ERA5 Europe NetCDF → Parquet Transformation")
    print("=" * 70)
    print(f"  Input:  {nc_input_path}")
    print(f"  Output: {parquet_output_path}")

    import netCDF4 as nc
    import numpy as np

    ds = nc.Dataset(nc_input_path, "r")

    # Handle 'valid_time' or 'time' dimension
    time_var_name = "valid_time" if "valid_time" in ds.variables else "time"
    time_var = ds.variables[time_var_name]
    times = nc.num2date(time_var[:], time_var.units,
                        calendar=getattr(time_var, "calendar", "standard"))
    lats = ds.variables["latitude"][::SPATIAL_STEP]
    lons = ds.variables["longitude"][::SPATIAL_STEP]

    available = [v for v in ERA5_VARS if v in ds.variables]
    print(f"\n  Time steps: {len(times)}")
    print(f"  Grid: {len(lats)} lat × {len(lons)} lon  (step={SPATIAL_STEP})")
    print(f"  Cells per step: {len(lats) * len(lons):,}")
    print(f"  Variables: {available}")

    # Group time indices by year
    year_indices = {}
    for i, t in enumerate(times):
        y = int(t.year)
        year_indices.setdefault(y, []).append(i)

    years_sorted = sorted(year_indices.keys())
    print(f"  Years: {years_sorted[0]}–{years_sorted[-1]} ({len(years_sorted)} years)")
    print()

    SECONDS_PER_MONTH = 30.44 * 24 * 3600

    first_year = True
    total_rows = 0
    for yi, year in enumerate(years_sorted):
        t_indices = year_indices[year]
        rows = process_year(ds, times, t_indices, lats, lons, year)
        print(f"  [{yi+1}/{len(years_sorted)}] Year {year}: {len(rows):,} rows ... ", end="")

        df = spark.createDataFrame(rows, schema=SCHEMA)

        # --- unit conversions ---
        df = (
            df
            .withColumn("t2m_celsius", F.col("t2m") - 273.15)
            .withColumn("skt_celsius", F.col("skt") - 273.15)
            .withColumn("tp_mm", F.col("tp") * 1000)
            .withColumn("ssrd_wm2", F.col("ssrd") / SECONDS_PER_MONTH)
            .withColumn("ssr_wm2", F.col("ssr") / SECONDS_PER_MONTH)
            .withColumn("str_wm2", F.col("str_rad") / SECONDS_PER_MONTH)
            .withColumn("wind_speed_ms",
                        F.sqrt(F.col("u10") ** 2 + F.col("v10") ** 2))
            .withColumn("sp_hpa", F.col("sp") / 100.0)
            # derived temporal
            .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
            .withColumn("season",
                        F.when((F.col("month") >= 3) & (F.col("month") <= 5), "Spring")
                         .when((F.col("month") >= 6) & (F.col("month") <= 8), "Summer")
                         .when((F.col("month") >= 9) & (F.col("month") <= 11), "Autumn")
                         .otherwise("Winter"))
            .withColumn("lat_band",
                        F.when(F.col("latitude") >= 63, "Arctic")
                         .when(F.col("latitude") >= 55, "Boreal")
                         .when(F.col("latitude") >= 48, "Northern")
                         .when(F.col("latitude") >= 42, "Central")
                         .otherwise("Mediterranean"))
        )

        # select final columns
        df_final = df.select(
            "year", "month", "decade", "season", "latitude", "longitude", "lat_band",
            "t2m_celsius", "skt_celsius", "tp_mm",
            "ssrd_wm2", "ssr_wm2", "str_wm2", "wind_speed_ms", "sp_hpa",
            "swvl1", "sde", "u10", "v10"
        )

        mode = "overwrite" if first_year else "append"
        df_final.write.mode(mode).partitionBy("year").parquet(parquet_output_path)
        total_rows += len(rows)
        first_year = False
        print("✓")

    ds.close()

    print(f"\n{'=' * 70}")
    print(f"✓ ERA5 Europe transformation completed: {total_rows:,} rows written")
    print(f"{'=' * 70}")

    spark.stop()
