"""
Script to convert ERA5 NetCDF to CSV format.
Run this locally on your host machine (outside Docker).

Requirements: pip install netCDF4 pandas

Usage: python scripts/convert_era5_to_csv.py
"""
import netCDF4 as nc
import pandas as pd
from datetime import datetime
import numpy as np
import os

# Paths
input_nc = 'Airflow/files/data/climate/data_stream-moda.nc'  # Actual NetCDF file after unzipping
output_csv = 'Airflow/files/data/climate/era5_belgrade_monthly.csv'

print(f"Reading NetCDF file: {input_nc}")
ds = nc.Dataset(input_nc, 'r')

print("Variables in dataset:", list(ds.variables.keys()))
print("Dimensions:", list(ds.dimensions.keys()))

# Get time variable (named 'valid_time' in this dataset)
time_var = ds.variables['valid_time']
time_units = time_var.units
time_calendar = getattr(time_var, 'calendar', 'gregorian')

# Convert time to datetime
time_values = nc.num2date(time_var[:], units=time_units, calendar=time_calendar)

# Get lat/lon values (they are 7x7 grid, we'll take the center point for Belgrade)
lat = ds.variables['latitude'][:]
lon = ds.variables['longitude'][:]
print(f"Grid size: {len(lat)} x {len(lon)}")
print(f"Latitude range: {lat.min():.2f} to {lat.max():.2f}")
print(f"Longitude range: {lon.min():.2f} to {lon.max():.2f}")

# For Belgrade, we'll take the center point of the grid (3, 3)
center_lat_idx = len(lat) // 2
center_lon_idx = len(lon) // 2

# Extract climate variables at the center point
t2m = ds.variables['t2m'][:, center_lat_idx, center_lon_idx]  # 2m temperature (Kelvin)
tp = ds.variables['tp'][:, center_lat_idx, center_lon_idx]    # Total precipitation (meters)
ssr = ds.variables['ssr'][:, center_lat_idx, center_lon_idx]  # Surface solar radiation (J/m²)
u10 = ds.variables['u10'][:, center_lat_idx, center_lon_idx]  # U wind component (m/s)
v10 = ds.variables['v10'][:, center_lat_idx, center_lon_idx]  # V wind component (m/s)

print(f"\nProcessing {len(time_values)} time steps")

# Create DataFrame
# Convert cftime to standard datetime objects
timestamps = [datetime(t.year, t.month, t.day, t.hour, t.minute, t.second) for t in time_values]

data = {
    'timestamp': timestamps,
    'latitude': [lat[center_lat_idx]] * len(time_values),
    'longitude': [lon[center_lon_idx]] * len(time_values),
    'temperature_2m': t2m - 273.15,  # Convert Kelvin to Celsius
    'precipitation': tp * 1000,       # Convert meters to mm
    'surface_solar_radiation': ssr / 3600,  # Convert J/m² to Wh/m²
    'u_wind_10m': u10,
    'v_wind_10m': v10,
}

df = pd.DataFrame(data)

# Calculate wind speed from u and v components
df['wind_speed'] = np.sqrt(df['u_wind_10m']**2 + df['v_wind_10m']**2)

# Add derived time fields
df['year'] = df['timestamp'].dt.year
df['month'] = df['timestamp'].dt.month
df['decade'] = (df['year'] // 10) * 10

# Select final columns
df = df[['timestamp', 'year', 'month', 'decade', 'latitude', 'longitude',
         'temperature_2m', 'precipitation', 'surface_solar_radiation', 
         'wind_speed', 'u_wind_10m', 'v_wind_10m']]

print(f"\nDataFrame shape: {df.shape}")
print("\nFirst few rows:")
print(df.head())
print("\nData summary:")
print(df.describe())

# Save to CSV
print(f"\nSaving to: {output_csv}")
df.to_csv(output_csv, index=False)

print(f"\nConversion complete! CSV file created: {output_csv}")
print(f"Records: {len(df)}")
print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")

# Close NetCDF dataset
ds.close()
