# Climate Data Download Setup

## Prerequisites

### 1. Install Python Dependencies

```bash
pip install cdsapi netCDF4
```

### 2. Configure CDS API Credentials

Create `~/.cdsapirc` with your Copernicus Climate Data Store credentials:

```
url: https://cds.climate.copernicus.eu/api/v2
key: YOUR_UID:YOUR_API_KEY
```

**To get your credentials:**
1. Register at https://cds.climate.copernicus.eu/
2. Login and go to your profile page
3. Copy your UID and API key
4. Replace `YOUR_UID:YOUR_API_KEY` in `~/.cdsapirc`

**Example:**
```
url: https://cds.climate.copernicus.eu/api/v2
key: 12345:abcdef12-3456-7890-abcd-ef1234567890
```

### 3. Verify Setup

Test your credentials:
```bash
python -c "import cdsapi; c = cdsapi.Client(); print('CDS API configured correctly')"
```

---

## Download Climate Data

### Run the Download Script

```bash
# From project root
python scripts/download_climate_data.py
```

Or make it executable and run directly:
```bash
chmod +x scripts/download_climate_data.py
./scripts/download_climate_data.py
```

---

## What Gets Downloaded

**Dataset:** ERA5-Land Monthly Means  
**Time Range:** 1950-2024 (75 years)  
**Temporal Resolution:** Monthly  
**Format:** NetCDF (.nc)

**Variables:**
- 2m temperature
- Total precipitation
- Surface net solar radiation
- Volumetric soil water (layer 1)
- 10m U-component of wind
- 10m V-component of wind

**Output Location:** `/data/raw/climate/era5_land_monthly.nc`

**Expected Size:** ~2-5 GB (depending on spatial coverage)

---

## Running Inside Docker

If you want to run the script inside the Airflow container:

```bash
# Copy the script into the container
docker cp scripts/download_climate_data.py airflow-airflow-worker-1:/tmp/

# Execute inside container
docker exec -it airflow-airflow-worker-1 python /tmp/download_climate_data.py
```

Make sure your `~/.cdsapirc` is mounted or copied into the container.

---

## Troubleshooting

**Error: "Missing/incomplete .cdsapirc"**
- Make sure `~/.cdsapirc` exists with valid credentials

**Error: "Invalid API key"**
- Re-check your UID and API key from CDS profile
- Ensure no extra spaces in `~/.cdsapirc`

**Error: "Connection timeout"**
- CDS API can be slow; retry after a few minutes
- Check https://cds.climate.copernicus.eu/live/status

**File already exists**
- Script skips download if file exists
- Delete `/data/raw/climate/era5_land_monthly.nc` to re-download

---

## Next Steps

After downloading the raw climate data:
1. Create Spark transformation job to process NetCDF
2. Extract summer statistics for Belgrade
3. Store results in transformed/curated zones
