# Metabase Setup Guide for Climate Analysis

## Automatic MongoDB Connection Setup

The MongoDB connection should be configured automatically. If not, here are the connection details:

- **Database type:** MongoDB
- **Display name:** Climate Analysis MongoDB
- **Host:** `mongodb`
- **Port:** `27017`
- **Database name:** `analytical`
- **Username:** `admin`
- **Password:** `admin`
- **Authentication database:** `admin`

## Creating Visualizations

Once connected to MongoDB, follow these steps:

### 1. Access the Data

1. Click **"+ New"** in the top right
2. Select **"Question"**
3. Choose **"Climate Analysis MongoDB"** as your database
4. Select the **"climate_summer_analysis"** collection

### 2. Temperature Trend Over Time

**Chart Type:** Line Chart

**Configuration:**
- **X-axis:** `decade`
- **Y-axis:** `avg_summer_temp_kaggle`
- **Title:** "Belgrade Summer Temperature Trends (1950-2020)"

**Steps:**
1. Click "Visualize"
2. Select "Line" chart type
3. Click on "decade" for X-axis
4. Click on "avg_summer_temp_kaggle" for Y-axis
5. Click "Settings" → Add second line for `avg_summer_temp_era5` to compare both datasets
6. Save with a descriptive name

### 3. Decade-over-Decade Temperature Change

**Chart Type:** Bar Chart

**Configuration:**
- **X-axis:** `decade`
- **Y-axis:** `temp_change_from_prev_decade`
- **Title:** "Temperature Change from Previous Decade"

**Steps:**
1. Create new question
2. Select climate_summer_analysis
3. Choose "Bar" chart
4. Set decade as X-axis
5. Set temp_change_from_prev_decade as Y-axis
6. Filter out NULL values (1950s has no previous decade)

### 4. Temperature vs Precipitation Correlation

**Chart Type:** Scatter Plot

**Configuration:**
- **X-axis:** `avg_summer_temp_kaggle`
- **Y-axis:** `avg_summer_precip_mm`
- **Size/Color:** `decade` (to show temporal progression)
- **Title:** "Summer Temperature vs Precipitation"

**Steps:**
1. Create new question
2. Select "Scatter" plot
3. Configure axes as above
4. Add decade as bubble color or label

### 5. Multi-Metric Dashboard

**Chart Type:** Combo Chart/Multiple Lines

**Configuration:**
- **X-axis:** `decade`
- **Y-axes:** 
  - Left: `avg_summer_temp_kaggle` (°C)
  - Right: `avg_summer_precip_mm` (mm)
  - Right: `avg_summer_solar_radiation_w_m2` (W/m²)

### 6. Summary Statistics Table

**Chart Type:** Table

**Columns to Display:**
- `decade`
- `avg_summer_temp_kaggle` (rename: "Avg Temp °C")
- `temp_change_from_prev_decade` (rename: "Δ Temp °C")
- `avg_summer_precip_mm` (rename: "Avg Precip mm")
- `avg_summer_solar_radiation_w_m2` (rename: "Solar W/m²")
- `observation_count` (rename: "Samples")

**Formatting:**
- Round temperature to 2 decimals
- Round other values to 1 decimal
- Sort by decade ascending

### 7. Creating a Dashboard

1. Click **"+ New"** → **"Dashboard"**
2. Name it: "Belgrade Summer Climate Analysis"
3. Click **"Add a filter"** if you want decade range filtering
4. Add all your saved questions/charts
5. Arrange them in a logical layout:
   - Top: Temperature trend line chart (full width)
   - Middle row: Temperature change bar chart + Correlation scatter
   - Bottom: Summary statistics table (full width)
6. Save the dashboard

## Key Insights to Highlight

Based on your data:

- **Overall Warming:** +1.49°C from 1950s to 2010s
- **Acceleration:** Warming is accelerating in recent decades
- **Correlations:**
  - Temperature & Precipitation: -0.48 (negative correlation)
  - Temperature & Solar Radiation: +0.38 (positive correlation)
  - Precipitation & Solar: -0.50 (negative correlation)

## Tips

1. **Use Filters:** Add decade range filters to focus on specific periods
2. **Add Annotations:** Click on data points to add notes about significant climate events
3. **Share Publicly:** Enable public sharing to embed charts in presentations
4. **Schedule Emails:** Set up daily/weekly email reports with the dashboard
5. **Color Schemes:** Use warm colors (reds/oranges) for temperature, cool colors (blues) for precipitation

## Troubleshooting

**Can't see the collection?**
- Verify MongoDB connection is active (green checkmark)
- Click "Sync database schema" in Admin → Databases
- Refresh the page

**Charts look wrong?**
- Check data types (decade should be numeric, not text)
- Verify NULL values are filtered out where needed
- Ensure aggregations are set correctly

**Performance issues?**
- The dataset is small (7 records), so performance should be instant
- If slow, check MongoDB connection health
