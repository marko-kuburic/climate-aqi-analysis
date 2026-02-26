# 🌍 AQICN Real-Time Air Quality Streaming

This directory contains the **REAL streaming data source** for the project using the AQICN (Air Quality Index China Network) API.

## 🔑 Getting Your API Token

1. Visit: https://aqicn.org/data-platform/token/
2. Register for a **free API token**
3. Copy your token (you'll need it in step 3 below)

## 🚀 Quick Start

### 1. Stop Current Services

```bash
cd /home/marko/Downloads/asvsp-project-main/Streaming
docker-compose down
```

### 2. Add Your API Token

Edit the [`.env`](.env) file:

```bash
# Replace 'your_token_here' with your actual AQICN API token
AQICN_API_TOKEN=your_actual_token_here

# Other settings (can leave as-is)
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=air-quality-stream
CITY=belgrade
POLL_INTERVAL=60
```

**Important**: 
- ✅ The `.env` file is in `.gitignore` (won't be committed)
- ❌ Never commit API tokens to Git
- ⚠️ Free tier: 1000 requests/minute (we poll every 60 seconds = plenty)

### 3. Rebuild and Start

```bash
# Rebuild producer with new code
docker-compose build air-quality-producer

# Start all services
docker-compose up -d

# Watch real-time data
docker logs -f air-quality-producer
```

## 📊 What You'll See

```
======================================================================
AQICN Air Quality Data Producer for Belgrade, Serbia
======================================================================
API Endpoint: https://api.waqi.info/feed/belgrade/
Kafka Bootstrap Servers: kafka:9092
Kafka Topic: air-quality-stream
City: belgrade
Poll Interval: 60 seconds
======================================================================
✓ Connected to Kafka at kafka:9092

🔄 Starting data collection...

[00001] 2026-01-30T14:30:00.123456Z
         AQI:  87 (Moderate                  ) | Dominant: pm25
         PM2.5:  34.2 | Temp:  12.5°C
         → Partition: 0 | Offset: 0

[00002] 2026-01-30T14:31:00.789012Z
         AQI:  89 (Moderate                  ) | Dominant: pm25
         PM2.5:  35.1 | Temp:  12.7°C
         → Partition: 0 | Offset: 1
```

**Notice**: AQI now changes **gradually** (87 → 89) because it's REAL data from the same monitoring station!

## 🔍 Verify Data Flow

### Check Kafka Messages

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic air-quality-stream \
  --from-beginning \
  --max-messages 3 | jq .
```

Output:
```json
{
  "timestamp": "2026-01-30T14:30:00.123456Z",
  "city": "Belgrade",
  "country": "Serbia",
  "latitude": 44.8176,
  "longitude": 20.4565,
  "aqi": 87,
  "aqi_category": "Moderate",
  "dominant_pollutant": "pm25",
  "pm2_5": 34.2,
  "pm10": 51.7,
  "o3": 62.1,
  "no2": 28.4,
  "temperature": 12.5,
  "humidity": 68.2,
  "measurement_time": "2026-01-30T14:00:00+00:00",
  "station_name": "Belgrade, Serbia",
  "station_url": "https://aqicn.org/city/serbia/belgrade/"
}
```

### Check HDFS Storage

```bash
# Wait 2-3 minutes for consumer to write batches
docker exec namenode hdfs dfs -ls /data/raw/streaming/air_quality/

# View data
docker exec namenode hdfs dfs -cat /data/raw/streaming/air_quality/*.json | head -5
```

## 📋 Data Schema

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `timestamp` | string | ISO 8601 UTC timestamp | `2026-01-30T14:30:00Z` |
| `city` | string | City name | `Belgrade` |
| `country` | string | Country name | `Serbia` |
| `latitude` | float | Station latitude | `44.8176` |
| `longitude` | float | Station longitude | `20.4565` |
| `aqi` | integer | Air Quality Index (US EPA) | `87` |
| `aqi_category` | string | AQI category | `Moderate` |
| `dominant_pollutant` | string | Main pollutant | `pm25` |
| `pm2_5` | float | PM2.5 concentration (μg/m³) | `34.2` |
| `pm10` | float | PM10 concentration (μg/m³) | `51.7` |
| `o3` | float | Ozone (ppb) | `62.1` |
| `no2` | float | Nitrogen dioxide (ppb) | `28.4` |
| `so2` | float | Sulfur dioxide (ppb) | `8.3` |
| `co` | float | Carbon monoxide (ppm) | `0.8` |
| `temperature` | float | Temperature (°C) | `12.5` |
| `humidity` | float | Relative humidity (%) | `68.2` |
| `pressure` | float | Atmospheric pressure (hPa) | `1013.2` |
| `wind_speed` | float | Wind speed (m/s) | `3.5` |
| `measurement_time` | string | Official measurement timestamp | `2026-01-30T14:00:00+00:00` |
| `station_name` | string | Monitoring station name | `Belgrade, Serbia` |
| `station_url` | string | AQICN station URL | `https://aqicn.org/city/...` |

**Note**: Not all fields are available for every station. Missing fields are omitted from the JSON.

## 🔧 Configuration

Edit [`.env`](.env) to customize:

- `AQICN_API_TOKEN`: Your API token (required)
- `CITY`: City name (default: `belgrade`)
- `POLL_INTERVAL`: Seconds between API calls (default: `60`)
- `KAFKA_TOPIC`: Kafka topic name (default: `air-quality-stream`)

## 🐛 Troubleshooting

### Error: "AQICN_API_TOKEN not set!"

```bash
# Make sure you edited .env with your actual token
cat Streaming/.env | grep AQICN_API_TOKEN

# Should show: AQICN_API_TOKEN=abc123def456...
# NOT: AQICN_API_TOKEN=your_token_here
```

### Error: "API error: Invalid key"

- Double-check your token is correct
- Visit: https://aqicn.org/data-platform/token/
- Regenerate token if needed

### Error: "API request timeout"

- Check internet connectivity
- Try increasing timeout in code (currently 10 seconds)
- AQICN API might be slow during peak hours

### No data in HDFS after 5 minutes

```bash
# Check consumer logs
docker logs air-quality-consumer

# Manually create HDFS directory
docker exec namenode hdfs dfs -mkdir -p /data/raw/streaming/air_quality

# Restart consumer
docker-compose restart air-quality-consumer
```

## 📚 API Documentation

- **AQICN API Docs**: https://aqicn.org/api/
- **Free Tier**: 1000 requests/minute
- **Rate Limit**: We poll every 60 seconds (safe)
- **Supported Cities**: https://aqicn.org/city/all/

## 🎯 Why AQICN Instead of Simulation?

| Aspect | Simulated Data | AQICN Real Data |
|--------|----------------|-----------------|
| **Realism** | ❌ Random jumps (45→117 AQI in 5 sec) | ✅ Gradual changes (87→89 AQI in 1 min) |
| **Temporal Continuity** | ❌ No correlation between readings | ✅ Time-series with real trends |
| **Academic Value** | ⚠️ Demonstrates architecture only | ✅ Real-world data + architecture |
| **KT2 Compliance** | ✅ Satisfies requirements | ✅ Exceeds requirements |
| **Streaming Analytics** | ⚠️ Meaningless aggregations | ✅ Real insights (trends, alerts) |
| **Location Consistency** | ✅ Same coordinates | ✅ Same monitoring station |
| **Cost** | Free | Free (1000 req/min) |

## 🏗️ Architecture Integration

```
┌─────────────────────────────────────────────────────────────┐
│                     Lambda Architecture                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────┐         ┌─────────────────────────────┐
│   BATCH LAYER       │         │      SPEED LAYER (NEW)      │
│   (Historical)      │         │      (Real-Time)            │
├─────────────────────┤         ├─────────────────────────────┤
│ • ERA5 Climate Data │         │ • AQICN API (Belgrade)      │
│ • Kaggle Temps      │         │ • 60-second polling         │
│ • Airflow DAGs      │         │ • Kafka streaming           │
│ • Spark Transform   │         │ • HDFS raw ingestion        │
│ • HDFS (7 decades)  │         │ • Time-series continuity    │
└──────────┬──────────┘         └──────────┬──────────────────┘
           │                               │
           └───────────┬───────────────────┘
                       ↓
              ┌─────────────────┐
              │  SERVING LAYER  │
              ├─────────────────┤
              │ • MongoDB       │
              │ • Metabase      │
              │ • REST APIs     │
              └─────────────────┘
```

## ✅ KT2 Compliance

- [x] Streaming ingestion containerized (Docker)
- [x] Real external data source (AQICN API)
- [x] Kafka message broker with topic
- [x] HDFS raw zone integration
- [x] Time-series data with temporal continuity
- [x] Logically related to climate domain (environmental monitoring)
- [x] Batch pipeline remains untouched
- [x] Lambda Architecture complete (Batch + Speed + Serving)

## 🔐 Security Best Practices

✅ **What we did right**:
- API token in `.env` (not hardcoded)
- `.env` in `.gitignore` (won't be committed)
- `env_file` in docker-compose (secure injection)
- Validation on startup (clear error if missing)

❌ **Never do this**:
- Hardcode tokens in source code
- Commit `.env` to Git
- Share tokens in screenshots/logs
- Use production tokens in development

## 📖 Next Steps (Future KT3)

Once streaming ingestion is stable, you can add:

1. **Streaming Analytics** (Spark Structured Streaming)
   - Real-time AQI alerts (>150 = unhealthy)
   - 1-hour moving averages
   - Anomaly detection (sudden spikes)

2. **Stream-Batch Joins**
   - Correlate air quality with weather patterns
   - Compare real-time vs historical averages
   - Seasonal trends

3. **Additional Data Sources**
   - Weather API (OpenWeatherMap)
   - Traffic data (HERE API)
   - Industrial emissions

4. **Advanced Processing**
   - Machine learning predictions
   - Time-series forecasting
   - Multi-city comparisons

---

**Created**: 2026-01-30  
**Status**: ✅ Production-ready with REAL AQICN data  
**API**: AQICN (https://aqicn.org/)  
**Architecture**: Lambda (Batch + Speed + Serving)
