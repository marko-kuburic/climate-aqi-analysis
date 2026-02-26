# 🔄 Switching from Simulated to Real AQICN Data

## What Changed?

| Before (Simulated) | After (AQICN Real Data) |
|-------------------|------------------------|
| ❌ Random AQI (45→117 in 5 sec) | ✅ Gradual AQI (87→89 in 1 min) |
| ❌ No temporal correlation | ✅ Time-series continuity |
| ⏱️ Every 5 seconds | ⏱️ Every 60 seconds (API-friendly) |
| 📝 `produce_air_quality.py` | 📝 `produce_air_quality_aqicn.py` |
| 🎲 Fake data generator | 🌍 AQICN REST API |

## Steps to Switch

### 1. Get Your Free API Token

Visit: **https://aqicn.org/data-platform/token/**

![AQICN Token Page](https://aqicn.org/data-platform/images/token-screenshot.png)

1. Enter your email
2. Agree to terms
3. Click "Request Token"
4. Check your email for token

### 2. Configure API Token

Edit [`Streaming/.env`](./home/marko/Downloads/asvsp-project-main/Streaming/.env):

```bash
# Replace this line:
AQICN_API_TOKEN=your_token_here

# With your actual token:
AQICN_API_TOKEN=abc123def456ghi789...
```

### 3. Rebuild and Restart

```bash
cd /home/marko/Downloads/asvsp-project-main/Streaming

# Stop current services
docker-compose down

# Rebuild producer with AQICN code
docker-compose build air-quality-producer

# Start everything
docker-compose up -d

# Watch the magic happen!
docker logs -f air-quality-producer
```

### 4. Verify Real Data

You should see:

```
======================================================================
AQICN Air Quality Data Producer for Belgrade, Serbia
======================================================================
✓ Connected to Kafka at kafka:9092

🔄 Starting data collection...

[00001] 2026-01-30T14:30:00.123456Z
         AQI:  87 (Moderate                  ) | Dominant: pm25
         PM2.5:  34.2 | Temp:  12.5°C
         → Partition: 0 | Offset: 0
```

**Key indicators of real data**:
- ✅ Consistent AQI values (not wild jumps)
- ✅ `Dominant: pm25` (actual pollutant data)
- ✅ Real temperature and humidity
- ✅ Updates every 60 seconds
- ✅ Matches current Belgrade conditions

### 5. Compare with AQICN Website

Open: **https://aqicn.org/city/serbia/belgrade/**

Compare:
- Your producer AQI vs website AQI → Should match! ✅
- Dominant pollutant → Should match! ✅
- Temperature → Should match! ✅

## Troubleshooting

### "ERROR: AQICN_API_TOKEN not set!"

```bash
# Check your .env file
cat Streaming/.env | grep AQICN_API_TOKEN

# Should NOT be:
AQICN_API_TOKEN=your_token_here  ❌

# Should be your actual token:
AQICN_API_TOKEN=abc123...  ✅
```

**Fix**: Edit `.env` with your real token from https://aqicn.org/data-platform/token/

### "API error: Invalid key"

Your token is incorrect or expired.

1. Visit: https://aqicn.org/data-platform/token/
2. Request a new token
3. Update `.env` with new token
4. Restart: `docker-compose restart air-quality-producer`

### "API request timeout"

Network issue or AQICN API is slow.

**Quick fix**:
```bash
# Check if you can reach AQICN
curl "https://api.waqi.info/feed/belgrade/?token=YOUR_TOKEN"

# Should return JSON with AQI data
```

If curl works but container fails → check Docker network settings.

### Still seeing simulated data format

You need to rebuild:

```bash
cd Streaming
docker-compose down
docker-compose build --no-cache air-quality-producer
docker-compose up -d
docker logs -f air-quality-producer
```

## What to Expect

### First Minute
```
✓ Connected to Kafka
🔄 Starting data collection...
[00001] 2026-01-30T14:30:00Z
         AQI:  87 (Moderate) | Dominant: pm25
```

### After 5 Minutes (5 measurements)
```
[00005] 2026-01-30T14:34:00Z
         AQI:  89 (Moderate) | Dominant: pm25
         PM2.5:  35.8 | Temp:  12.7°C
📊 Stats: 5 events sent | 0 errors | 100.0% success rate
```

### In HDFS (after 10-15 minutes)
```bash
docker exec namenode hdfs dfs -cat /data/raw/streaming/air_quality/*.json | head -1
```

Output:
```json
{"timestamp":"2026-01-30T14:30:00Z","city":"Belgrade","aqi":87,"pm2_5":34.2,...}
```

## Benefits of Real Data

### For KT2 Evaluation

✅ **Demonstrates real-world skills**:
- External API integration
- Error handling (timeouts, rate limits)
- Environment variable security
- RESTful data ingestion

✅ **Better academic narrative**:
- "We integrated AQICN global air quality network"
- "Real-time data from Belgrade monitoring station"
- "Temporal continuity validates streaming architecture"

### For Future Analytics (KT3)

✅ **Meaningful aggregations**:
- Daily AQI averages (real trends)
- Pollution spike detection (real alerts)
- Time-series forecasting (real patterns)

❌ **Simulated data would give**:
- Random noise (no trends)
- False alerts (meaningless spikes)
- Unpredictable patterns (no forecasting possible)

## API Quota Management

**AQICN Free Tier**: 1000 requests/minute

**Our usage**: 1 request/60 seconds = **1 request/minute**

**Safety margin**: 999 requests/minute unused = **99.9% headroom** ✅

**Cost**: $0 forever (free tier is permanent)

## Files Changed

```
Streaming/
├── .env                              ← NEW: API token config
├── .gitignore                        ← NEW: Protect secrets
├── README_AQICN.md                   ← NEW: Full documentation
├── SETUP_AQICN.md                    ← YOU ARE HERE
├── producer/
│   ├── Dockerfile                    ← UPDATED: +requests library
│   ├── produce_air_quality.py        ← OLD: Simulated (kept for reference)
│   └── produce_air_quality_aqicn.py  ← NEW: Real AQICN API
└── docker-compose.yml                ← UPDATED: env_file configuration
```

## Verification Checklist

Before submitting KT2:

- [ ] `.env` has your real AQICN token
- [ ] Producer logs show real AQI values (not random jumps)
- [ ] Kafka topic contains air-quality messages
- [ ] HDFS has `/data/raw/streaming/air_quality/*.json` files
- [ ] AQI values match https://aqicn.org/city/serbia/belgrade/
- [ ] Consumer is writing batches every ~1-2 minutes
- [ ] Batch pipeline still works (Airflow DAGs untouched)

## Next: Test the Setup

```bash
cd /home/marko/Downloads/asvsp-project-main/Streaming

# 1. Stop everything
docker-compose down

# 2. Edit .env with your token
nano .env  # or use VS Code

# 3. Rebuild producer
docker-compose build air-quality-producer

# 4. Start everything
docker-compose up -d

# 5. Watch real data flow
docker logs -f air-quality-producer
```

Press `Ctrl+C` to stop watching logs.

---

**Ready to go?** → [Get your token](https://aqicn.org/data-platform/token/) → Edit `.env` → `docker-compose up -d`

**Questions?** → Check [README_AQICN.md](README_AQICN.md) for full documentation
