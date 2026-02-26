# 🚀 Streaming Layer Quick Start

## TL;DR - Start Streaming in 2 Minutes

```bash
# 1. Start streaming services
cd /home/marko/Downloads/asvsp-project-main/Streaming
docker-compose up -d

# 2. Watch real-time data flowing
docker logs -f air-quality-producer

# 3. Verify data in HDFS (wait 1 minute)
docker exec namenode hdfs dfs -ls /data/raw/streaming/air_quality/
```

---

## What You Get

✅ **Kafka**: Real-time message broker  
✅ **Air Quality Producer**: Generates realistic environmental data every 5 seconds  
✅ **HDFS Consumer**: Writes data to `/data/raw/streaming/air_quality/`  
✅ **Complete Lambda Architecture**: Batch + Speed + Serving layers

---

## Step-by-Step

### 1. Start Services

```bash
cd Streaming
docker-compose up -d
```

**Wait 15 seconds** for Kafka to initialize.

### 2. Verify All Running

```bash
docker ps | grep -E "kafka|zookeeper|air-quality"
```

You should see 4 containers:
- `zookeeper` - Running
- `kafka` - Running  
- `air-quality-producer` - Running
- `air-quality-consumer` - Running

### 3. Watch Producer (Real-time)

```bash
docker logs -f air-quality-producer
```

Output:
```
[00001] 2026-01-30T12:00:00Z | AQI:  87 (Moderate) | PM2.5:  34.2 μg/m³
[00002] 2026-01-30T12:00:05Z | AQI: 112 (Unhealthy for Sensitive) | PM2.5:  56.3 μg/m³
...
```

Press `Ctrl+C` to exit logs.

### 4. Watch Consumer (Batching to HDFS)

```bash
docker logs -f air-quality-consumer
```

Output:
```
  [00001] Received: 2026-01-30T12:00:00Z | AQI: 87
  [00010] Received: 2026-01-30T12:00:45Z | AQI: 95
✓ Batch 000001 written to HDFS (10 events)
```

### 5. Check Data in HDFS

```bash
# List files
docker exec namenode hdfs dfs -ls /data/raw/streaming/air_quality/

# View first batch
docker exec namenode hdfs dfs -cat /data/raw/streaming/air_quality/air_quality_batch_000001_*.json | head -5
```

### 6. Monitor Kafka Topic

```bash
# Topic details
docker exec kafka kafka-topics --describe \
  --topic air-quality-stream \
  --bootstrap-server localhost:9092

# Read messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic air-quality-stream \
  --from-beginning \
  --max-messages 3
```

---

## Data Schema

Each event contains:
- **timestamp**: ISO 8601 UTC
- **location**: Belgrade, Serbia (44.8176°N, 20.4565°E)
- **aqi**: Air Quality Index (0-500)
- **pm2_5, pm10**: Particulate matter
- **o3, no2, so2, co**: Pollutant concentrations
- **temperature, humidity, wind_speed**: Weather conditions

---

## Common Commands

```bash
# Stop streaming
docker-compose down

# Restart producer only
docker-compose restart air-quality-producer

# View all logs
docker-compose logs -f

# Clean Kafka data (delete topic)
docker exec kafka kafka-topics --delete \
  --topic air-quality-stream \
  --bootstrap-server localhost:9092
```

---

## Troubleshooting

**Producer not sending data?**
```bash
docker logs air-quality-producer
docker-compose restart air-quality-producer
```

**Consumer not writing to HDFS?**
```bash
# Check consumer logs
docker logs air-quality-consumer

# Manually create HDFS directory
docker exec namenode hdfs dfs -mkdir -p /data/raw/streaming/air_quality
```

**Kafka connection issues?**
```bash
# Restart Kafka
docker-compose restart kafka
sleep 15  # Wait for startup
docker-compose restart air-quality-producer air-quality-consumer
```

---

## Integration with Batch Layer

The streaming layer **does not interfere** with your batch climate analysis:

- **Batch data**: `/data/raw/climate/` → `/data/transformed/climate/`
- **Stream data**: `/data/raw/streaming/air_quality/`

Both layers are independent and can run simultaneously.

---

## What's Next?

For detailed documentation, see:
- **Full docs**: `docs/STREAMING_LAYER.md`
- **Architecture**: `PROJECT_STATE.md`
- **Batch layer**: Existing Airflow DAGs

## KT2 Compliance

✅ **Streaming ingestion**: Kafka producer/consumer  
✅ **Containerized**: All services in Docker  
✅ **Data lake integration**: HDFS raw zone  
✅ **Logically related**: Air quality complements climate analysis  
✅ **Lambda Architecture**: Complete batch + speed + serving layers
