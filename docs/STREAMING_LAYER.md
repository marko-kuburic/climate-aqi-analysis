# Streaming Layer Documentation

## Overview

The streaming layer implements the **Speed Layer** of a Lambda Architecture, providing real-time air quality data ingestion to complement the existing batch climate analysis.

**Purpose**: Demonstrate architectural readiness for real-time environmental monitoring while maintaining clean separation from the batch layer.

---

## Architecture

### Components

1. **Apache Kafka** (Message Broker)
   - Distributed streaming platform
   - Topic: `air-quality-stream`
   - Partitions: 3 (for parallelism)
   - Port: 9092 (internal), 9093 (external)

2. **Apache Zookeeper** (Coordination Service)
   - Manages Kafka cluster metadata
   - Port: 2181

3. **Air Quality Producer** (Data Generator)
   - Simulates real-time air quality sensors
   - Emits measurements every 5 seconds
   - Generates realistic AQI, PM2.5, PM10, O3, NO2, SO2, CO values

4. **HDFS Consumer** (Data Sink)
   - Consumes from Kafka topic
   - Writes batches to HDFS
   - Output: `/data/raw/streaming/air_quality/`

### Data Flow

```
Air Quality Sensors (Simulated)
         ↓
   Kafka Producer
         ↓
  Kafka Topic (air-quality-stream)
         ↓
   Kafka Consumer
         ↓
  HDFS (/data/raw/streaming/air_quality/)
```

---

## Data Schema

### Air Quality Event

```json
{
  "timestamp": "2026-01-30T12:34:56.789Z",
  "city": "Belgrade",
  "country": "Serbia",
  "latitude": 44.8176,
  "longitude": 20.4565,
  "aqi": 87,
  "aqi_category": "Moderate",
  "pm2_5": 34.2,
  "pm10": 51.7,
  "o3": 62.1,
  "no2": 45.3,
  "so2": 18.7,
  "co": 2.4,
  "temperature": 18.5,
  "humidity": 65.2,
  "wind_speed": 4.3
}
```

### Field Descriptions

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| timestamp | string (ISO 8601) | - | UTC timestamp of measurement |
| city | string | - | Location (Belgrade) |
| country | string | - | Country (Serbia) |
| latitude | float | degrees | Latitude coordinate |
| longitude | float | degrees | Longitude coordinate |
| **aqi** | int | index | Air Quality Index (0-500) |
| aqi_category | string | - | AQI category (Good, Moderate, etc.) |
| **pm2_5** | float | μg/m³ | Fine particulate matter (< 2.5 μm) |
| **pm10** | float | μg/m³ | Coarse particulate matter (< 10 μm) |
| **o3** | float | ppb | Ozone |
| **no2** | float | ppb | Nitrogen Dioxide |
| **so2** | float | ppb | Sulfur Dioxide |
| **co** | float | ppm | Carbon Monoxide |
| temperature | float | °C | Ambient temperature |
| humidity | float | % | Relative humidity |
| wind_speed | float | m/s | Wind speed |

---

## How to Run

### 1. Start Streaming Services

```bash
cd /home/marko/Downloads/asvsp-project-main/Streaming
docker-compose up -d
```

**Services started:**
- ✅ Zookeeper
- ✅ Kafka
- ✅ Kafka Topic Initializer
- ✅ Air Quality Producer
- ✅ HDFS Consumer

### 2. Verify Services are Running

```bash
docker ps | grep -E "kafka|zookeeper|air-quality"
```

Expected output:
```
kafka                Running
zookeeper            Running
air-quality-producer Running
air-quality-consumer Running
```

### 3. Check Producer Logs (Real-time Data Generation)

```bash
docker logs -f air-quality-producer
```

Expected output:
```
==================================================================
Air Quality Data Producer for Belgrade, Serbia
==================================================================
Kafka Bootstrap Servers: kafka:9092
Kafka Topic: air-quality-stream
City: Belgrade
Emit Interval: 5 seconds
==================================================================
✓ Connected to Kafka at kafka:9092
[00001] 2026-01-30T12:00:00.123Z | AQI:  87 (Moderate             ) | PM2.5:  34.2 μg/m³ | PM10:  51.7 μg/m³ | Partition: 1 | Offset: 0
[00002] 2026-01-30T12:00:05.456Z | AQI: 112 (Unhealthy for Sensitive) | PM2.5:  56.3 μg/m³ | PM10:  84.5 μg/m³ | Partition: 2 | Offset: 0
...
```

### 4. Check Consumer Logs (HDFS Writing)

```bash
docker logs -f air-quality-consumer
```

Expected output:
```
==================================================================
Air Quality HDFS Consumer
==================================================================
Kafka Bootstrap Servers: kafka:9092
Kafka Topic: air-quality-stream
HDFS Output Path: /data/raw/streaming/air_quality
Batch Size: 10 events
Batch Timeout: 30 seconds
==================================================================
✓ Connected to Kafka at kafka:9092
✓ Subscribed to topic: air-quality-stream
✓ Consumer ready, waiting for messages...
  [00001] Received: 2026-01-30T12:00:00.123Z | AQI: 87
  [00002] Received: 2026-01-30T12:00:05.456Z | AQI: 112
  ...
  [00010] Received: 2026-01-30T12:00:45.789Z | AQI: 95
✓ Batch 000001 written to HDFS: /data/raw/streaming/air_quality/air_quality_batch_000001_20260130_120046.json (10 events)
```

### 5. Verify Data in HDFS

```bash
# List streaming directory
docker exec namenode hdfs dfs -ls /data/raw/streaming/air_quality/

# View content of a batch file
docker exec namenode hdfs dfs -cat /data/raw/streaming/air_quality/air_quality_batch_000001_*.json | head -20
```

Expected output:
```
-rw-r--r--   3 root supergroup       3245 2026-01-30 12:00 /data/raw/streaming/air_quality/air_quality_batch_000001_20260130_120046.json
-rw-r--r--   3 root supergroup       3198 2026-01-30 12:01 /data/raw/streaming/air_quality/air_quality_batch_000002_20260130_120116.json
...
```

### 6. Monitor Kafka Topic

```bash
# Check topic details
docker exec kafka kafka-topics --describe --topic air-quality-stream --bootstrap-server localhost:9092

# Count messages in topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic air-quality-stream --time -1
```

### 7. Stop Streaming Services

```bash
cd /home/marko/Downloads/asvsp-project-main/Streaming
docker-compose down
```

---

## Integration with Batch Layer

### Current Architecture (Lambda Architecture)

```
┌─────────────────────────────────────────────────────────────┐
│                     BATCH LAYER                             │
│  ┌────────────┐   ┌────────────┐   ┌─────────────────┐    │
│  │  Airflow   │──▶│   Spark    │──▶│  HDFS (Parquet) │    │
│  │   (DAGs)   │   │ Transform  │   │  /transformed/  │    │
│  └────────────┘   └────────────┘   └─────────────────┘    │
│                                              │               │
│  Data: Historical Climate (1950-2024)       │               │
│  Frequency: Batch (manual/scheduled)        ▼               │
└────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
                                    ┌──────────────────┐
                                    │  SERVING LAYER   │
                                    │                  │
                                    │    MongoDB       │
                                    │   Analytical     │
                                    │   Collections    │
                                    │        +         │
                                    │    Metabase      │
                                    │  Visualization   │
                                    └──────────────────┘
                                               ▲
┌─────────────────────────────────────────────│───────────────┐
│                    SPEED LAYER              │               │
│  ┌────────────┐   ┌────────────┐   ┌──────▼──────────┐    │
│  │ Air Quality│──▶│   Kafka    │──▶│  HDFS (JSON)    │    │
│  │  Producer  │   │   Stream   │   │  /raw/streaming/│    │
│  └────────────┘   └────────────┘   └─────────────────┘    │
│                                                             │
│  Data: Real-time Air Quality                               │
│  Frequency: Continuous (5s intervals)                      │
└─────────────────────────────────────────────────────────────┘
```

### Data Lake Structure

```
HDFS: hdfs://namenode:9000
├── /data/
    ├── raw/
    │   ├── climate/                    ← BATCH: Raw climate data
    │   │   ├── temperatures/
    │   │   │   └── GlobalLandTemperaturesByCity.csv
    │   │   └── era5/
    │   │       └── era5_belgrade_monthly.csv
    │   │
    │   └── streaming/                  ← SPEED: Raw streaming data
    │       └── air_quality/
    │           ├── air_quality_batch_000001_20260130_120046.json
    │           ├── air_quality_batch_000002_20260130_120116.json
    │           └── ...
    │
    ├── transformed/                    ← BATCH: Transformed data
    │   └── climate/
    │       ├── temperatures/           (Parquet, partitioned by year)
    │       └── era5/                   (Parquet, partitioned by year)
    │
    └── curated/                        ← RESERVED: Future analytics
        └── (not yet implemented)
```

### Separation of Concerns

| Aspect | Batch Layer | Speed Layer |
|--------|-------------|-------------|
| **Data Source** | Kaggle CSV, ERA5 NetCDF | Kafka Stream |
| **Update Frequency** | Manual/Scheduled | Real-time (5s) |
| **Processing** | Spark transformations | Simple ingestion |
| **Storage Format** | Parquet (optimized) | JSON (flexible) |
| **Data Path** | `/data/raw/climate/` → `/data/transformed/climate/` | `/data/raw/streaming/air_quality/` |
| **Orchestration** | Airflow DAGs | Kafka consumers |
| **Latency** | Hours/Days | Seconds |

---

## Configuration

### Environment Variables

#### Producer (`air-quality-producer`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)
- `KAFKA_TOPIC`: Kafka topic name (default: `air-quality-stream`)
- `CITY`: City name for data generation (default: `Belgrade`)
- `EMIT_INTERVAL`: Seconds between emissions (default: `5`)

#### Consumer (`air-quality-consumer`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address
- `KAFKA_TOPIC`: Kafka topic to consume from
- `KAFKA_GROUP_ID`: Consumer group ID (default: `air-quality-hdfs-writer`)
- `HDFS_NAMENODE`: HDFS namenode URL (default: `hdfs://namenode:9000`)
- `HDFS_OUTPUT_PATH`: HDFS output directory (default: `/data/raw/streaming/air_quality`)
- `BATCH_SIZE`: Number of events per batch (default: `10`)
- `BATCH_TIMEOUT`: Max seconds before flushing batch (default: `30`)

### Kafka Configuration
- **Topic**: `air-quality-stream`
- **Partitions**: 3
- **Replication Factor**: 1 (single broker)
- **Retention**: 168 hours (7 days)

---

## Monitoring and Troubleshooting

### Check Service Health

```bash
# All streaming services
docker ps -a | grep -E "kafka|zookeeper|air-quality"

# Kafka broker health
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Zookeeper health
docker exec zookeeper nc -z localhost 2181 && echo "OK" || echo "FAIL"
```

### View Logs

```bash
# Producer logs
docker logs air-quality-producer

# Consumer logs
docker logs air-quality-consumer

# Kafka logs
docker logs kafka

# Follow logs in real-time
docker logs -f air-quality-producer
```

### Common Issues

#### 1. Kafka Connection Refused
**Symptom**: Producer/consumer can't connect to Kafka
**Solution**:
```bash
# Check if Kafka is running
docker ps | grep kafka

# Restart Kafka
cd Streaming
docker-compose restart kafka

# Wait 10-15 seconds for Kafka to be ready
```

#### 2. HDFS Directory Not Found
**Symptom**: Consumer can't write to HDFS
**Solution**:
```bash
# Manually create HDFS directory
docker exec namenode hdfs dfs -mkdir -p /data/raw/streaming/air_quality

# Verify directory exists
docker exec namenode hdfs dfs -ls /data/raw/streaming/
```

#### 3. No Data Appearing in HDFS
**Symptom**: Producer is running but no files in HDFS
**Check**:
```bash
# 1. Verify producer is sending messages
docker logs air-quality-producer | tail -20

# 2. Verify consumer is receiving messages
docker logs air-quality-consumer | tail -20

# 3. Check Kafka topic has messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic air-quality-stream \
  --from-beginning \
  --max-messages 5
```

#### 4. Container Keeps Restarting
**Symptom**: Service restarts repeatedly
**Check**:
```bash
# View exit logs
docker logs air-quality-producer

# Check resource usage
docker stats

# Inspect container
docker inspect air-quality-producer
```

---

## Performance Characteristics

### Producer Performance
- **Throughput**: ~12 events/minute (5-second intervals)
- **Message Size**: ~350 bytes/event (JSON compressed with gzip)
- **Daily Volume**: ~17,280 events/day (~6 MB/day uncompressed)

### Consumer Performance
- **Batch Size**: 10 events
- **Batch Frequency**: ~50 seconds (10 events × 5s interval)
- **HDFS Write Frequency**: ~1 batch/minute
- **Daily Files**: ~1,440 batch files/day

### Storage Requirements
- **Kafka Retention**: 7 days × 6 MB/day = ~42 MB
- **HDFS Storage**: 6 MB/day × 30 days = ~180 MB/month

---

## Future Enhancements (Not Implemented)

### Streaming Analytics (Not Required for KT2)
- Spark Structured Streaming jobs
- Real-time air quality alerts
- Moving average calculations
- Anomaly detection

### Integration with Batch Layer
- Join streaming air quality with batch climate data
- Correlation analysis (temperature vs AQI)
- Combined visualizations in Metabase

### Additional Features
- Multiple city producers
- Data quality validation
- Schema evolution with Avro
- Exactly-once semantics with Kafka transactions
- Auto-scaling consumers based on lag

---

## Academic Context (KT2 Compliance)

### Lambda Architecture Components

✅ **Batch Layer**: Implemented and functional
- Historical climate analysis (1950-2024)
- Airflow orchestration
- Spark batch processing
- HDFS storage (Parquet format)

✅ **Speed Layer**: Implemented (this document)
- Real-time air quality ingestion
- Kafka streaming platform
- Continuous data flow
- HDFS storage (JSON format)

✅ **Serving Layer**: Implemented and functional
- MongoDB for analytical results
- Metabase for visualization
- REST APIs available

### Demonstrated Skills

1. **Stream Processing**: Kafka producer/consumer patterns
2. **Data Engineering**: ETL pipelines, data lake architecture
3. **Containerization**: Docker, docker-compose orchestration
4. **Distributed Systems**: Kafka, HDFS, Spark integration
5. **Data Formats**: JSON (streaming), Parquet (batch), CSV (raw)
6. **System Architecture**: Separation of concerns, scalability

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Lambda Architecture](http://lambda-architecture.net/)
- [Air Quality Index (AQI) Standards](https://www.airnow.gov/aqi/aqi-basics/)
