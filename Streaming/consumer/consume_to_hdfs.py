#!/usr/bin/env python3
"""
Air Quality HDFS Consumer
Consumes air quality data from Kafka and writes to HDFS in batches
"""

import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from hdfs import InsecureClient

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'air-quality-stream')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'air-quality-hdfs-writer')
HDFS_NAMENODE_HOST = os.getenv('HDFS_NAMENODE_HOST', 'namenode')
HDFS_NAMENODE_PORT = os.getenv('HDFS_NAMENODE_PORT', '9870')
HDFS_OUTPUT_PATH = os.getenv('HDFS_OUTPUT_PATH', '/data/raw/streaming/air_quality')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '30'))

def get_hdfs_client():
    """Create HDFS client using WebHDFS"""
    try:
        client = InsecureClient(f'http://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}', user='root')
        return client
    except Exception as e:
        print(f"✗ Failed to create HDFS client: {e}")
        return None

def ensure_hdfs_directory():
    """Ensure HDFS output directory exists using WebHDFS"""
    max_retries = 10
    retry_delay = 5
    
    print(f"Ensuring HDFS directory exists: {HDFS_OUTPUT_PATH}")
    
    for attempt in range(max_retries):
        try:
            client = get_hdfs_client()
            if client:
                # Create directory (makedirs creates parent directories too)
                client.makedirs(HDFS_OUTPUT_PATH, permission=755)
                print(f"✓ HDFS directory ready: {HDFS_OUTPUT_PATH}")
                return True
        except Exception as e:
            # Directory might already exist
            if "FileAlreadyExistsException" in str(e) or "already exists" in str(e).lower():
                print(f"✓ HDFS directory already exists: {HDFS_OUTPUT_PATH}")
                return True
            print(f"✗ Attempt {attempt + 1}/{max_retries}: HDFS error: {e}")
        
        if attempt < max_retries - 1:
            print(f"  Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    print("⚠ Could not create HDFS directory, will try on each write")
    return False

def write_batch_to_hdfs(events, batch_number):
    """Write batch to HDFS using WebHDFS"""
    try:
        client = get_hdfs_client()
        if not client:
            return write_batch_to_local(events, batch_number)
        
        # Create filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"air_quality_batch_{batch_number:06d}_{timestamp}.json"
        hdfs_path = f"{HDFS_OUTPUT_PATH}/{filename}"
        
        # Write to HDFS
        with client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
            for event in events:
                writer.write(json.dumps(event) + '\n')
        
        print(f"✓ Batch {batch_number} written to HDFS: {hdfs_path} ({len(events)} events)")
        
        # Also write to local for backup
        write_batch_to_local(events, batch_number)
        return True
            
    except Exception as e:
        print(f"✗ Error writing batch to HDFS: {e}")
        return write_batch_to_local(events, batch_number)

def write_batch_to_local(events, batch_number):
    """Fallback: Write batch to local filesystem"""
    try:
        # Create output directory
        local_output = "/tmp/output"
        os.makedirs(local_output, exist_ok=True)
        
        # Create filename
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"air_quality_batch_{batch_number:06d}_{timestamp}.json"
        filepath = os.path.join(local_output, filename)
        
        # Write to file
        with open(filepath, 'w') as f:
            for event in events:
                f.write(json.dumps(event) + '\n')
        
        print(f"✓ Batch {batch_number} written to local: {filepath} ({len(events)} events)")
        print(f"  ⚠ Note: HDFS unavailable, using local fallback")
        return True
        
    except Exception as e:
        print(f"✗ Error writing batch locally: {e}")
        return False

def create_kafka_consumer():
    """Create and configure Kafka consumer with retry logic"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=BATCH_TIMEOUT * 1000
            )
            print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            print(f"✓ Subscribed to topic: {KAFKA_TOPIC}")
            return consumer
        except KafkaError as e:
            print(f"✗ Attempt {attempt + 1}/{max_retries}: Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                print(f"  Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"✗ Failed to connect to Kafka after {max_retries} attempts")
                raise

def main():
    """Main consumer loop"""
    print("=" * 70)
    print("Air Quality HDFS Consumer")
    print("=" * 70)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Kafka Group ID: {KAFKA_GROUP_ID}")
    print(f"HDFS Namenode: http://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}")
    print(f"HDFS Output Path: {HDFS_OUTPUT_PATH}")
    print(f"Batch Size: {BATCH_SIZE} events")
    print(f"Batch Timeout: {BATCH_TIMEOUT} seconds")
    print("=" * 70)
    
    # Ensure HDFS directory exists
    hdfs_available = ensure_hdfs_directory()
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    batch_number = 0
    events_batch = []
    total_events = 0
    total_batches = 0
    batch_start_time = time.time()
    
    try:
        print("\n✓ Consumer ready, waiting for messages...")
        
        for message in consumer:
            try:
                event = message.value
                events_batch.append(event)
                total_events += 1
                
                # Show progress
                aqi = event.get('aqi', 'N/A')
                timestamp = event.get('timestamp', 'N/A')
                print(f"  [{total_events:05d}] Received: {timestamp} | AQI: {aqi}")
                
                # Check if batch is ready
                batch_elapsed = time.time() - batch_start_time
                batch_ready = (
                    len(events_batch) >= BATCH_SIZE or 
                    batch_elapsed >= BATCH_TIMEOUT
                )
                
                if batch_ready and events_batch:
                    batch_number += 1
                    
                    # Write to HDFS (with local backup)
                    write_batch_to_hdfs(events_batch, batch_number)
                    
                    total_batches += 1
                    events_batch = []
                    batch_start_time = time.time()
                    
                    print(f"  📊 Progress: {total_events} events, {total_batches} batches written")
                    
            except Exception as e:
                print(f"✗ Error processing message: {e}")
    
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("Consumer stopped by user")
    
    finally:
        # Write remaining events
        if events_batch:
            batch_number += 1
            write_batch_to_hdfs(events_batch, batch_number)
            total_batches += 1
        
        consumer.close()
        
        print(f"Total events consumed: {total_events}")
        print(f"Total batches written: {total_batches}")
        print("=" * 70)
        print("✓ Kafka consumer closed")

if __name__ == "__main__":
    main()
