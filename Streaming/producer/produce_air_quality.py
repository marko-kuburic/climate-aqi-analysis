#!/usr/bin/env python3
"""
Air Quality Data Producer for Kafka Streaming Pipeline
Simulates real-time air quality measurements for Belgrade, Serbia
"""

import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'air-quality-stream')
CITY = os.getenv('CITY', 'Belgrade')
EMIT_INTERVAL = int(os.getenv('EMIT_INTERVAL', '5'))  # seconds

# Air Quality Index categories (US EPA standard)
AQI_CATEGORIES = {
    'Good': (0, 50),
    'Moderate': (51, 100),
    'Unhealthy for Sensitive': (101, 150),
    'Unhealthy': (151, 200),
    'Very Unhealthy': (201, 300)
}

def get_realistic_aqi_values():
    """
    Generate realistic air quality values
    Belgrade typically has moderate to unhealthy air quality
    """
    # Base AQI (weighted toward moderate-unhealthy range)
    aqi_category = random.choices(
        ['Good', 'Moderate', 'Unhealthy for Sensitive', 'Unhealthy', 'Very Unhealthy'],
        weights=[10, 40, 30, 15, 5]  # Realistic distribution for urban area
    )[0]
    
    min_aqi, max_aqi = AQI_CATEGORIES[aqi_category]
    aqi = random.randint(min_aqi, max_aqi)
    
    # PM2.5 (fine particulate matter) - highly correlated with AQI
    # Typical range: 0-500 μg/m³
    pm2_5 = round(aqi * 0.5 + random.uniform(-10, 10), 1)
    pm2_5 = max(0, min(pm2_5, 500))
    
    # PM10 (coarse particulate matter) - usually higher than PM2.5
    # Typical range: 0-600 μg/m³
    pm10 = round(pm2_5 * 1.5 + random.uniform(-15, 15), 1)
    pm10 = max(0, min(pm10, 600))
    
    # O3 (Ozone) - varies by time of day, higher in summer
    # Typical range: 0-200 ppb
    o3 = round(random.uniform(20, 120) + (aqi * 0.3), 1)
    o3 = max(0, min(o3, 200))
    
    # NO2 (Nitrogen Dioxide) - traffic-related
    # Typical range: 0-200 ppb
    no2 = round(random.uniform(10, 80) + (aqi * 0.2), 1)
    no2 = max(0, min(no2, 200))
    
    # SO2 (Sulfur Dioxide) - industrial pollution
    # Typical range: 0-100 ppb
    so2 = round(random.uniform(5, 40) + (aqi * 0.1), 1)
    so2 = max(0, min(so2, 100))
    
    # CO (Carbon Monoxide) - traffic-related
    # Typical range: 0-50 ppm
    co = round(random.uniform(0.3, 5.0) + (aqi * 0.02), 2)
    co = max(0, min(co, 50))
    
    # Temperature (related to air quality - inversions trap pollution)
    # Belgrade typical range: -5°C to 35°C
    temperature = round(random.uniform(0, 30), 1)
    
    # Humidity affects particulate matter behavior
    humidity = round(random.uniform(30, 85), 1)
    
    # Wind speed - higher wind disperses pollution
    wind_speed = round(random.uniform(0.5, 15.0), 1)
    
    return {
        'aqi': aqi,
        'aqi_category': aqi_category,
        'pm2_5': pm2_5,
        'pm10': pm10,
        'o3': o3,
        'no2': no2,
        'so2': so2,
        'co': co,
        'temperature': temperature,
        'humidity': humidity,
        'wind_speed': wind_speed
    }

def generate_air_quality_event():
    """Generate a single air quality measurement event"""
    values = get_realistic_aqi_values()
    
    event = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'city': CITY,
        'country': 'Serbia',
        'latitude': 44.8176,
        'longitude': 20.4565,
        **values
    }
    
    return event

def create_kafka_producer():
    """Create and configure Kafka producer with retry logic"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            print(f"✗ Attempt {attempt + 1}/{max_retries}: Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                print(f"  Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"✗ Failed to connect to Kafka after {max_retries} attempts")
                raise

def main():
    """Main producer loop"""
    print("=" * 70)
    print("Air Quality Data Producer for Belgrade, Serbia")
    print("=" * 70)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"City: {CITY}")
    print(f"Emit Interval: {EMIT_INTERVAL} seconds")
    print("=" * 70)
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    event_count = 0
    error_count = 0
    
    try:
        while True:
            try:
                # Generate air quality event
                event = generate_air_quality_event()
                
                # Send to Kafka
                future = producer.send(KAFKA_TOPIC, value=event)
                
                # Wait for send confirmation (with timeout)
                record_metadata = future.get(timeout=10)
                
                event_count += 1
                
                # Log every event with key metrics
                print(f"[{event_count:05d}] {event['timestamp']} | "
                      f"AQI: {event['aqi']:3d} ({event['aqi_category']:20s}) | "
                      f"PM2.5: {event['pm2_5']:6.1f} μg/m³ | "
                      f"PM10: {event['pm10']:6.1f} μg/m³ | "
                      f"Partition: {record_metadata.partition} | "
                      f"Offset: {record_metadata.offset}")
                
                # Wait before next emission
                time.sleep(EMIT_INTERVAL)
                
            except KafkaError as e:
                error_count += 1
                print(f"✗ Kafka error ({error_count}): {e}")
                time.sleep(5)  # Back off on errors
                
            except Exception as e:
                error_count += 1
                print(f"✗ Unexpected error ({error_count}): {e}")
                time.sleep(5)
    
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("Producer stopped by user")
        print(f"Total events produced: {event_count}")
        print(f"Total errors: {error_count}")
        print("=" * 70)
    
    finally:
        producer.close()
        print("✓ Kafka producer closed")

if __name__ == "__main__":
    main()
