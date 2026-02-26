#!/usr/bin/env python3
"""
AQICN Air Quality Data Producer for Kafka Streaming Pipeline
Fetches REAL air quality data from AQICN API for Belgrade, Serbia
"""

import json
import time
import os
import sys
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration from environment
AQICN_API_TOKEN = os.getenv('AQICN_API_TOKEN', '')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'air-quality-stream')
CITY = os.getenv('CITY', 'belgrade')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '60'))  # seconds

# AQICN API endpoint
AQICN_API_URL = f"https://api.waqi.info/feed/{CITY}/"

def validate_config():
    """Validate required configuration"""
    if not AQICN_API_TOKEN or AQICN_API_TOKEN == 'your_token_here':
        print("=" * 70)
        print("ERROR: AQICN_API_TOKEN not set!")
        print("=" * 70)
        print("Please set your API token in Streaming/.env file:")
        print("")
        print("  AQICN_API_TOKEN=your_actual_token")
        print("")
        print("Get a free token at: https://aqicn.org/data-platform/token/")
        print("=" * 70)
        sys.exit(1)

def fetch_air_quality_data():
    """
    Fetch real-time air quality data from AQICN API
    Returns parsed data dict or None on error
    """
    try:
        params = {'token': AQICN_API_TOKEN}
        response = requests.get(AQICN_API_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Check API response status
        if data.get('status') != 'ok':
            print(f"✗ API error: {data.get('data', 'Unknown error')}")
            return None
        
        raw_data = data.get('data', {})
        
        # Extract measurements
        iaqi = raw_data.get('iaqi', {})
        
        # Parse pollutant values (AQICN returns individual AQI for each pollutant)
        event = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'city': raw_data.get('city', {}).get('name', CITY),
            'country': 'Serbia',
            'latitude': raw_data.get('city', {}).get('geo', [0, 0])[0],
            'longitude': raw_data.get('city', {}).get('geo', [0, 0])[1],
            'aqi': raw_data.get('aqi', None),
            'aqi_category': get_aqi_category(raw_data.get('aqi', 0)),
            'dominant_pollutant': raw_data.get('dominentpol', None),
            # Individual pollutant measurements (sub-index values)
            'pm2_5': iaqi.get('pm25', {}).get('v', None),
            'pm10': iaqi.get('pm10', {}).get('v', None),
            'o3': iaqi.get('o3', {}).get('v', None),
            'no2': iaqi.get('no2', {}).get('v', None),
            'so2': iaqi.get('so2', {}).get('v', None),
            'co': iaqi.get('co', {}).get('v', None),
            # Weather data
            'temperature': iaqi.get('t', {}).get('v', None),
            'humidity': iaqi.get('h', {}).get('v', None),
            'pressure': iaqi.get('p', {}).get('v', None),
            'wind_speed': iaqi.get('w', {}).get('v', None),
            # Metadata
            'measurement_time': raw_data.get('time', {}).get('iso', None),
            'station_name': raw_data.get('city', {}).get('name', None),
            'station_url': raw_data.get('city', {}).get('url', None)
        }
        
        # Remove None values for cleaner JSON
        event = {k: v for k, v in event.items() if v is not None}
        
        return event
        
    except requests.exceptions.Timeout:
        print(f"✗ API request timeout after 10 seconds")
        return None
    except requests.exceptions.RequestException as e:
        print(f"✗ API request failed: {e}")
        return None
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        print(f"✗ Failed to parse API response: {e}")
        return None

def get_aqi_category(aqi):
    """Convert AQI value to category (US EPA standard)"""
    if aqi is None:
        return 'Unknown'
    
    if aqi <= 50:
        return 'Good'
    elif aqi <= 100:
        return 'Moderate'
    elif aqi <= 150:
        return 'Unhealthy for Sensitive'
    elif aqi <= 200:
        return 'Unhealthy'
    elif aqi <= 300:
        return 'Very Unhealthy'
    else:
        return 'Hazardous'

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
            print(f"✗ Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"  Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    print(f"✗ Failed to connect to Kafka after {max_retries} attempts")
    sys.exit(1)

def main():
    """Main producer loop"""
    print("=" * 70)
    print("AQICN Air Quality Data Producer for Belgrade, Serbia")
    print("=" * 70)
    print(f"API Endpoint: {AQICN_API_URL}")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"City: {CITY}")
    print(f"Poll Interval: {POLL_INTERVAL} seconds")
    print("=" * 70)
    
    # Validate configuration
    validate_config()
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    event_count = 0
    error_count = 0
    
    print("\n🔄 Starting data collection...\n")
    
    try:
        while True:
            # Fetch data from AQICN API
            event = fetch_air_quality_data()
            
            if event:
                try:
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=event)
                    record_metadata = future.get(timeout=10)
                    
                    event_count += 1
                    
                    # Format output
                    aqi = event.get('aqi', 'N/A')
                    category = event.get('aqi_category', 'Unknown')
                    dominant = event.get('dominant_pollutant', 'N/A')
                    pm25 = event.get('pm2_5', 'N/A')
                    temp = event.get('temperature', 'N/A')
                    
                    print(f"[{event_count:05d}] {event['timestamp']}")
                    print(f"         AQI: {aqi:>3} ({category:<25}) | Dominant: {dominant}")
                    print(f"         PM2.5: {pm25:>5} | Temp: {temp:>5}°C")
                    print(f"         → Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
                    print()
                    
                except KafkaError as e:
                    error_count += 1
                    print(f"✗ Failed to send message to Kafka: {e}")
            else:
                error_count += 1
                print(f"⚠ Skipping this poll cycle (API error)\n")
            
            # Show statistics every 10 events
            if event_count > 0 and event_count % 10 == 0:
                success_rate = ((event_count / (event_count + error_count)) * 100)
                print(f"📊 Stats: {event_count} events sent | {error_count} errors | {success_rate:.1f}% success rate")
                print()
            
            # Wait before next poll
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down producer...")
        print(f"📊 Final Stats: {event_count} events sent | {error_count} errors")
    finally:
        producer.close()
        print("✓ Producer closed")

if __name__ == '__main__':
    main()
