"""
Kafka Producer — Crime Event Simulator
Streams the Crime dataset row-by-row as JSON to the 'crime_events' Kafka topic.
Publication rate is configurable via config/config.yaml.
"""
import csv
import json
import time
import yaml
import os
import sys
from kafka import KafkaProducer

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

KAFKA_BROKER = config['kafka']['broker']
TOPIC = config['kafka']['topic']
RATE = config['kafka']['producer_rate']  # rows per second

# Path to crime CSV
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
crime_csv = os.path.join(data_dir, config['paths']['crime_csv'])


def create_producer():
    """Create Kafka producer with retry logic."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                retries=3,
                acks='all'
            )
            print(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(3)
    raise ConnectionError(f"Could not connect to Kafka broker at {KAFKA_BROKER}")


def stream_crimes():
    """Stream crime records to Kafka topic."""
    producer = create_producer()
    print(f"Starting to stream crimes to topic: {TOPIC}")
    print(f"  Source: {crime_csv}")
    print(f"  Rate: {RATE} rows/second")

    sent_count = 0
    error_count = 0

    try:
        with open(crime_csv, mode='r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Extract required fields
                    message = {
                        "CASE NUMBER": row.get("Case Number", ""),
                        "DATE": row.get("Date", ""),
                        "BLOCK": row.get("Block", ""),
                        "PRIMARY TYPE": row.get("Primary Type", ""),
                        "DISTRICT": row.get("District", ""),
                        "ARREST": row.get("Arrest", ""),
                        "LATITUDE": row.get("Latitude", ""),
                        "LONGITUDE": row.get("Longitude", ""),
                        "DESCRIPTION": row.get("Description", ""),
                        "COMMUNITY AREA": row.get("Community Area", ""),
                        "BEAT": row.get("Beat", "")
                    }

                    # Filter out rows with missing critical data
                    if not message["CASE NUMBER"] or not message["DISTRICT"]:
                        error_count += 1
                        continue

                    producer.send(TOPIC, value=message)
                    sent_count += 1

                    if sent_count % 100 == 0:
                        print(f"  Sent {sent_count} messages (errors: {error_count})")

                    time.sleep(1.0 / RATE)

                except Exception as e:
                    error_count += 1
                    if error_count % 100 == 0:
                        print(f"  Row error ({error_count} total): {e}")
                    continue

    except FileNotFoundError:
        print(f"ERROR: Crime CSV not found at {crime_csv}")
        print("  Please run download_datasets.py first.")
        sys.exit(1)

    print(f"\nStreaming complete. Sent: {sent_count}, Errors: {error_count}")


if __name__ == "__main__":
    try:
        stream_crimes()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Fatal error: {e}")
