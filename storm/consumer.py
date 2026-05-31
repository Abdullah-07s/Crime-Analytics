"""
Storm Consumer — Reads from Kafka 'crime_events' topic and feeds each
message through the CrimeTopology simulation pipeline.

Usage:
    python storm/consumer.py
"""
import os
import sys
import json
import yaml
import time

# Add storm/ to path
STORM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, STORM_DIR)

from kafka import KafkaConsumer
from topology.topology import CrimeTopology

# Load config
config_path = os.path.join(STORM_DIR, "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

KAFKA_BROKER = config['kafka']['broker']
TOPIC = config['kafka']['topic']


def create_consumer():
    """Create Kafka consumer with retry logic."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='storm-topology-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=60000  # 60s timeout if no new messages
            )
            print(f"Connected to Kafka broker at {KAFKA_BROKER}")
            print(f"Subscribed to topic: {TOPIC}")
            return consumer
        except Exception as e:
            print(f"Kafka consumer attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(3)
    raise ConnectionError(f"Could not connect to Kafka at {KAFKA_BROKER}")


def run():
    """Main consumer loop — feeds messages through the Storm topology."""
    print("=" * 60)
    print("  Storm Topology Consumer — Real-Time Anomaly Detection")
    print("=" * 60)
    print(f"  Window: {config['storm']['window_size_minutes']}min, "
          f"Slide: {config['storm']['slide_interval_minutes']}min, "
          f"Threshold: {config['storm']['anomaly_threshold']}")

    consumer = create_consumer()
    topology = CrimeTopology()

    try:
        print("\nWaiting for messages...\n")
        for message in consumer:
            topology.process_message(message.value)

        print("\nNo more messages (consumer timeout reached).")

    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        topology.close()
        consumer.close()
        print("Consumer shut down.")


if __name__ == "__main__":
    run()
