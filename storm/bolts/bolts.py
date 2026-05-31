"""
Storm Bolts — Python simulation of the Storm topology bolts.
Each bolt class processes tuples and passes results downstream.
AlertBolt persists alerts to both PostgreSQL and MongoDB.
"""
import json
import time
import os
import yaml
from collections import deque
from datetime import datetime

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)


class ParseBolt:
    """Deserializes each Kafka message, validates required fields,
    and emits a structured tuple downstream. Malformed messages are logged and discarded."""

    def process(self, raw_value):
        try:
            if isinstance(raw_value, str):
                msg = json.loads(raw_value)
            elif isinstance(raw_value, bytes):
                msg = json.loads(raw_value.decode('utf-8'))
            elif isinstance(raw_value, dict):
                msg = raw_value
            else:
                print(f"[ParseBolt] Unknown message type: {type(raw_value)}")
                return None

            # Validate required fields
            required = ["CASE NUMBER", "DISTRICT", "DATE", "PRIMARY TYPE"]
            if all(msg.get(k) for k in required):
                return msg
            else:
                missing = [k for k in required if not msg.get(k)]
                print(f"[ParseBolt] Malformed — missing: {missing}")
                return None
        except Exception as e:
            print(f"[ParseBolt] Error parsing: {e}")
            return None


class DistrictBolt:
    """Groups incoming tuples by police district.
    Emits district-tagged tuples for per-district counting."""

    def process(self, msg):
        district = msg.get("DISTRICT", "UNKNOWN")
        return district, msg


class WindowBolt:
    """Maintains a sliding count window (configurable size and slide interval).
    Emits per-district crime counts at each slide interval."""

    def __init__(self, window_size_sec=300, slide_interval_sec=60):
        self.window_size = window_size_sec
        self.slide_interval = slide_interval_sec
        self.windows = {}  # district -> deque of timestamps

    def process(self, district, msg):
        now = time.time()

        if district not in self.windows:
            self.windows[district] = deque()

        self.windows[district].append(now)

        # Remove events outside the window
        while self.windows[district] and self.windows[district][0] < now - self.window_size:
            self.windows[district].popleft()

        current_count = len(self.windows[district])
        return district, current_count, now


class AnomalyBolt:
    """Compares each district's window count against a configurable baseline threshold.
    Emits an anomaly tuple when the threshold is exceeded."""

    def __init__(self, threshold=10):
        self.threshold = threshold

    def process(self, district, count, timestamp):
        if count > self.threshold:
            return {
                "district": district,
                "event_count": count,
                "threshold": self.threshold,
                "timestamp": timestamp
            }
        return None


class AlertBolt:
    """Consumes anomaly tuples and persists structured alert records
    to both MongoDB (alert_logs) and PostgreSQL (alerts table)."""

    def __init__(self):
        self._pg_conn = None
        self._mongo_db = None

    def _get_pg_connection(self):
        if self._pg_conn is None or self._pg_conn.closed:
            import psycopg2
            pg = config['postgres']
            self._pg_conn = psycopg2.connect(
                host=pg['host'],
                port=pg['port'],
                database=pg['database'],
                user=pg['user'],
                password=pg['password']
            )
            self._pg_conn.autocommit = True
        return self._pg_conn

    def _get_mongo_db(self):
        if self._mongo_db is None:
            from pymongo import MongoClient
            mongo = config['mongodb']
            client = MongoClient(mongo['uri'])
            self._mongo_db = client[mongo['database']]
        return self._mongo_db

    def process(self, anomaly):
        """Persist alert to PostgreSQL and MongoDB."""
        ts = datetime.fromtimestamp(anomaly['timestamp'])
        severity = "HIGH" if anomaly['event_count'] > anomaly['threshold'] * 2 else "MEDIUM"

        alert = {
            "district": str(anomaly['district']),
            "timestamp": ts.isoformat(),
            "event_count": int(anomaly['event_count']),
            "threshold": int(anomaly['threshold']),
            "severity": severity
        }

        # ── Write to PostgreSQL ──
        try:
            conn = self._get_pg_connection()
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO alerts (district, timestamp, event_count, threshold, severity)
                   VALUES (%s, %s, %s, %s, %s)""",
                (alert['district'], ts, alert['event_count'],
                 alert['threshold'], alert['severity'])
            )
            cur.close()
        except Exception as e:
            print(f"[AlertBolt] PostgreSQL write error: {e}")

        # ── Write to MongoDB ──
        try:
            db = self._get_mongo_db()
            collection = db[config['mongodb']['alert_collection']]
            mongo_doc = alert.copy()
            mongo_doc['timestamp'] = ts  # Store as datetime in Mongo
            collection.insert_one(mongo_doc)
        except Exception as e:
            print(f"[AlertBolt] MongoDB write error: {e}")

        print(f"  🚨 ALERT: District {alert['district']} | "
              f"Count: {alert['event_count']} > Threshold: {alert['threshold']} | "
              f"Severity: {alert['severity']} | Time: {ts.strftime('%H:%M:%S')}")

        return alert

    def close(self):
        """Clean up connections."""
        if self._pg_conn and not self._pg_conn.closed:
            self._pg_conn.close()
