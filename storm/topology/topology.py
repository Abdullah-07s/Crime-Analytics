"""
Storm Topology Simulation — Wires all bolts together into a processing pipeline.
In production, this would be a TopologyBuilder submitted to Storm Nimbus.
Here we simulate it in Python with persistent bolt instances.
"""
import os
import sys
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from bolts.bolts import ParseBolt, DistrictBolt, WindowBolt, AnomalyBolt, AlertBolt

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)


class CrimeTopology:
    """
    Simulates a multi-bolt Storm topology:
        KafkaSpout → ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt

    All bolt instances are persistent (created once), so stateful bolts like
    WindowBolt maintain their sliding windows across messages.
    """

    def __init__(self):
        self.parse_bolt = ParseBolt()
        self.district_bolt = DistrictBolt()
        self.window_bolt = WindowBolt(
            window_size_sec=config['storm']['window_size_minutes'] * 60,
            slide_interval_sec=config['storm']['slide_interval_minutes'] * 60
        )
        self.anomaly_bolt = AnomalyBolt(
            threshold=config['storm']['anomaly_threshold']
        )
        self.alert_bolt = AlertBolt()

        self.processed = 0
        self.alerts_triggered = 0

    def process_message(self, raw_message):
        """Process a single message through the full topology pipeline."""
        # 1. Parse
        parsed = self.parse_bolt.process(raw_message)
        if parsed is None:
            return None

        # 2. District routing
        district, msg = self.district_bolt.process(parsed)

        # 3. Sliding window count
        district, count, timestamp = self.window_bolt.process(district, msg)

        # 4. Anomaly detection
        anomaly = self.anomaly_bolt.process(district, count, timestamp)

        self.processed += 1

        if anomaly:
            # 5. Alert persistence
            alert = self.alert_bolt.process(anomaly)
            self.alerts_triggered += 1
            return alert

        # Log progress periodically
        if self.processed % 500 == 0:
            print(f"  [Topology] Processed: {self.processed} | "
                  f"Alerts: {self.alerts_triggered} | "
                  f"Window sizes: {self._window_summary()}")

        return None

    def _window_summary(self):
        """Get summary of current window counts per district."""
        summary = {}
        for district, window in self.window_bolt.windows.items():
            summary[district] = len(window)
        # Show top 5 districts by count
        sorted_districts = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:5]
        return dict(sorted_districts)

    def close(self):
        """Clean up resources."""
        self.alert_bolt.close()
        print(f"\n[Topology] Final stats — Processed: {self.processed}, "
              f"Alerts: {self.alerts_triggered}")
