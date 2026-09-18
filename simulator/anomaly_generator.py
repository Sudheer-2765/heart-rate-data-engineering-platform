import random
import copy
from datetime import datetime, timedelta

class AnomalyGenerator:
    def __init__(self, config):
        self.config = config
        self.anomaly_counts = {
            "invalid": 0,
            "duplicate": 0,
            "late": 0,
            "missing": 0,
            "normal": 0
        }
        self.previous_events = [] # Keep a small buffer for generating duplicates

    def apply_anomalies(self, event: dict, current_time: datetime) -> list[dict]:
        events_to_return = []
        is_normal = True
        
        evt = copy.deepcopy(event)
        
        # 1. Missing values
        if random.random() < self.config.missing_value_rate:
            field_to_remove = random.choice(["heart_rate_bpm", "spo2", "battery_level"])
            evt[field_to_remove] = None
            self.anomaly_counts["missing"] += 1
            is_normal = False
            
        # 2. Invalid values (only if not missing)
        elif random.random() < self.config.invalid_value_rate:
            field_to_corrupt = random.choice(["heart_rate_bpm", "spo2", "battery_level"])
            if field_to_corrupt == "heart_rate_bpm":
                # below 30 or above 220
                evt["heart_rate_bpm"] = random.choice([random.randint(0, 29), random.randint(221, 300)])
            elif field_to_corrupt == "spo2":
                # below 70 or above 100
                evt["spo2"] = round(random.choice([random.uniform(0.0, 69.9), random.uniform(100.1, 150.0)]), 1)
            elif field_to_corrupt == "battery_level":
                # below 0 or above 100
                evt["battery_level"] = random.choice([random.randint(-50, -1), random.randint(101, 200)])
            self.anomaly_counts["invalid"] += 1
            is_normal = False
            
        # 3. Late events (can happen to any event)
        if random.random() < self.config.late_event_rate:
            # Shift by the configured delay (plus a small random jitter)
            delay_minutes = self.config.late_event_delay_minutes + random.uniform(0, 5)
            late_time = current_time - timedelta(minutes=delay_minutes)
            evt["event_timestamp"] = late_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            self.anomaly_counts["late"] += 1
            is_normal = False
            
        if is_normal:
            self.anomaly_counts["normal"] += 1
            
        events_to_return.append(evt)
        
        # 4. Duplicate events (yields an extra event using past data)
        if random.random() < self.config.duplicate_rate and self.previous_events:
            duplicate_evt = copy.deepcopy(random.choice(self.previous_events))
            events_to_return.append(duplicate_evt)
            self.anomaly_counts["duplicate"] += 1
            
        # Buffer for duplicates
        self.previous_events.append(copy.deepcopy(evt))
        if len(self.previous_events) > 200:
            self.previous_events.pop(0)
            
        return events_to_return
