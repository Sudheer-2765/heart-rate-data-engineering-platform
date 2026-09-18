import random
import uuid
from datetime import datetime

class DeviceState:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.heart_rate = random.uniform(60.0, 100.0)
        self.spo2 = random.uniform(95.0, 100.0)
        self.battery_level = random.uniform(80.0, 100.0)
        
    def next_event(self, current_time: datetime) -> dict:
        # Simulate gradual changes (random walk)
        self.heart_rate += random.uniform(-2.0, 2.0)
        # Keep within generally normal bounds for the base value
        self.heart_rate = max(50.0, min(120.0, self.heart_rate)) 
        
        self.spo2 += random.uniform(-0.5, 0.5)
        self.spo2 = max(90.0, min(100.0, self.spo2))
        
        # Battery slowly drains
        self.battery_level -= random.uniform(0.01, 0.05)
        self.battery_level = max(0.0, self.battery_level)
        
        timestamp_str = current_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        return {
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "event_timestamp": timestamp_str,
            "heart_rate_bpm": int(self.heart_rate),
            "spo2": round(self.spo2, 1),
            "battery_level": int(self.battery_level),
            "ingestion_timestamp": timestamp_str
        }
