import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SimulatorConfig:
    num_devices: int = 10
    events_per_second_per_device: int = 1
    duration_seconds: int = 30
    
    # Anomaly rates
    invalid_value_rate: float = 0.05
    duplicate_rate: float = 0.03
    late_event_rate: float = 0.02
    missing_value_rate: float = 0.02
    late_event_delay_minutes: int = 20

    # Output
    output_path: str = "data/telemetry.jsonl"
    mode: str = "local"
    
    # Fabric Configuration
    fabric_eventhub_connection_string: str = field(default_factory=lambda: os.getenv("FABRIC_EVENTHUB_CONNECTION_STRING", ""))
    fabric_eventhub_name: str = field(default_factory=lambda: os.getenv("FABRIC_EVENTHUB_NAME", ""))
