from dataclasses import dataclass

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
