import pytest
from datetime import datetime, timezone
from simulator.device_state import DeviceState
from simulator.config import SimulatorConfig
from simulator.anomaly_generator import AnomalyGenerator

def test_device_state_normal_event():
    device = DeviceState("WATCH-001")
    current_time = datetime.now(timezone.utc)
    event = device.next_event(current_time)
    
    assert event["device_id"] == "WATCH-001"
    assert "event_id" in event
    assert "event_timestamp" in event
    assert 30 <= event["heart_rate_bpm"] <= 240
    assert 70.0 <= event["spo2"] <= 100.0
    assert 0 <= event["battery_level"] <= 100
    assert event["ingestion_timestamp"] is not None
    assert event["event_timestamp"] == event["ingestion_timestamp"]

def test_multiple_devices_unique_ids():
    device1 = DeviceState("WATCH-001")
    device2 = DeviceState("WATCH-002")
    current_time = datetime.now(timezone.utc)
    
    event1 = device1.next_event(current_time)
    event2 = device2.next_event(current_time)
    
    assert event1["device_id"] != event2["device_id"]
    assert event1["event_id"] != event2["event_id"]

def test_anomaly_generator_missing_value():
    config = SimulatorConfig(missing_value_rate=1.0, invalid_value_rate=0, late_event_rate=0, duplicate_rate=0)
    generator = AnomalyGenerator(config)
    device = DeviceState("WATCH-001")
    event = device.next_event(datetime.now(timezone.utc))
    
    events = generator.apply_anomalies(event, datetime.now(timezone.utc))
    assert len(events) == 1
    
    missing_count = 0
    for field in ["heart_rate_bpm", "spo2", "battery_level"]:
        if events[0][field] is None:
            missing_count += 1
            
    assert missing_count == 1
    assert generator.anomaly_counts["missing"] == 1

def test_anomaly_generator_duplicate():
    config = SimulatorConfig(missing_value_rate=0, invalid_value_rate=0, late_event_rate=0, duplicate_rate=1.0)
    generator = AnomalyGenerator(config)
    device = DeviceState("WATCH-001")
    current_time = datetime.now(timezone.utc)
    
    # First event (buffered)
    event1 = device.next_event(current_time)
    events1 = generator.apply_anomalies(event1, current_time)
    assert len(events1) == 1
    
    # Second event - should trigger a duplicate of a buffered event
    event2 = device.next_event(current_time)
    events2 = generator.apply_anomalies(event2, current_time)
    
    assert len(events2) == 2
    # events2[1] should be the duplicate of event1
    assert events2[1]["event_id"] == event1["event_id"]
    assert generator.anomaly_counts["duplicate"] == 1

def test_anomaly_generator_late_event():
    config = SimulatorConfig(missing_value_rate=0, invalid_value_rate=0, late_event_rate=1.0, duplicate_rate=0)
    generator = AnomalyGenerator(config)
    device = DeviceState("WATCH-001")
    current_time = datetime.now(timezone.utc)
    
    event = device.next_event(current_time)
    events = generator.apply_anomalies(event, current_time)
    
    late_time = datetime.fromisoformat(events[0]["event_timestamp"].replace('Z', '+00:00'))
    ingestion_time = datetime.fromisoformat(events[0]["ingestion_timestamp"].replace('Z', '+00:00'))
    
    # Late event should be more than 15 minutes (900s) older
    diff = current_time - late_time
    assert diff.total_seconds() > 900
    
    diff_ingestion = ingestion_time - late_time
    assert diff_ingestion.total_seconds() > 900

def test_anomaly_generator_invalid_value():
    config = SimulatorConfig(missing_value_rate=0, invalid_value_rate=1.0, late_event_rate=0, duplicate_rate=0)
    generator = AnomalyGenerator(config)
    device = DeviceState("WATCH-001")
    current_time = datetime.now(timezone.utc)
    
    event = device.next_event(current_time)
    events = generator.apply_anomalies(event, current_time)
    
    evt = events[0]
    invalid_count = 0
    if not (30 <= evt["heart_rate_bpm"] <= 240): invalid_count += 1
    if not (70.0 <= evt["spo2"] <= 100.0): invalid_count += 1
    if not (0 <= evt["battery_level"] <= 100): invalid_count += 1
        
    assert invalid_count == 1
