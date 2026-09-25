import argparse
import time
import json
import os
import logging
from datetime import datetime, timezone
from simulator.config import SimulatorConfig
from simulator.device_state import DeviceState
from simulator.anomaly_generator import AnomalyGenerator
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Wearable IoT Telemetry Simulator")
    parser.add_argument("--mode", choices=["local", "fabric"], default="local", help="Running mode")
    parser.add_argument("--duration", type=int, default=30, help="Simulation duration in seconds")
    parser.add_argument("--devices", type=int, default=10, help="Number of devices")
    parser.add_argument("--eps", type=int, default=1, help="Events per second per device")
    parser.add_argument("--output", type=str, default="data/telemetry.jsonl", help="Output file path")
    
    args = parser.parse_args()
    
    config = SimulatorConfig(
        num_devices=args.devices,
        events_per_second_per_device=args.eps,
        duration_seconds=args.duration,
        output_path=args.output,
        mode=args.mode
    )
    
    logger.info(f"Starting simulator for {config.duration_seconds}s with {config.num_devices} devices")
    
    if config.mode == "local":
        os.makedirs(os.path.dirname(config.output_path), exist_ok=True)
        # Clear existing file if any
        open(config.output_path, 'w').close()
        
    # Initialize devices WATCH-001, WATCH-002...
    devices = [DeviceState(f"WATCH-{str(i).zfill(3)}") for i in range(1, config.num_devices + 1)]
    anomaly_generator = AnomalyGenerator(config)
    
    start_time = time.time()
    total_events = 0
    
    try:
        producer_client = None
        f = None
        
        if config.mode == "fabric":
            if not config.fabric_eventhub_connection_string or not config.fabric_eventhub_name:
                logger.error("Fabric Event Hub configuration missing in .env")
                return
            try:
                producer_client = EventHubProducerClient.from_connection_string(
                    conn_str=config.fabric_eventhub_connection_string,
                    eventhub_name=config.fabric_eventhub_name
                )
            except Exception as e:
                logger.error(f"Failed to create EventHubProducerClient: {type(e).__name__}")
                return
                
        if config.mode == "local":
            f = open(config.output_path, 'a')
            
        while time.time() - start_time < config.duration_seconds:
            loop_start = time.time()
            current_dt = datetime.now(timezone.utc)
            
            event_data_batch = None
            if config.mode == "fabric":
                event_data_batch = producer_client.create_batch()
            
            batch_events_count = 0
            for device in devices:
                for _ in range(config.events_per_second_per_device):
                    normal_event = device.next_event(current_dt)
                    generated_events = anomaly_generator.apply_anomalies(normal_event, current_dt)
                    
                    for evt in generated_events:
                        if config.mode == "local":
                            f.write(json.dumps(evt) + "\n")
                        elif config.mode == "fabric":
                            try:
                                event_data_batch.add(EventData(json.dumps(evt)))
                            except ValueError:
                                # Batch is full, send and create new
                                producer_client.send_batch(event_data_batch)
                                event_data_batch = producer_client.create_batch()
                                event_data_batch.add(EventData(json.dumps(evt)))
                            batch_events_count += 1
                        total_events += 1
            
            if config.mode == "fabric" and batch_events_count > 0:
                try:
                    producer_client.send_batch(event_data_batch)
                    logger.info(f"Sent {batch_events_count} events to Fabric Eventstream.")
                except Exception as e:
                    logger.error(f"Failed to send batch to Fabric Eventstream: {type(e).__name__}")
            
            # Sleep to maintain tick rate roughly at 1 second
            elapsed = time.time() - loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user")
    finally:
        if 'f' in locals() and f:
            f.close()
        if 'producer_client' in locals() and producer_client:
            producer_client.close()
        
    logger.info(f"Simulation complete. Generated {total_events} events.")
    logger.info(f"Anomaly counts: {anomaly_generator.anomaly_counts}")
    if config.mode == "local":
        logger.info(f"Data written to {config.output_path}")

if __name__ == "__main__":
    main()
