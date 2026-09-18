import argparse
import time
import json
import os
import logging
from datetime import datetime, timezone
from simulator.config import SimulatorConfig
from simulator.device_state import DeviceState
from simulator.anomaly_generator import AnomalyGenerator

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
        if config.mode == "local":
            with open(config.output_path, 'a') as f:
                while time.time() - start_time < config.duration_seconds:
                    loop_start = time.time()
                    current_dt = datetime.now(timezone.utc)
                    
                    for device in devices:
                        for _ in range(config.events_per_second_per_device):
                            normal_event = device.next_event(current_dt)
                            generated_events = anomaly_generator.apply_anomalies(normal_event, current_dt)
                            
                            for evt in generated_events:
                                f.write(json.dumps(evt) + "\n")
                                total_events += 1
                    
                    # Sleep to maintain tick rate roughly at 1 second
                    elapsed = time.time() - loop_start
                    if elapsed < 1.0:
                        time.sleep(1.0 - elapsed)
        else:
            logger.info("Fabric mode not yet implemented for Stage 2.")
            
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user")
        
    logger.info(f"Simulation complete. Generated {total_events} events.")
    logger.info(f"Anomaly counts: {anomaly_generator.anomaly_counts}")
    if config.mode == "local":
        logger.info(f"Data written to {config.output_path}")

if __name__ == "__main__":
    main()
