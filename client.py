# encoding: utf-8
"""
This file contains the modified client simulator code.
- Generates more realistic methanol plant simulation data.
- Reads server configuration from config.ini.
"""

import socket
import time
import json
import random
import configparser
import logging

def get_client_logger():
    """Configures the client logger."""
    logging.basicConfig(level=logging.INFO, format="[CLIENT] %(asctime)s %(levelname)s: %(message)s")
    return logging.getLogger(__name__)

client_logger = get_client_logger()

def generate_random_data() -> dict:
    """Generates a set of simulated data for a methanol production scenario."""
    
    # Define normal operating ranges for parameters
    POWER_NORMAL, POWER_RANGE = 5000, 500
    OEE_NORMAL, OEE_RANGE = 0.85, 0.05
    REACTOR_TEMP_NORMAL, REACTOR_TEMP_RANGE = 250.0, 5.0
    REACTOR_PRESSURE_NORMAL, REACTOR_PRESSURE_RANGE = 5.0, 0.5
    
    # 1. Energy Consumption
    realtime_power = round(random.uniform(POWER_NORMAL - POWER_RANGE, POWER_NORMAL + POWER_RANGE), 2)
    operating_rate = round(random.uniform(0.8, 1.0), 2)
    
    # 2. Operational Status
    oee = round(random.uniform(OEE_NORMAL - OEE_RANGE, OEE_NORMAL + OEE_RANGE), 3)
    
    # 3. Main Workshop
    storage_tank_level = round(random.uniform(0.2, 0.95), 2)
    condenser_level = round(random.uniform(0.3, 0.8), 2)
    reactor_pressure = round(random.uniform(REACTOR_PRESSURE_NORMAL - REACTOR_PRESSURE_RANGE, REACTOR_PRESSURE_NORMAL + REACTOR_PRESSURE_RANGE), 2)
    reactor_temp = round(random.uniform(REACTOR_TEMP_NORMAL - REACTOR_TEMP_RANGE, REACTOR_TEMP_NORMAL + REACTOR_TEMP_RANGE), 2)

    # Assemble into JSON format
    data = {
        # Timestamp and device ID are fundamental
        "timestamp": time.time(),
        "device_id": "methanol_plant_main",

        # Park-level data (to be stored in separate columns)
        "energy_consumption": {
            "realtime_power_kw": realtime_power,
            "today_energy_mwh": round(realtime_power * 8 / 1000, 2), # Assuming 8 hours of operation
            "unit_energy_consumption": round(realtime_power / (100 * operating_rate), 2) if operating_rate > 0 else 0
        },
        "operational_status": {
            "operating_rate": operating_rate,
            "oee": oee
        },
        
        # Detailed equipment-level data (to be stored in a JSONB column)
        "equipment_status": {
            "water_tank_1": {
                "temperature_c": round(random.uniform(60.0, 80.0), 2),
                "temperature_threshold_c": 90.0
            }
        },
        "main_workshop": {
            "storage_area": {
                "tank_model": "T-101A",
                "level_percentage": storage_tank_level,
                "pressure_mpa": round(random.uniform(0.1 - 0.02, 0.1 + 0.02), 3),
                "temperature_c": round(random.uniform(25.0 - 5.0, 25.0 + 5.0), 2)
            },
            "condensation_area": {
                "condenser_id": "C-201",
                "level_percentage": condenser_level,
                "hot_side_in_temp_c": round(random.uniform(90.0 - 2.0, 90.0 + 2.0), 2),
                "hot_side_out_temp_c": round(random.uniform(45.0 - 2.0, 45.0 + 2.0), 2),
            },
            "reaction_area": {
                "reactor_model": "R-301",
                "level_percentage": round(random.uniform(0.7, 0.9), 2),
                "pressure_mpa": reactor_pressure,
                "temperature_c": reactor_temp,
                "co_conversion_rate": round(random.uniform(0.95 - 0.05, 0.95), 3),
                "h2_utilization_rate": round(random.uniform(0.90 - 0.05, 0.90), 3)
            }
        }
    }
    return data

def run_client():
    """Main client function to connect to the server and send data in a loop."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    host = config.get('server', 'host', fallback='localhost')
    port = config.getint('server', 'port', fallback=9000)
    
    client_logger.info(f"Attempting to connect to {host}:{port}")

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                client_logger.info(f"Connected to server at {host}:{port}")
                while True:
                    data = generate_random_data()
                    message = json.dumps(data, ensure_ascii=False) + '\n'
                    
                    s.sendall(message.encode('utf-8'))
                    client_logger.info(f"Sent data: {json.dumps(data, indent=2)}")
                    
                    time.sleep(5) # Send data every 5 seconds
        
        except ConnectionRefusedError:
            client_logger.error("Connection refused. Is the server running? Retrying in 10 seconds...")
            time.sleep(10)
        except (BrokenPipeError, ConnectionResetError):
            client_logger.warning("Connection lost. Attempting to reconnect in 10 seconds...")
            time.sleep(10)
        except Exception as e:
            client_logger.critical(f"An unexpected error occurred: {e}")
            break

if __name__ == '__main__':
    run_client()
