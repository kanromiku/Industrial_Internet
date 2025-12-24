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
import colorlog


def get_client_logger(level=logging.INFO):
    """Configures the client logger."""
    logger = logging.getLogger("server")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    color_formatter = colorlog.ColoredFormatter(
        '%(log_color)s[CLIENT] %(levelname)s: %(message)s',
        log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}
    )
    console_handler.setFormatter(color_formatter)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger

client_logger = get_client_logger()

def generate_random_data() -> dict:
    """Generates a set of simulated data for a methanol production scenario."""
    
    # Define normal operating ranges for parameters
    POWER_NORMAL, POWER_RANGE = 30, 20
    OEE_NORMAL, OEE_RANGE = 0.92, 0.06
    REACTOR_TEMP_NORMAL, REACTOR_TEMP_RANGE = 250.0, 5.0
    REACTOR_PRESSURE_NORMAL, REACTOR_PRESSURE_RANGE = 5.0, 0.5
    
    # 1. Energy Consumption
    realtime_power = round(random.uniform(POWER_NORMAL - POWER_RANGE, POWER_NORMAL + POWER_RANGE), 2)
    operating_rate = round(random.uniform(0.8, 1.0), 2)
    
    # 2. Operational Status
    oee = round(random.uniform(OEE_NORMAL - OEE_RANGE, OEE_NORMAL + OEE_RANGE), 3)
    
    # 3. Main Workshop
    storage_tank_total_height = 25
    storage_tank_level_percentage = round(random.uniform(0.2, 0.95), 2)
    storage_tank_level_height = storage_tank_total_height * storage_tank_level_percentage
    condenser_tank_total_height = 25
    condenser_level_percentage = round(random.uniform(0.3, 0.8), 2)
    condenser_level_height = condenser_tank_total_height * condenser_level_percentage
    reactor_total_height = 20
    reactor_level_percentage = round(random.uniform(0.7, 0.9), 2)
    reactor_level_height = reactor_level_percentage * reactor_total_height
    reactor_pressure = round(random.uniform(REACTOR_PRESSURE_NORMAL - REACTOR_PRESSURE_RANGE, REACTOR_PRESSURE_NORMAL + REACTOR_PRESSURE_RANGE), 2)
    reactor_temp = round(random.uniform(REACTOR_TEMP_NORMAL - REACTOR_TEMP_RANGE, REACTOR_TEMP_NORMAL + REACTOR_TEMP_RANGE), 2)

    # Assemble into JSON format
    data = {
        # Timestamp and device ID are fundamental
        "timestamp": time.time(),
        "device_id": "methanol_plant_main",

        # Park-level data (to be stored in separate columns)
        "energy_consumption": {
            "realtime_power": str(realtime_power) + 'MW',
            "today_energy": str(round(realtime_power * 8, 2)) + 'MWh', # Assuming 8 hours of operation
            "unit_energy_consumption": str(round(random.uniform(1200, 1600), 0)) + 'kgce/t' # kg of coal equivalent per ton of methanol
        },
        "operational_status": {
            "operating_rate": str(round(operating_rate * 100, 1)) + '%',
            "oee": str(round(oee * 100, 1)) + '%'
        },
        
        # Detailed equipment-level data (to be stored in a JSONB column)
        "equipment_status": {
            "water_tank": {
                "temperature": str(round(random.uniform(60.0, 80.0), 2)) + "\xB0C",
                "temperature_threshold": "90.0\xB0C"
            },
            "boiler": {
                "temperature": str(round(random.uniform(60.0, 80.0), 2)) + "\xB0C",
                "temperature_threshold": "90.0\xB0C"
            }
        },
        "main_workshop": {
            "storage_area": {
                0:{
                    "tank_model": "T-101A",
                    "level_height": str(storage_tank_level_height) + 'm',
                    "level_percentage": str(round(storage_tank_level_percentage * 100, 1)) + '%',
                    "pressure": str(round(random.uniform(0.1 - 0.02, 0.1 + 0.02), 3)) + 'MPa',
                    "temperature": str(round(random.uniform(25.0 - 5.0, 25.0 + 5.0), 2)) + "\xB0C"
                 }
            },
            "condensation_area": {
                0:{
                    "condenser_id": "C-201",
                    "level_height": str(condenser_level_height) + 'm',
                    "level_percentage": str(round(condenser_level_percentage * 100, 1)) + '%',
                    "hot_side_in_temp": str(round(random.uniform(90.0 - 2.0, 90.0 + 2.0), 2)) + '\xB0C',
                    "hot_side_out_temp": str(round(random.uniform(45.0 - 2.0, 45.0 + 2.0), 2)) + '\xB0C',
                    "cold_side_in_temp": str(round(random.uniform(15.0 - 2.0, 15.0 + 2.0), 2)) + '\xB0C',
                    "cold_side_out_temp": str(round(random.uniform(25.0 - 2.0, 25.0 + 2.0), 2)) + '\xB0C',
                }
            },
            "reaction_area": {
                0:{
                    "reactor_model": "R-301",
                    "level_height": str(reactor_level_height) + 'm',
                    "level_percentage": str(round(reactor_level_percentage * 100, 1)) + '%',
                    "pressure": str(reactor_pressure) + 'MPa',
                    "temperature": str(reactor_temp) + '\xB0C',
                    "co_conversion_rate": str(round(random.uniform(0.9, 0.95) * 100, 3)) + '%',
                    "h2_utilization_rate": str(round(random.uniform(0.90 - 0.05, 0.90) * 100, 3)) + '%'
                }
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

    min_interval = config.getint('client_simulator', 'min_send_interval', fallback=5)
    max_interval = config.getint('client_simulator', 'max_send_interval', fallback=10)
    
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
                    
                    time.sleep(random.uniform(min_interval, max_interval)) # Send data every 5 seconds
        
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
