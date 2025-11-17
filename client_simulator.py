# client_simulator.py
# A simple TCP client to simulate IoT devices sending data to the server.

import asyncio
import json
import random
import time
from datetime import datetime, timezone

# --- Configuration ---
# Match this with your server's host and port
SERVER_HOST = "localhost"
SERVER_PORT = 9000

# --- Simulation Parameters ---
# List of device IDs to simulate
DEVICE_IDS = [
    "temp-sensor-01",
    "pressure-sensor-01",
    "humidity-sensor-A",
    "vibration-monitor-X",
    "power-meter-B1"
]

# How many devices to run concurrently
CONCURRENT_DEVICES = 5

# Min/Max delay between sending messages for a single device (in seconds)
MIN_SEND_INTERVAL = 2
MAX_SEND_INTERVAL = 10


def generate_sensor_data(device_id: str) -> dict:
    """Generates a plausible sensor data payload."""
    now = datetime.now(timezone.utc)
    payload = {
        "device_id": device_id,
        # Send timestamp in ISO 8601 format, as handled by the server
        "ts": now.isoformat(),
        "status": "ok",
        "metrics": {}
    }

    if "temp" in device_id:
        # Temperature in Celsius
        payload["metrics"]["temperature"] = round(random.uniform(15.0, 35.0), 2)
    elif "pressure" in device_id:
        # Pressure in kPa
        payload["metrics"]["pressure"] = round(random.uniform(100.0, 105.0), 2)
    elif "humidity" in device_id:
        # Humidity in %
        payload["metrics"]["humidity"] = round(random.uniform(30.0, 60.0), 2)
    elif "vibration" in device_id:
        # Vibration in g
        payload["metrics"]["vibration_x"] = round(random.uniform(0.01, 0.5), 4)
        payload["metrics"]["vibration_y"] = round(random.uniform(0.01, 0.5), 4)
        payload["metrics"]["vibration_z"] = round(random.uniform(0.05, 1.5), 4)
    elif "power" in device_id:
        # Power consumption in kW
        payload["metrics"]["voltage"] = round(random.uniform(215.0, 225.0), 1)
        payload["metrics"]["current"] = round(random.uniform(1.0, 15.0), 2)
        payload["metrics"]["power"] = round(
            payload["metrics"]["voltage"] * payload["metrics"]["current"] / 1000, 3
        )
    else:
        payload["metrics"]["value"] = round(random.uniform(0, 100), 2)

    return payload


async def run_device_simulator(device_id: str):
    """A coroutine that simulates a single device connecting and sending data."""
    log_prefix = f"[{device_id}]"
    print(f"{log_prefix} Starting simulation.")

    while True:
        try:
            print(f"{log_prefix} Attempting to connect to {SERVER_HOST}:{SERVER_PORT}...")
            reader, writer = await asyncio.open_connection(SERVER_HOST, SERVER_PORT)
            print(f"{log_prefix} Connection successful.")

            while True:
                # Generate data
                data_payload = generate_sensor_data(device_id)
                # Convert to JSON string and add a newline character
                message_str = json.dumps(data_payload, ensure_ascii=False)
                message_bytes = (message_str + "\n").encode("utf-8")

                # Send data
                writer.write(message_bytes)
                await writer.drain()
                print(f"{log_prefix} Sent: {message_str}")

                # Wait for a random interval
                sleep_time = random.uniform(MIN_SEND_INTERVAL, MAX_SEND_INTERVAL)
                await asyncio.sleep(sleep_time)

        except (ConnectionRefusedError, ConnectionResetError, OSError) as e:
            print(f"{log_prefix} Connection error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"{log_prefix} An unexpected error occurred: {e}. Retrying in 10 seconds...")
            await asyncio.sleep(10)


async def main():
    """Main function to launch all device simulators."""
    print("--- Starting IoT Device Simulator ---")
    if CONCURRENT_DEVICES > len(DEVICE_IDS):
        print("Warning: CONCURRENT_DEVICES is greater than available DEVICE_IDS.")

    tasks = []
    # Create a task for each simulated device
    for i in range(min(CONCURRENT_DEVICES, len(DEVICE_IDS))):
        device_id = DEVICE_IDS[i]
        task = asyncio.create_task(run_device_simulator(device_id))
        tasks.append(task)

    # Wait for all tasks to complete (they run forever, so this will wait until cancelled)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n--- Simulator stopped by user. ---")


