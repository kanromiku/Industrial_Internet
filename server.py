# encoding: utf-8
"""
This file contains the modified server code.
- Parses the new data structure from the client.
- Inserts the data into the `methanol_plant_log` table in PostgreSQL.
"""

import asyncio
import json
import logging
import configparser
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import colorlog

import asyncpg
try:
    import aio_pika
except ImportError:
    aio_pika = None

# -- Logging Setup --
def get_logger(level=logging.INFO):
    """Configures the server logger."""
    logger = logging.getLogger("server")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    color_formatter = colorlog.ColoredFormatter(
        '%(log_color)s[SERVER] %(levelname)s: %(message)s',
        log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}
    )
    console_handler.setFormatter(color_formatter)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger

logger = get_logger()

# --- Configuration ---
config = configparser.ConfigParser()
config.read('config.ini')

DB_DSN = config.get('database', 'dsn', fallback='postgresql://postgres:postgres@localhost:5432/postgres')
TCP_HOST = config.get('server', 'host', fallback='localhost')
TCP_PORT = config.getint('server', 'port', fallback=9000)
RABBITMQ_ENABLED = config.getboolean('rabbitmq', 'enabled', fallback=False)
RABBITMQ_URL = config.get('rabbitmq', 'url', fallback='amqp://admin:admin@localhost:5672/')
RABBITMQ_EXCHANGE = config.get('rabbitmq', 'exchange', fallback='iot_exchange')
RABBITMQ_ROUTING_KEY = config.get('rabbitmq', 'routing_key', fallback='iot.data')

# --- SQL for inserting into the new table ---
INSERT_SQL_PLANT_LOG = """
INSERT INTO methanol_plant_log(
    record_time, 
    realtime_power_kw, today_energy_mwh, unit_energy_consumption,
    operating_rate, oee,
    workshop_data
)
VALUES($1, $2, $3, $4, $5, $6, $7::jsonb)
"""

def parse_plant_message(line: bytes or str) -> Optional[Dict[str, Any]]:
    """
    Parses a line of JSON from the client simulator into a dictionary
    structured for database insertion.
    """
    if isinstance(line, bytes):
        raw = line.decode("utf-8", errors="ignore")
    else:
        raw = str(line)
    raw = raw.strip()
    if not raw:
        return None

    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON received: {e}")
        return None

    # Extract top-level data
    ts_val = obj.get("timestamp") or obj.get("ts")
    if isinstance(ts_val, (int, float)):
        record_time = datetime.fromtimestamp(float(ts_val), tz=timezone.utc)
    else:
        record_time = datetime.now(timezone.utc)

    energy = obj.get("energy_consumption", {})
    ops = obj.get("operational_status", {})
    
    # Prepare workshop_data for JSONB column
    workshop_data = {
        "equipment_status": obj.get("equipment_status", {}),
        "main_workshop": obj.get("main_workshop", {})
    }

    # Assemble the final dictionary for insertion
    db_record = {
        "record_time": record_time,
        "realtime_power_kw": energy.get("realtime_power_kw"),
        "today_energy_mwh": energy.get("today_energy_mwh"),
        "unit_energy_consumption": energy.get("unit_energy_consumption"),
        "operating_rate": ops.get("operating_rate"),
        "oee": ops.get("oee"),
        "workshop_data": json.dumps(workshop_data, ensure_ascii=False)
    }
    
    return db_record


class TcpToDbServer:
    def __init__(self):
        self.db_pool: Optional[asyncpg.pool.Pool] = None
        self.rabbit_conn = None
        self.rabbit_channel = None
        self.rabbit_exchange = None

    async def init_db(self):
        self.db_pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
        logger.info("Connected to PostgreSQL: %s", DB_DSN)

    async def init_rabbit(self):
        if not RABBITMQ_ENABLED:
            logger.info("RabbitMQ disabled by config")
            return
        if aio_pika is None:
            logger.warning("aio_pika not installed; RabbitMQ disabled")
            return
        try:
            self.rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
            self.rabbit_channel = await self.rabbit_conn.channel()
            self.rabbit_exchange = await self.rabbit_channel.declare_exchange(
                RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
            )
            logger.info("Connected to RabbitMQ: %s", RABBITMQ_URL)
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")


    async def close(self):
        if self.rabbit_conn and not self.rabbit_conn.is_closed:
            await self.rabbit_conn.close()
        if self.db_pool:
            await self.db_pool.close()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Client connected: %s", addr)
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                
                db_record = parse_plant_message(line)
                if not db_record:
                    continue

                # Insert into DB
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute(
                            INSERT_SQL_PLANT_LOG,
                            db_record["record_time"],
                            db_record["realtime_power_kw"],
                            db_record["today_energy_mwh"],
                            db_record["unit_energy_consumption"],
                            db_record["operating_rate"],
                            db_record["oee"],
                            db_record["workshop_data"]
                        )
                        logger.info(f"Successfully inserted data for {db_record['record_time']}")
                except Exception as e:
                    logger.exception("DB insert failed for %s: %s", addr, e)

                # Optional: publish raw to RabbitMQ
                if self.rabbit_exchange is not None:
                    try:
                        message_body = json.dumps(db_record, default=str).encode("utf-8")
                        message = aio_pika.Message(body=message_body)
                        rk = f"{RABBITMQ_ROUTING_KEY}.plant_log"
                        await self.rabbit_exchange.publish(message, routing_key=rk)
                    except Exception:
                        logger.exception("RabbitMQ publish failed")

        except asyncio.IncompleteReadError:
            logger.info("Client disconnected: %s", addr)
        except Exception:
            logger.exception("Error handling client %s", addr)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("Connection closed: %s", addr)

    async def run(self):
        await self.init_db()
        await self.init_rabbit()
        server = await asyncio.start_server(self.handle_client, TCP_HOST, TCP_PORT)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logger.info("TCP server listening on %s", addrs)
        async with server:
            try:
                await server.serve_forever()
            finally:
                await self.close()

if __name__ == "__main__":
    srv = TcpToDbServer()
    try:
        asyncio.run(srv.run())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
