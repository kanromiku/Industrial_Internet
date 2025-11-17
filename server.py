# Async TCP server that receives JSON lines, stores into PostgreSQL, and optionally publishes to RabbitMQ.
# Configuration via environment variables.

import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any

import asyncpg
try:
    import aio_pika
except Exception:
    aio_pika = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Configuration (environment variables)
DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:lpc2005@192.168.1.110:5432/postgres")
TCP_HOST = os.getenv("TCP_HOST", "localhost")
TCP_PORT = int(os.getenv("TCP_PORT", "9000"))
RABBITMQ_ENABLED = os.getenv("RABBITMQ_ENABLED", "true").lower() in ("1", "true", "yes")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://admin:lpc2005@192.168.1.110:5672/test")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "iot_exchange")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "iot.data")

# SQL for inserting data
INSERT_SQL = """
INSERT INTO sensor_data(device_id, ts, payload, received_at)
VALUES($1, $2, $3::jsonb, $4)
"""


def _parse_iso8601_to_datetime(value: str) -> Optional[datetime]:
    """Parse common ISO8601 formats. Returns timezone-aware UTC datetime or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        # ensure tz-aware in UTC
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        s = str(value)
        # handle trailing Z
        if s.endswith("Z"):
            # Python fromisoformat doesn't accept Z, convert to +00:00
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def parse_message_line(line: bytes or str) -> Tuple[Optional[str], datetime, str]:
    """Parse a single line (bytes or str). Expect a JSON object per line.

    Returns (device_id, ts(datetime UTC), payload_json_str).
    If timestamp missing or invalid, ts is current UTC time.
    """
    if isinstance(line, bytes):
        raw = line.decode("utf-8", errors="ignore")
    else:
        raw = str(line)
    raw = raw.strip()
    if not raw:
        raise ValueError("empty line")
    # try parse JSON
    obj: Dict[str, Any]
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid json: {e}")

    device_id = obj.get("device_id") or obj.get("dev_id") or "unknown"

    ts_val = obj.get("timestamp") or obj.get("ts")
    ts = None
    if ts_val is not None:
        # numeric epoch
        if isinstance(ts_val, (int, float)):
            try:
                ts = datetime.fromtimestamp(float(ts_val), tz=timezone.utc)
            except Exception:
                ts = None
        else:
            ts = _parse_iso8601_to_datetime(ts_val)
    if ts is None:
        ts = datetime.now(timezone.utc)

    payload_json = json.dumps(obj, ensure_ascii=False)

    return device_id, ts, payload_json


class TcpToDbServer:
    def __init__(self):
        self.db_pool: Optional[asyncpg.pool.Pool] = None
        self.rabbit_conn = None
        self.rabbit_channel = None
        self.rabbit_exchange = None

    async def init_db(self):
        self.db_pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
        logging.info("Connected to PostgreSQL: %s", DB_DSN)

    async def init_rabbit(self):
        if not RABBITMQ_ENABLED:
            logging.info("RabbitMQ disabled by config")
            return
        if aio_pika is None:
            logging.warning("aio_pika not installed; RabbitMQ disabled")
            return
        self.rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
        self.rabbit_channel = await self.rabbit_conn.channel()
        self.rabbit_exchange = await self.rabbit_channel.declare_exchange(
            RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        logging.info("Connected to RabbitMQ: %s", RABBITMQ_URL)

    async def close(self):
        if self.rabbit_conn:
            await self.rabbit_conn.close()
        if self.db_pool:
            await self.db_pool.close()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logging.info("Client connected: %s", addr)
        try:
            async with self.db_pool.acquire() as conn:
                while not reader.at_eof():
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        device_id, ts, payload_json = parse_message_line(line)
                    except ValueError as e:
                        logging.warning("Skipping line from %s: %s", addr, e)
                        continue

                    # insert into DB
                    try:
                        await conn.execute(INSERT_SQL, device_id, ts, payload_json, datetime.now(timezone.utc))
                    except Exception:
                        logging.exception("DB insert failed for %s", addr)

                    # optional: publish raw to RabbitMQ
                    if self.rabbit_exchange is not None:
                        try:
                            message = aio_pika.Message(body=payload_json.encode("utf-8"))
                            rk = f"{RABBITMQ_ROUTING_KEY}.{device_id}"
                            await self.rabbit_exchange.publish(message, routing_key=rk)
                        except Exception:
                            logging.exception("RabbitMQ publish failed")

        except asyncio.IncompleteReadError:
            logging.info("Client disconnected: %s", addr)
        except Exception:
            logging.exception("Error handling client %s", addr)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logging.info("Connection closed: %s", addr)

    async def run(self):
        await self.init_db()
        await self.init_rabbit()
        server = await asyncio.start_server(self.handle_client, TCP_HOST, TCP_PORT)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logging.info("TCP server listening on %s", addrs)
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
        logging.info("Server stopped by user")

