# Async TCP server that receives JSON lines, stores into PostgreSQL, and optionally publishes to RabbitMQ.
# Configuration via config.ini file.

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
except Exception:
    aio_pika = None

# -- Logging Setup --
def get_logger(level=logging.INFO):
    # 创建logger对象
    logger = logging.getLogger()
    logger.setLevel(level)
    # 创建控制台日志处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    # 定义颜色输出格式
    color_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)s: %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    # 将颜色输出格式添加到控制台日志处理器
    console_handler.setFormatter(color_formatter)
    # 移除默认的handler
    for handler in logger.handlers:
        logger.removeHandler(handler)
    # 将控制台日志处理器添加到logger对象
    logger.addHandler(console_handler)
    return logger


logger = get_logger()
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Configuration ---
config = configparser.ConfigParser()
config.read('config.ini')

# Database configuration
DB_DSN = config.get('database', 'dsn', fallback='postgresql://postgres:postgres@localhost:5432/postgres')

# TCP Server configuration
TCP_HOST = config.get('server', 'host', fallback='localhost')
TCP_PORT = config.getint('server', 'port', fallback=9000)

# RabbitMQ configuration
RABBITMQ_ENABLED = config.getboolean('rabbitmq', 'enabled', fallback=True)
RABBITMQ_URL = config.get('rabbitmq', 'url', fallback='amqp://admin:admin@loaclhost:5672/')
RABBITMQ_EXCHANGE = config.get('rabbitmq', 'exchange', fallback='iot_exchange')
RABBITMQ_ROUTING_KEY = config.get('rabbitmq', 'routing_key', fallback='iot.data')

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


def parse_message_line(line: bytes or str) -> tuple[Optional[str], datetime, str]:
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
        logger.info("Connected to PostgreSQL: %s", DB_DSN)

    async def init_rabbit(self):
        if not RABBITMQ_ENABLED:
            logger.info("RabbitMQ disabled by config")
            return
        if aio_pika is None:
            logger.warning("aio_pika not installed; RabbitMQ disabled")
            return
        self.rabbit_conn = await aio_pika.connect_robust(RABBITMQ_URL)
        self.rabbit_channel = await self.rabbit_conn.channel()
        self.rabbit_exchange = await self.rabbit_channel.declare_exchange(
            RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        logger.info("Connected to RabbitMQ: %s", RABBITMQ_URL)

    async def close(self):
        if self.rabbit_conn:
            await self.rabbit_conn.close()
        if self.db_pool:
            await self.db_pool.close()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        logger.info("Client connected: %s", addr)
        try:
            async with self.db_pool.acquire() as conn:
                while not reader.at_eof():
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        device_id, ts, payload_json = parse_message_line(line)
                    except ValueError as e:
                        logger.warning("Skipping line from %s: %s", addr, e)
                        continue

                    # insert into DB
                    try:
                        await conn.execute(INSERT_SQL, device_id, ts, payload_json, datetime.now(timezone.utc))
                    except Exception:
                        logger.exception("DB insert failed for %s", addr)

                    # optional: publish raw to RabbitMQ
                    if self.rabbit_exchange is not None:
                        try:
                            message = aio_pika.Message(body=payload_json.encode("utf-8"))
                            rk = f"{RABBITMQ_ROUTING_KEY}.{device_id}"
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

