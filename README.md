# IndusNet - TCP Receiver Backend

This component is a minimal async TCP server that receives one JSON object per line, stores it into PostgreSQL (JSONB), and optionally publishes the raw JSON message to RabbitMQ.

Files:
- `server.py` - async TCP server implementation
- `db_init.sql` - SQL to create the `sensor_data` table
- `requirements.txt` - Python dependencies

Configuration (environment variables):
- DB_DSN - PostgreSQL DSN, e.g. `postgresql://postgres:password@127.0.0.1:5432/iotdb`
- TCP_HOST - host to bind, default `0.0.0.0`
- TCP_PORT - port to bind, default `9000`
- RABBITMQ_ENABLED - `true` to enable RabbitMQ publish (default `false`)
- RABBITMQ_URL - RabbitMQ URL, default `amqp://guest:guest@127.0.0.1:5672/`
- RABBITMQ_EXCHANGE - exchange name (topic), default `iot_exchange`
- RABBITMQ_ROUTING_KEY - base routing key, default `iot.data`

Quick start (using Docker for PostgreSQL and RabbitMQ):

1. Start PostgreSQL:

```powershell
:: Windows cmd.exe example
docker run -d --name pg -e POSTGRES_PASSWORD=password -e POSTGRES_DB=iotdb -p 5432:5432 postgres:14
```

Then apply schema:

```powershell
:: copy db_init.sql to container and psql
docker cp db_init.sql pg:/db_init.sql
docker exec -it pg psql -U postgres -d iotdb -f /db_init.sql
```

2. Start RabbitMQ (optional):

```powershell
:: management UI on 15672
docker run -d --hostname rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

3. Install dependencies and run server:

```powershell
python -m pip install -r requirements.txt
set DB_DSN=postgresql://postgres:password@127.0.0.1:5432/iotdb
set RABBITMQ_ENABLED=true
set RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
python server.py
```

4. Test by sending a JSON line (Windows example using ncat or PowerShell):

```powershell
:: using PowerShell
$payload = '{"device_id":"dev01","temperature":23.5,"timestamp":"2025-01-01T12:00:00Z"}'
$bytes = [System.Text.Encoding]::UTF8.GetBytes($payload + "\n")
$client = New-Object System.Net.Sockets.TcpClient
$client.Connect('127.0.0.1',9000)
$stream = $client.GetStream()
$stream.Write($bytes,0,$bytes.Length)
$stream.Close()
$client.Close()
```

Notes and suggestions:
- The server expects one JSON object per line. Keep messages small enough to fit in memory.
- If message rate is high, consider batching inserts or using a queue (RabbitMQ) to decouple ingestion and DB writes.
- For production: add monitoring, backpressure handling, connection limits and authentication.

