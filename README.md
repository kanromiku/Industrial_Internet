# 空天地一体化化工智慧园区 - 数据接收与处理后端

## 1. 项目概述

本项目是“空天地一体化化工智慧园区”课程案例的后端实现部分，旨在构建一个稳定、高效、可扩展的数据接收与处理平台。

根据案例要求，本后端服务实现了**一套标准化的数据传输协议**，能够接收来自园区内部署的各类传感器（如温度、压力、湿度等）的**多源数据**。系统采用异步架构，确保在高并发场景下数据传输的低时延和高可靠性。
本段代码纯个人使用，请勿借鉴。

## 2. 核心功能

*   **高性能数据接收**: 基于 Python `asyncio` 构建的异步TCP服务器，能够同时处理大量设备连接。
*   **标准化数据协议**: 接收以换行符分隔的JSON格式数据，并对数据进行解析和验证。
*   **持久化数据存储**: 将解析后的传感器数据（包括设备ID、时间戳、原始负载）存入 **PostgreSQL** 数据库，使用 `JSONB` 类型以实现灵活存储和高效查询。
*   **消息队列集成**: （可选）将原始数据发布到 **RabbitMQ** 消息队列。这实现了系统解耦，便于后续增加数据分析、实时监控、异常报警等下游服务。

## 3. 系统架构与文件说明

*   `server.py`: 核心后端服务。一个异步TCP服务器，负责监听端口、接收和处理数据。
*   `client_simulator.py`: 一个模拟客户端程序。用于模拟多个物联网设备，并发地向服务器发送随机生成的传感器数据，方便进行测试和演示。
*   `db_init.sql`: 数据库初始化脚本。用于在 PostgreSQL 中创建所需的 `sensor_data` 表和索引。
*   `requirements.txt`: Python 依赖包列表。
*   `README.md`: 本文档。

## 4. 数据传输协议

本服务实现的数据协议满足了案例的技术指标要求，具体规范如下：

*   **传输层**: `TCP`
*   **数据格式**: `JSON`
*   **消息分隔**: 每个JSON对象必须以换行符 `\n` 结尾。
*   **核心字段**:
    *   `device_id` (或 `dev_id`): 字符串，设备的唯一标识符。
    *   `ts` (或 `timestamp`): 数据产生的时间戳。支持 **Unix时间戳** (如 `1672531200`) 和 **ISO 8601 格式字符串** (如 `"2023-01-01T00:00:00Z"`)。
*   **示例**:
    ```json
    {"device_id": "temp-sensor-01", "ts": "2023-10-27T10:00:00Z", "metrics": {"temperature": 25.4}}
    ```

## 5. 快速开始

### 步骤 1: 启动依赖服务 (PostgreSQL & RabbitMQ)

建议使用 Docker 启动所需的基础服务。

*   **启动 PostgreSQL**:
    ```powershell
    # 启动一个PostgreSQL 14容器，数据库名为iotdb，密码为password
    docker run -d --name pg -e POSTGRES_PASSWORD=password -e POSTGRES_DB=iotdb -p 5432:5432 postgres:14
    ```

*   **初始化数据库表**:
    ```powershell
    # 将初始化脚本复制到容器内
    docker cp db_init.sql pg:/db_init.sql
    # 执行脚本创建 sensor_data 表
    docker exec -it pg psql -U postgres -d iotdb -f /db_init.sql
    ```

*   **启动 RabbitMQ (可选)**:
    ```powershell
    # 启动RabbitMQ并暴露管理后台端口15672
    docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    ```
    访问 `http://localhost:15672`，使用 `guest`/`guest` 登录管理后台。

### 步骤 2: 安装 Python 依赖

```powershell
pip install -r requirements.txt
```

### 步骤 3: 配置并运行后端服务器

在终端中设置环境变量并启动 `server.py`。

> **注意**: 请根据您在步骤1中设置的密码和您服务器的实际IP地址修改以下配置。

```powershell
# --- PowerShell 语法 ---

# 数据库连接字符串
$env:DB_DSN="postgresql://postgres:password@127.0.0.1:5432/iotdb"

# (可选) 启用并配置RabbitMQ
$env:RABBITMQ_ENABLED="true"
$env:RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/"

# 启动服务器
python server.py
```

服务器成功启动后，您会看到日志 `TCP server listening on ...`。

### 步骤 4: 运行模拟客户端发送数据

打开**一个新的终端**，运行 `client_simulator.py`。

```powershell
python client_simulator.py
```

您将在模拟器终端看到发送数据的日志，同时在服务器终端看到接收、处理和存储数据的日志。

## 6. 详细配置 (环境变量)

*   `DB_DSN`: **(必需)** PostgreSQL数据库连接字符串。
    *   格式: `postgresql://<user>:<password>@<host>:<port>/<dbname>`
    *   示例: `postgresql://postgres:password@127.0.0.1:5432/iotdb`
*   `TCP_HOST`: TCP服务器监听的主机地址。
    *   默认值: `localhost`
*   `TCP_PORT`: TCP服务器监听的端口。
    *   默认值: `9000`
*   `RABBITMQ_ENABLED`: 是否启用RabbitMQ功能。
    *   有效值: `true`, `1`, `yes`
    *   默认值: `false`
*   `RABBITMQ_URL`: RabbitMQ连接字符串。
    *   默认值: `amqp://guest:guest@127.0.0.1:5672/`
*   `RABBITMQ_EXCHANGE`: RabbitMQ交换机名称。
    *   默认值: `iot_exchange`
*   `RABBITMQ_ROUTING_KEY`: 发布消息时使用的基础路由键。
    *   默认值: `iot.data` (最终路由键为 `iot.data.<device_id>`)

