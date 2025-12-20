-- PostgreSQL schema for sensor data storage

CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sensor_device ON sensor_data (device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_ts ON sensor_data (ts);

