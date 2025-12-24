-- 删除已存在的表以进行重新创建
DROP TABLE IF EXISTS methanol_plant_log;

-- 创建一个单一的日志表来存储所有数据
-- 使用 JSONB 数据类型来存储嵌套的设备信息，它在查询和索引方面性能更优
CREATE TABLE methanol_plant_log (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- 园区级数据
    realtime_power TEXT NOT NULL,
    today_energy TEXT NOT NULL,
    unit_energy_consumption TEXT NOT NULL,
    operating_rate TEXT NOT NULL,
    oee TEXT NOT NULL,

    -- 车间和设备数据 (存储为 JSONB)
    workshop_data JSONB NOT NULL
);

-- 为表和列添加注释
COMMENT ON TABLE methanol_plant_log IS '甲醇工厂生产日志总表';
COMMENT ON COLUMN methanol_plant_log.id IS '日志唯一ID';
COMMENT ON COLUMN methanol_plant_log.record_time IS '记录时间 (带时区)';
COMMENT ON COLUMN methanol_plant_log.realtime_power IS '实时功率 (kW)';
COMMENT ON COLUMN methanol_plant_log.today_energy IS '今日累计能耗 (MWh)';
COMMENT ON COLUMN methanol_plant_log.unit_energy_consumption IS '单位产品能耗';
COMMENT ON COLUMN methanol_plant_log.operating_rate IS '开工率 (%)';
COMMENT ON COLUMN methanol_plant_log.oee IS '设备综合效率 (OEE)';
COMMENT ON COLUMN methanol_plant_log.workshop_data IS '包含所有车间设备详细数据的JSONB对象';

-- 为时间戳创建索引以加速时序查询
CREATE INDEX idx_methanol_plant_log_record_time ON methanol_plant_log (record_time) ;

-- (可选) 为JSONB数据中的特定键创建GIN索引，以加速对JSON内部的查询
-- 例如，为反应器温度创建索引
-- CREATE INDEX idx_workshop_data_reactor_temp ON methanol_plant_log USING GIN ((workshop_data -> 'reaction_area' ->> 'temperature_c'));
