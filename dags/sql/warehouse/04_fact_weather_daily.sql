CREATE TABLE IF NOT EXISTS warehouse.fact_weather_daily (
    date_key DATE PRIMARY KEY,
    avg_temperature FLOAT,
    min_temperature FLOAT,
    max_temperature FLOAT,
    avg_humidity FLOAT
);

INSERT INTO warehouse.fact_weather_daily
SELECT
    date,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity
FROM weather_daily_summary
ON CONFLICT (date_key) DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    min_temperature = EXCLUDED.min_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_humidity = EXCLUDED.avg_humidity;
