CREATE TABLE IF NOT EXISTS warehouse.fact_weather_hourly (
    timestamp TIMESTAMP PRIMARY KEY,
    date_key DATE,
    temperature FLOAT,
    humidity FLOAT
);

INSERT INTO warehouse.fact_weather_hourly
SELECT
    wd.timestamp,
    wd.timestamp::DATE,
    wd.temperature,
    wd.humidity
FROM weather_data wd
ON CONFLICT (timestamp) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_hourly_date'
    ) THEN
        ALTER TABLE warehouse.fact_weather_hourly
        ADD CONSTRAINT fk_hourly_date
        FOREIGN KEY (date_key)
        REFERENCES warehouse.dim_date(date_key);
    END IF;
END $$;

-- The line 33 is intentional to simulate a failure for testing alerting mechanisms.
-- Only use it in local or development environments.
-- SELECT 1/0;