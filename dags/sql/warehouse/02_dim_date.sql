CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekend BOOLEAN
);

INSERT INTO warehouse.dim_date
SELECT
    d::DATE AS date_key,
    EXTRACT(YEAR FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d),
    TO_CHAR(d, 'Day'),
    TO_CHAR(d, 'Month'),
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
FROM generate_series(
    '2024-01-01'::DATE,
    '2026-12-31'::DATE,
    INTERVAL '1 day'
) d
ON CONFLICT (date_key) DO NOTHING;
