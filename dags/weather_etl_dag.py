from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import json
import boto3
from datetime import datetime
import io

# etl functions

def extract_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -6.2,
        "longitude": 106.8,
        "hourly": "temperature_2m,relativehumidity_2m",
    }

    response = requests.get(url, params=params)
    data = response.json()

    # S3 (MinIO) client
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    # File naming (partition by date)
    run_date = context["ds"]  # Airflow execution date
    object_key = f"weather_{run_date}.json"

    s3.put_object(
        Bucket="weather-bronze",
        Key=object_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

def transform_weather_silver(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    run_date = context["ds"]
    object_key = f"weather_{run_date}.json"

    # Read raw JSON from Bronze
    response = s3.get_object(
        Bucket="weather-bronze",
        Key=object_key
    )

    raw_data = json.loads(response["Body"].read())

    # Transform
    df = pd.DataFrame({
        "timestamp": raw_data["hourly"]["time"],
        "temperature": raw_data["hourly"]["temperature_2m"],
        "humidity": raw_data["hourly"]["relativehumidity_2m"],
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Quality checks
    if df.isnull().any().any():
        raise ValueError("NULL values detected in Silver layer")

    if not df["humidity"].between(0, 100).all():
        raise ValueError("Invalid humidity values")

    # Write Silver output
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    s3.put_object(
        Bucket="weather-silver",
        Key=f"weather_{run_date}.csv",
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )

def load_weather(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    run_date = context["ds"]
    object_key = f"weather_{run_date}.csv"

    response = s3.get_object(
        Bucket="weather-silver",
        Key=object_key
    )

    df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            timestamp TIMESTAMP PRIMARY KEY,
            temperature FLOAT,
            humidity FLOAT
        );
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO weather_data (timestamp, temperature, humidity)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
        """, (row["timestamp"], row["temperature"], row["humidity"]))

    conn.commit()
    cur.close()
    conn.close()

def data_quality_checks(**context):
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # row count must be > 0
    cur.execute("SELECT COUNT(*) FROM weather_data;")
    (count,) = cur.fetchone()

    if count == 0:
        raise ValueError("Data quality failed: weather_data table is empty!")

    # null value check
    cur.execute("""
        SELECT COUNT(*) 
        FROM weather_data
        WHERE timestamp IS NULL
           OR temperature IS NULL
           OR humidity IS NULL;
    """)
    (nulls,) = cur.fetchone()

    if nulls > 0:
        raise ValueError(f"Data quality failed: Found {nulls} NULL values!")

    # temperature range check
    cur.execute("""
        SELECT COUNT(*)
        FROM weather_data
        WHERE temperature < -50 OR temperature > 60;
    """)
    (bad_temp,) = cur.fetchone()

    if bad_temp > 0:
        raise ValueError(
            f"Invalid temperature values detected: {bad_temp} rows."
            )

    # humidity range check
    cur.execute("""
        SELECT COUNT(*)
        FROM weather_data
        WHERE humidity < 0 OR humidity > 100;
    """)
    (bad_hum,) = cur.fetchone()

    if bad_hum > 0:
        raise ValueError(
            f"Invalid humidity values detected: {bad_hum} rows."
            )

    print("All data quality checks passed.")

    cur.close()
    conn.close()
    
def build_weather_gold(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    run_date = context["ds"]
    object_key = f"weather_{run_date}.csv"

    response = s3.get_object(
        Bucket="weather-silver",
        Key=object_key
    )

    df = pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date

    gold_df = df.groupby("date").agg(
        avg_temperature=("temperature", "mean"),
        min_temperature=("temperature", "min"),
        max_temperature=("temperature", "max"),
        avg_humidity=("humidity", "mean"),
    ).reset_index()
    
    # Write Gold to MinIO
    gold_buffer = io.StringIO()
    gold_df.to_csv(gold_buffer, index=False)

    s3.put_object(
        Bucket="weather-gold",
        Key=f"weather_daily_summary_{run_date}.csv",
        Body=gold_buffer.getvalue(),
        ContentType="text/csv"
    )

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_daily_summary (
            date DATE PRIMARY KEY,
            avg_temperature FLOAT,
            min_temperature FLOAT,
            max_temperature FLOAT,
            avg_humidity FLOAT
        );
    """)

    for _, row in gold_df.iterrows():
        cur.execute("""
            INSERT INTO weather_daily_summary
            (date, avg_temperature, min_temperature, max_temperature, avg_humidity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                avg_temperature = EXCLUDED.avg_temperature,
                min_temperature = EXCLUDED.min_temperature,
                max_temperature = EXCLUDED.max_temperature,
                avg_humidity = EXCLUDED.avg_humidity;
        """, (
            row["date"],
            row["avg_temperature"],
            row["min_temperature"],
            row["max_temperature"],
            row["avg_humidity"]
        ))

    conn.commit()
    cur.close()
    conn.close()

# dag

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )

    load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather
    )
    
    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks
    )
    
    silver = PythonOperator(
    task_id="transform_weather_silver",
    python_callable=transform_weather_silver
    )
    
    gold = PythonOperator(
    task_id="build_weather_gold",
    python_callable=build_weather_gold
    )

    extract >> silver >> gold >> load >> data_quality