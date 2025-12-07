from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

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
    context['ti'].xcom_push(key="weather_raw", value=data)

def transform_weather(**context):
    raw = context['ti'].xcom_pull(key="weather_raw")
    
    df = pd.DataFrame({
        "timestamp": raw["hourly"]["time"],
        "temperature": raw["hourly"]["temperature_2m"],
        "humidity": raw["hourly"]["relativehumidity_2m"],
    })
    
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    context['ti'].xcom_push(key="weather_df", value=df.to_json())

def load_weather(**context):
    df_json = context['ti'].xcom_pull(key="weather_df")
    df = pd.read_json(df_json)

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

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather
    )

    load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather
    )

    extract >> transform >> load