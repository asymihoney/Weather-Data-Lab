from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from utils.alerting import task_failure_alert
# uncomment this success alert line below if needed
from utils.alerting_success_test import task_success_alert

def check_fact_weather_hourly():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    count = hook.get_first(
        "SELECT COUNT(*) FROM warehouse.fact_weather_hourly"
    )[0]

    if count == 0:
        raise AirflowSkipException(
            "No data in fact_weather_hourly, skipping downstream tasks"
        )

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="warehouse_models_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_default",
        sql="sql/warehouse/01_create_schema.sql",
    )

    dim_date = SQLExecuteQueryOperator(
        task_id="build_dim_date",
        conn_id="postgres_default",
        sql="sql/warehouse/02_dim_date.sql",
    )

    fact_hourly = SQLExecuteQueryOperator(
        task_id="build_fact_weather_hourly",
        conn_id="postgres_default",
        sql="sql/warehouse/03_fact_weather_hourly.sql",
        sla=timedelta(minutes=2),
    )

    check_fact_weather = PythonOperator(
        task_id="check_fact_weather_hourly",
        python_callable=check_fact_weather_hourly,
    )

    fact_daily = SQLExecuteQueryOperator(
        task_id="build_fact_weather_daily",
        conn_id="postgres_default",
        sql="sql/warehouse/04_fact_weather_daily.sql",
        # uncomment this success alert line below if needed
        on_success_callback=task_success_alert,
    )

    create_schema >> dim_date >> fact_hourly >> check_fact_weather >> fact_daily
