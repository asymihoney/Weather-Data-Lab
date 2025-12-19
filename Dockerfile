FROM apache/airflow:2.10.1

USER airflow
RUN pip install --no-cache-dir boto3