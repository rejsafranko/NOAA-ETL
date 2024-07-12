from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.noaa_etl import run_noaa_pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 12),
    "email": None,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("noaa_dag", default_args=default_args, description="NOAA Weather ETL")

run_etl = PythonOperator(task_id="noaa_etl", python_callable=run_noaa_pipeline, dag=dag)

run_etl