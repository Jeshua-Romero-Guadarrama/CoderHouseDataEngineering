import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# El script ETL vive en /app (Docker) o en ../scripts (repositorio)
sys.path.append('/app')
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts'))
from jeshua_script import main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_data_extraction',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

run_etl = PythonOperator(
    task_id='extract_transform_load',
    python_callable=main,
    dag=dag
)

run_etl
