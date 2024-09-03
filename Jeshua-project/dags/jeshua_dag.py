from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_data_extraction',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

run_etl = PythonOperator(
    task_id='extract_transform_load',
    python_callable=main,
    dag=dag
)

run_etl