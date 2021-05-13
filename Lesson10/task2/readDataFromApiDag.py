from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from script.readDataFromApi import load_config, main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def read_data_from_api_task():
    config = load_config()
    main(config)


with DAG(
        dag_id='read_data_from_api_dag',
        default_args=default_args,
        description='Read data from api and disk',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 5, 9, 12),
) as dag:
    t1 = PythonOperator(
        task_id='read_data_from_api_task',
        dag=dag,
        python_callable=read_data_from_api_task
    )
