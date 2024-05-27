from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
import json
import pandas as pd
from clickhouse_driver import Client
from pandas import json_normalize
import re
from dag_script import *

# Функция для запуска Python скрипта
def run_python_script():
    exec(open('/opt/airflow/dags/dag_script.py').read())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'clickhouse_script_execution',
    default_args=default_args,
    description='Run a Python script to interact with ClickHouse',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

run_script = PythonOperator(
    task_id='run_dag_script',
    python_callable=run_python_script,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> run_script >> end
