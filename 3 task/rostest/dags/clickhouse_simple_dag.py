from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator

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
    'clickhouse_simple_dag',
    default_args=default_args,
    description='A simple DAG to query ClickHouse',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

query_clickhouse = SimpleHttpOperator(
    task_id='query_clickhouse',
    http_conn_id='clickhouse_default',  
    endpoint='',  
    method='POST',
    data="SELECT * FROM db_test.logss LIMIT 10;",  
    headers={"Content-Type": "application/sql"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> query_clickhouse >> end
