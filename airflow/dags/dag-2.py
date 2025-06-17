from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
def print_hello():
    print('Hello world!')
with DAG(
    'hello_world',
    description='Simple tutorial DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2021, 1, 7),
    catchup=False,
    tags=['example']
) as dag:
    dummy_operator = DummyOperator(
        task_id='dummy_task',
        retries=3
    )
    hello_operator = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )
    dummy_operator >> hello_operator