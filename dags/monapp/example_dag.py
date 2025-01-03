from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
        'example_dag',
        schedule_interval='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, Airflow!"',
    )
