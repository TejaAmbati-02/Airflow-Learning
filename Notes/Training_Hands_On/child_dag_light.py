from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='child_dag_light',
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
    ) as dag:

    light_task=BashOperator(
        task_id='light_cute_part_1',
        bash_command='echo Running light for Jaswinder'
    )
