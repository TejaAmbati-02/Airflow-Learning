from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='child_dag_heavy',
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
    ) as dag:

    task_1=BashOperator(
        task_id='heavy_etl_part_1',
        bash_command='echo Running heavy processing step 1 for Jaswinder'
    )


    
    task_2=BashOperator(
        task_id='heavy_etl_part_2',
        bash_command='echo Running heavy processing step 2 for Jaswinder'
    )


    
    task_3=BashOperator(
        task_id='heavy_etl_part_3',
        bash_command='echo Running heavy processing step 3 for Jaswinder'
    )

    task_1 >> [task_2,task_3]