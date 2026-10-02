from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='parent_master_dag_utube',
    start_date=datetime(2026,1,1),
    schedule='@daily',
    catchup=False
    ) as dag:

    start_pipeline=BashOperator(
        task_id='start_pipeline',
        bash_command='echo Initializing our Dag'
    )


    trigger_heavy_dag= TriggerDagRunOperator(
        task_id='Trigger_Heavy_Dag',
        trigger_dag_id='child_dag_heavy',
        wait_for_completion=True,
        poke_interval=10   #Checks Child Status every 10 secoonds
    )

    trigger_light_dag= TriggerDagRunOperator(
        task_id='Trigger_Light_Dag',
        trigger_dag_id='child_dag_light',
        wait_for_completion=True,
        poke_interval=10   #Checks Child Status every 10 secoonds
    )
    
    
    end_pipeline=BashOperator(
        task_id='end_pipeline',
        bash_command='echo Well Done u had completed the Airflow 3.0 tutorial'
    )
    

    start_pipeline >> [trigger_heavy_dag,trigger_light_dag]  >> end_pipeline