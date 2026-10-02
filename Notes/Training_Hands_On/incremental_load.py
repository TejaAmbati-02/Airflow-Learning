from airflow.sdk import dag,task
from datetime import datetime

@dag(
    dag_id="automatic_incremental_pipeline",
    start_date=datetime(2026,7,11),
    schedule="@daily"
)
def incremental_demo():
    @task
    def extract_records(data_interval_start=None,data_internal_end=None):
        incremental_query=f"""
         Select * from orders where order_dt >= '{data_interval_start}' and order_dt < '{data_internal_end}'"""
        print(incremental_query)
    extract_records()
incremental_demo()