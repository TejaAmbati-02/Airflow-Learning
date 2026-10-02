from airflow.sdk import dag, task, Asset
from datetime import datetime,timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# @dag(
#     dag_id="Scheduled_method_cron_preset",
#     start_date=datetime(2026,7,11),
#     schedule="@daily",
#     catchup=True
# )


# def cron_preset_dag():
#     @task
#     def run_daily():
#         print("running cron present")
    
#     run_daily()
# cron_preset_dag()


# @dag(
#     dag_id="Scheduled_method_cron_expressions",
#     start_date=datetime(2026,7,11),
#     schedule="1 0 * * *",
#     catchup=False,
#     is_paused_upon_creation=False
# )

# def cron_expression_dag():
#     @task
#     def run_daily():
#         print("running cron present")
    
#     run_daily()
# cron_expression_dag()


# @dag(
#     dag_id="Scheduled_method_time_delta",
#     start_date=datetime(2026,7,11),
#     schedule=timedelta(minutes=45),
#     catchup=False,
#     is_paused_upon_creation=False
# )

# def time_delta_dag():
#     @task
#     def run_daily():
#         print("running time_delta_dag")
    
#     run_daily()
# time_delta_dag()

# jaswinder = Asset("s3://airflowdemoimp/Airflow/simple.txt")

# @dag(dag_id="aws_s3_asset_based_scheduling",
#      start_date=datetime(2026,7,9),
#      schedule="@hourly",
#      catchup=False
#      )
# def s3_reader_pipeline():
#     @task
#     def fetch_s3_content():
#         print("Connecting....")
#         s3_hook=S3Hook(aws_conn_id="aws_default")
#         if not s3_hook.check_for_key(key=S3_KEY,bucket_name=BUCKET_NAME):
#             raise FileNotFoundError(f"the File {S3_KEY} is not present")
#         file_content=s3_hook.read_key(key=S3_KEY,bucket_name=BUCKET_NAME)

#         print("================================")
#         print(file_content)
#     fetch_s3_content()
# s3_reader_pipeline()


