from airflow.sdk import dag,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

from s3_file_config import S3_DATA_ASSET,BUCKET_NAME,S3_KEY

@dag(
    dag_id="asset_producerss_pipeline",
    start_date=datetime(2026,7,12),
    schedule=None,
    tags=['producer']
)



def producer_workflow():
    # an outlet is a parameter used inside a task to declare that the task produces or updates a specific piece of data
    @task(outlets=[S3_DATA_ASSET])
    def upload_raw_file():
        print("Connecting to AWS....")
        s3_hook=S3Hook(aws_conn_id="aws_default")

        payload=f"Data-2-Dollars production run, Extracted at {datetime.now()}"
        print("Uploading Sir....")

        s3_hook.load_string(
            string_data=payload,
            key=S3_KEY,
            bucket_name=BUCKET_NAME,
            replace=True
        )

        print("Upload is succesfull Jassi")
    upload_raw_file()  
producer_workflow()