from airflow.sdk import dag,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

from s3_file_config import S3_DATA_ASSET,BUCKET_NAME,S3_KEY

@dag(
    dag_id="asset_consumerss_pipeline",
    start_date=datetime(2026,7,12),
    schedule=[S3_DATA_ASSET],
    tags=['consumer']
)


def consumer_workflow():
    @task
    def process_new_file():
        print("Comsumer wating for Asset Notification...")

        s3_hook=S3Hook(aws_conn="aws_default")


        print("Fetching Data ....")

        file_content=s3_hook.read_key(key=S3_KEY,bucket_name=BUCKET_NAME)

        print('======================================')
        print(f"File Content: {file_content}")
    process_new_file()
consumer_workflow()


