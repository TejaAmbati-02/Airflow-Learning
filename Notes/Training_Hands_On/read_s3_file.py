from airflow.sdk import dag,task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

BUCKET_NAME="airflowdemoimp"
S3_KEY="Airflow/simple.txt"

@dag(dag_id="read_s3_files",
     tags=['aws','s3'])
def s3_reader_pipeline():
    @task
    def fetch_s3_content():
        print("Connecting....")
        s3_hook=S3Hook(aws_conn_id="aws_default")
        if not s3_hook.check_for_key(key=S3_KEY,bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"the File {S3_KEY} is not present")
        file_content=s3_hook.read_key(key=S3_KEY,bucket_name=BUCKET_NAME)

        print("================================")
        print(file_content)
    fetch_s3_content()
s3_reader_pipeline()
