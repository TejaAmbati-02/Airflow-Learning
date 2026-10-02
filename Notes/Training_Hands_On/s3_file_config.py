from airflow.sdk import Asset


BUCKET_NAME = "airflowdemoimp"
S3_KEY = "Airflow/simple.txt"

S3_DATA_ASSET = Asset(f"s3://{BUCKET_NAME}/{S3_KEY}")