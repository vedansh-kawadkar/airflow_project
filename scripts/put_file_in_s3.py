import boto3
import json

def upload_to_s3():
    s3_client = boto3.client("s3")
    bucket_name = "airflow-etl-project-bucket-456321"
    bucket_key = "airflow_etl_project/input/"
    s3_file_name = "order_status.csv"

    s3_client.upload_file("lookup/order_status.csv", bucket_name, bucket_key+s3_file_name)
    
upload_to_s3()