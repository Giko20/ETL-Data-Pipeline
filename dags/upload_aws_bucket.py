import os
import glob
from airflow.sdk import dag, task
import boto3
import logging
from datetime import datetime, date
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# DAG definition
@dag(
    dag_id='aws_s3_upload',
    schedule=None,
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=["aws", "s3"],
)
def aws_s3_upload():
    @task
    def upload_to_s3():
        access_key = os.getenv("access_key")
        secret_access_key = os.getenv("secret_access_key")
        aws_s3_bucket_name = os.getenv("aws_s3_bucket_name")
        aws_region = os.getenv("aws_region")

        # Local directories with wildcards
        data_files = {
            "general": "/opt/bitnami/spark/parquet_data/general_data.parquet/part*.parquet",
            "platform": "/opt/bitnami/spark/parquet_data/platform_data.parquet/part*.parquet",
            "quote": "/opt/bitnami/spark/parquet_data/quote_data.parquet/part*.parquet",
            "statuslog": "/opt/bitnami/spark/parquet_data/statuslog_data.parquet/part*.parquet",
        }

        # Initialize S3 client
        s3_client = boto3.client(
            service_name='s3',
            region_name=aws_region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )

        try:
            for data_type, pattern in data_files.items():
                matched_files = glob.glob(pattern)
                if not matched_files:
                    logger.warning(f"No files found for pattern: {pattern}")
                    continue

                for file_path in matched_files:
                    file_name = os.path.basename(file_path)
                    s3_key = f"uploads/{data_type}/{file_name}_{date.today()}.parquet"
                    s3_client.upload_file(file_path, aws_s3_bucket_name, s3_key)
                    logger.info(f"Uploaded {file_path} to s3://{aws_s3_bucket_name}/{s3_key}")

        except Exception as e:
            logger.error(f"Failed to upload files: {e}")
            raise

    upload_to_s3()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_aws_athena_upload",
        trigger_dag_id="aws_athena_upload",
    )

    upload_to_s3() >> trigger_next

aws_s3_upload_dag = aws_s3_upload()
