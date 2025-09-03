import os
from airflow.sdk import dag, task
import logging
from datetime import datetime, date
from pyathena import connect
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
    dag_id='aws_athena_upload',
    schedule=None,
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=["aws", "athena"],
)
def aws_athena_upload():
    @task
    def upload_to_athena():
        # Athena + S3 settings
        DATABASE = os.getenv("athena_database")
        S3_OUTPUT = f's3://{os.getenv("aws_s3_bucket_name")}/query_results/'
        S3_PLATFORM_DATA_LOCATION = f's3://{os.getenv("aws_s3_bucket_name")}/uploads/platform/'
        S3_QUOTE_DATA_LOCATION = f's3://{os.getenv("aws_s3_bucket_name")}/uploads/quote/'
        S3_GENERAL_DATA_LOCATION = f's3://{os.getenv("aws_s3_bucket_name")}/uploads/general/'
        S3_STATUSLOG_DATA_LOCATION = f's3://{os.getenv("aws_s3_bucket_name")}/uploads/statuslog/'
        PLATFORM_TABLE = 'platform'
        QUOTE_TABLE = 'quote'
        GENERAL_TABLE = 'general'
        STATUSLOG_TABLE = 'statuslog'

        try:
            # Athena connection
            conn = connect(
                s3_staging_dir=S3_OUTPUT,
                region_name='eu-north-1'#os.getenv("aws_region")
            )

            # Create DB if not exists
            create_db_query = f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE}
            """
            conn.cursor().execute(create_db_query)

            # Create external tables
            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{PLATFORM_TABLE} (
                credit_count int,
                elapsed int,
                error_message string,
                notice string,
                `timestamp` timestamp,
                total_count int
            )
            STORED AS PARQUET
            LOCATION '{S3_PLATFORM_DATA_LOCATION}'
            """

            cursor = conn.cursor()
            cursor.execute(create_table_query)
            logger.info(f"Table {PLATFORM_TABLE} created successfully!")



            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{QUOTE_TABLE} (
                Quote_Id int,
                Price double,
                Volume_24h double,
                Volume_Change_24h double,
                Percent_Change_1h double,
                Percent_Change_24h double,
                Percent_Change_7d double,
                Percent_change_30d double,
                Percent_change_60d double,
                Percent_change_90d double,
                Market_cap double,
                Market_cap_dominance double,
                Fully_diluted_market_cap double,
                Tvl string,
                Last_updated timestamp,
                INS_DATE date
            )
            STORED AS PARQUET
            LOCATION '{S3_QUOTE_DATA_LOCATION}'
            """

            cursor = conn.cursor()
            cursor.execute(create_table_query)
            logger.info(f"Table {QUOTE_TABLE} created successfully!")


            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{GENERAL_TABLE} (
                id int,
                name string,
                symbol string,
                slug string,
                num_market_pairs int,
                date_added timestamp,
                tags array<string>,
                max_supply string,
                circulating_supply decimal(30, 20),
                total_supply decimal(30, 20),
                infinite_supply boolean,
                cmc_rank int,
                self_reported_circulating_supply int,
                self_reported_market_cap decimal(30, 20),
                tvl_ratio string,
                last_updated timestamp,
                platform_id int,
                INS_DATE date
            )
            STORED AS PARQUET
            LOCATION '{S3_GENERAL_DATA_LOCATION}'
            """ 

            cursor = conn.cursor()
            cursor.execute(create_table_query)
            logger.info(f"Table {GENERAL_TABLE} created successfully!")



            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{STATUSLOG_TABLE} (
                credit_count int,
                elapsed int,
                error_message string,
                notice string,
                `timestamp` timestamp,
                total_count int
            )
            STORED AS PARQUET
            LOCATION '{S3_STATUSLOG_DATA_LOCATION}'
            """ 

            cursor = conn.cursor()
            cursor.execute(create_table_query)
            logger.info(f"Table {STATUSLOG_TABLE} created successfully!")
        except Exception as e:
            logger.error(f"Failed to upload to Athena: {e}")
            raise

    upload_to_athena()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dbt_run_athena",
        trigger_dag_id="dbt_run_athena",
    )

    upload_to_athena() >> trigger_next

aws_athena_upload_dag = aws_athena_upload()