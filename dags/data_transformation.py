from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_date, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, MapType, IntegerType, DateType, TimestampType, DecimalType
import sys
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, date
import requests
import logging
import json
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# DAG definition
@dag(
    dag_id='pyspark_transformations',
    schedule=None,
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=["pyspark", "transformation"]
)

def pyspark_transformations():

    @task
    def transform_data(app_name="DataTransformationApp"):

        try:
            logger.info("Creating Spark session...")
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Set log level to reduce verbose output
            spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Connection: {spark}")
        except Exception as err:
            logger.error(f"Unexpected error: {err}")


        try:
            logger.info("Transforming data...")
            # Load the data
            logger.info("Reading data from json file...")
            # df = spark.read.json(f"/opt/bitnami/spark/data/crypto_data_{date.today()}.json")
            df_raw = spark.read.option("multiLine", "true").json(f"/opt/bitnami/spark/data/crypto_data_{date.today()}.json")
            logger.info("Data loaded successfully.")

            # Extract the actual coin data from 'data' array
            df_flat = df_raw.select(explode("data").alias("coin_data"))
            df_result = df_flat.select("coin_data.*")
            logger.info(f"Data flattened successfully.\n{df_result.show()}")

            # You can also extract 'status' if needed:
            status_df = df_raw.select("status.*")

            # General data schema and dataframe creation
            logger.info("Creating General Data schema...")
            general_data_schema = StructType([
                StructField("Id", IntegerType(), False),
                StructField("Name", StringType(), False),
                StructField("Symbol", StringType(), False),
                StructField("Slug", StringType(), False),
                StructField("Num_market_pairs", IntegerType(), True),
                StructField("Date_added", TimestampType(), True),
                StructField("Tags", ArrayType(StringType()), True),
                StructField("Max_supply", StringType(), True),
                StructField("Circulating_supply", DoubleType(), True),
                StructField("Total_supply", DoubleType(), True),
                StructField("Infinite_supply", BooleanType(), True),
                StructField("Cmc_rank", IntegerType(), True),
                StructField("Self_reported_circulating_supply", DoubleType(), True),
                StructField("Self_reported_market_cap", DoubleType(), True),
                StructField("Tvl_ratio", StringType(), True),
                StructField("Last_updated", TimestampType(), True),
                StructField("Platform_Id", IntegerType(), True),
                StructField("INS_DATE", DateType(), False)  # Adding INS_DATE field
            ])

            # Perform transformations
            general_data = df_result.select(
                col("id"),
                col("name"),
                col("symbol"),
                col("slug"),
                col("num_market_pairs"),
                to_timestamp(col("date_added"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("Date_added"), # converting Date_added to timestamp
                col("tags"),
                col("max_supply"),
                col("circulating_supply"),
                col("total_supply"),
                col("infinite_supply"),
                col("cmc_rank"),
                col("self_reported_circulating_supply"),
                col("self_reported_market_cap"),
                col("tvl_ratio"),
                col("platform.id").alias("platform_id"),
                to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("Last_updated") # converting Last_updated to timestamp
            )
            general_data = general_data.withColumn("ins_date", current_date()) # adding new column with current date
            
            logger.info("creating GeneralData DataFrame with schema...")
            gen_data = spark.createDataFrame(general_data.rdd, schema=general_data_schema) # create DataFrame with schema
            gen_data.printSchema()
            gen_data.show()
            logger.info(f"General Data DataFrame created successfully!")

            gen_data.write.mode("overwrite").parquet("/opt/bitnami/spark/parquet_data/general_data.parquet")  # Save the DataFrame as Parquet file
            # End of general data transformation



            # Platform schema and dataframe creation
            logger.info("Creating Platform schema...")
            platform_schema = StructType([
                StructField("Platform_Id", IntegerType(), True),
                StructField("Name", StringType(), True),
                StructField("Symbol", StringType(), True),
                StructField("Slug", StringType(), True),
                StructField("Token_address", StringType(), True),
                StructField("INS_DATE", DateType(), False)  # Adding INS_DATE field
            ])

            logger.info("Creating Platform DataFrame...")
            platform_data = df_result.select(
                col("platform.id").alias("platform_id"),
                col("platform.name").alias("name"),
                col("platform.symbol").alias("symbol"),
                col("platform.slug").alias("slug"),
                col("platform.token_address").alias("token_address")
            )
            platform_data = platform_data.withColumn("ins_date", current_date()) # adding new column with current date
            
            platform_df = spark.createDataFrame(platform_data.rdd, schema=platform_schema) # create DataFrame with schema
            platform_df.printSchema()
            platform_df.show()
            logger.info("Platform DataFrame created successfully.")

            platform_df.write.mode("overwrite").parquet("/opt/bitnami/spark/parquet_data/platform_data.parquet")  # Save the DataFrame as Parquet file
            # End of platform data transformation

            # Quote schema and dataframe creation
            logger.info("Creating Quote schema...")
            quote_schema = StructType([
                StructField("Quote_Id", IntegerType(), True),
                StructField("Price", DoubleType(), True),
                StructField("Volume_24h", DoubleType(), True),
                StructField("Percent_Change_1h", DoubleType(), True),
                StructField("Percent_Change_24h", DoubleType(), True),
                StructField("Percent_Change_7d", DoubleType(), True),
                StructField("Percent_change_30d", DoubleType(), True),
                StructField("Percent_change_60d", DoubleType(), True),
                StructField("Percent_change_90d", DoubleType(), True),
                StructField("Market_cap", DoubleType(), True),
                StructField("Market_cap_dominance", DoubleType(), True),
                StructField("Fully_diluted_market_cap", DoubleType(), True),
                StructField("Tvl", StringType(), True),
                StructField("Last_updated", TimestampType(), True),
                StructField("INS_DATE", DateType(), False)  # Adding INS_DATE field
            ])
            quote_data = df_result.select(
                col("id").alias("quote_id"),
                col("quote.USD.price").alias("price"),
                col("quote.USD.volume_24h").alias("volume_24h"),
                col("quote.USD.percent_change_1h").alias("percent_change_1h"),
                col("quote.USD.percent_change_24h").alias("percent_change_24h"),
                col("quote.USD.percent_change_7d").alias("percent_change_7d"),
                col("quote.USD.percent_change_30d").alias("percent_change_30d"),
                col("quote.USD.percent_change_60d").alias("percent_change_60d"),
                col("quote.USD.percent_change_90d").alias("percent_change_90d"),
                col("quote.USD.market_cap").alias("market_cap"),
                col("quote.USD.market_cap_dominance").alias("market_cap_dominance"),
                col("quote.USD.fully_diluted_market_cap").alias("fully_diluted_market_cap"),
                col("quote.USD.tvl").alias("tvl"),
                to_timestamp(col("quote.USD.last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("last_updated") # converting last_updated to timestamp
            )

            quote_data = quote_data.withColumn("ins_date", current_date())

            quote_df = spark.createDataFrame(quote_data.rdd, schema=quote_schema) # create DataFrame with schema
            quote_df.printSchema()
            quote_df.show()
            logger.info("Quote DataFrame created successfully.")

            quote_df.write.mode("overwrite").parquet("/opt/bitnami/spark/parquet_data/quote_data.parquet")  # Save the DataFrame as Parquet file
            # End of quote data transformation


            # StatusLog schema and dataframe creation
            logger.info("Creating StatusLog schema...")
            statuslog_schema = StructType([
                StructField("credit_count", IntegerType(), False),
                StructField("elapsed", IntegerType(), True),
                StructField("error_message", StringType(), True),
                StructField("notice", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("total_count", IntegerType(), False)
            ])

            logger.info("Creating StatusLog DataFrame...")
            statuslog_data = status_df.select(
                col("credit_count"),
                col("elapsed"),
                col("error_message"),
                col("notice"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
                col("total_count")
            )
            
            statuslog_df = spark.createDataFrame(statuslog_data.rdd, schema=statuslog_schema) # create DataFrame with schema
            statuslog_df.printSchema()
            statuslog_df.show()
            logger.info("StatusLog DataFrame created successfully.")

            statuslog_df.write.mode("overwrite").parquet("/opt/bitnami/spark/parquet_data/statuslog_data.parquet")  # Save the DataFrame as Parquet file
            # End of StatusLog data transformation

            
        except Exception as err:
            logger.error(f"Unexpected error: {err}")

        finally:
            # Clean up Spark session
            if spark:
                spark.stop()
                logger.info("Spark session stopped.")

    transform_data()

    trigger_next1 = TriggerDagRunOperator(
        task_id="trigger_postgres_datastore",
        trigger_dag_id="postgres_datastore",
    )

    trigger_next2 = TriggerDagRunOperator(
        task_id="trigger_aws_s3_upload",
        trigger_dag_id="aws_s3_upload",
    )

    transform_data() >> [trigger_next1, trigger_next2]  # Set dependencies to trigger next DAGs

# Instantiate DAG
crypto_dag = pyspark_transformations()

