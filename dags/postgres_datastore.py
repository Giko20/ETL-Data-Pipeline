import os
import logging
from datetime import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# DAG definition
@dag(
    dag_id='postgres_datastore',
    schedule=None,
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=["postgres", "datawarehouse"]
)


def postgres_datastore():

    @task
    def get_postgres_connection():
        try:
            logger.info("Creating Spark session...")
            """Create Spark session with optimized configurations"""
            spark = SparkSession.builder \
                .appName("ParquetToPostgres") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
                .getOrCreate()
            
            # Set log level to reduce verbose output
            spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Connection: {spark}")
        except Exception as err:
            logger.error(f"Unexpected error: {err}")

        try:

            logger.info("Reading Parquet files...")
            gen_df = spark.read.parquet("/opt/bitnami/spark/parquet_data/general_data.parquet")
            platform_df = spark.read.parquet("/opt/bitnami/spark/parquet_data/platform_data.parquet")
            quote_df = spark.read.parquet("/opt/bitnami/spark/parquet_data/quote_data.parquet")
            statuslog_df = spark.read.parquet("/opt/bitnami/spark/parquet_data/statuslog_data.parquet")
            logger.info("✅ Parquet files read successfully.")

            database = 'projects'#os.getenv("POSTGRES_DB")
            postgresuser = 'project_user'#os.getenv("POSTGRES_USER")
            postgrespassword = 'MyProjects'#os.getenv("POSTGRES_PASSWORD")

            # Define the SQL query to create the table
            logger.info("Creating table General if it does not exist...")
            gen_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{database}") \
                .option("dbtable", "general") \
                .option("user", f"{postgresuser}") \
                .option("password", f"{postgrespassword}") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("✅ Table General created or already exists.")

            logger.info("Creating table Platform if it does not exist...")
            platform_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{database}") \
                .option("dbtable", "platform") \
                .option("user", f"{postgresuser}") \
                .option("password", f"{postgrespassword}") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("✅ Table Platform created or already exists.")

            logger.info("Creating table Quote if it does not exist...")
            quote_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{database}") \
                .option("dbtable", "quote") \
                .option("user", f"{postgresuser}") \
                .option("password", f"{postgrespassword}") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("✅ Table Quote created or already exists.")

            logger.info("Creating table Statuslog if it does not exist...")
            statuslog_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{database}") \
                .option("dbtable", "statuslog") \
                .option("user", f"{postgresuser}") \
                .option("password", f"{postgrespassword}") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("✅ Table Statuslog created or already exists.")

            try:
                conn_string = f'postgresql+psycopg2://{postgresuser}:{postgrespassword}@postgres:5432/{database}'       
                engine = create_engine(conn_string)

                with engine.connect() as conn:
                    result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='statuslog' AND column_name='con_id')")).scalar()

                    if not result:
                        conn.execute(text('ALTER TABLE statuslog ADD COLUMN con_id SERIAL PRIMARY KEY;'))
                        conn.execute(text('ALTER TABLE quote ADD COLUMN quote_id SERIAL PRIMARY KEY;'))
                        conn.execute(text('ALTER TABLE platform ADD COLUMN platform_id SERIAL PRIMARY KEY;'))
                        conn.execute(text('ALTER TABLE general ADD COLUMN general_id SERIAL PRIMARY KEY;'))
                        print("Primary key column added to table.")
                    else:
                        print("Primary key column already exists in table.")

            except Exception as e:
                print(f"Error checking table: {e}") # Print the error
                raise  

        except Exception as e:
            logger.info(f"🚨 Error : {e}")
    
    get_postgres_connection()

postgres_dag = postgres_datastore()