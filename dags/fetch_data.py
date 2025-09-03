from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, date
import requests
import logging
import json
import os
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
    schedule='@daily',
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=["crypto", "api"]
)
def crypto_api_dag():

    @task
    def fetch_crypto_data():
        API_KEY = os.getenv("API_KEY")
        url = os.getenv("url")

        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": API_KEY,
        }

        params = {
            "start": "1",
            "limit": "20",
            "convert": "USD"
        }

        try:
            #fetch data from CoinMarketCap API
            logger.info("Sending request to CoinMarketCap API...")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully fetched data from API.")
            # end of fetch data from CoinMarketCap API

            # Save data to JSON file
            logger.info("Saving data to JSON file...")
            # directory = "/opt/bitnami/spark/data"
            file_path = f"/opt/bitnami/spark/data/crypto_data_{date.today()}.json"

            # file_path = os.path.join(directory, file_name)

            # os.makedirs(file_path, exist_ok=True)

            with open(file_path, "w") as json_file:
                json.dump(data, json_file, indent=4)
            logger.info(f"Data saved to {file_path}")
            # end of save data to JSON file

            # Store data in a dictionary
            # Extract the first entry
            coin = data.get("data")[0]
            result = {
                "Id": coin.get("id"),
                "Name": coin.get("name"),
                "Symbol": coin.get("symbol"),
                "Slug": coin.get("slug"),
                "Num_market_pairs": coin.get("num_market_pairs"),
                "Date_added": coin.get("date_added"),
                "Tags": coin.get("tags"),
                "Max_supply": coin.get("max_supply"),
                "Circulating_supply": coin.get("circulating_supply"),
                "Total_supply": coin.get("total_supply"),
                "Infinite_supply": coin.get("infinite_supply"),
                "Platform": coin.get("platform"),
                "Cmc_rank": coin.get("cmc_rank"),
                "Self_reported_circulating_supply": coin.get("self_reported_circulating_supply"),
                "Self_reported_market_cap": coin.get("self_reported_market_cap"),
                "Tvl_ratio": coin.get("tvl_ratio"),
                "Last_updated": coin.get("last_updated"),
                "Quote": coin.get("quote").get("USD")
            }
            # End of store data in a dictionary
            logger.info(f"Data: {result}")
            return result

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error: {http_err}")
        except Exception as err:
            logger.error(f"Unexpected error: {err}")

    fetch_crypto_data()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_transformations",  # This must match the dag_id of the second DAG
    )
    fetch_crypto_data() >> trigger_next

# Instantiate DAG
crypto_dag = crypto_api_dag()

