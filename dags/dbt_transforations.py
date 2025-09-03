from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import logging

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def dbt_run_athena():

    @task()
    def run_dbt():
        logger = logging.getLogger("airflow.task")

        try:
            dbt_cmd = (
                "cd /opt/airflow/dbt/dbt_athena && "
                "dbt debug --profiles-dir /home/airflow/.dbt && "
                "dbt run --profiles-dir /home/airflow/.dbt"
            )
            result = subprocess.run(dbt_cmd, shell=True, check=True, capture_output=True, text=True)
            logger.info("DBT Output:\n%s", result.stdout)

        except RuntimeError as e:
            logger.error(f"DBT run failed:\n{e}")
            raise

    run_dbt()

dbt_run_athena = dbt_run_athena()
