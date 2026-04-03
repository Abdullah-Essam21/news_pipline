from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


# --- Configuration Constants ---
BASE_PATH = "/usr/local/airflow/include"
SCRAPY_DIR = f"{BASE_PATH}/youm7_scrape"
SPIDER_NAME = "download_new_articles_jsonl"
RAW_DATA_PATH = f"{BASE_PATH}/data/raw/new_urgent.jsonl"
PARQUET_DATA_PATH = f"{BASE_PATH}/data/intermediate/new_urgent.parquet"



# --- DAG Definition ---
@dag(
    dag_id="youm7_scrape_dag",
    description="DAG to scrape Youm7 news, convert the jsonl file to parquet, and send the parquet file to Google Cloud Storage then to BigQuery",
    start_date=datetime(2026, 4, 3),
    schedule=None,
    catchup=False)
def youm7_scraping_pipline():

    # Task 1: Scrapy
    run_spider = BashOperator(
        task_id="run_scrapy_spider",
        bash_command=f"cd {SCRAPY_DIR} && scrapy crawl {SPIDER_NAME} -o {RAW_DATA_PATH}")

    
    # Task 2: Convert jsonl to parquet
    @task
    def convert_jsonl_to_parquet():
        from task_scripts.jsonl_to_parquet import run_conversion_function
        run_conversion_function(RAW_DATA_PATH, PARQUET_DATA_PATH)
    



    run_spider >> convert_jsonl_to_parquet()

youm7_scraping_pipline()