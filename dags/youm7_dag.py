from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# --- Configuration Constants ---
# Using the standard Astro/Docker path
BASE_PATH = "/usr/local/airflow/include"
SCRAPY_DIR = os.path.join(BASE_PATH, "youm7_scrape")
SPIDER_NAME = "download_new_articles_jsonl"

# Default Arguments
default_args = {
    'owner': 'Abdullah Essam',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="youm7_scrape_etl_v1",
    description="ETL: Scrape Youm7 -> Convert Parquet -> Cloud Ready",
    start_date=datetime(2026, 4, 3),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=['news', 'data_engineering']
)
def youm7_scraping_pipeline():

    category = "urgent"

    # 1. Setup Directories Task
    @task
    def setup_dirs():
        """Ensure local data directories exist inside the container."""
        for folder in ["raw", "intermediate"]:
            path = os.path.join(BASE_PATH, "data", folder)
            os.makedirs(path, exist_ok=True)

    # 2. Scrapy Task (Templated Path)
    # We use {{ ts_nodash }} here because BashOperator supports Jinja templates
    raw_output_path = f"{BASE_PATH}/data/raw/{category}_{{{{ ts_nodash }}}}.jsonl"
    
    run_spider = BashOperator(
        task_id="run_scrapy_spider",
        bash_command=f"cd {SCRAPY_DIR} && scrapy crawl {SPIDER_NAME} -a category={category} -o {raw_output_path}"
    )

    # 3. Conversion Task (Python Decorator)
    @task
    def convert_to_parquet(category_name, **context):
        """
        Extracts paths from context to ensure the macro 'ts_nodash' matches 
        exactly what the Scrapy task produced.
        """
        from task_scripts.jsonl_to_parquet import run_conversion_function
        
        # Pulling the timestamp from the execution context
        ts = context['ts_nodash']
        input_p = f"{BASE_PATH}/data/raw/{category_name}_{ts}.jsonl"
        output_p = f"{BASE_PATH}/data/intermediate/{category_name}_{ts}.parquet"
        
        print(f"Starting conversion: {input_p} -> {output_p}")
        run_conversion_function(input_p, output_p)
        return output_p

    # --- Task Dependencies ---
    setup_step = setup_dirs()
    
    # Setup folders -> Run Spider -> Convert to Parquet
    setup_step >> run_spider >> convert_to_parquet(category)

# Initialize the DAG
youm7_scraping_pipeline()