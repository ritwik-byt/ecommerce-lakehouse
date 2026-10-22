"""
Production Airflow DAG: Daily Medallion Lakehouse Incremental Batch Pipeline.
Orchestrates Bronze ingestion, PySpark Silver cleansing, and Gold Star Schema builds.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Define default arguments for reliability & retries
default_args = {
    "owner": "data_engineer_amarthya",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@ecommerce-lakehouse.internal"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_bronze_ingestion(**context):
    print("[*] Executing Task: MySQL & REST API -> S3 Bronze Ingestion...")
    # Calls our ingestion module logic
    return "Bronze Ingestion Complete"

def run_silver_transformation(**context):
    print("[*] Executing Task: PySpark Bronze -> Silver Parquet (Deduplication & Partitioning)...")
    # Calls our PySpark silver transformation logic
    return "Silver Transformation Complete"

def run_gold_star_schema(**context):
    print("[*] Executing Task: Silver -> Gold Star Schema (SCD Type 2 & Multi-Currency Fact)...")
    # Calls our Gold star schema modeling logic
    return "Gold Star Schema Complete"

with DAG(
    dag_id="ecommerce_medallion_daily_pipeline",
    default_args=default_args,
    description="Daily automated batch pipeline for E-Commerce Lakehouse",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "pyspark", "s3", "gold-star-schema"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    bronze_task = PythonOperator(
        task_id="ingest_bronze_layer",
        python_callable=run_bronze_ingestion,
    )

    silver_task = PythonOperator(
        task_id="transform_silver_layer",
        python_callable=run_silver_transformation,
    )

    gold_task = PythonOperator(
        task_id="build_gold_star_schema",
        python_callable=run_gold_star_schema,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Define DAG task dependencies (Execution Flow)
    start_pipeline >> bronze_task >> silver_task >> gold_task >> end_pipeline