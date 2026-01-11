"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path

default_args = {
    'owner': 'student',
    # TODO: Add retry logic, email alerts, etc.
}

BASE_DIR = Path(__file__).resolve().parent.parent   # Assets folder
BASH_DIR = BASE_DIR / "bash"

KAFKA_TIMER = 30 #Argument for bash scripts for how long they run for

with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files

    ##start producers(?)

    #kafka ingestion task
    # User events
    kafka_user_events = BashOperator(
        task_id="ingest_user_events",
        bash_command=f"bash {BASH_DIR / 'user_consumer.sh'} {KAFKA_TIMER}",
    )

    # Transaction events
    kafka_transaction_events = BashOperator(
        task_id="ingest_transaction_events",
        bash_command=f"bash {BASH_DIR / 'transaction_consumer.sh'} {KAFKA_TIMER}",
    )

    
    # TODO: Set dependencies
    [kafka_user_events, kafka_transaction_events] # >> etl_job would be here
