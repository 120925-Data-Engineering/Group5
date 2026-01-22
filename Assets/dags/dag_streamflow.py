from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import os

default_args = {
    'owner': 'student',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# rewriting the directories to match the airflow directory in docker
BASE_DIR = '/opt'
JOBS_DIR = f"{BASE_DIR}/spark-jobs"
KAFKA_TIMER = 10 

def validate_outputs(**context):
    gold_dir = Path(f"{BASE_DIR}/spark-data/gold")
    files = list(gold_dir.rglob("*.csv"))

    if not files:
        raise FileNotFoundError(f"No CSV files found in {gold_dir} or its subfolders.")

    print(f"Validation successful. Found {len(files)} files:")
    for f in files:
        print(f" - {f.relative_to(gold_dir)}")

def archive_landing_data(**context):
    """Moves processed JSON files from Landing to Landing/processed."""
    landing_dir = Path(f"{BASE_DIR}/spark-data/landing")
    processed_dir = landing_dir / "processed"
    
    # Create processed directory if it doesn't exist
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all JSON files in the landing root
    files = list(landing_dir.glob("*.json"))
    
    if not files:
        print("No files to archive.")
        return

    print(f"Moving {len(files)} files to {processed_dir}...")
    for f in files:
        # Move file to processed folder
        shutil.move(str(f), processed_dir / f.name)


with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Kafka ingestion tasks
    kafka_user_events = BashOperator(
        task_id="ingest_user_events",
        bash_command=f"bash {JOBS_DIR}/run_consumer.sh transaction_events {KAFKA_TIMER}")

    kafka_transaction_events = BashOperator(
        task_id="ingest_transaction_events",
        bash_command=f"bash {JOBS_DIR}/run_consumer.sh user_events {KAFKA_TIMER}"
    )

    # Spark ETL job
    etl_job = BashOperator(
        task_id='etl_job',
        bash_command=(
            f"spark-submit "
            f"{JOBS_DIR}/etl_job.py "
            f"--input_path '{BASE_DIR}/spark-data/landing/*.json' "
            f"--output_path {BASE_DIR}/spark-data/gold"
        ),
    )
    
    # Check outputs
    validate_outputs_task = PythonOperator(
        task_id='validate_outputs',
        python_callable=validate_outputs,
    )

    # Archive inputs
    archive_task = PythonOperator(
        task_id='archive_landing_files',
        python_callable=archive_landing_data,
    )

    # Set dependencies
    [kafka_user_events, kafka_transaction_events] >> etl_job >> validate_outputs_task >> archive_task