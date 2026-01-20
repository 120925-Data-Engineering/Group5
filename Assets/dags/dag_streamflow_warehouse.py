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
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
# BASE_DIR = Path(__file__).resolve().parent.parent   # Assets folder

# rewriting the directories to match the airflow directory in docker!!!!!!
BASE_DIR = '/opt'
JOBS_DIR = f"{BASE_DIR}/spark-jobs"

KAFKA_TIMER = 30 #Argument for bash scripts for how long they run for

# Creating a function to validate output in the gold zone
def validate_outputs(**context):
    gold_dir = Path(f"{BASE_DIR}/spark-data/gold")
    files = list(gold_dir.glob("*.csv"))

    if not files:
        raise FileNotFoundError("No CSV files found in gold zone")

    print("Validation successful:")
    for f in files:
        print(f" - {f}")


with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # DONE: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files

    ##start producers(?)
    
    #kafka ingestion task
    # User events
    
    #According to the Project Requirements we don't start the producers through the dag
    """
    kafka_user_events = BashOperator(
        task_id="ingest_user_events",
        #bash_command=f"bash {JOBS_DIR}/user_consumer.sh {KAFKA_TIMER}",
        bash_command=f"bash {JOBS_DIR}/run_consumer.sh transaction_events {KAFKA_TIMER}")

    # Transaction events
    kafka_transaction_events = BashOperator(
        task_id="ingest_transaction_events",
        bash_command=f"bash {JOBS_DIR}/run_consumer.sh user_events {KAFKA_TIMER}"
    )
    # These set xcom variables for the new json files it created
    # xcom(k,v) = (f"{topic}_json", f"{filename}")
    # We would pass in {topic}_json as the --input_path
"""
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
    
    # Checks to make sure gold zone received the CSV files
    validate_outputs_task = PythonOperator(
        task_id = 'validate_outputs',
        python_callable = validate_outputs,
    )


    # DONE: Set dependencies
    # [kafka_user_events, kafka_transaction_events] >> 
    etl_job >> validate_outputs_task
