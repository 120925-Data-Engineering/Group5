"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#from airflow.providers.snowflake.operators import SnowflakeOperator
from datetime import datetime, timedelta
from pathlib import Path

DATABASE_ID = "STREAMFLOW_DW"

default_args = {
    'owner': 'student',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
# BASE_DIR = Path(__file__).resolve().parent.parent   # Assets folder

# rewriting the directories to match the airflow directory in docker!!!!!!
BASE_DIR = '/opt'
JOBS_DIR = f"{BASE_DIR}/spark-jobs"
GOLD_DIR = f"'{BASE_DIR}/spark-data/landing/*.json'"

KAFKA_TIMER = 30 #Argument for bash scripts for how long they run for

# 1. Task to upload files from local path to Snowflake Stage
# Using a PythonOperator with SnowflakeHook is often more flexible for local files
#@task
def upload_json_to_stage_temp():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    local_path = '/opt/spark-data/gold/user_events_batch*.json'
    stage_path = '@STREAMFLOW_DW.BRONZE.LANDING_STAGE'
    
    # The PUT command moves files from the Airflow worker to the stage
    # AUTO_COMPRESS=TRUE is recommended for JSON performance
    put_sql = f"PUT file://{local_path} {stage_path} AUTO_COMPRESS=TRUE OVERWRITE=FALSE;"
    hook.run(put_sql)


with DAG(
    dag_id='streamflow_warehouse',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    upload_to_stage = PythonOperator(
    task_id='upload_to_stage',
    python_callable=upload_json_to_stage_temp
)
    
    #Snowflake Task
    copy_csv_into_snowflake = SnowflakeOperator(
        task_id='copy_csv_into_snowflake',
        snowflake_conn_id='snowflake_connection',
        sql="""
    CREATE OR REPLACE FILE FORMAT LANDING_JSON_FF
      TYPE = JSON
      STRIP_OUTER_ARRAY = TRUE;

    CREATE OR REPLACE STAGE LANDING_JSON_STAGE
      FILE_FORMAT = LANDING_JSON_FF;

    PUT file:///opt/spark-data/landing/*.json @LANDING_JSON_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

    COPY INTO STREAMFLOW_DB.BRONZE.RAW_CUSTOMERS (RAW, FILENAME, LOAD_TS)
    FROM (
      SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
      FROM @LANDING_JSON_STAGE
    )
    FILE_FORMAT = (TYPE = BRONZE.JSON);
    """
    )

    copy_customer_json = SnowflakeOperator(
        task_id='copy_customer_json',
        snowflake_conn_id='snowflake_connection',
        sql=[
            "PUT file:///path/to/your/customers.json @STREAMFLOW_DB.BRONZE.%raw_customers;",
            "COPY INTO STREAMFLOW_DB.BRONZE.raw_customers FROM @STREAMFLOW_DB.BRONZE.%raw_customers_test FILE_FORMAT = (TYPE = 'JSON');"
        ]
    )
    #Path should be /opt/spark-data/gold
    # The files are user_events_batch{date}.json
    # I need to upload the new ones, but for now all of the json
    # At the moment it just goes into a variant
    # 2. Task to execute the remaining SQL logic
    load_raw_customers = SnowflakeOperator(
        task_id='load_raw_customers',
        snowflake_conn_id='snowflake_connection',
        sql="""
        COPY INTO STREAMFLOW_DW.BRONZE.raw_user_events_test (RAW_PAYLOAD, SOURCE_FILE)
        FROM (
        SELECT 
        $1,
        METADATA$FILENAME
        FROM @STREAMFLOW_DW.BRONZE.LANDING_STAGE
        )
        FILE_FORMAT = (FORMAT_NAME = STREAMFLOW_DW.BRONZE.JSON_FORMAT);
    """
)

    #Task dependencies
    #copy_csv_into_snowflake
    upload_to_stage >> load_raw_customers