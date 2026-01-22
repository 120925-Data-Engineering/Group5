"""
StreamFlow Phase 2 DAG - Loads Gold Zone CSVs into Snowflake Bronze tables.

Prerequisites:
    1. Configure Airflow Connection 'snowflake_default' in Admin → Connections
    2. Create CSV_STAGE in Snowflake BRONZE schema:
       Internal stage (temporary cloud storage for file uploads).
       Snowflake cannot load local files directly - files must first be
       uploaded to a stage, then copied into tables.
       Example: CREATE STAGE CSV_STAGE;
    3. Create Bronze tables (raw_user_events, raw_transactions, etc.)
"""

# data is in
# /opt/spark-data/gold

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os
import glob

# Path where Spark ETL writes Gold Zone CSVs (shared Docker volume)
GOLD_ZONE_PATH = '/opt/spark-data/gold'
# this used to be snow_conn = "snowflake_connection"
# I am changing it to "snowflake_default" to be consistent with
# snowflake_conn_id because there may be inconsistencies with it if I don't
snow_conn = "snowflake_connection"
BASE_DIR = '/opt'
JOBS_DIR = f"{BASE_DIR}/spark-jobs"
# Maps CSV file patterns to their corresponding Bronze table names
CSV_TO_TABLE = {
    'user_events/part-*.csv': 'raw_user_events',
    'transactions/part-*.csv': 'raw_transactions',
    'products*.csv': 'raw_products',
    'customers*.csv': 'raw_customers',
}


def load_to_snowflake(**context):
    """Upload Gold Zone CSVs to Snowflake Bronze tables."""
    
    if not os.path.exists(GOLD_ZONE_PATH):
        raise ValueError(f"CRITICAL ERROR: Path {GOLD_ZONE_PATH} does not exist on this worker.")
    
    print(f"Listing contents of {GOLD_ZONE_PATH}:")
    files_in_dir = os.listdir(GOLD_ZONE_PATH)
    print(files_in_dir)
    
    if not files_in_dir:
        print("WARNING: Directory is empty.")

    # SnowflakeHook reads connection details from Airflow Connection 'snowflake_default'
    hook = SnowflakeHook(snowflake_conn_id=snow_conn)
    # Inserting a try except block here for error handling throughout the process
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("USE DATABASE STREAMFLOW_DW")
        cursor.execute("USE SCHEMA BRONZE")

        for pattern, table in CSV_TO_TABLE.items():
            # Find all CSVs matching this pattern (e.g., user_events_001.csv, user_events_002.csv)
            for csv_file in glob.glob(os.path.join(GOLD_ZONE_PATH, pattern)):
                # PUT uploads local file to Snowflake internal stage
                try:
                    cursor.execute(f"PUT file://{csv_file} @CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                    print(f"Uploaded: {csv_file}")
                except Exception as e:
                    print(f"ERROR uploading {csv_file}: {e}")
                    raise
        
            # COPY INTO loads staged files into the Bronze table (inline CSV format)
            try: 
                cursor.execute(f"""
                    COPY INTO STREAMFLOW_DW.BRONZE.{table}
                    FROM @STREAMFLOW_DW.BRONZE.CSV_STAGE
                    FILE_FORMAT = (FORMAT_NAME = 'STREAMFLOW_DW.BRONZE.CSV_FORMAT')
                    ON_ERROR = 'CONTINUE'
                """)
                print(f"COPY INTO completed for table: {table}")
            except Exception as e:
                print(f"ERROR during COPY INTO for table {table}: {e}")
                raise
    
        # Clean up staged files (REMOVE deletes files only, not the stage itself - stage persists for reuse)
        cursor.execute("REMOVE @STREAMFLOW_DW.BRONZE.CSV_STAGE")
        conn.commit()
    except Exception as e:
        print(f"FATAL ERROR in load_to_snowflake: {e}")
        raise # This lets Airflow handle retries
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id='dag_snowflake',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manually triggered
    catchup=False,
) as dag:
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
    # Single task: load all Gold Zone CSVs to Snowflake Bronze
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    # Trigger the root Snowflake task
    trigger_root = SnowflakeOperator(
        task_id="trigger_root_task",
        sql="EXECUTE TASK STREAMFLOW_DW.SILVER.TASK_USER_EVENTS_SILVER;",
        snowflake_conn_id=snow_conn,
    )

    etl_job >> load_task >> trigger_root