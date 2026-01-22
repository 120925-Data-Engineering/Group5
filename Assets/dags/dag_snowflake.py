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
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import glob

# Path where Spark ETL writes Gold Zone CSVs (shared Docker volume)
GOLD_ZONE_PATH = '/opt/spark-data/gold'
snow_conn = "snowflake_connection"

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
    conn = hook.get_conn()
    cursor = conn.cursor()


    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    cursor.execute("USE DATABASE STREAMFLOW_DW")
    cursor.execute("USE SCHEMA BRONZE")

    for pattern, table in CSV_TO_TABLE.items():
        # Find all CSVs matching this pattern (e.g., user_events_001.csv, user_events_002.csv)
        for csv_file in glob.glob(os.path.join(GOLD_ZONE_PATH, pattern)):
            # PUT uploads local file to Snowflake internal stage
            cursor.execute(f"PUT file://{csv_file} @CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        # COPY INTO loads staged files into the Bronze table (inline CSV format)
        cursor.execute(f"""
            COPY INTO STREAMFLOW_DW.BRONZE.{table}
            FROM @STREAMFLOW_DW.BRONZE.CSV_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'STREAMFLOW_DW.BRONZE.CSV_FORMAT')
            ON_ERROR = 'CONTINUE'
        """)
    
    # Clean up staged files (REMOVE deletes files only, not the stage itself - stage persists for reuse)
    cursor.execute("REMOVE @STREAMFLOW_DW.BRONZE.CSV_STAGE")
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id='phase_2',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manually triggered
    catchup=False,
) as dag:
    
    # Single task: load all Gold Zone CSVs to Snowflake Bronze
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    load_task