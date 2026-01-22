from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import glob
import shutil  

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
    """Upload Gold Zone CSVs to Snowflake Bronze tables and archive them."""
    
    if not os.path.exists(GOLD_ZONE_PATH):
        raise ValueError(f"CRITICAL ERROR: Path {GOLD_ZONE_PATH} does not exist on this worker.")
    
    print(f"Listing contents of {GOLD_ZONE_PATH}:")
    print(os.listdir(GOLD_ZONE_PATH))

    # SnowflakeHook reads connection details from Airflow Connection 'snowflake_default'
    hook = SnowflakeHook(snowflake_conn_id=snow_conn)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    cursor.execute("USE DATABASE STREAMFLOW_DW")
    cursor.execute("USE SCHEMA BRONZE")

    for pattern, table in CSV_TO_TABLE.items():
        # We capture the list here so we can iterate over it twice (Upload -> Move)
        full_pattern_path = os.path.join(GOLD_ZONE_PATH, pattern)
        found_files = glob.glob(full_pattern_path)

        if not found_files:
            print(f"No files found for pattern: {pattern}")
            continue

        print(f"Processing {len(found_files)} files for table {table}...")
        for csv_file in found_files:
            cursor.execute(f"PUT file://{csv_file} @CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        cursor.execute(f"""
            COPY INTO STREAMFLOW_DW.BRONZE.{table}
            FROM @STREAMFLOW_DW.BRONZE.CSV_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'STREAMFLOW_DW.BRONZE.CSV_FORMAT')
            ON_ERROR = 'CONTINUE'
        """)
        for csv_file in found_files:
            file_dir = os.path.dirname(csv_file)
            file_name = os.path.basename(csv_file)
            processed_dir = os.path.join(file_dir, 'processed')
            if not os.path.exists(processed_dir):
                os.makedirs(processed_dir)

            destination = os.path.join(processed_dir, file_name)
            print(f"Moving {file_name} to {processed_dir}")
            shutil.move(csv_file, destination)
    
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
    
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    load_task