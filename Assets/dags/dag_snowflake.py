from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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

# Tables to monitor for row count changes
MONITORED_TABLES = [
    'STREAMFLOW_DW.BRONZE.raw_user_events',
    'STREAMFLOW_DW.BRONZE.raw_transactions',
    'STREAMFLOW_DW.SILVER.stg_transactions',
    'STREAMFLOW_DW.SILVER.stg_user_events',
    'STREAMFLOW_DW.GOLD.fact_transactions',
    'STREAMFLOW_DW.GOLD.fact_user_events'
]

def get_table_counts(**context):
    """
    Queries Snowflake to get the current row count for all monitored tables.
    Returns a dictionary: {'schema.table': count}
    """
    hook = SnowflakeHook(snowflake_conn_id=snow_conn)
    counts = {}
    
    print("Fetching current row counts...")
    for table in MONITORED_TABLES:
        try:
            # get_first returns a tuple, e.g., (1050,)
            sql = f"SELECT COUNT(*) FROM {table}"
            result = hook.get_first(sql)
            count = result[0] if result else 0
            counts[table] = count
            print(f" -> {table}: {count}")
        except Exception as e:
            print(f"Error fetching count for {table}: {str(e)}")
            counts[table] = 0
            
    return counts

def load_to_snowflake(**context):
    """Upload Gold Zone CSVs to Snowflake Bronze tables and archive them."""
    
    if not os.path.exists(GOLD_ZONE_PATH):
        raise ValueError(f"CRITICAL ERROR: Path {GOLD_ZONE_PATH} does not exist on this worker.")
    
    print(f"Listing contents of {GOLD_ZONE_PATH}:")
    print(os.listdir(GOLD_ZONE_PATH))

    hook = SnowflakeHook(snowflake_conn_id=snow_conn)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    cursor.execute("USE DATABASE STREAMFLOW_DW")
    cursor.execute("USE SCHEMA BRONZE")

    for pattern, table in CSV_TO_TABLE.items():
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
        
        # Archive files
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

def log_data_summary(**context):
    ti = context['ti']
    
    # Pull the dictionaries from the start and end tasks
    initial_counts = ti.xcom_pull(task_ids='check_initial_counts')
    final_counts = ti.xcom_pull(task_ids='check_final_counts')
    
    # Handle case where XCom might be None (first run failure, etc)
    if not initial_counts: initial_counts = {}
    if not final_counts: final_counts = {}

    print("\n" + "="*90)
    print("DATA PIPELINE SUMMARY: RECORD INSERTION REPORT")
    print("="*90)
    print(f"{'Table Name':<45} | {'Before':<10} | {'After':<10} | {'Rows Added':<10}")
    print("-" * 90)

    total_added = 0
    
    for table in MONITORED_TABLES:
        start = initial_counts.get(table, 0)
        end = final_counts.get(table, 0)
        diff = end - start
        total_added += diff
        
        # Highlight rows with activity
        prefix = ">> " if diff > 0 else "   "
        
        print(f"{prefix}{table:<42} | {start:<10} | {end:<10} | {diff:<10}")

    print("="*90)
    print(f"Total Records Added Across All Tables: {total_added}")
    print("="*90 + "\n")

with DAG(
    dag_id='phase_2',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # 1. Pre-Check: Get counts before anything runs
    check_initial = PythonOperator(
        task_id='check_initial_counts',
        python_callable=get_table_counts
    )

    # 2. Bronze Load
    load_csv_into_bronze = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    # 3. Silver Transformation
    load_bronze_into_silver = SnowflakeOperator(
        task_id='load_bronze_into_silver',
        snowflake_conn_id=snow_conn,
        sql="CALL STREAMFLOW_DW.SILVER.SP_TRANSFORM_TO_SILVER();",
        warehouse='COMPUTE_WH', 
        database='STREAMFLOW_DW',
        schema='SILVER',
        autocommit=True
    )

    # 4. Gold Transformation
    transform_gold_task = SnowflakeOperator(
        task_id='transform_to_gold',
        snowflake_conn_id=snow_conn,
        sql="CALL STREAMFLOW_DW.GOLD.SP_TRANSFORM_TO_GOLD();",
        warehouse='COMPUTE_WH',
        database='STREAMFLOW_DW',
        schema='GOLD',
        autocommit=True
    )

    # 5. Post-Check: Get counts after everything finishes
    check_final = PythonOperator(
        task_id='check_final_counts',
        python_callable=get_table_counts
    )

    # 6. Summary: Calculate and print the delta
    log_summary_task = PythonOperator(
        task_id='log_data_summary',
        python_callable=log_data_summary,
        provide_context=True
    )

    # Define the execution flow
    check_initial >> load_csv_into_bronze >> load_bronze_into_silver >> transform_gold_task >> check_final >> log_summary_task