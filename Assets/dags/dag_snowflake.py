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

silver_transformation_sql = """
    MERGE INTO STREAMFLOW_DW.SILVER.stg_transactions AS target
    USING (
        SELECT 
            transaction_id, user_id, transaction_type,
            CAST(transaction_ts AS TIMESTAMP) AS transaction_ts,
            status, payment_method, currency, item_product_id,
            item_product_name, item_category,
            CAST(item_quantity AS INTEGER) AS item_quantity,
            CAST(item_unit_price AS DECIMAL(18, 2)) AS item_unit_price,
            CAST(subtotal AS DECIMAL(18, 2)) AS subtotal,
            CAST(tax AS DECIMAL(18, 2)) AS tax,
            CAST(total AS DECIMAL(18, 2)) AS total,
            billing_street, billing_city, billing_state,
            billing_zip, billing_country, shipping_street,
            shipping_city, shipping_state, shipping_zip, shipping_country
        FROM STREAMFLOW_DW.BRONZE.raw_transactions
    ) AS source
    ON target.transaction_id = source.transaction_id
    WHEN NOT MATCHED THEN
        INSERT (
            transaction_id, user_id, transaction_type, transaction_ts, status, 
            payment_method, currency, item_product_id, item_product_name, item_category, 
            item_quantity, item_unit_price, subtotal, tax, total, 
            billing_street, billing_city, billing_state, billing_zip, billing_country, 
            shipping_street, shipping_city, shipping_state, shipping_zip, shipping_country
        )
        VALUES (
            source.transaction_id, source.user_id, source.transaction_type, source.transaction_ts, source.status, 
            source.payment_method, source.currency, source.item_product_id, source.item_product_name, source.item_category, 
            source.item_quantity, source.item_unit_price, source.subtotal, source.tax, source.total, 
            source.billing_street, source.billing_city, source.billing_state, source.billing_zip, source.billing_country, 
            source.shipping_street, source.shipping_city, source.shipping_state, source.shipping_zip, source.shipping_country
        );

    MERGE INTO STREAMFLOW_DW.SILVER.stg_user_events AS target
    USING (
        SELECT 
            event_id, user_id, session_id, event_type,
            CAST(event_ts AS TIMESTAMP) AS event_ts,
            page, device, browser, ip_address, country, city, product_id,
            CAST(quantity AS INTEGER) AS quantity
        FROM STREAMFLOW_DW.BRONZE.raw_user_events
    ) AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT (
            event_id, user_id, session_id, event_type, event_ts, 
            page, device, browser, ip_address, country, city, product_id, quantity
        )
        VALUES (
            source.event_id, source.user_id, source.session_id, source.event_type, source.event_ts, 
            source.page, source.device, source.browser, source.ip_address, source.country, source.city, source.product_id, source.quantity
        );
"""
gold_transformation_sql = """
    -- 1. Dim Customers (From Silver Customers)
    MERGE INTO STREAMFLOW_DW.GOLD.dim_customers AS t
    USING STREAMFLOW_DW.SILVER.stg_customers AS s
    ON t.user_id = s.user_id
    WHEN MATCHED THEN UPDATE SET 
        email = s.email, loyalty_points = s.loyalty_points, state = s.state, account_type = s.account_type
    WHEN NOT MATCHED THEN INSERT (user_id, email, first_name, last_name, registration_date, account_type, date_of_birth, loyalty_points, state)
    VALUES (s.user_id, s.email, s.first_name, s.last_name, s.registration_date, s.account_type, s.date_of_birth, s.loyalty_points, s.state);

    -- 2. Dim Products (From Silver Products)
    MERGE INTO STREAMFLOW_DW.GOLD.dim_products AS t
    USING STREAMFLOW_DW.SILVER.stg_products AS s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN UPDATE SET 
        msrp = s.msrp, cost_price = s.cost_price, is_active = s.is_active, product_name = s.product_name
    WHEN NOT MATCHED THEN INSERT (product_id, product_name, description, category, subcategory, brand, manufacturer, msrp, cost_price, created_date, is_active)
    VALUES (s.product_id, s.product_name, s.description, s.category, s.subcategory, s.brand, s.manufacturer, s.msrp, s.cost_price, s.created_date, s.is_active);

    -- 3. Fact Transactions (From Silver Transactions - Header only)
    MERGE INTO STREAMFLOW_DW.GOLD.fact_transactions AS t
    USING (
        SELECT DISTINCT 
            transaction_id, user_id, transaction_type, transaction_ts, status, 
            payment_method, currency, subtotal, tax, total, 
            billing_street, billing_city, billing_state, billing_zip, billing_country, 
            shipping_street, shipping_city, shipping_country
        FROM STREAMFLOW_DW.SILVER.stg_transactions
    ) AS s
    ON t.transaction_id = s.transaction_id
    WHEN NOT MATCHED THEN INSERT (
        transaction_id, user_id, transaction_type, transaction_ts, status, payment_method, currency, 
        subtotal, tax, total, billing_street, billing_city, billing_state, billing_zip, billing_country, 
        shipping_street, shipping_city, shipping_country
    )
    VALUES (
        s.transaction_id, s.user_id, s.transaction_type, s.transaction_ts, s.status, s.payment_method, s.currency, 
        s.subtotal, s.tax, s.total, s.billing_street, s.billing_city, s.billing_state, s.billing_zip, s.billing_country, 
        s.shipping_street, s.shipping_city, s.shipping_country
    );

    -- 4. Fact Transaction Line Items (From Silver Transactions - Lines only)
    MERGE INTO STREAMFLOW_DW.GOLD.fact_transaction_line_items AS t
    USING (
        SELECT 
            MD5(transaction_id || item_product_id) as generated_line_id,
            transaction_id, item_product_id, item_product_name, item_category, 
            item_quantity, item_unit_price
        FROM STREAMFLOW_DW.SILVER.stg_transactions
    ) AS s
    ON t.line_item_id = s.generated_line_id
    WHEN NOT MATCHED THEN INSERT (
        line_item_id, transaction_id, product_id, product_name, category, quantity, unit_price
    )
    VALUES (
        s.generated_line_id, s.transaction_id, s.item_product_id, s.item_product_name, s.item_category, s.item_quantity, s.item_unit_price
    );

    -- 5. Fact User Events (From Silver User Events)
    MERGE INTO STREAMFLOW_DW.GOLD.fact_user_events AS t
    USING STREAMFLOW_DW.SILVER.stg_user_events AS s
    ON t.event_id = s.event_id
    WHEN NOT MATCHED THEN INSERT (
        event_id, user_id, session_id, event_type, event_ts, page, device, browser, ip_address, country, city, product_id, quantity
    )
    VALUES (
        s.event_id, s.user_id, s.session_id, s.event_type, s.event_ts, s.page, s.device, s.browser, s.ip_address, s.country, s.city, s.product_id, s.quantity
    );
"""

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

def log_data_summary(**context):
    ti = context['ti']
    
    silver_results = ti.xcom_pull(task_ids='transform_to_silver')
    gold_results = ti.xcom_pull(task_ids='transform_to_gold')

    print("================ DATA PIPELINE SUMMARY ================")
    print(f"Silver Layer Updates: {silver_results}")
    print(f"Gold Layer Updates:   {gold_results}")
    print("=======================================================")

with DAG(
    dag_id='phase_2',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manually triggered
    catchup=False,
) as dag:
    
    load_csv_into_bronze = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    load_bronze_into_silver = SnowflakeOperator(
        task_id='load_bronze_into_silver',
        snowflake_conn_id=snow_conn,
        sql=silver_transformation_sql,
        warehouse='COMPUTE_WH', 
        database='STREAMFLOW_DW',
        schema='SILVER'
    )

    transform_gold_task = SnowflakeOperator(
        task_id='transform_to_gold',
        snowflake_conn_id=snow_conn,
        sql=gold_transformation_sql,
        warehouse='COMPUTE_WH',
        database='STREAMFLOW_DW',
        schema='GOLD'
    )

    log_summary_task = PythonOperator(
        task_id='log_data_summary',
        python_callable=log_data_summary,
        provide_context=True
    )

    load_csv_into_bronze >> load_bronze_into_silver >> transform_gold_task >> log_summary_task