from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException # Imported to handle missing files gracefully
import argparse
from spark_session_factory import create_spark_session
from pathlib import Path

def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    Separates logic by file pattern (transaction* vs user*).
    """
    
    # We assume input_path comes in as ".../*.json" or similar wildcard.
    # We need to target specific file patterns to get separate DataFrames.
    
    # --- LOGIC FOR TRANSACTIONS ---
    # Construct specific path for transactions
    trans_input = input_path.replace("*", "transaction*")
    
    print(f"Attempting to read transactions from: {trans_input}")
    
    try:
        trans_df = spark.read.json(trans_input)
    
        if 'transaction_type' in trans_df.columns:
            print("Starting transaction transformations: ")
            # Line items can have multiple values
            exploded_trans_df = trans_df.withColumn("item", F.explode(F.col("line_items")))
            
            purchase_df = exploded_trans_df.select(
                F.col("transaction_id"),
                F.col("user_id"),
                F.col("transaction_type"),
                F.col("timestamp"),
                F.col("status"),
                F.col("payment_method"),
                F.col("currency"),
                
                # Fields from the exploded 'item' struct
                F.col("item.product_id").alias("item_product_id"),
                F.col("item.product_name").alias("item_product_name"),
                F.col("item.category").alias("item_category"),
                F.col("item.quantity").alias("item_quantity"),
                F.col("item.unit_price").alias("item_unit_price"),
                
                F.col("subtotal"),
                F.col("tax"),
                F.col("total"),
                
                # Flatten Billing Address
                F.col("billing_address.street").alias("billing_street"),
                F.col("billing_address.city").alias("billing_city"),
                F.col("billing_address.state").alias("billing_state"),
                F.col("billing_address.zip_code").alias("billing_zip"),
                F.col("billing_address.country").alias("billing_country"),
                
                # Flatten Shipping Address
                F.col("shipping_address.street").alias("shipping_street"),
                F.col("shipping_address.city").alias("shipping_city"),
                F.col("shipping_address.state").alias("shipping_state"),
                F.col("shipping_address.zip_code").alias("shipping_zip"),
                F.col("shipping_address.country").alias("shipping_country")
            )

        print(f" Count of all completed purchases: {purchase_df.count()}")
            
        trans_output = f"{output_path}/transactions"
        print(f"Writing to: {trans_output}")
            
        purchase_df.coalesce(1).write.csv(
                trans_output,
                mode="overwrite",
                header=True
            )
        print(purchase_df.head(10))
            
    except AnalysisException:
        print(f"No transaction files found at {trans_input}, skipping...")
    except Exception as e:
        print(f"Error processing transactions: {e}")


    # --- LOGIC FOR USER EVENTS ---
    # Construct specific path for user events
    user_input = input_path.replace("*", "user*")
    
    print(f"Attempting to read user events from: {user_input}")
    
    try:
        user_df = spark.read.json(user_input)

        if 'event_type' in user_df.columns:
            print('Starting user transformations: ')

            # Replaced aggregation with direct column selection based on your JSON schema            
            # We select the specific columns found in your JSON to ensure clean output
            # If a column (like 'quantity') is missing in some rows, Spark handles it gracefully
            user_activity_df = user_df.select(
                F.col("event_id"),
                F.col("user_id"),
                F.col("session_id"),
                F.col("event_type"),
                F.col("timestamp"),
                F.col("page"),
                F.col("device"),
                F.col("browser"),
                F.col("ip_address"),
                F.col("country"),
                F.col("city"),
                F.col("product_id"),
                F.col("quantity")
            )
            print(f" User activity records: {user_activity_df.count()}")
            
            user_output = f"{output_path}/user_events"
            print(f"Writing to: {user_output}")
            
            user_activity_df.coalesce(1).write.csv(
                user_output,
                mode="overwrite",
                header=True
            )
            print(user_activity_df.head(10))
            
    except AnalysisException:
        print(f"No user event files found at {user_input}, skipping...")
    except Exception as e:
        print(f"Error processing user events: {e}")


if __name__ == "__main__":
    
    # Setup paths
    BASE_DIR = Path(__file__).resolve().parent
    
    # Note: These defaults are relative to where the script is run
    parser = argparse.ArgumentParser(description="Spark Arguments")
    parser.add_argument("--app_name", default="StreamFlow_ETL")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE")
    
    # Input default assumes a wildcard structure
    parser.add_argument("--input_path", default='../assets/data/landing/*.json')
    parser.add_argument("--output_path", default="../assets/data/gold")

    args = parser.parse_args()
    
    config_overrides = {}
    for c in args.conf:
        if "=" not in c: continue
        key, value = c.split("=", 1)
        config_overrides[key.strip()] = value

    spark_session = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        config_overrides=config_overrides if config_overrides else None
    )

    run_etl(
        spark_session,
        input_path=args.input_path,
        output_path=args.output_path
    )

    spark_session.stop()