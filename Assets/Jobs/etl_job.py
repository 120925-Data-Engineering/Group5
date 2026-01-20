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
        
        # specific check to ensure we actually have data and the column we expect
        if 'transaction_type' in trans_df.columns:
            print("Starting transaction transformations: ")
            purchase_df = trans_df.groupBy('user_id').agg(
                  F.count('*').alias('event_count'),
                  F.count(F.when(F.col('transaction_type')=='purchase', 1)).alias('purchase_count'),
                  F.count(F.when(F.col('status')=='completed', 1)).alias('completed_purchases')
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
            user_activity_df = user_df.groupBy('user_id').agg(
                  F.count('*').alias('event_count'),
                  F.count(F.when(F.col('event_type')=='search', 1)).alias('search_count'),
                  F.count(F.when(F.col('event_type')=='add_to_cart', 1)).alias('amount_added')
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