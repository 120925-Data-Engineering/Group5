"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse
from spark_session_factory import create_spark_session


def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # Done: Implement
    # inferSchema=True   -- chatGPT says Spark always infers schema for JSON so this option doesn't exist

    #Read json data into df
    print('Reading json into a df for transformations: ')
    df = spark.read.json(
         input_path
         )
    cols=df.columns
    print(f'Input path: {input_path}')
    #Transformations
    #TODO: Make actual transformation to the data
    if 'transaction_type' in cols: # do transaction calculations here
        print("Starting transaction transformations: ")
        purchase_df=df.groupBy('user_id').agg(
              F.count('*').alias('event_count'),
              F.count(F.when(F.col('transaction_type')=='purchase',1)).alias('purchase_count'),
              F.count(F.when(F.col('status')=='completed',1)).alias('completed_purhases')
        )
        print(f" Count of all completed purchases: {purchase_df.count()}")
            #Output
        #Write csv to gold zone
        purchase_df.write.csv(output_path,
                    mode="append",
                    header=True
                    )
        print(purchase_df.head(10))

    if 'event_type' in cols: # do user calculations here
        print('Starting user transformations: ')
        user_activity_df=df.groupBy('user_id').agg(
              F.count('*').alias('event_count'),
              F.count(F.when(F.col('event_type')=='search',1)).alias('search_count'),
              F.count(F.when(F.col('event_type')=='add_to_cart',1)).alias('amount_added')
        )
        print(f" User activity records: {user_activity_df.count()}")
        #Output
        #Write csv to gold zone
        user_activity_df.write.csv(output_path,
                    mode="append",
                    header=True
                    )
        print(user_activity_df.head(10))
    else:
         print('No matching schema found.')
    # #Output
    # #Write csv to gold zone
    # df.write.csv(output_path,
    #              mode="append",
    #              header=True
    #              )


if __name__ == "__main__":
    # DONE: Create SparkSession, parse args, run ETL
    parser = argparse.ArgumentParser(description="Spark Arguments")
    #spark session arguments
    parser.add_argument("--app_name", default="app_name")
    parser.add_argument("--master", default="local[*]")
    #handle config overrides 
    parser.add_argument("--conf",
                         action="append",
                         default=[],
                         metavar="KEY=VALUE",
                         help="Spark configurations"
                         )

    # input path, output path
    #input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
    parser.add_argument("--input_path", default='../assets/data/landing/*.json')
                        #'../assets/data/landing/*.json') 
    parser.add_argument("--output_path", default="../assets/data/gold")

    args = parser.parse_args()
    
    config_overrides = {}

    for c in args.conf:
        if "=" not in c:
            raise SystemExit(f"--conf must be KEY=VALUE\nInput: \"{c}\"")
        key, value = c.split("=", 1)
        key = key.strip()
        if key == "":
                raise SystemError(f"Empty conf key in: {c}")
        if value == "":
                raise SystemError(f"Empty conf value in: {c}")
        config_overrides[key] = value

    if not config_overrides:
        config_overrides = None

    spark_session = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        config_overrides=config_overrides
    )

    run_etl(
        spark_session,
        input_path= args.input_path,
        output_path=args.output_path
    )

    spark_session.stop()