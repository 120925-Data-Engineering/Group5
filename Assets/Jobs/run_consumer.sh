#!/usr/bin/env bash
set -euo pipefail 
# Hello Zach
# This script now takes in arguments for the topic so we don't need 2
# It takes in jinja variables because I am trying to use xcom variables so you don't need *.json
 
# Hello Pedro, run these commands to run the script
# bash run_consumer.sh transaction_events 10
# bash run_consumer.sh user_events 10
# NOTE: -- make sure the docker and the producers are running

#Optional argument of time
# default to 10 seconds if no time argument give

# SECONDS=${2:-10}
# Changing the seconds to this because there was an unbound variable error
SECONDS="${2-10}" # : "${SECONDS:=10}"


# {{ dag.dag_id }} {{ task.task_id }} {{ run_id }}

# Check if it's a number
if ! [[ "$SECONDS" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 <topic> [seconds]"
  echo "Error: seconds must be a positive number"
  exit 1
fi

echo "Running User Transaction Consumer for $SECONDS seconds..."
#Was having issues with run_id 

#I am adding :-unknown to the IDs below because it makes them safe
# ..supposedly

# I am changing the line so that we can work inside windows
# again...this is for inside of airflow but we need it outside
# python3 /opt/spark-jobs/ingest_kafka_to_landing.py \
python3 ingest_kafka_to_landing.py \
  --topic "$1" \
  --batch_duration "$SECONDS" \
  --dag_id "${AIRFLOW_CTX_DAG_ID:-unknown}" \
  --task_id "${AIRFLOW_CTX_TASK_ID:-unknown}" \
  --run_id "${AIRFLOW_CTX_DAG_RUN_ID:-unknown}"
  # --dag_id "$AIRFLOW_CTX_DAG_ID" \
  # --task_id "$AIRFLOW_CTX_TASK_ID" \
  # --run_id "$AIRFLOW_CTX_DAG_RUN_ID"
