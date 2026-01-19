#!/usr/bin/env bash
set -euo pipefail 
# Hello Zach
# This script now takes in arguments for the topic so we don't need 2
# It takes in jinja variables because I am trying to use xcom variables so you don't need *.json
 
#Optional argument of time
# default to 10 seconds if no time argument give

SECONDS=${2:-10}

# {{ dag.dag_id }} {{ task.task_id }} {{ run_id }}

# Check if it's a number
if ! [[ "$SECONDS" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 [seconds]"
  echo "Error: seconds must be a positive number"
  exit 1
fi

echo "Running User Transaction Consumer for $SECONDS seconds..."
#Was having issues with run_id 
python3 /opt/spark-jobs/ingest_kafka_to_landing.py \
  --topic "$1" \
  --batch_duration "$2" \
  --dag_id "$AIRFLOW_CTX_DAG_ID" \
  --task_id "$AIRFLOW_CTX_TASK_ID" \
  --run_id "$AIRFLOW_CTX_DAG_RUN_ID"
