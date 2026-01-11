#!/usr/bin/env bash

#Optional argument of time
# default to 10 seconds if no time argument give

SECONDS=${1:-10}

# Check if it's a number
if ! [[ "$SECONDS" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 [seconds]"
  echo "Error: seconds must be a positive number"
  exit 1
fi

echo "Running User Transaction Consumer for $SECONDS seconds..."
python3 Jobs/ingest_kafka_to_landing.py --topic transaction_events

