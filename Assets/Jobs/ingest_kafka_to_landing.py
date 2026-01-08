"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    # DONE: Implement
    kafka_consumer = KafkaConsumer(
           topic, #Topic we are consuming from
           bootstrap_servers=['localhost:9094'], #This needs to match the yaml file. Should change to var
           enable_auto_commit=True, #idk if this is necessary
           value_deserializer = lambda v: json.loads(v.decode('utf-8'))
           )

   #Consume for batch_duration_sec
    batch_messages = []
    start_time = time.time()
    end_time = start_time + batch_duration_sec

    while time.time() < end_time():
       interval_msg = kafka_consumer.poll(timeout_ms=1000)

       for messages in interval_msg.items():
           for message in messages:
               batch_messages.append(message.value)

    kafka_consumer.close()

    #Export to file
    if batch_messages:
        #Check if dir exists and create if it doesn't
        os.makedirs(output_path, exist_ok=True)
        #Create file
        timestamp = time.time() #replace with timestamp?
        filename = f"{topic}_batch_{timestamp}.json"
        path= os.path.join(output_path, filename)

        #Write to file
        try:
            with open(path, 'w') as file:
                json.dump(batch_messages, file, indent=4)
                print(f"Wrote {len(batch_messages)} to {path}")
                kafka_consumer.commit()
        except IOError as e:
                print(f"Failed to write to {path}")
                print(e)
                return 0
    else:
        print("There are no messages")
    kafka_consumer.close()
    #Return the amount of messages processed
    return len(batch_messages)


if __name__ == "__main__":
    # DONE: Parse args and call consume_batch
    parser = argparse.ArgumentParser(description="Kafka Arguments")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--batch_duration", required=True, type=int)
    
    #/data/landing/{topic}_{timestamp}.json
    parser.add_argument("--output_path", default="../data/landing")
    
    args = parser.parse_args()

    consume_batch(
        topic = args.topic,
        batch_duration_sec=args.batch_duration,
        output_path=args.output_path
    )