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
from pathlib import Path
from airflow.models import TaskInstance
from airflow.utils.session import create_session

dag_id = ""
task_id = ""
run_id = ""

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
           bootstrap_servers=['kafka:9092'], #This needs to match the yaml file. Should change to var
           value_deserializer = lambda v: json.loads(v.decode('utf-8')),
           group_id = f"{topic}_id"
           )

   #Consume for batch_duration_sec
    batch_messages = []
    start_time = time.time()
    end_time = start_time + batch_duration_sec

    while time.time() < end_time:
       interval_msg = kafka_consumer.poll(timeout_ms=1000)

       for partition, messages in interval_msg.items():
           for message in messages:
               batch_messages.append(message.value)


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
               # json.dump(batch_messages, file, indent=4)
                #file.write(str(batch_messages))
                for mes in batch_messages:
                    file.write(json.dumps(mes) + '\n')
                print(f"Wrote {len(batch_messages)} to {path}")

                #COMMIT
                #Onnly if we were able to read and write
                kafka_consumer.commit()

                #Set XCom variable for spark to be able to pull             
                with create_session() as session:
                    ti = TaskInstance.get_task_instance(
                        dag_id=dag_id,
                        task_id=task_id,
                        run_id=run_id,
                        map_index = -1,
                        session=session
                    )
                    if ti is None:
                        raise RuntimeError(f"No TaskInstance found for dag_id={dag_id} task_id={task_id} run_id={run_id}")
                    ti.xcom_push(key=f"{topic}_path", value=path, session=session)

        except IOError as e:
                    print(f"Failed to write to {path}")
                    print(e)
                    return 0
    else:
        print("There are no messages")
            
    print("Kafka Consumer Finished")
    kafka_consumer.close()
    #Return the amount of messages processed
    return len(batch_messages)


if __name__ == "__main__":
    #Write to the directory
    # BASE_DIR = Path(__file__).resolve().parent
    # LANDING_DIR = (BASE_DIR / ".." / "data" / "landing").resolve()
    BASE_DIR = '/opt'
    LANDING_DIR = f"{BASE_DIR}/spark-data/landing"

    # DONE: Parse args and call consume_batch
    parser = argparse.ArgumentParser(description="Kafka Arguments")
    parser.add_argument("--topic", default="user_events")
    parser.add_argument("--batch_duration", type=int, default=5)
    
    #/data/landing/{topic}_{timestamp}.json
    #Didn't have permissions to make directoru /opt/spark-data, had to make it manually
    #parser.add_argument("--output_path", default="/opt/spark-data")
    parser.add_argument("--output_path", default=LANDING_DIR)
    parser.add_argument("--dag_id", required=True)
    parser.add_argument("--task_id", required=True)
    parser.add_argument("--run_id", required=True)
    
    args = parser.parse_args()

    dag_id= args.dag_id
    task_id = args.task_id
    run_id = args.run_id
    if(dag_id == "" or task_id == "" or run_id == ""):
        print("Did not receive metadata correctly")

    message_count = consume_batch(
        topic = args.topic,
        batch_duration_sec=args.batch_duration,
        output_path=args.output_path
    )
    print(message_count)

