"""
User Events Producer
Generates mock user activity events and publishes them to a Kafka topic.

================================================================================
USAGE EXAMPLES:
================================================================================

1. REAL-TIME MODE (default) - Continuous streaming with delay:
   python user_events_producer.py

2. BULK MODE - Generate 50,000 historical events (3 months back) as fast as possible:
   python user_events_producer.py --bulk --count 50000

3. BULK + CONTINUE - Generate 50k bulk then stream with custom interval:
   python user_events_producer.py --bulk --count 50000 --continue-after
   python user_events_producer.py --bulk --count 50000 --continue-after --interval 0.5

4. CUSTOM INTERVAL - Slower/faster streaming:
   python user_events_producer.py --interval 0.5  # 2 events/sec
   python user_events_producer.py --interval 2.0  # 1 event every 2 sec

================================================================================
DEPENDENCIES:
   pip install kafka-python faker
================================================================================
"""

import argparse
import json
import time
import random
import os
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer

# Historical data configuration for bulk mode
HISTORICAL_MONTHS = 3  # How far back to generate historical data


fake = Faker()

# Load dimension data from JSON files for consistent joins
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

def load_dimension_ids():
    """Load user and product IDs from dimension JSON files."""
    customers_file = os.path.join(DATA_DIR, "customers.json")
    products_file = os.path.join(DATA_DIR, "products.json")
    
    with open(customers_file, "r") as f:
        customers = json.load(f)
    with open(products_file, "r") as f:
        products = json.load(f)
    
    user_ids = [c["user_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    return user_ids, product_ids

USER_POOL, PRODUCT_POOL = load_dimension_ids()

EVENT_TYPES = ["login", "logout", "page_view", "click", "search", "add_to_cart", "remove_from_cart"]
PAGES = ["home", "products", "product_detail", "cart", "checkout", "profile", "settings", "help"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]


def generate_user_event(event_timestamp=None):
    """Generate a single mock user event.
    
    Args:
        event_timestamp: Optional datetime for the event. If None, uses current UTC time.
                        Used for generating historical data in bulk mode.
    """
    user_id = random.choice(USER_POOL)  # Select from shared pool for joinability
    session_id = fake.uuid4()[:12]
    # Weighted distribution to increase cart events for meaningful product_id joins
    # login=10%, logout=5%, page_view=20%, click=10%, search=10%, add_to_cart=30%, remove_from_cart=15%
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.10, 0.05, 0.20, 0.10, 0.10, 0.30, 0.15]
    )[0]
    
    # Use provided timestamp or current time
    if event_timestamp is None:
        event_timestamp = datetime.utcnow()
    
    event = {
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "timestamp": event_timestamp.isoformat() + "Z",
        "page": random.choice(PAGES),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "ip_address": fake.ipv4(),
        "country": fake.country_code(),
        "city": fake.city(),
    }
    
    # Add event-specific fields
    if event_type == "search":
        event["search_query"] = fake.word()
    elif event_type == "click":
        event["element_id"] = f"btn_{fake.word()}_{random.randint(1, 100)}"
    elif event_type in ["add_to_cart", "remove_from_cart"]:
        event["product_id"] = random.choice(PRODUCT_POOL)  # Select from shared pool for joinability
        event["quantity"] = random.randint(1, 5)
    
    return event


def create_producer(bootstrap_servers):
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )


def main():
    parser = argparse.ArgumentParser(description="Generate mock user events to Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9094", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="user_events", help="Kafka topic name")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events (used in real-time mode and continue-after)")
    parser.add_argument("--count", type=int, default=None, help="Number of events to generate (infinite if not set)")
    parser.add_argument("--bulk", action="store_true", help="Bulk mode: generate historical events (3 months back) as fast as possible")
    parser.add_argument("--continue-after", action="store_true", help="After bulk count reached, continue in real-time mode with --interval")
    args = parser.parse_args()
    
    producer = create_producer(args.bootstrap_servers)
    print(f"Connected to Kafka at {args.bootstrap_servers}")
    print(f"Publishing to topic: {args.topic}")
    if args.bulk:
        print(f"Mode: BULK (max speed, {HISTORICAL_MONTHS}-month historical data)")
        if args.count:
            print(f"Target: {args.count:,} events")
    else:
        print(f"Mode: Real-time (interval: {args.interval}s)")
    print("-" * 50)
    
    event_count = 0
    bulk_complete = False
    start_time = time.time()
    
    # For bulk mode, pre-calculate the historical time range
    # Events will be distributed evenly across the past HISTORICAL_MONTHS months
    if args.bulk and args.count:
        end_timestamp = datetime.utcnow()
        start_timestamp = end_timestamp - timedelta(days=HISTORICAL_MONTHS * 30)
        # Calculate seconds per event to distribute evenly
        total_seconds = (end_timestamp - start_timestamp).total_seconds()
        seconds_per_event = total_seconds / args.count
    
    try:
        while args.count is None or event_count < args.count or (bulk_complete and args.continue_after):
            # Determine timestamp for this event
            if args.bulk and not bulk_complete and args.count:
                # Generate historical timestamp spread across 3 months
                event_timestamp = start_timestamp + timedelta(seconds=event_count * seconds_per_event)
            else:
                # Real-time mode: use current time
                event_timestamp = None
            
            event = generate_user_event(event_timestamp)
            key = event["user_id"]
            
            producer.send(args.topic, key=key, value=event)
            event_count += 1
            
            # Progress logging
            if args.bulk and not bulk_complete and event_count % 5000 == 0:
                elapsed = time.time() - start_time
                rate = event_count / elapsed
                print(f"[BULK] {event_count:,} events | {rate:.0f} events/sec")
            elif not args.bulk or bulk_complete:
                print(f"[{event_count}] {event['event_type']:15} | user={event['user_id']} | page={event['page']}")
            
            # Handle bulk -> continue transition
            if args.bulk and args.count and event_count >= args.count and not bulk_complete:
                bulk_complete = True
                elapsed = time.time() - start_time
                print(f"\n{'='*50}")
                print(f"BULK COMPLETE: {event_count:,} events in {elapsed:.1f}s ({event_count/elapsed:.0f}/sec)")
                print(f"Historical data range: {start_timestamp.isoformat()}Z to {end_timestamp.isoformat()}Z")
                print(f"{'='*50}")
                if args.continue_after:
                    print(f"Continuing in real-time mode (interval: {args.interval}s)...")
                    args.count = None  # Allow infinite
                    args.bulk = False
                else:
                    break
            
            # Delay only in non-bulk mode
            if not args.bulk:
                time.sleep(args.interval)
                
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\nStopped. Total events: {event_count:,} in {elapsed:.1f}s")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

