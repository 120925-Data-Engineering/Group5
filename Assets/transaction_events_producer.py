"""
Transaction Events Producer
Generates mock e-commerce transaction events and publishes them to a Kafka topic.

================================================================================
USAGE EXAMPLES:
================================================================================

1. REAL-TIME MODE (default) - Continuous streaming with delay:
   python transaction_events_producer.py

2. BULK MODE - Generate 10,000 transactions as fast as possible:
   python transaction_events_producer.py --bulk --count 10000

3. BULK + CONTINUE - Generate 10k bulk then stream:
   python transaction_events_producer.py --bulk --count 10000 --continue-after

4. CUSTOM INTERVAL - Slower/faster streaming:
   python transaction_events_producer.py --interval 1.0  # 1 event/sec
   python transaction_events_producer.py --interval 5.0  # 1 event every 5 sec

================================================================================
NOTE: Run both producers together for realistic e-commerce simulation:
  Terminal 1: python user_events_producer.py --bulk --count 50000
  Terminal 2: python transaction_events_producer.py --bulk --count 10000
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
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer


fake = Faker()

# Load dimension data from JSON files for consistent joins
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

def load_dimension_data():
    """Load full dimension data from JSON files for consistent joins."""
    customers_file = os.path.join(DATA_DIR, "customers.json")
    products_file = os.path.join(DATA_DIR, "products.json")
    
    with open(customers_file, "r") as f:
        customers = json.load(f)
    with open(products_file, "r") as f:
        products = json.load(f)
    
    # Create lookup dict for products by ID
    product_lookup = {p["product_id"]: p for p in products}
    user_ids = [c["user_id"] for c in customers]
    product_ids = list(product_lookup.keys())
    
    return user_ids, product_ids, product_lookup

USER_POOL, PRODUCT_IDS, PRODUCT_CATALOG = load_dimension_data()

TRANSACTION_TYPES = ["purchase", "refund", "chargeback"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
CURRENCIES = ["USD"]  # All US customers, single currency
STATUSES = ["pending", "completed", "failed", "cancelled"]


def generate_line_item():
    """Generate a single line item using actual product data from catalog."""
    product_id = random.choice(PRODUCT_IDS)
    product = PRODUCT_CATALOG[product_id]
    
    quantity = random.randint(1, 5)
    # Use actual MSRP with small random discount (0-15%)
    discount = random.uniform(0, 0.15)
    unit_price = round(product["msrp"] * (1 - discount), 2)
    
    return {
        "product_id": product_id,
        "product_name": product["product_name"],
        "category": product["category"],
        "brand": product["brand"],
        "quantity": quantity,
        "unit_price": unit_price
    }


def generate_transaction_event():
    """Generate a single mock transaction event."""
    user_id = random.choice(USER_POOL)  # Select from shared pool for joinability
    transaction_type = random.choices(
        TRANSACTION_TYPES, 
        weights=[0.85, 0.12, 0.03]  # 85% purchases, 12% refunds, 3% chargebacks
    )[0]
    
    # Generate 1-5 line items per transaction
    line_items = [generate_line_item() for _ in range(random.randint(1, 5))]
    subtotal = sum(item["quantity"] * item["unit_price"] for item in line_items)
    tax_rate = random.uniform(0.05, 0.10)
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)
    
    # For refunds/chargebacks, reference an original transaction
    original_transaction_id = None
    if transaction_type in ["refund", "chargeback"]:
        original_transaction_id = fake.uuid4()
        total = -total  # Negative amount for refunds
    
    event = {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "transaction_type": transaction_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": random.choices(STATUSES, weights=[0.05, 0.88, 0.05, 0.02])[0],
        "payment_method": random.choice(PAYMENT_METHODS),
        "currency": "USD",
        "line_items": line_items,
        "subtotal": round(subtotal, 2),
        "tax": tax,
        "total": total,
        "billing_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": "US"
        },
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "country": "US"
        }
    }
    
    if original_transaction_id:
        event["original_transaction_id"] = original_transaction_id
    
    return event


def create_producer(bootstrap_servers):
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )


def main():
    parser = argparse.ArgumentParser(description="Generate mock transaction events to Kafka")
    #Changed from 9092 to 9094
    parser.add_argument("--bootstrap-servers", default="localhost:9094", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transaction_events", help="Kafka topic name")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between events (ignored in bulk mode)")
    parser.add_argument("--count", type=int, default=None, help="Number of events to generate (infinite if not set)")
    parser.add_argument("--bulk", action="store_true", help="Bulk mode: generate events as fast as possible (no delay)")
    parser.add_argument("--continue-after", action="store_true", help="After bulk count reached, continue in real-time mode")
    args = parser.parse_args()
    
    producer = create_producer(args.bootstrap_servers)
    print(f"Connected to Kafka at {args.bootstrap_servers}")
    print(f"Publishing to topic: {args.topic}")
    if args.bulk:
        print(f"Mode: BULK (max speed)")
        if args.count:
            print(f"Target: {args.count:,} transactions")
    else:
        print(f"Mode: Real-time (interval: {args.interval}s)")
    print("-" * 50)
    
    event_count = 0
    total_revenue = 0.0
    bulk_complete = False
    start_time = time.time()
    
    try:
        while args.count is None or event_count < args.count or (bulk_complete and args.continue_after):
            event = generate_transaction_event()
            key = event["user_id"]
            
            producer.send(args.topic, key=key, value=event)
            event_count += 1
            total_revenue += event["total"]
            
            # Progress logging
            if args.bulk and event_count % 1000 == 0:
                elapsed = time.time() - start_time
                rate = event_count / elapsed
                print(f"[BULK] {event_count:,} transactions | ${total_revenue:,.2f} revenue | {rate:.0f}/sec")
            elif not args.bulk:
                status_icon = "+" if event["total"] > 0 else "-"
                print(f"[{event_count}] {event['transaction_type']:12} | {status_icon}${abs(event['total']):>8.2f} | {len(event['line_items'])} items | {event['status']}")
            
            # Handle bulk -> continue transition
            if args.bulk and args.count and event_count >= args.count and not bulk_complete:
                bulk_complete = True
                elapsed = time.time() - start_time
                print(f"\n{'='*50}")
                print(f"BULK COMPLETE: {event_count:,} transactions in {elapsed:.1f}s")
                print(f"Total Revenue: ${total_revenue:,.2f}")
                print(f"Rate: {event_count/elapsed:.0f} transactions/sec")
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
        print(f"\nStopped. Total: {event_count:,} transactions | ${total_revenue:,.2f} revenue | {elapsed:.1f}s")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

