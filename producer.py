# --- shim for kafka-python vendored six quirk ---
import sys, six
sys.modules['kafka.vendor.six'] = six
sys.modules['kafka.vendor.six.moves'] = six.moves
# -----------------------------------------------

from kafka import KafkaProducer
import time, json, uuid, random
from datetime import datetime
from faker import Faker

TOPIC = "orders_json"              # keep this, just remember to create this topic
BOOTSTRAP = "localhost:9094"       # matches docker-compose

fake = Faker()

def generate_synthetic_transaction():
    """
    Generates synthetic credit card transaction data.
    Domain: card payments (not e-commerce orders anymore).
    """
    merchants = ["Amazon", "Walmart", "Target", "Uber", "DoorDash",
                 "Netflix", "Starbucks", "Best Buy"]
    categories = ["Groceries", "Transport", "Entertainment",
                  "Retail", "Dining", "Subscriptions"]
    cities = ["New York", "Chicago", "San Francisco",
              "Durham", "Austin", "Seattle"]
    statuses = ["Approved", "Declined", "Reversed"]
    
    merchant = random.choice(merchants)
    category = random.choice(categories)
    city = random.choice(cities)
    status = random.choice(statuses)
    
    amount = round(random.uniform(5, 500), 2)
    is_international = random.choice([False, False, False, True])  # mostly domestic
    
    # simple anomaly rule for "Creativity" points
    is_anomalous = (
        amount > 300 or
        (is_international and amount > 200)
    )

    return {
        "tx_id": str(uuid.uuid4())[:8],
        "card_id": fake.credit_card_number(card_type=None),
        "merchant": merchant,
        "category": category,
        "city": city,
        "status": status,
        "amount": amount,
        "currency": "USD",
        "timestamp": datetime.now().isoformat(),
        "is_international": is_international,
        "is_anomalous": is_anomalous,
    }

def run_producer():
    print(f"[Producer] Connecting to Kafka at {BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=30000,
        max_block_ms=60000,
        retries=5,
    )
    print("[Producer] ✓ Connected to Kafka successfully!")
    
    i = 0
    while True:
        tx = generate_synthetic_transaction()
        print(f"[Producer] Sending tx #{i}: {tx}")
        producer.send(TOPIC, value=tx).get(timeout=10)
        producer.flush()
        i += 1
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    run_producer()