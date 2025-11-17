# --- shim for kafka-python vendored six quirk ---
import sys, six
sys.modules['kafka.vendor.six'] = six
sys.modules['kafka.vendor.six.moves'] = six.moves
# -----------------------------------------------

from kafka import KafkaConsumer
import json, psycopg2

TOPIC = "orders_json"
BOOTSTRAP = "localhost:9094"

def safe_json(v: bytes):
    if not v:
        return None
    s = v.decode("utf-8", errors="ignore").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None   # skip junk

def run_consumer():
    print(f"[Consumer] Connecting to Kafka at {BOOTSTRAP}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="transactions-cg-v1",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=safe_json,
    )
    print("[Consumer] ✓ Connected to Kafka successfully!")

    print("[Consumer] Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5434",   # matches your compose
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("[Consumer] ✓ Connected to PostgreSQL successfully!")

    # NEW TABLE for credit card transactions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            tx_id VARCHAR(50) PRIMARY KEY,
            card_id VARCHAR(50),
            merchant VARCHAR(100),
            category VARCHAR(50),
            city VARCHAR(100),
            status VARCHAR(20),
            amount NUMERIC(10, 2),
            currency VARCHAR(10),
            timestamp TIMESTAMP,
            is_international BOOLEAN,
            is_anomalous BOOLEAN
        );
    """)
    print("[Consumer] ✓ Table 'transactions' ready.")
    print("[Consumer] 🎧 Listening for messages...\n")

    n = 0
    for msg in consumer:
        try:
            tx = msg.value
            if not tx:
                continue

            cur.execute("""
                INSERT INTO transactions (
                    tx_id, card_id, merchant, category, city,
                    status, amount, currency, timestamp,
                    is_international, is_anomalous
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tx_id) DO NOTHING;
            """, (
                tx["tx_id"],
                tx["card_id"],
                tx["merchant"],
                tx["category"],
                tx["city"],
                tx["status"],
                tx["amount"],
                tx["currency"],
                tx["timestamp"],
                tx["is_international"],
                tx["is_anomalous"],
            ))
            n += 1
            print(
                f"[Consumer] ✓ #{n} Inserted tx {tx['tx_id']} | "
                f"{tx['merchant']} | ${tx['amount']} | {tx['city']} "
                f"| anomalous={tx['is_anomalous']}"
            )
        except Exception as e:
            print(f"[Consumer ERROR] Failed to process message: {e}")
            continue

if __name__ == "__main__":
    run_consumer()