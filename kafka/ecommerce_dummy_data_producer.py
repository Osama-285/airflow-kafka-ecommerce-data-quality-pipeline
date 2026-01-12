import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
INVALID_EVENT_PROBABILITY = 0.08
# -----------------------------
# Kafka Configuration
# -----------------------------
producer = Producer(
    {
        "bootstrap.servers": "localhost:9094",
        "acks": "all",
        "linger.ms": 10,
        "retries": 5,
    }
)

TOPIC = "storetransactions"

# -----------------------------
# Static Reference Data
# -----------------------------
PRODUCTS = [
    {"product_id": f"prod_{i}", "name": f"Product_{i}", "category": random.choice(
        ["Electronics", "Clothing", "Home", "Beauty", "Sports"]
    ), "price": round(random.uniform(10, 500), 2)}
    for i in range(1, 51)
]

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "UPI", "WALLET"]
EVENT_TYPES = ["order_created", "payment_success", "payment_failed", "order_cancelled"]
COUNTRIES = ["US", "UK", "IN", "DE", "CA"]
DEVICES = ["android", "ios", "web"]

# -----------------------------
# Helper Functions
# -----------------------------
def current_utc_time():
    return datetime.now(timezone.utc).isoformat()

def random_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

# -----------------------------
# Event Generator
# -----------------------------
def generate_transaction_event():
    product = random.choice(PRODUCTS)
    order_id = str(uuid.uuid4())

    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.4, 0.4, 0.15, 0.05]
    )[0]

    payment_status = "SUCCESS"
    if event_type == "payment_failed":
        payment_status = "FAILED"
    elif event_type == "order_cancelled":
        payment_status = "CANCELLED"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": current_utc_time(),

        "transaction": {
            "transaction_id": str(uuid.uuid4()),
            "order_id": order_id,
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_status": payment_status,
            "amount": product["price"],
            "currency": "USD",
        },

        "customer": {
            "customer_id": f"cust_{random.randint(1000, 9999)}",
            "country": random.choice(COUNTRIES),
            "city": "NA",
            "device": random.choice(DEVICES),
        },

        "product": {
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "price": product["price"],
        },

        "metadata": {
            "source": random.choice(["web", "mobile_app"]),
            "ip_address": random_ip(),
        },
    }

    # -----------------------------
    # Inject Bad Data (INTENTIONAL)
    # -----------------------------
    if random.random() < INVALID_EVENT_PROBABILITY:
        bad_case = random.choice([
            "negative_price",
            "missing_product_id",
            "invalid_payment_status",
            "null_amount",
        ])

        if bad_case == "negative_price":
            event["transaction"]["amount"] = -100

        elif bad_case == "missing_product_id":
            del event["product"]["product_id"]

        elif bad_case == "invalid_payment_status":
            event["transaction"]["payment_status"] = "UNKNOWN"

        elif bad_case == "null_amount":
            event["transaction"]["amount"] = None

        event["metadata"]["corrupted"] = True

    return order_id, event

# -----------------------------
# Delivery Report
# -----------------------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent event to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )

# -----------------------------
# Main Loop
# -----------------------------
if __name__ == "__main__":
    print("Mall E-Commerce Kafka Producer Started")

    try:
        while True:
            key, event = generate_transaction_event()

            producer.produce(
                topic=TOPIC,
                key=key,
                value=json.dumps(event),
                on_delivery=delivery_report,
            )

            producer.poll(0)
            time.sleep(random.uniform(0.2, 1.2)) 

    except KeyboardInterrupt:
        print("\n Shutting down producer")

    finally:
        producer.flush()
