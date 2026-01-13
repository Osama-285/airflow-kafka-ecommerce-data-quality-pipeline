from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from confluent_kafka import Consumer
from datetime import datetime, timedelta, timezone
import pendulum
from airflow.sdk import get_current_context
import json, glob
from airflow.sensors.filesystem import FileSensor
import os
from airflow.sensors.time_delta import TimeDeltaSensor


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
DAG_ID = "kafka_ecommerce_ingestion_with_dq"

KAFKA_CONFIG = {
    "bootstrap.servers": "broker2:29094",
    "group.id": "airflow_try_40_ecommerce-validator",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
safe_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
TOPIC = "storetransactions"
BASE_PATH = "/opt/airflow/utils"

REQUIRED_FIELDS = [
    "event_id",
    "event_type",
    "event_time",
    "transaction",
    "customer",
    "product",
]

VALID_PAYMENT_STATUS = {"SUCCESS", "FAILED", "CANCELLED"}


# ------------------------------------------------------------------
# TASK 1: Consume Kafka
# ------------------------------------------------------------------
def consume_kafka_events(ti):

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    messages = []
    empty_polls = 0

    try:
        while len(messages) < 100 and empty_polls < 5:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                raise Exception(msg.error())

            event = json.loads(msg.value().decode())
            messages.append(event)

        # COMMIT OFFSETS
        consumer.commit(asynchronous=False)

    finally:
        consumer.close()

    os.makedirs(BASE_PATH, exist_ok=True)
    raw_file = f"{BASE_PATH}/raw_events_{safe_ts}.json"

    with open(raw_file, "w") as f:
        json.dump(messages, f, indent=2)

    ti.xcom_push(key="raw_events_file", value=raw_file)
    print(f" Consumed {len(messages)} events")


# ------------------------------------------------------------------
# TASK 2: Validate Events
# ------------------------------------------------------------------
def validate_raw_events(ti, xcom_key, xcom_task_id, filetype):
    print("KEYS", xcom_key,xcom_task_id)
    context = get_current_context()
    input_file = ti.xcom_pull(key=xcom_key, task_ids=xcom_task_id)
    print("TASK3: ", input_file, context)

    if filetype == "invalid":
        with open(input_file["invalid"]) as f:
            events = json.load(f)
    else:
        with open(input_file) as f:
            events = json.load(f)

    valid_events, invalid_events = [], []

    for event in events:
        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            event["error"] = f"Missing fields: {missing}"
            invalid_events.append(event)
            continue

        transaction = event.get("transaction")
        product = event.get("product")

        if not isinstance(transaction, dict):
            event["error"] = "transaction must be an object"
            invalid_events.append(event)
            continue

        if not isinstance(product, dict):
            event["error"] = "product must be an object"
            invalid_events.append(event)
            continue

        amount = transaction.get("amount")
        payment_status = transaction.get("payment_status")
        price = product.get("price")

        if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
            event["error"] = "Transaction amount must be a positive number"
            invalid_events.append(event)
            continue

        if payment_status not in VALID_PAYMENT_STATUS:
            event["error"] = "Invalid payment status"
            invalid_events.append(event)
            continue

        if price is None or not isinstance(price, (int, float)) or price <= 0:
            event["error"] = "Product price must be a positive number"
            invalid_events.append(event)
            continue

        valid_events.append(event)

    valid_file = f"{BASE_PATH}/valid_events_{safe_ts}.json"
    invalid_file = f"{BASE_PATH}/invalid_events_{safe_ts}.json"

    with open(valid_file, "w") as f:
        json.dump(valid_events, f, indent=2)

    with open(invalid_file, "w") as f:
        json.dump(invalid_events, f, indent=2)
    print("===============================================")
    print(f"Total events processed: {len(events)}")
    print(f"Valid events: {len(valid_events)}")
    print(f"Invalid events: {len(invalid_events)}")
    print(f"Valid events file path: {valid_file}")
    print(f"Invalid events file path: {invalid_file}")
    print("===============================================")
    ti.xcom_push(
        key="validated_files", value={"valid": valid_file, "invalid": invalid_file}
    )

    print(f"Valid: {len(valid_events)} | ❌ Invalid: {len(invalid_events)}")


# ------------------------------------------------------------------
# TASK 3: Push Valid Records to Postgres
# ------------------------------------------------------------------
def load_events_to_postgres(ti, source_task_id, xcom_key, filetype):
    context = get_current_context()
    files = ti.xcom_pull(key=xcom_key, task_ids=source_task_id)
    print("TASK3: ", files)

    if isinstance(files, dict):
        if filetype == "valid":
            valid_file = files["valid"]
        else:
            valid_file = files["invalid"]
    else:
        valid_file = files

    with open(valid_file) as f:
        records = json.load(f)

    if not records:
        print("⚠️ No records to insert")
        return
    rows = []
    for e in records:
        row = (
            e.get("event_id"),
            e.get("event_type"),
            e.get("event_time"),
            e.get("transaction", {}).get("transaction_id"),
            e.get("transaction", {}).get("order_id"),
            e.get("transaction", {}).get("payment_method"),
            e.get("transaction", {}).get("payment_status"),
            e.get("transaction", {}).get("amount"),
            e.get("customer", {}).get("customer_id"),
            e.get("customer", {}).get("country"),
            e.get("customer", {}).get("device"),
            e.get("product", {}).get("product_id"),
            e.get("product", {}).get("product_name"),
            e.get("product", {}).get("category"),
            e.get("product", {}).get("price"),
        )
        rows.append(row)

    hook = PostgresHook(postgres_conn_id="postgreSQL")

    hook.insert_rows(
        table="ecommerce_transactions",
        rows=rows,
        target_fields=[
            "event_id",
            "event_type",
            "event_time",
            "transaction_id",
            "order_id",
            "payment_method",
            "payment_status",
            "amount",
            "customer_id",
            "country",
            "device",
            "product_id",
            "product_name",
            "category",
            "price",
        ],
    )
    inserted_count = len(rows)
    print(f"Inserted {len(rows)} records into ecommerce_transactions table")
    return {"inserted_count": inserted_count}


invalid_sensor = FileSensor(
    task_id="invalid_file_sensor",
    filepath="/opt/airflow/utils/invalid_events_*.json",
    fs_conn_id="fs_default",
    poke_interval=60,
    timeout=300,
    mode="reschedule",
)


def aggregate_recent_transactions():
    hook = PostgresHook(postgres_conn_id="postgreSQL")
    conn = hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=5)

    query = """
        SELECT category, payment_method, country, COUNT(*) as total_orders, SUM(amount) as total_amount
        FROM ecommerce_transactions
        WHERE created_at >= %s
        GROUP BY category, payment_method, country
        ORDER BY total_amount DESC
    """

    cursor.execute(query, (window_start,))
    results = cursor.fetchall()

    print(" Aggregated results for the last 5 minutes:")
    print("Category | Payment Method | Country | Total Orders | Total Amount")
    print("---------------------------------------------------------------")
    for row in results:
        print(row)
    cursor.close()
    conn.close()


def validate_event_list(events):
    valid, invalid = [], []

    for event in events:
        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            event["error"] = f"Missing fields: {missing}"
            invalid.append(event)
            continue

        txn = event["transaction"]
        prod = event["product"]

        if txn.get("amount", 0) <= 0:
            event["error"] = "Invalid amount"
            invalid.append(event)
            continue

        if txn.get("payment_status") not in VALID_PAYMENT_STATUS:
            event["error"] = "Invalid payment status"
            invalid.append(event)
            continue

        if prod.get("price", 0) <= 0:
            event["error"] = "Invalid product price"
            invalid.append(event)
            continue

        valid.append(event)

    return valid, invalid

# ------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.UTC),
    schedule=None,
    catchup=False,
    tags=["kafka", "data-quality", "dlq", "idempotent"],
) as dag:

    consume_kafka = PythonOperator(
        task_id="consume_kafka",
        python_callable=consume_kafka_events,
    )

    validate = PythonOperator(
        task_id="validate_events",
        python_callable=validate_raw_events,
        op_kwargs={
            "xcom_task_id": "consume_kafka",
            "xcom_key": "raw_events_file",
            "filetype": "valid",
        },
    )
    validate_invalid = PythonOperator(
        task_id="validate_invalid_events",
        python_callable=validate_raw_events,
        op_kwargs={
            "xcom_task_id": "validate_events",
            "xcom_key": "validated_files",
            "filetype": "invalid",
        },
    )

    wait_5_minutes = TimeDeltaSensor(
        task_id="wait_5_minutes_for_dlq",
        delta=timedelta(minutes=5),
        mode="reschedule",
    )

    push_main = PythonOperator(
        task_id="push_valid_to_postgres",
        python_callable=load_events_to_postgres
,
        op_kwargs={
            "source_task_id": "validate_events",
            "xcom_key": "validated_files",
            "filetype": "valid",
        },
    )

    push_recovered = PythonOperator(
        task_id="push_recovered_to_postgres",
        python_callable=load_events_to_postgres
,
        op_kwargs={
            "source_task_id": "validate_events",
            "xcom_key": "validated_files",
            "filetype": "invalid",
        },
    )

    aggregate = PythonOperator(
        task_id="aggregate_last_5_minutes",
        python_callable=aggregate_recent_transactions
,
    )
    consume_kafka >> validate
    validate >> push_main >> aggregate
    validate >> wait_5_minutes
    wait_5_minutes >> validate_invalid
    validate_invalid >> push_recovered >> aggregate
