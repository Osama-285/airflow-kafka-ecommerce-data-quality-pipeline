# Airflow Kafka E-Commerce Data Quality Pipeline

## 📌 Overview

This project implements a **production-style, event-driven data pipeline** using **Apache Kafka** and **Apache Airflow** to validate, clean, and persist real-time e-commerce events into PostgreSQL with **DLQ (Dead Letter Queue) handling**.

The pipeline runs **twice daily**, validates incoming events, routes invalid records to a DLQ, retries them after **15 minutes**, and performs near-real-time aggregations.

---

## 🏗 Architecture

**Kafka → Airflow → PostgreSQL**

**High-level flow:**

1. Consume events from Kafka
2. Validate schema and business rules
3. Persist valid records to PostgreSQL
4. Route invalid events to DLQ
5. Reprocess DLQ after 15 minutes
6. Aggregate transactional metrics

---

## ⚙️ Tech Stack

* **Apache Kafka**
* **Apache Airflow**
* **Python**
* **PostgreSQL**
* **Docker**
* **Confluent Kafka Python Client**

---

## ⏱ Scheduling

* **Frequency:** Twice per day
* **DLQ Reprocessing Window:** 15 minutes
* **Mode:** Event-driven batch ingestion

---

## 🔍 Data Validation Rules

* Required field validation
* Nested object validation (`transaction`, `customer`, `product`)
* Business rule checks:

  * Positive transaction amount
  * Valid payment status
  * Valid product pricing
* Invalid events tagged with error reasons

---

## 🚨 Dead Letter Queue (DLQ)

* Invalid events are written to DLQ JSON files
* FileSensor waits **15 minutes**
* DLQ events are revalidated and recovered if possible
* Successfully recovered records are inserted into PostgreSQL

---

## 📊 Aggregations

* 5-minute rolling window aggregation
* Metrics:

  * Orders by category
  * Payment method distribution
  * Country-level revenue
* Results printed and can be extended to BI dashboards

---

## 🗂 Project Structure

```
.
├── dags/
│   └── kafka_airflow_pipeline.py
├── utils/
│   ├── raw_events_*.json
│   ├── valid_events_*.json
│   └── invalid_events_*.json
├── docker-compose.yml
└── README.md
```

---

## 🚀 How to Run

1. Start services:

```bash
docker-compose up -d
```

2. Create Kafka topic:

```bash
kafka-topics.sh --create --topic storetransactions
```

3. Trigger DAG manually or wait for scheduled execution

---

## 🔮 Future Enhancements

* Integrate **Apache Spark** for advanced analytics and large-scale aggregations
* Implement **schema registry** for contract enforcement
* Add **monitoring & alerting**
* Extend pipeline to **real-time streaming analytics**
