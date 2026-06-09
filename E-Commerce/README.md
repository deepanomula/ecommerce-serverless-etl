# Serverless E-Commerce OLTP-to-OLAP ETL Pipeline

An end-to-end data engineering pipeline that simulates an operational e-commerce ecosystem, extracts transactional data, transforms it into an analytical model, and loads it into a PostgreSQL data warehouse.

## 🏗️ Architecture & Data Flow

1. **Transactional Layer (OLTP):** `raw_ecommerce_generator.py` simulates real-time retail purchases, creating structured transactional data.
2. **Ingestion & Extraction:** `lambda_function.py` executes serverless file-discovery loops to ingest raw operational logs into cloud storage.
3. **Transformation & Star Schema Modeling:** `transform_and_model.py` cleanses the raw data payloads and structures them into a multi-dimensional Star Schema (`fact_orders`, `dim_products`, `dim_customers`).
4. **Idempotent Data Loading:** `load_to_postgres.py` utilizes the `pg8000` driver to stream data into Amazon RDS using strict **UPSERT logic** to guarantee 100% relational integrity without data duplication.
5. **Infrastructure Automation:** `setup_infrastructure.py` handles database table generation and schema initialization securely using environment variables.

## 🚀 Key Engineering Highlights
* **Secure Credential Isolation:** Fully decoupled database connection parameters utilizing Python's `os.environ` to completely protect system host details.
* **In-Memory Streaming:** Implemented `io.StringIO` streams to capture execution telemetry logs on-the-fly, reducing local disk write overhead and improving pipeline execution latency.