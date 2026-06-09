# End-to-End Serverless E-Commerce ETL & Analytics Pipeline

An enterprise-grade, serverless data pipeline designed to ingest, transform, and model high-volume transactional data for analytical reporting. This project showcases cloud automation, Infrastructure as Code (IaC), big data processing, and multi-dimensional data modeling.

## 🏗️ Architecture Blueprint

1. **Infrastucture as Code:** Automated cloud resource provisioning using **Terraform** configurations.
2. **Data Ingestion (OLTP-to-Cloud):** **AWS Lambda** script executes lightweight file-discovery loops to capture and land multi-source transactional datasets securely into an **Amazon S3** landing zone.
3. **Distributed Processing (OLAP Warehouse):** **AWS Glue** orchestrates batch-scheduled jobs, utilizing **PySpark** to handle structural data transformations, process skewed keys, and write optimized clean datasets back to an analytical S3 storage tier.
4. **Data Modeling:** Transformed data maps directly into a multi-dimensional **Star Schema** optimized for downstream BI and reporting layers.

## 📁 Repository Structure

* `terraform/` - Declarative cloud infrastructure configuration files.
* `aws_lambda_nyctaxi_to_s3_parquet.py` - Ingestion engine with integrated logging and stream error handling.
* `aws_glue_s3_rawparquet_to_s3_cleanparquet.py` - PySpark processing layer executing business logic transformations.
* `NOTES.md` - Technical runbooks and development logs.

## 🚀 Key Engineering Implementations

* **Idempotent Data Design:** Ingestion and transformation phases are architected for safety, ensuring repeat runs do not generate duplicate records.
* **Resilient Logging:** Integrated execution telemetry tracking to isolate malformed schemas cleanly without crashing the core execution pipeline.
* **Storage Optimization:** Raw data payloads are converted to heavily compressed, column-oriented Parquet formats to optimize downstream query speeds and lower overall cloud storage overhead.