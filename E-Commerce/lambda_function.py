import os
import json
import logging
import io
from datetime import datetime
import boto3
import pandas as pd
import pg8000.native

class EcommerceETLPipeline:
    def __init__(self):
        """Initializes infrastructure connections and cloud settings."""
        # 1. Setup in-memory log capture buffer
        self.log_buffer = io.StringIO()
        self.logger = logging.getLogger("s3_pipeline")
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers to prevent duplicate logging in Lambda
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        # Add stream handler to route logs to both CloudWatch and our memory buffer
        string_handler = logging.StreamHandler(self.log_buffer)
        string_handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s'))
        self.logger.addHandler(string_handler)
        
        # Also route to standard console output for CloudWatch monitoring
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s'))
        self.logger.addHandler(console_handler)

        # 2. Initialize AWS Clients
        self.s3_client = boto3.client('s3')
        
        # 3. Cache Database Configuration Environment Variables
        self.db_host = os.environ['DB_HOST']
        self.db_name = os.environ['DB_NAME']
        self.db_user = os.environ['DB_USER']
        self.db_password = os.environ['DB_PASSWORD']
        self.db_port = int(os.environ['DB_PORT'])

    def get_db_connection(self):
        """Establishes a connection using the layer-native pg8000 driver."""
        self.logger.info("Opening database socket connection to RDS instance...")
        return pg8000.native.Connection(
            host=self.db_host, database=self.db_name,
            user=self.db_user, password=self.db_password, port=self.db_port
        )

    def extract_s3_file(self, bucket_name, file_key):
        """Downloads the target CSV from S3 and reads it into a Pandas DataFrame."""
        local_path = '/tmp/raw_data.csv'
        self.logger.info(f"Extracting raw data object: s3://{bucket_name}/{file_key}")
        self.s3_client.download_file(bucket_name, file_key, local_path)
        df = pd.read_csv(local_path)
        self.logger.info(f"Extraction successful. Parsed {len(df)} rows into memory.")
        return df

    def transform_star_schema(self, df_raw):
        """Transforms flat files into structured Star Schema Dimensions and Fact frames."""
        self.logger.info("Transforming staging records into relational dimensions...")
        
        dim_products = df_raw[['product_id', 'product_name', 'product_category']].drop_duplicates()
        dim_geography = df_raw[['delivery_zip', 'delivery_city', 'delivery_state']].drop_duplicates()
        
        unique_cust_ids = df_raw['customer_id'].unique()
        dim_customers = pd.DataFrame({
            'customer_id': unique_cust_ids,
            'customer_name': [f"Customer_{i}" for i in range(len(unique_cust_ids))],
            'segment': ['Consumer' if i % 2 == 0 else 'Corporate' for i in range(len(unique_cust_ids))]
        })
        
        fact_orders = df_raw[[
            'order_id', 'order_timestamp', 'customer_id', 'product_id', 
            'delivery_zip', 'quantity', 'total_revenue', 'shipping_cost'
        ]].copy()
        
        self.logger.info(f"Transform complete. Shapes -> Products: {dim_products.shape}, Orders Fact: {fact_orders.shape}")
        return dim_products, dim_geography, dim_customers, fact_orders

    def initialize_tables(self, db):
        """Ensures tables exist without dropping existing historical data rows."""
        self.logger.info("Verifying structural integrity of target schemas...")
        ddl_queries = [
            "CREATE TABLE IF NOT EXISTS dim_products (product_id VARCHAR(50) PRIMARY KEY, product_name VARCHAR(150), product_category VARCHAR(100));",
            "CREATE TABLE IF NOT EXISTS dim_geography (zip_code VARCHAR(20) PRIMARY KEY, city VARCHAR(100), state VARCHAR(50));",
            "CREATE TABLE IF NOT EXISTS dim_customers (customer_id VARCHAR(50) PRIMARY KEY, customer_name VARCHAR(100), segment VARCHAR(50));",
            "CREATE TABLE IF NOT EXISTS fact_orders (order_id VARCHAR(50) PRIMARY KEY, order_timestamp TIMESTAMP, customer_id VARCHAR(50) REFERENCES dim_customers(customer_id), product_id VARCHAR(50) REFERENCES dim_products(product_id), zip_code VARCHAR(20) REFERENCES dim_geography(zip_code), quantity INT, total_revenue NUMERIC(10,2), shipping_cost NUMERIC(10,2));"
        ]
        for q in ddl_queries:
            db.run(q)

    def bulk_load_data(self, db, dim_products, dim_geography, dim_customers, fact_orders):
        """Appends unique multi-dimensional tables into the target engine using keyword parameters."""
        self.logger.info("Appending dimensional records with duplicate protection (UPSERT)...")
        
        # 1. Products
        for row in dim_products.itertuples(index=False):
            params = {
                "prod_id": row.product_id,
                "prod_name": row.product_name,
                "prod_cat": row.product_category
            }
            db.run("INSERT INTO dim_products VALUES (:prod_id, :prod_name, :prod_cat) ON CONFLICT (product_id) DO NOTHING", **params)
            
        # 2. Geography
        for row in dim_geography.itertuples(index=False):
            params = {
                "zip": row.delivery_zip,
                "city": row.delivery_city,
                "state": row.delivery_state
            }
            db.run("INSERT INTO dim_geography VALUES (:zip, :city, :state) ON CONFLICT (zip_code) DO NOTHING", **params)
            
        # 3. Customers
        for row in dim_customers.itertuples(index=False):
            params = {
                "cust_id": row.customer_id,
                "cust_name": row.customer_name,
                "segment": row.segment
            }
            db.run("INSERT INTO dim_customers VALUES (:cust_id, :cust_name, :segment) ON CONFLICT (customer_id) DO NOTHING", **params)
            
        self.logger.info("Appending transaction records into core Fact table...")
        # 4. Fact Orders
        for row in fact_orders.itertuples(index=False):
            params = {
                "ord_id": row.order_id,
                "ts": row.order_timestamp,
                "cust_id": row.customer_id,
                "prod_id": row.product_id,
                "zip": row.delivery_zip,
                "qty": row.quantity,
                "rev": row.total_revenue,
                "ship": row.shipping_cost
            }
            db.run(
                "INSERT INTO fact_orders VALUES (:ord_id, :ts, :cust_id, :prod_id, :zip, :qty, :rev, :ship) "
                "ON CONFLICT (order_id) DO NOTHING", 
                **params
            )
            
        self.logger.info("Incremental load complete.")

    def archive_processed_file(self, bucket_name, file_key, base_file_name):
        """Moves processed files out of landing zone into an archive folder."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_key = f"archive/{timestamp}_{base_file_name}.csv"
            
            self.logger.info(f"🚚 Archiving source file: moving raw data to s3://{bucket_name}/{archive_key}")
            
            # 1. Copy file to archive directory
            self.s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=archive_key
            )
            # 2. Delete original file from landing folder
            self.s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            self.logger.info("🧹 Landing zone folder cleaned successfully.")
        except Exception as archive_error:
            self.logger.error(f"⚠️ Archiving phase failed: {str(archive_error)}")

    def ship_logs_to_s3(self, bucket_name, base_file_name):
        """Pushes the execution log buffer up to your dedicated storage path."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_filename = f"logs/pipeline_run_{base_file_name}_{timestamp}.log"
            
            # Fetch content out of the string memory stream
            log_content = self.log_buffer.getvalue()
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=log_filename,
                Body=log_content,
                ContentType='text/plain'
            )
            print(f"📁 Execution logs archived safely to: s3://{bucket_name}/{log_filename}")
        except Exception as upload_error:
            print(f"⚠️ Failed to ship logs to S3: {str(upload_error)}")

    def main(self, event):
        """Main orchestrator sequence execution flow control loop."""
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        base_file_name = file_key.split('/')[-1].replace('.csv', '')
        
        try:
            self.logger.info(f"🚀 Initializing automated ETL processing for event file: s3://{bucket_name}/{file_key}")
            
            # Run pipeline pipeline stages
            df_raw = self.extract_s3_file(bucket_name, file_key)
            dim_products, dim_geography, dim_customers, fact_orders = self.transform_star_schema(df_raw)
            
            db = self.get_db_connection()
            self.initialize_tables(db)
            self.bulk_load_data(db, dim_products, dim_geography, dim_customers, fact_orders)
            db.close()
            
            self.logger.info("🎉 ETL processing completed successfully without interruptions.")

            self.archive_processed_file(bucket_name, file_key, base_file_name)
        
            return {"statusCode": 200, "body": "Success"}
            
        except Exception as pipeline_error:
            self.logger.error(f"❌ CRITICAL PROCESS ABORTED: {str(pipeline_error)}", exc_info=True)
            return {"statusCode": 500, "body": "Failure"}
            
        finally:
            # The 'finally' block GUARANTEES logs upload even if database or mapping code completely crashes
            self.ship_logs_to_s3(bucket_name, base_file_name)


# ==========================================
# 🚀 OUTSIDE WRAPPER (AWS LAMBDA ENTRYPOINT)
# ==========================================

def lambda_handler(event, context):
    # Instantiate the application shell (Triggers __init__)
    pipeline = EcommerceETLPipeline()
    
    # Run the core logic routine
    result = pipeline.main(event)
    return result