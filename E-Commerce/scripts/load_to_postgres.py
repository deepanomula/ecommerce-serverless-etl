from sqlalchemy import create_engine, text
from transform_and_model import transform_raw_data
import os

# 1. Get our transformed dataframes from Step 2
prod_df, geo_df, cust_df, fact_df = transform_raw_data("raw_ecommerce_orders.csv")

# 2. Database Connection Configuration
# TODO: Replace with your actual username, password, host endpoint, and database name
USER = os.environ.get("DB_USER", "postgres")
PASSWORD = os.environ.get("DB_PASSWORD")
HOST = os.environ.get("DB_HOST", "localhost")  # Falls back to local machine if not set
PORT = os.environ.get("DB_PORT", "5432")       # Default PostgreSQL port
DB_NAME = os.environ.get("DB_NAME", "ecommerce_db")

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# 3. Create the Relational Schema (DDL)
create_tables_sql = """
-- Drop tables if they exist to start fresh (Order matters due to Foreign Keys!)
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_geography;
DROP TABLE IF EXISTS dim_customers;

-- Create Dimension: Products
CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    product_category VARCHAR(100)
);

-- Create Dimension: Geography
CREATE TABLE dim_geography (
    zip_code VARCHAR(20) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50)
);

-- Create Dimension: Customers
CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

-- Create Fact: Orders
CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_timestamp TIMESTAMP NOT NULL,
    customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    zip_code VARCHAR(20) REFERENCES dim_geography(zip_code),
    quantity INT,
    total_revenue NUMERIC(10, 2),
    shipping_cost NUMERIC(10, 2)
);
"""

def load_data():
    print("\nConnecting to AWS RDS PostgreSQL...")
    with engine.begin() as connection:
        print("Creating Star Schema tables and constraints...")
        connection.execute(text(create_tables_sql))
        print("Tables created successfully!")
        
        # 4. LOAD: Push the Pandas dataframes directly into PostgreSQL
        print("\nLoading data into Dimension Tables...")
        prod_df.to_sql('dim_products', con=connection, if_exists='append', index=False)
        geo_df.to_sql('dim_geography', con=connection, if_exists='append', index=False)
        cust_df.to_sql('dim_customers', con=connection, if_exists='append', index=False)
        
        print("Loading data into Central Fact Table...")
        fact_df.to_sql('fact_orders', con=connection, if_exists='append', index=False)
        
    print("\n🎉 ETL pipeline successful! All Star Schema data loaded into AWS RDS.")

if __name__ == "__main__":
    load_data()