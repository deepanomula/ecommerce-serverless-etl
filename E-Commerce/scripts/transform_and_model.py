import pandas as pd

def transform_raw_data(csv_path):
    print("🚀 Starting ETL Transformation...")
    
    # 1. EXTRACT: Read the raw flat file
    df_raw = pd.read_csv(csv_path)
    
    # 2. TRANSFORM: Normalize into a Star Schema
    
    # --- Table 1: dim_products ---
    # Extract unique products and keep only product-specific attributes
    dim_products = df_raw[['product_id', 'product_name', 'product_category']].drop_duplicates().reset_index(drop=True)
    
    # --- Table 2: dim_geography ---
    # Extract unique locations based on zip code
    dim_geography = df_raw[['delivery_zip', 'delivery_city', 'delivery_state']].drop_duplicates().reset_index(drop=True)
    # Rename columns to match clean database standards
    dim_geography.columns = ['zip_code', 'city', 'state']
    
    # --- Table 3: dim_customers ---
    # Simulate a customer dimension table since our raw stream just has IDs
    unique_cust_ids = df_raw['customer_id'].unique()
    dim_customers = pd.DataFrame({
        'customer_id': unique_cust_ids,
        'customer_name': [f"Customer_{i}" for i in range(len(unique_cust_ids))],
        'segment': [pd.Series(['Consumer', 'Corporate', 'Home Office']).sample(1).values[0] for _ in range(len(unique_cust_ids))]
    })
    
    # --- Table 4: fact_orders ---
    # The central fact table only keeps metrics and foreign keys (IDs)
    fact_orders = df_raw[[
        'order_id', 
        'order_timestamp', 
        'customer_id', 
        'product_id', 
        'delivery_zip', 
        'quantity', 
        'total_revenue', 
        'shipping_cost'
    ]].copy()
    fact_orders.rename(columns={'delivery_zip': 'zip_code'}, inplace=True)
    
    print("✅ Transformation complete! Data split into Star Schema elements.")
    
    # Return all dataframes
    return dim_products, dim_geography, dim_customers, fact_orders

if __name__ == "__main__":
    prod_df, geo_df, cust_df, fact_df = transform_raw_data("raw_ecommerce_orders.csv")
    
    # Print shapes to verify compression/normalization
    print(f"\n📊 Row Counts After Normalization:")
    print(f" - Fact Orders: {fact_df.shape[0]} rows (Full transactional log)")
    print(f" - Dim Products: {prod_df.shape[0]} rows (Unique products)")
    print(f" - Dim Geography: {geo_df.shape[0]} rows (Unique locations)")
    print(f" - Dim Customers: {cust_df.shape[0]} rows (Unique customers)")