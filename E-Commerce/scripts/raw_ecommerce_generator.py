import pandas as pd
import random
from datetime import datetime, timedelta

# 1. Setup seed data for our Dimension Tables
PRODUCT_CATEGORIES = ['Electronics', 'Apparel', 'Home & Kitchen', 'Books', 'Beauty']
PRODUCTS = {
    'PROD_001': {'name': 'Wireless Headphones', 'category': 'Electronics', 'price': 99.99},
    'PROD_002': {'name': 'Ergonomic Office Chair', 'category': 'Home & Kitchen', 'price': 149.50},
    'PROD_003': {'name': 'Leather Wallet', 'category': 'Apparel', 'price': 45.00},
    'PROD_004': {'name': 'Data Engineering 101 Book', 'category': 'Books', 'price': 29.99},
    'PROD_005': {'name': 'Sunscreeen SPF 50', 'category': 'Beauty', 'price': 18.25}
}

STATES_CITIES = [
    {'city': 'Austin', 'state': 'TX', 'zip': '78701'},
    {'city': 'San Marcos', 'state': 'TX', 'zip': '78666'},
    {'city': 'New York', 'state': 'NY', 'zip': '10001'},
    {'city': 'Los Angeles', 'state': 'CA', 'zip': '90012'},
    {'city': 'Miami', 'state': 'FL', 'zip': '33101'}
]

def generate_raw_orders(num_records=1000):
    orders_data = []
    start_date = datetime(2026, 1, 1)
    
    for i in range(num_records):
        order_id = f"ORD_{10000 + i}"
        # Simulate a random timestamp over the last few months
        random_days = random.randint(0, 140)
        random_seconds = random.randint(0, 86400)
        order_time = start_date + timedelta(days=random_days, seconds=random_seconds)
        
        # Pick random dimension attributes
        cust_id = f"CUST_{random.randint(100, 150)}"
        prod_id = random.choice(list(PRODUCTS.keys()))
        geo = random.choice(STATES_CITIES)
        
        # Quantity and Calculations
        qty = random.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        unit_price = PRODUCTS[prod_id]['price']
        total_rev = round(qty * unit_price, 2)
        shipping = round(random.uniform(2.50, 15.00), 2)
        
        orders_data.append({
            'order_id': order_id,
            'order_timestamp': order_time.strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': cust_id,
            'product_id': prod_id,
            'product_name': PRODUCTS[prod_id]['name'], # Denormalized raw data
            'product_category': PRODUCTS[prod_id]['category'],
            'delivery_city': geo['city'],
            'delivery_state': geo['state'],
            'delivery_zip': geo['zip'],
            'quantity': qty,
            'total_revenue': total_rev,
            'shipping_cost': shipping
        })
        
    return pd.DataFrame(orders_data)

if __name__ == "__main__":
    df_raw = generate_raw_orders(1000)
    print("--- Sample Raw E-Commerce Stream ---")
    print(df_raw.head())
    
    # Save it down as a raw landing file
    df_raw.to_csv("raw_ecommerce_orders.csv", index=False)
    print("\nSuccessfully saved 1,000 raw denormalized records to raw_ecommerce_orders.csv!")