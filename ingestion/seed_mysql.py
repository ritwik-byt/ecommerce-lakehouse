import os
import random
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load the secret credentials from our .env file
load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")

# 2. Create the connection bridge to MySQL (like JDBC in Java)
connection_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
engine = create_engine(connection_url)

def generate_and_load_data():
    print("Step 1: Generating 50,000 Customers...")
    cities = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre"]
    states = ["SP", "RJ", "MG", "PR", "RS"]
    
    customers = []
    for i in range(1, 50001):
        idx = random.randint(0, 4)
        customers.append({
            "customer_id": f"CUST_{i:06d}",
            "customer_name": f"Customer_{i}",
            "email": f"user_{i}@example.com" if random.random() > 0.05 else None, # 5% missing emails (dirty data!)
            "city": cities[idx],
            "state": states[idx],
            "updated_at": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S")
        })
    df_customers = pd.DataFrame(customers)
    
    print("Step 2: Generating 500,000 Orders (with ~2% intentional duplicates for Silver layer cleanup)...")
    statuses = ["delivered", "shipped", "processing", "canceled", None] # Includes some NULL statuses
    base_date = datetime(2025, 1, 1)
    
    orders = []
    for i in range(1, 500001):
        order_date = base_date + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86400))
        orders.append({
            "order_id": f"ORD_{i:07d}",
            "customer_id": f"CUST_{random.randint(1, 50000):06d}",
            "product_id": f"PROD_{random.randint(1, 500):04d}",
            "order_status": random.choices(statuses, weights=[80, 10, 5, 3, 2])[0],
            "order_amount_brl": round(random.uniform(20.0, 1500.0), 2),
            "order_purchase_timestamp": order_date.strftime("%Y-%m-%d %H:%M:%S")
        })
        
    df_orders = pd.DataFrame(orders)
    
    # Inject 10,000 exact duplicate rows so we can prove deduplication in Phase 2!
    duplicates = df_orders.sample(n=10000, random_state=42)
    df_orders = pd.concat([df_orders, duplicates], ignore_index=True)
    
    print(f"Total rows created: {len(df_customers):,} customers and {len(df_orders):,} orders (Total: {len(df_customers) + len(df_orders):,} records).")

    # 3. Load into MySQL in chunks of 25,000 rows so our laptop RAM doesn't spike
    print("Step 3: Inserting Customers into MySQL...")
    df_customers.to_sql("customers", con=engine, if_exists="replace", index=False, chunksize=25000)
    
    print("Step 4: Inserting 510,000 Orders into MySQL (this takes ~20-40 seconds)...")
    df_orders.to_sql("orders", con=engine, if_exists="replace", index=False, chunksize=25000)
    
    print("SUCCESS! All 560,000 records are now live in your MySQL database!")

if __name__ == "__main__":
    generate_and_load_data()