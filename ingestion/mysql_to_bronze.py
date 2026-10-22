import os
import io
from datetime import datetime, timezone
import pandas as pd
import boto3
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load credentials from .env
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")

# 2. Connect to MySQL and AWS S3
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
s3_client = boto3.client("s3", region_name=AWS_REGION)

TABLES = ["customers", "orders"]
CHUNK_SIZE = 100000  # Pull 100,000 rows at a time to protect RAM

def extract_mysql_to_s3():
    # Get current UTC timestamp for folder partitioning
    now = datetime.now(timezone.utc)
    date_folder = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"

    for table in TABLES:
        print(f"\n[*] Starting extraction for table: `{table}`...")
        query = f"SELECT * FROM {table}"
        
        chunk_number = 0
        total_rows = 0

        # Read from MySQL in chunks of 100k rows
        for chunk_df in pd.read_sql_query(query, con=engine, chunksize=CHUNK_SIZE):
            chunk_number += 1
            rows_in_chunk = len(chunk_df)
            total_rows += rows_in_chunk

            # Add Bronze Layer Audit Columns (Interview gold!)
            chunk_df["_ingested_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            chunk_df["_source_system"] = "mysql_ecommerce_oltp"
            chunk_df["_batch_id"] = f"{table}_part_{chunk_number}"

            # Convert DataFrame to CSV in memory (no local disk clutter)
            csv_buffer = io.StringIO()
            chunk_df.to_csv(csv_buffer, index=False)

            # Define the S3 file path (Key)
            s3_key = f"bronze/mysql/{table}/{date_folder}/{table}_part_{chunk_number:02d}.csv"

            # Upload directly to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=csv_buffer.getvalue().encode("utf-8")
            )
            print(f"  --> Uploaded Chunk #{chunk_number} ({rows_in_chunk:,} rows) to s3://{S3_BUCKET}/{s3_key}")

        print(f"[✓] Finished `{table}`! Total rows uploaded to Bronze: {total_rows:,}")

if __name__ == "__main__":
    extract_mysql_to_s3()