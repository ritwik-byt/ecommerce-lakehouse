import os
import shutil
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, coalesce, lit, lower, trim, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Load environment variables
load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

s3_client = boto3.client("s3", region_name=AWS_REGION)

# Local staging directories (cleaned up automatically)
LOCAL_BRONZE_DIR = "data/staging_bronze"
LOCAL_SILVER_DIR = "data/staging_silver"

def download_s3_prefix(prefix, local_dir):
    """Downloads raw files from S3 Bronze to local staging for PySpark processing."""
    os.makedirs(local_dir, exist_ok=True)
    paginator = s3_client.get_paginator("list_objects_v2")
    total_bytes = 0
    file_count = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv") or key.endswith(".json"):
                local_file = os.path.join(local_dir, os.path.basename(key))
                s3_client.download_file(S3_BUCKET, key, local_file)
                total_bytes += obj["Size"]
                file_count += 1
    return total_bytes, file_count

def upload_silver_to_s3(local_dir, s3_prefix):
    """Uploads Snappy Parquet files from local Silver staging to S3 Silver layer."""
    total_parquet_bytes = 0
    file_count = 0

    for root, _, files in os.walk(local_dir):
        for file in files:
            # Skip hidden Spark CRC/metadata files; upload only actual Parquet data files
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, local_dir)
                s3_key = f"{s3_prefix}/{rel_path}"

                file_size = os.path.getsize(local_path)
                total_parquet_bytes += file_size
                file_count += 1

                s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    return total_parquet_bytes, file_count

def run_silver_pipeline():
    # Clean any old staging folders
    shutil.rmtree(LOCAL_BRONZE_DIR, ignore_errors=True)
    shutil.rmtree(LOCAL_SILVER_DIR, ignore_errors=True)

    print("\n[1/5] Starting Distributed PySpark Session...")
    spark = (
        SparkSession.builder
        .appName("Ecommerce_Lakehouse_Bronze_To_Silver")
        .master("local[*]")  # Uses all CPU cores on your MacBook
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ==========================================
    # PART A: PROCESS ORDERS (510K -> 500K Clean)
    # ==========================================
    print("\n[2/5] Downloading Raw Orders CSVs from S3 Bronze...")
    raw_orders_dir = os.path.join(LOCAL_BRONZE_DIR, "orders")
    bronze_orders_bytes, _ = download_s3_prefix("bronze/mysql/orders/", raw_orders_dir)
    bronze_orders_mb = bronze_orders_bytes / (1024 * 1024)
    print(f"  --> Raw Bronze Orders Size: {bronze_orders_mb:.2f} MB")

    # Strict Schema Definition (Schema Enforcement!)
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_amount_brl", DoubleType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
        StructField("_batch_id", StringType(), True)
    ])

    df_orders_raw = spark.read.csv(raw_orders_dir, header=True, schema=orders_schema)
    raw_count = df_orders_raw.count()

    print("[3/5] Running PySpark Transformations on Orders (Deduplication & Cleansing)...")
    df_orders_silver = (
        df_orders_raw
        # 1. Remove exact duplicate orders based on primary key order_id
        .dropDuplicates(["order_id"])
        # 2. Enforce timestamp type and handle NULL statuses
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_status", coalesce(lower(trim(col("order_status"))), lit("unknown")))
        # 3. Filter out invalid negative/zero order amounts
        .filter(col("order_amount_brl") > 0)
        # 4. Extract partition columns for Athena Partition Pruning
        .withColumn("order_year", year(col("order_purchase_timestamp")))
        .withColumn("order_month", month(col("order_purchase_timestamp")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    silver_count = df_orders_silver.count()
    duplicates_removed = raw_count - silver_count
    print(f"  --> Rows Before: {raw_count:,} | Rows After: {silver_count:,} | Duplicates Removed: {duplicates_removed:,}")

    # Write as Date-Partitioned Snappy Parquet
    silver_orders_out = os.path.join(LOCAL_SILVER_DIR, "orders")
    # coalesce(1) per partition prevents the "Small File Problem" in S3!
    df_orders_silver.coalesce(4).write.mode("overwrite").partitionBy("order_year", "order_month").parquet(silver_orders_out)

    print("  --> Uploading Partitioned Snappy Parquet Orders to S3 Silver...")
    silver_orders_bytes, parquet_files = upload_silver_to_s3(silver_orders_out, "silver/orders")
    silver_orders_mb = silver_orders_bytes / (1024 * 1024)
    savings_pct = ((bronze_orders_mb - silver_orders_mb) / bronze_orders_mb) * 100

    # ==========================================
    # PART B: PROCESS CUSTOMERS (50K Clean)
    # ==========================================
    print("\n[4/5] Processing Customers Table to Silver Parquet...")
    raw_cust_dir = os.path.join(LOCAL_BRONZE_DIR, "customers")
    download_s3_prefix("bronze/mysql/customers/", raw_cust_dir)

    df_cust_raw = spark.read.csv(raw_cust_dir, header=True, inferSchema=True)
    df_cust_silver = (
        df_cust_raw
        .dropDuplicates(["customer_id"])
        .withColumn("email", coalesce(lower(trim(col("email"))), lit("no_email@provided.com")))
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("_silver_processed_at", current_timestamp())
    )

    silver_cust_out = os.path.join(LOCAL_SILVER_DIR, "customers")
    df_cust_silver.coalesce(1).write.mode("overwrite").parquet(silver_cust_out)
    upload_silver_to_s3(silver_cust_out, "silver/customers")
    print(f"  --> Uploaded {df_cust_silver.count():,} cleaned customer records to s3://{S3_BUCKET}/silver/customers/")

    # ==========================================
    # PART C: PRINT BENCHMARK METRICS FOR README
    # ==========================================
    print("\n" + "="*60)
    print(" 🏆 PHASE 2 SILVER LAYER BENCHMARK METRICS (FOR GITHUB README)")
    print("="*60)
    print(f" • Raw Bronze Orders CSV Size    : {bronze_orders_mb:.2f} MB")
    print(f" • Clean Silver Parquet Size     : {silver_orders_mb:.2f} MB ({parquet_files} partitioned files)")
    print(f" • S3 Storage Footprint Reduction: {savings_pct:.1f}%  <-- RESUME METRIC!")
    print(f" • Duplicate Records Eliminated  : {duplicates_removed:,} rows")
    print("="*60 + "\n")

    spark.stop()
    shutil.rmtree(LOCAL_BRONZE_DIR, ignore_errors=True)
    shutil.rmtree(LOCAL_SILVER_DIR, ignore_errors=True)

if __name__ == "__main__":
    run_silver_pipeline()