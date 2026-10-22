-- ============================================================================
-- PHASE 4: GOLD LAYER STAR SCHEMA DDL & BULK COPY SCRIPTS
-- Supports both Amazon Redshift Serverless (OLAP) & Amazon Athena (Lakehouse)
-- ============================================================================

-- 1. AMAZON REDSHIFT SERVERLESS DDL (WITH DISTKEY & SORTKEY OPTIMIZATION)
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_sk VARCHAR(128) PRIMARY KEY,
    customer_id VARCHAR(64),
    customer_name VARCHAR(128),
    email VARCHAR(256),
    city VARCHAR(128),
    state VARCHAR(32),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
)
DISTSTYLE ALL
SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_sk VARCHAR(128) PRIMARY KEY,
    product_id VARCHAR(64),
    category_name VARCHAR(128),
    is_active BOOLEAN
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_sk VARCHAR(128),
    customer_id VARCHAR(64),
    product_sk VARCHAR(128),
    product_id VARCHAR(64),
    order_status VARCHAR(32),
    order_date DATE,
    order_year INT,
    order_month INT,
    order_amount_brl DOUBLE PRECISION,
    order_amount_usd DOUBLE PRECISION,
    order_amount_inr DOUBLE PRECISION
)
DISTKEY (customer_sk)
SORTKEY (order_date, order_year, order_month);

-- 2. BULK PARALLEL COPY FROM S3 GOLD LAYER INTO REDSHIFT
COPY gold.dim_customers
FROM 's3://ecommerce-lakehouse-bronze-silver-gold-2026/gold/dim_customers/'
IAM_ROLE 'arn:aws:iam::865579549359:role/RedshiftS3LakehouseRole'
FORMAT AS PARQUET;

COPY gold.dim_products
FROM 's3://ecommerce-lakehouse-bronze-silver-gold-2026/gold/dim_products/'
IAM_ROLE 'arn:aws:iam::865579549359:role/RedshiftS3LakehouseRole'
FORMAT AS PARQUET;

COPY gold.fact_orders
FROM 's3://ecommerce-lakehouse-bronze-silver-gold-2026/gold/fact_orders/'
IAM_ROLE 'arn:aws:iam::865579549359:role/RedshiftS3LakehouseRole'
FORMAT AS PARQUET;