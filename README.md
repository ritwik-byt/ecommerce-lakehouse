# 🚀 Cloud E-Commerce Medallion Data Lakehouse (End-to-End Pipeline)

A production-grade, end-to-end data engineering project built to ingest, transform, model, and orchestrate over **560,000+ transactional e-commerce records** across a 3-tier Medallion architecture on AWS and Databricks.

---

## 🏗️ Architecture Overview

```text
[MySQL OLTP (510K Orders)] ──┐
                              ├──► [S3 Bronze (Raw CSV/JSON)] ──► [Databricks PySpark Silver (Snappy Parquet)] ──► [S3 Gold Star Schema & Athena] ──► [Streamlit Executive Dashboard]
[REST API (Live FX Rates)] ──┘