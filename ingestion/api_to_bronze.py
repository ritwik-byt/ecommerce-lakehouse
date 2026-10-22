import os
import json
from datetime import datetime, timezone
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Public, free Exchange Rate API (No API key needed!)
API_URL = "https://open.er-api.com/v6/latest/BRL"

def fetch_api_to_s3():
    now = datetime.now(timezone.utc)
    date_folder = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"

    print("\n[*] Calling external REST API for live BRL exchange rates...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()  # Stops the script if the API is down
    
    data = response.json()

    # Add Bronze audit metadata
    data["_ingested_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    data["_source_system"] = "open_er_rest_api"

    s3_key = f"bronze/rest_api/exchange_rates/{date_folder}/fx_rates.json"

    # Upload JSON to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    
    usd_rate = data["rates"].get("USD")
    inr_rate = data["rates"].get("INR")
    print(f"  --> Live Rates Captured: 1 BRL = {usd_rate} USD | 1 BRL = {inr_rate} INR")
    print(f"[✓] Uploaded API JSON to s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    fetch_api_to_s3()