"""
Website Classification Worker (AWS ECS + SQS + S3)

This script runs inside a Fargate container and polls an SQS queue for messages 
containing S3 paths to input CSVs. For each CSV:

1. Downloads it from S3.
2. Performs zero-shot classification on each row using Hugging Face pipeline.
3. Uploads the labeled result to a separate S3 path.

Expected SQS message format:
    { "s3_path": "lemay/outputs_sqs/filename.csv" }

Output is saved to:
    s3://lemay/labeled_websites/

Dependencies: boto3, pandas, transformers, torch
"""

import boto3, json, pandas as pd
from classifier import WebsiteClassifier
import os
import time


# AWS clients
sqs = boto3.client("sqs", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

# Config
queue_url = "https://sqs.us-east-1.amazonaws.com/066372890447/EcsTaskQueue"
bucket_output = "lemay"
output_prefix = "labeled_websites/"
classifier = WebsiteClassifier()

def extract_s3_path(message_body):
    try:
        # If your message is: {"s3_path": "lemay/outputs_sqs/filename.csv"}
        parsed = json.loads(message_body)
        if "s3_path" in parsed:
            return parsed["s3_path"]

        # Handle raw S3 event directly from SQS (not SNS-wrapped)
        record = parsed["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        return f"{bucket}/{key}"

    except Exception as e:
        print(f"⚠️ Failed to extract s3_path: {e}", flush=True)
        return None

def classify_and_upload(s3_path):
    try:
        bucket, key = s3_path.split("/", 1)
        local_input = "/tmp/input.csv"
        local_output = "/tmp/output.csv"

        print(f"⬇️ Downloading {s3_path}...", flush=True)
        s3.download_file(bucket, key, local_input)

        print("🧠 Running classification...", flush=True)
        classifier.add_label_column(local_input, local_output)

        result_key = output_prefix + os.path.basename(key)
        print(f"⬆️ Uploading to s3://{bucket_output}/{result_key}", flush=True)
        s3.upload_file(local_output, bucket_output, result_key)
    except Exception as e:
        print(f"❌ Classification/upload failed: {e}", flush=True)

def main():
    print("🔁 Starting infinite SQS polling loop...", flush=True)
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=60
            )
            messages = resp.get("Messages", [])
            if not messages:
                print("📭 No messages. Waiting...", flush=True)
                time.sleep(5)
                continue

            for msg in messages:
                s3_path = extract_s3_path(msg["Body"])
                if s3_path:
                    print(f"✅ Received s3_path: {s3_path}", flush=True)
                    classify_and_upload(s3_path)
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                else:
                    print("⚠️ No valid s3_path found.", flush=True)

        except Exception as e:
            print(f"❌ Error polling SQS: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
