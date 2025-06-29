import sys
import boto3
import json

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init('chunking-job', args={})  # Replace with args['JOB_NAME'] if needed

# === Configuration ===
s3_bucket = "lemay"
input_key = "inputs/urls/static_websites.txt"
output_prefix = "inputs/chunks/"
num_chunks = 10
sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/066372890447/website-chunks-queue"

# === Read input file from S3 ===
s3 = boto3.client("s3")
obj = s3.get_object(Bucket=s3_bucket, Key=input_key)
lines = obj["Body"].read().decode("utf-8").splitlines()

# === Chunking ===
chunk_size = len(lines) // num_chunks + (len(lines) % num_chunks > 0)
chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

# === Write each chunk to S3 and enqueue metadata to SQS ===
sqs = boto3.client("sqs")
for idx, chunk in enumerate(chunks):
    content = "\n".join(chunk)
    chunk_key = f"{output_prefix}chunk_{idx}.txt"
    
    # Upload chunk file to S3
    s3.put_object(Bucket=s3_bucket, Key=chunk_key, Body=content.encode("utf-8"))
    
    # Send metadata to SQS
    sqs.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps({
            "bucket": s3_bucket,
            "key": chunk_key
        })
    )

print(f"✅ Created and enqueued {len(chunks)} chunks.")

# === Invoke Lambda to start ECS Service ===
lambda_client = boto3.client("lambda")

response = lambda_client.invoke(
    FunctionName="EcsServiceCreator",
    InvocationType="Event",  # Async
    Payload=json.dumps({
        "trigger": "start-ecs-service"
    })
)

print("✅ Lambda 'EcsServiceCreator' invoked to start ECS tasks.")


# === Commit the Glue job ===
job.commit()
