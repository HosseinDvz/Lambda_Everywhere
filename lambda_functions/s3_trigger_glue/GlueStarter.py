import json
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    # Extract bucket and key from the event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    # Only run job if file is in the correct path
    if not key.startswith("inputs/urls/") or not key.endswith(".txt"):
        return {"statusCode": 400, "body": f"Skipped non-matching file: {key}"}

    # Start the Glue job
    #response = glue.start_job_run(JobName="chunker")
    response = glue.start_job_run(JobName="ChunkCreate-SendSQS")
    

    return {
        "statusCode": 200,
        "body": f"Started Glue job 'chunker'. Run ID: {response['JobRunId']}"
    }
