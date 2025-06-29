import boto3
from botocore.exceptions import ClientError

# Initialize clients
s3 = boto3.client('s3')
ecs = boto3.client('ecs')

# Configuration
bucket_name = "lemay"
prefix = "inputs/chunks/"
cluster_name = "smart-turtle-094f4w-SQS"
service_name = "ClassificationService"
task_definition = "ClassificationTask-SQS"
subnets = [
    "subnet-0d409ab1c12507356",
    "subnet-0d17c4e60c12fbefb",
    "subnet-09000a27cbd8133ea",
    "subnet-018802f9e99ba3e92",
    "subnet-03c034141ea421cbf",
    "subnet-02a45cdddd9a396d2"
]
security_groups = ["sg-0faaab5055af01ce9"]  # Replace with your actual security group ID
launch_type = "FARGATE"

def lambda_handler(event, context):
    # Step 1: Count the number of .txt files in the S3 prefix
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        chunk_count = len([obj for obj in response.get("Contents", []) if obj["Key"].endswith(".txt")])
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"❌ Failed to list S3 objects: {str(e)}"
        }

    if chunk_count == 0:
        return {
            "statusCode": 200,
            "body": "ℹ️ No chunk files found. ECS service not created."
        }

    # Step 2: Attempt to create the ECS service
    try:
        ecs.create_service(
            cluster=cluster_name,
            serviceName=service_name,
            taskDefinition=task_definition,
            desiredCount=chunk_count,
            launchType=launch_type,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnets,
                    'securityGroups': security_groups,
                    'assignPublicIp': 'ENABLED'
                }
            }
        )
        return {
            "statusCode": 200,
            "body": f"✅ ECS service '{service_name}' created with {chunk_count} tasks."
        }

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            return {
                "statusCode": 409,
                "body": f"⚠️ ECS service '{service_name}' already exists."
            }
        else:
            return {
                "statusCode": 500,
                "body": f"❌ Error creating ECS service: {str(e)}"
            }
