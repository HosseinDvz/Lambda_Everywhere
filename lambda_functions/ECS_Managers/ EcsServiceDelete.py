import boto3

s3 = boto3.client('s3')
ecs = boto3.client('ecs')

# Configuration
bucket_name = "lemay"
input_prefix = "inputs/chunks/"
output_prefix = "labeled_websites/"
cluster_name = "smart-turtle-094f4w-SQS"
service_name = "ClassificationService"

def count_csv_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return len([obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")])

def count_txt_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return len([obj for obj in response.get("Contents", []) if obj["Key"].endswith(".txt")])


def lambda_handler(event, context):
    try:
        chunk_count = count_txt_files(bucket_name, input_prefix)
        labeled_count = count_csv_files(bucket_name, output_prefix)

        print(f"🧮 Chunks: {chunk_count}, Labeled: {labeled_count}")

        if chunk_count == 0:
            return {"statusCode": 200, "body": "❌ No chunks found."}
        
        if labeled_count < chunk_count:
            return {"statusCode": 200, "body": f"⌛ Waiting: {labeled_count} of {chunk_count} files labeled."}

        # Delete ECS service
        ecs.update_service(
            cluster=cluster_name,
            service=service_name,
            desiredCount=0
        )

        ecs.delete_service(
            cluster=cluster_name,
            service=service_name,
            force=True
        )

        return {
            "statusCode": 200,
            "body": f"🧹 Deleted service '{service_name}' after processing completed."
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"❌ Error: {str(e)}"
        }
