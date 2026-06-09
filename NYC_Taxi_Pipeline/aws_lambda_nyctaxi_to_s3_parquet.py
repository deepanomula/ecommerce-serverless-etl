import boto3
import urllib.request
import os

def lambda_handler(event, context):
    # 1. Define the targets (Example: Getting June 2025 Taxi Data)
    year = "2025"
    month = "06"
    
    # Target URL from the NYC website
    source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    
    # Your Raw S3 Bucket Configuration
    bucket_name = "nyc-taxi-pipeline-raw-deepa"
    s3_key = f"raw_data/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet"
    s3_path = f"s3://{bucket_name}/{s3_key}"
    
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    
    print(f"📥 Downloading file from internet and streaming directly to S3...")
    try:
        # Stream the file directly into S3 without saving it to Lambda's local disk
        with urllib.request.urlopen(source_url) as response:
            s3_client.upload_fileobj(response, bucket_name, s3_key)
        print(f"✅ Raw file successfully saved to S3: {s3_path}")
        
    except Exception as e:
        print(f"❌ Failed to download or upload file: {str(e)}")
        raise e

    # 2. Trigger AWS Glue and pass the file path as an argument
    print("🚀 Triggering AWS Glue transformation job...")
    try:
        response = glue_client.start_job_run(
            JobName='nyc-taxi-pipeline-transformer',  # Your actual Glue Job Name
            Arguments={
                '--INPUT_S3_PATH': s3_path,
                '--YEAR': year,
                '--MONTH': month
            }
        )
        job_run_id = response['JobRunId']
        print(f"✨ Glue job started successfully! Run ID: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': f"Successfully ingested data and triggered Glue Job Run: {job_run_id}"
        }
    except Exception as e:
        print(f"❌ Failed to trigger Glue job: {str(e)}")
        raise e