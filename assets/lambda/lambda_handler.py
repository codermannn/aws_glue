import json
import boto3

def lambda_handler(event, context):
    # Initialize Glue client
    glue = boto3.client('glue')
    
    # Extract the bucket name and key (file name) from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the file is in the correct folder (optional check)
    if file_key.startswith('source-folder/'):
        print(f"New file uploaded: {file_key} in bucket {bucket_name}")
        
        # Start the Glue job
        response = glue.start_job_run(
            JobName='your-glue-job-name',
            Arguments={
                '--SOURCE_S3_PATH': f's3://{bucket_name}/{file_key}',
                # Add any other job arguments you may need
            }
        )
        print(f"Started Glue job: {response['JobRunId']}")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Glue job triggered successfully')
    }
