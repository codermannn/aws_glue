import json
import boto3
import os

def lambda_handler(event, context):
    # Initialize Glue and SSM clients
    glue = boto3.client('glue')
    ssm = boto3.client('ssm')
    
    # Extract the bucket name and key (file name) from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(bucket_name)
    print(file_key)
    
    # Get the file name from the file_key
    file_name = file_key.split('/')[-1]

    print(file_name)
    
    # Get the Glue job name from SSM parameter store
    try:
        job_name = os.environ['glue_job_name']
        print(job_name)
    except Exception as e:
        print(f"Error retrieving Glue job name from SSM: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error retrieving Glue job name')
        }
    
    print(f"New file uploaded: {file_name} in bucket {bucket_name}")
        
    # Start the Glue job
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--data_file_name': file_name
            }
        )
        print(f"Started Glue job: {response['JobRunId']}")
    except Exception as e:
        print(f"Error starting Glue job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error starting Glue job')
        }
        
    return {
        'statusCode': 200,
        'body': json.dumps('Glue job triggered successfully')
    }
