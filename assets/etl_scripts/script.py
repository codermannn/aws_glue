import sys
import boto3
import csv
import logging
import traceback
from io import StringIO
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize boto3 clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Default values
DEFAULT_AGE = 0
DEFAULT_NAME = "Unknown"

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'job_name',
    'source_prefix',
    'destination_prefix',
    'failed_prefix',
    'dynamodb_table_name',
    'data_file_name'
])

job_name = args['job_name']
source_bucket, source_prefix = args['source_prefix'].split('/', 1)  
destination_bucket, destination_prefix = args['destination_prefix'].split('/', 1)
failed_bucket, failed_prefix = args['failed_prefix'].split('/', 1)
dynamodb_table_name = args['dynamodb_table_name']
data_file_name = args['data_file_name']

logger.info(f"Job name: {job_name}")
logger.info(f"S3 buckets and prefixes: source={source_bucket}/{source_prefix}, destination={source_bucket}/{destination_prefix}, failed={source_bucket}/{failed_prefix}")
logger.info(f"DynamoDB table name: {dynamodb_table_name}")
logger.info(f"Data file name: {data_file_name}")

dynamo_table = dynamodb.Table(dynamodb_table_name)

def move_file(source_bucket, source_prefix, destination_bucket, destination_prefix, file_name):
    """
    Moves a file from the source bucket/source_prefix to the destination bucket/destination_prefix.
    
    Parameters:
    - source_bucket: Name of the source S3 bucket.
    - source_prefix: Folder path in the source bucket (prefix).
    - destination_bucket: Name of the destination S3 bucket.
    - destination_prefix: Folder path in the destination bucket (prefix).
    - file_name: Name of the file to move.
    """
    try:
        # Construct the source key and destination key
        source_key = f"{source_prefix}/{file_name}"
        destination_key = f"{destination_prefix}/{file_name}"

        logger.info(f"Moving file from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")

        # Copy the object to the new location
        s3_client.copy_object(
            Bucket=destination_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=destination_key
        )
        logger.info(f"Successfully copied file to s3://{destination_bucket}/{destination_key}")

        # Delete the source object after successful copy
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info(f"Successfully deleted original file from s3://{source_bucket}/{source_key}")

    except ClientError as e:
        # Handle errors returned by AWS
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"ClientError: {error_code} - {error_message}")
        logger.error(traceback.format_exc())
        raise

    except Exception as e:
        # Handle any other unexpected exceptions
        logger.error(f"An unexpected error occurred: {str(e)}")
        logger.error(traceback.format_exc())

    finally:
        # Optional: Log completion of the move operation
        logger.info(f"Move operation completed from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key} (if successful)")

# Example usage
# move_file('source-bucket-name', 'source/folder/path', 'destination-bucket-name', 'destination/folder/path', 'file.txt')


def process_csv_data(content):
    """Processes the CSV data, filling missing values"""
    csv_data = []
    try:
        csv_reader = csv.DictReader(StringIO(content))
        for row in csv_reader:
            id = row.get('id')
            name = row.get('name', DEFAULT_NAME)
            age = int(row.get('age', DEFAULT_AGE))
            csv_data.append({'id': id, 'name': name, 'age': age})
        logger.info(f"Processed {len(csv_data)} rows of CSV data")
    except Exception as e:
        logger.error(f"Error processing CSV data: {e}", exc_info=True)
        raise
    return csv_data

def write_to_dynamodb(data):
    """Writes cleaned data to DynamoDB"""
    try:
        with dynamo_table.batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)
        logger.info(f"Successfully wrote {len(data)} items to DynamoDB table: {dynamodb_table_name}")
    except ClientError as e:
        logger.error(f"Failed to write to DynamoDB: {e}", exc_info=True)
        raise

def process_file(source_key):
    """Processes a single file"""
    logger.info(f"Processing file: s3://{source_bucket}/{source_key}")
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        file_content = response['Body'].read().decode('utf-8')
        csv_data = process_csv_data(file_content)
        write_to_dynamodb(csv_data)
        
        # Pass the actual file name instead of data_file_name
        file_name = source_key.split('/')[-1]
        move_file(source_bucket=source_bucket, source_prefix=source_prefix, destination_bucket=destination_bucket, destination_prefix=destination_prefix, file_name=file_name)
        logger.info(f"Successfully processed file: {source_key}")
    except Exception as e:
        logger.error(f"Error processing file {source_key}: {e}", exc_info=True)
        # Move to failed bucket using the actual file name
        file_name = source_key.split('/')[-1]
        move_file(source_bucket=source_bucket, source_prefix=source_prefix, destination_bucket=failed_bucket, destination_prefix=failed_prefix, file_name=file_name)
        raise

def handler():
    """Main handler function"""
    logger.info("Starting ETL job")
    try:
        source_key = f"{source_prefix}/{data_file_name}"
        process_file(source_key)
        logger.info("ETL job completed successfully")
    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    handler()
