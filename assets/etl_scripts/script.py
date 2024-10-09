import sys
import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, lit
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, BinaryType, TimestampType
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('[GLUE_ETL_JOB] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("[GLUE_ETL_JOB] Starting script execution")

# Initialize Glue and Spark contexts
logger.info("[GLUE_ETL_JOB] Initializing Glue and Spark contexts")
sc = SparkContext()
glueContext = GlueContext(sc)
glue_client = boto3.client('glue')
logger.info("[GLUE_ETL_JOB] Glue and Spark contexts initialized successfully")

# Initialize boto3 clients
logger.info("[GLUE_ETL_JOB] Initializing boto3 clients")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
logger.info("[GLUE_ETL_JOB] boto3 clients initialized successfully")

# Get job parameters from the Glue job
logger.info("[GLUE_ETL_JOB] Retrieving job parameters")
args = getResolvedOptions(sys.argv, [
    'job_name',
    'source_prefix',
    'destination_prefix',
    'failed_prefix',
    'dynamodb_table_name',
    'table_prefix',
    'data_file_name',
    'database_catalog_name'
])
logger.info(f"[GLUE_ETL_JOB] Retrieved job parameters: {args}")

# Extract job parameters
job_name = args['job_name']
source_bucket, source_prefix = args['source_prefix'].split('/', 1)  
destination_bucket, destination_prefix = args['destination_prefix'].split('/', 1)
failed_bucket, failed_prefix = args['failed_prefix'].split('/', 1)
dynamodb_table_name = args['dynamodb_table_name']
table_prefix = args['table_prefix']
data_file_name = args['data_file_name']
database_catalog_name = args['database_catalog_name']

# Log job parameters for debugging
logger.info(f"[GLUE_ETL_JOB] Job name: {job_name}")
logger.info(f"[GLUE_ETL_JOB] S3 buckets and prefixes: source={source_bucket}/{source_prefix}, destination={destination_bucket}/{destination_prefix}, failed={failed_bucket}/{failed_prefix}")
logger.info(f"[GLUE_ETL_JOB] DynamoDB table name: {dynamodb_table_name}")
logger.info(f"[GLUE_ETL_JOB] Data file name: {data_file_name}")
logger.info(f"[GLUE_ETL_JOB] Database catalog name: {database_catalog_name}")
logger.info(f"[GLUE_ETL_JOB] Table prefix: {table_prefix}")

# Initialize DynamoDB table
logger.info(f"[GLUE_ETL_JOB] Initializing DynamoDB table: {dynamodb_table_name}")
logger.info("[GLUE_ETL_JOB] DynamoDB table initialized successfully")

def move_file(source_bucket, source_prefix, destination_bucket, destination_prefix, file_name):
    logger.info(f"[GLUE_ETL_JOB] Starting move_file function for file: {file_name}")
    try:
        source_key = f"{source_prefix}/{file_name}"
        destination_key = f"{destination_prefix}/{file_name}"

        logger.info(f"[GLUE_ETL_JOB] Moving file from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")

        s3_client.copy_object(
            Bucket=destination_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=destination_key
        )
        logger.info(f"[GLUE_ETL_JOB] Successfully copied file to s3://{destination_bucket}/{destination_key}")

        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info(f"[GLUE_ETL_JOB] Successfully deleted original file from s3://{source_bucket}/{source_key}")

    except ClientError as e:
        logger.error(f"[GLUE_ETL_JOB] ClientError in move_file: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        raise
    except Exception as e:
        logger.error(f"[GLUE_ETL_JOB] An unexpected error occurred in move_file: {str(e)}")
        raise
    logger.info("[GLUE_ETL_JOB] move_file function completed successfully")

def write_to_dynamodb(dynamic_frame):
    """
    Writes the provided DynamicFrame to a DynamoDB table.

    Args:
    - dynamic_frame: The DynamicFrame to write.
    """
    
    dynamodb_options = {
        "tableName": dynamodb_table_name,
        "overwrite": "true"
    }
    # Specify connection options for DynamoDB
    try:
        # Write the DynamicFrame to DynamoDB
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options=dynamodb_options
        )
        logger.info(f"[GLUE_ETL_JOB] Successfully written to DynamoDB table: {dynamodb_table_name}")
    except Exception as e:
        logger.error(f"[GLUE_ETL_JOB] Failed to write to DynamoDB table: {dynamodb_table_name}, Error: {str(e)}")
        raise  # Re-raise the exception to be caught in the calling function

def process_file():
    logger.info("[GLUE_ETL_JOB] Starting process_file function")
    try:
        # Retrieve table from catalog
        logger.info(f"[GLUE_ETL_JOB] Retrieving table from catalog: {database_catalog_name}")
        response = glue_client.get_tables(DatabaseName=database_catalog_name)
        tables = response['TableList']
        filtered_tables = [table for table in tables if table['Name'].startswith(table_prefix)]
        
        if not filtered_tables:
            raise Exception(f"No tables found in database '{database_catalog_name}' with prefix '{table_prefix}'")
        
        table = filtered_tables[0]
        table_name = table['Name']
        logger.info(f"[GLUE_ETL_JOB] Found table: {table_name}")

        # Get catalog schema
        catalog_schema = {col['Name']: col['Type'] for col in table['StorageDescriptor']['Columns']}
        logger.info(f"[GLUE_ETL_JOB] Catalog schema: {catalog_schema}")

        # Define default values
        default_values = {
            "string": "", "int": 0, "bigint": 0, "double": 0.0,
            "float": 0.0, "boolean": False, "binary": bytearray(),
            "timestamp": "1970-01-01 00:00:00"
        }

        # Read data from S3
        logger.info(f"[GLUE_ETL_JOB] Reading file from S3: s3://{source_bucket}/{source_prefix}/{data_file_name}")
        source_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{source_bucket}/{source_prefix}/{data_file_name}"]},
            format="csv",
            format_options={"withHeader": True}
        )

        # Convert to DataFrame and apply schema
        logger.info("[GLUE_ETL_JOB] Applying schema and defaults")
        df = source_dyf.toDF()
        for col_name, col_type in catalog_schema.items():
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull() | (col(col_name) == ""), default_values.get(col_type.lower(), None))
                    .otherwise(col(col_name).cast(col_type))
                )
            else:
                df = df.withColumn(col_name, lit(default_values.get(col_type.lower(), None)).cast(col_type))

        # Ensure 'id' column is present and of type bigint
        if 'id' not in df.columns:
            logger.error("[GLUE_ETL_JOB] 'id' column is missing from the data")
            raise ValueError("'id' column is required as the primary key")
        df = df.withColumn('id', col('id').cast('bigint'))

        # Convert back to DynamicFrame
        logger.info("[GLUE_ETL_JOB] Converting back to DynamicFrame")
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

        # Log schema and sample data
        logger.info(f"[GLUE_ETL_JOB] Final schema: {dynamic_frame.schema()}")
        logger.info("[GLUE_ETL_JOB] Sample data (first 5 rows):")
        for row in dynamic_frame.toDF().limit(5).collect():
            logger.info(str(row.asDict()))

        # Write to DynamoDB
        logger.info("[GLUE_ETL_JOB] Writing to DynamoDB")
        write_to_dynamodb(dynamic_frame)

        # Move processed file
        logger.info("[GLUE_ETL_JOB] Moving processed file")
        move_file(source_bucket, source_prefix, destination_bucket, destination_prefix, data_file_name)
        
        logger.info(f"[GLUE_ETL_JOB] Successfully processed file: {data_file_name}")
    except Exception as e:
        logger.error(f"[GLUE_ETL_JOB] Error processing file {data_file_name}: {str(e)}")
        logger.info(f"[GLUE_ETL_JOB] Moving file to failed folder: s3://{failed_bucket}/{failed_prefix}/{data_file_name}")
        move_file(source_bucket, source_prefix, failed_bucket, failed_prefix, data_file_name)
        raise

    logger.info("[GLUE_ETL_JOB] process_file function completed")

def apply_transformations(dynamic_frame):
    logger.info("[GLUE_ETL_JOB] Starting apply_transformations function")
    # Add your transformations here
    logger.info("[GLUE_ETL_JOB] No transformations applied")
    return dynamic_frame

def handler():
    logger.info("[GLUE_ETL_JOB] Starting ETL job")
    try:
        process_file()
        logger.info("[GLUE_ETL_JOB] ETL job completed successfully")
    except Exception as e:
        logger.error(f"[GLUE_ETL_JOB] ETL job failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    logger.info("[GLUE_ETL_JOB] Script started")
    handler()
    logger.info("[GLUE_ETL_JOB] Script completed")