import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_S3_PATH', 'dynamodb_table_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from S3
source_path = args['SOURCE_S3_PATH']
data_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert DynamicFrame to DataFrame for transformation
df = data_frame.toDF()

# Fill in missing values for the 'age' column with a default value (0)
df_filled = df.fillna({'age': 0})

# Convert DataFrame back to DynamicFrame
dynamic_frame_filled = DynamicFrame.fromDF(df_filled, glueContext, "dynamic_frame_filled")

# Write transformed data to DynamoDB
dynamodb_table_name = args['DYNAMODB_TABLE']
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_filled,
    connection_type="dynamodb",
    connection_options={"tableName": dynamodb_table_name, "awsRegion": "us-east-1"}  # Change the region if necessary
)

# Commit the job
job.commit()
