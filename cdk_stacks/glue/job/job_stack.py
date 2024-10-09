from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_ssm as ssm,
    aws_dynamodb as dynamodb,
    aws_iam as iam
)
from constructs import Construct

class JobStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, 
                 glue_database: glue.CfnDatabase, 
                 s3_bucket: s3.Bucket, 
                 dynamo_table: dynamodb.Table, 
                 glue_role: iam.Role, 
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket_name = ssm.StringParameter.from_string_parameter_name(
            self, "BucketName",
            string_parameter_name="/glue-poc/bucket-name"
        ).string_value

        # Create string variables for SSM parameters
        source_folder_path = ssm.StringParameter.from_string_parameter_name(
            self, 'SourceFolder',
            string_parameter_name='/glue-poc/source-folder'
        ).string_value
        source_folder = f"{self.bucket_name}/{source_folder_path}"

        destination_folder_path = ssm.StringParameter.from_string_parameter_name(
            self, 'DestinationFolder',
            string_parameter_name='/glue-poc/destination-folder'
        ).string_value
        destination_folder = f"{self.bucket_name}/{destination_folder_path}"

        failed_folder_path = ssm.StringParameter.from_string_parameter_name(
            self, 'FailedFolder',
            string_parameter_name='/glue-poc/failed-folder'
        ).string_value
        failed_folder = f"{self.bucket_name}/{failed_folder_path}"

        dynamodb_table_name = ssm.StringParameter.from_string_parameter_name(
            self, "DynamoDBTableName",
            string_parameter_name="/glue-poc/dynamodb-table-name"
        ).string_value

        data_catalog_name = ssm.StringParameter.from_string_parameter_name(
            self, "DataCatalogName",
            string_parameter_name="/glue-poc/data-catalog-name"
        ).string_value

        table_prefix = ssm.StringParameter.from_string_parameter_name(
            self, "TablePrefix",
            string_parameter_name="/glue-poc/table-prefix"
        ).string_value

        etl_scripts_folder = ssm.StringParameter.from_string_parameter_name(
            self, "EtlScriptsFolder",
            string_parameter_name="/glue-poc/etl-scripts-folder"
        ).string_value
        
        job_name = ssm.StringParameter.from_string_parameter_name(
            self, "GlueJobName",
            string_parameter_name="/glue-poc/glue-job-name"
        ).string_value

        # Create Glue ETL Job
        glue_job = glue.CfnJob(
            self,
            "MyETLJob",
            name=job_name,
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=s3_bucket.s3_url_for_object(f"{etl_scripts_folder}/script.py"),
            ),
            default_arguments={
                "--job_name": job_name,
                "--source_prefix": source_folder,
                "--destination_prefix": destination_folder,
                "--failed_prefix": failed_folder,
                "--dynamodb_table_name": dynamodb_table_name,
                "--database_catalog_name": data_catalog_name,
                "--table_prefix": table_prefix,
            },
            glue_version="3.0",
            max_capacity=2,
            timeout=5,  # 5 minutes
        )
