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

                        # Create string variables for SSM parameters
        source_folder = ssm.StringParameter.from_string_parameter_name(
            self, "SourceFolder",
            string_parameter_name="/glue-poc/source-folder"
        ).string_value

        destination_folder = ssm.StringParameter.from_string_parameter_name(
            self, "DestinationFolder",
            string_parameter_name="/glue-poc/destination-folder"
        ).string_value

        temp_folder = ssm.StringParameter.from_string_parameter_name(
            self, "TempFolder",
            string_parameter_name="/glue-poc/temp-folder"
        ).string_value

        dynamodb_table_name = ssm.StringParameter.from_string_parameter_name(
            self, "DynamoDBTableName",
            string_parameter_name="/glue-poc/dynamodb-table-name"
        ).string_value

        etl_scripts_folder = ssm.StringParameter.from_string_parameter_name(
            self, "EtlScriptsFolder",
            string_parameter_name="/glue-poc/etl-scripts-folder"
        ).string_value

        # Create Glue ETL Job
        glue_job = glue.CfnJob(
            self,
            "MyETLJob",
            name="my_etl_job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=s3_bucket.s3_url_for_object(f"{etl_scripts_folder}/script.py"),
            ),
            default_arguments={
                "--source_folder": source_folder,
                "--destination_folder": destination_folder,
                "--temp_folder": temp_folder,
                "--dynamodb_table_name": dynamodb_table_name
            },
            glue_version="3.0",
            max_capacity=2,
            timeout=5,  # 5 minutes
        )
