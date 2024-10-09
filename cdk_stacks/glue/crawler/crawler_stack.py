from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ssm as ssm
)
from constructs import Construct

class CrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, glue_database: glue.CfnDatabase, s3_bucket: s3.Bucket, glue_role: iam.Role, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        database_name = ssm.StringParameter.from_string_parameter_name(
            self, "DatabaseName",
            string_parameter_name="/glue-poc/data-catalog-name"
        ).string_value

        table_prefix = ssm.StringParameter.from_string_parameter_name(
            self, "TablePrefix",
            string_parameter_name="/glue-poc/table-prefix"
        ).string_value

        schema_folder = ssm.StringParameter.from_string_parameter_name(
            self, "SchemaFolder",
            string_parameter_name="/glue-poc/schema-folder"
        ).string_value

        # Create Glue Crawler
        glue_crawler = glue.CfnCrawler(
            self,
            "GluePocCrawler",
            name="glue-poc-crawler",
            role=glue_role.role_arn,
            database_name=database_name,
            table_prefix=table_prefix,  # Add a prefix to the table name
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=s3_bucket.s3_url_for_object(f"{schema_folder}/")
                    )
                ]
            )
        )
