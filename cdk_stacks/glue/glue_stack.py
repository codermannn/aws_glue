from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam
)
from constructs import Construct

from cdk_stacks.glue.crawler.crawler_stack import CrawlerStack
from cdk_stacks.glue.database.database_stack import DatabaseStack
from cdk_stacks.glue.job.job_stack import JobStack

class GlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, bucket: s3.Bucket, dynamo_table: dynamodb.Table, glue_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an IAM role object from the provided ARN
        glue_role = iam.Role.from_role_arn(
            self,
            "ImportedGlueRole",
            role_arn=glue_role_arn
        )

        database_stack = DatabaseStack(self, "DatabaseStack")
        crawler_stack = CrawlerStack(self, "CrawlerStack", database_stack.glue_database, bucket, glue_role)
        job_stack = JobStack(self, "JobStack", database_stack.glue_database, bucket, dynamo_table, glue_role)