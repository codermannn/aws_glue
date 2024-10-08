import typing
from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_ssm as ssm,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
    aws_s3_notifications as s3n  # Add this import
)
from constructs import Construct

class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, iam_role_arn: str, s3_bucket_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        self.glue_job_name = ssm.StringParameter.from_string_parameter_name(
            self, "GlueJobName",
            string_parameter_name="/glue-poc/glue-job-name"
        ).string_value

        # Create Lambda function
        self.lambda_function = lambda_.Function(
            self,
            "S3EventProcessor",
            function_name="s3-event-processor",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset("assets/lambda"),
            environment={
                "glue_job_name": self.glue_job_name
            },
        )

        # Create an IAM role object from the provided ARN
        iam_role = iam.Role.from_role_arn(
            self,
            "ImportedIamRole",
            role_arn=iam_role_arn
        )

        self.lambda_function_arn = self.lambda_function.function_arn

        self.source_folder = ssm.StringParameter.from_string_parameter_name(
            self, "SourceFolder",
            string_parameter_name="/glue-poc/source-folder"
        ).string_value

        bucket = s3.Bucket.from_bucket_arn(
            self,
            "ImportedBucket",
            bucket_arn=s3_bucket_arn
        )

        self.lambda_function.add_permission("S3InvokePermission",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=bucket.bucket_arn,
        )

        # Grant SSM read permissions to the Lambda function for all params starting with /glue-poc/
        self.lambda_function.add_to_role_policy(iam.PolicyStatement(
            actions=["ssm:GetParameter"],
            resources=[f"arn:aws:ssm:{self.region}:{self.account}:parameter/glue-poc/*"]
        ))

        # Grant Glue job start permissions
        self.lambda_function.add_to_role_policy(iam.PolicyStatement(
            actions=["glue:StartJobRun"],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:job/*"]
        ))

        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(prefix=self.source_folder)
        )
