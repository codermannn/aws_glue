import typing
from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_ssm as ssm,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
)
from constructs import Construct

class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, iam_role: iam.Role, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Lambda function
        self.lambda_function = lambda_.Function(
            self,
            "S3EventProcessor",
            function_name="s3-event-processor",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=lambda_.Code.from_asset("assets/lambda")
        )

        self.lambda_function_arn = self.lambda_function.function_arn

        # Grant the IAM role permissions to invoke the Lambda function
        self.lambda_function.grant_invoke(iam_role)
