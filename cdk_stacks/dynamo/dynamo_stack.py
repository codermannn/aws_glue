from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_ssm as ssm
)
from constructs import Construct

class DynamoStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, iam_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an IAM role object from the provided ARN
        iam_role = iam.Role.from_role_arn(
            self,
            "ImportedIamRole",
            role_arn=iam_role_arn
        )

        table_name=ssm.StringParameter.from_string_parameter_name(
            self, "DynamoDBTableName",
            string_parameter_name="/glue-poc/dynamodb-table-name"
        ).string_value

        # Create DynamoDB table
        self.table = dynamodb.Table(
            self,
            "GluePocTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.NUMBER
            ),
            removal_policy=RemovalPolicy.DESTROY  # Use with caution in production
        )

        # Grant access to the IAM role
        self.table.grant_read_write_data(iam_role)