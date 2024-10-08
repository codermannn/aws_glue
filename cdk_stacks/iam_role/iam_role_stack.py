from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_kms as kms
)
from constructs import Construct

class IamRoleStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.iam_role = iam.Role(
            self,
            "GluePocRole",
            role_name="glue-poc-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        # Assign the IAM role ARN to a variable
        self.iam_role_arn = self.iam_role.role_arn
