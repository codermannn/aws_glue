from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_kms as kms
)
from constructs import Construct

class KmsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, iam_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a symmetric KMS key
        self.kms_key = kms.Key(self, "GluePocKmsKey",
            alias="alias/glue-poc-key",
            description="Symmetric KMS key for Glue POC",
            enable_key_rotation=True,
            key_spec=kms.KeySpec.SYMMETRIC_DEFAULT
        )
        # Get the account ID
        account_id = Stack.of(self).account

        # Create an IAM role object from the provided ARN
        iam_role = iam.Role.from_role_arn(
            self,
            "ImportedIamRole",
            role_arn=iam_role_arn,
            mutable=False
        )

        # Grant the IAM role access to the KMS key
        self.kms_key.grant_decrypt(iam_role)
        self.kms_key.grant_encrypt(iam_role)
