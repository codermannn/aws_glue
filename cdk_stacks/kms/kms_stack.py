from aws_cdk import (
    Stack,
    aws_kms as kms
)
from constructs import Construct

class KmsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a symmetric KMS key
        self.kms_key = kms.Key(self, "GluePocKmsKey",
            alias="alias/glue-poc-key",
            description="Symmetric KMS key for Glue POC",
            enable_key_rotation=True,
            key_spec=kms.KeySpec.SYMMETRIC_DEFAULT
        )
