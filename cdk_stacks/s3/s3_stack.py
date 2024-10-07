from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_kms as kms,
    aws_iam as iam,
    RemovalPolicy
)
from constructs import Construct

class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, kms_key: kms.Key, iam_role: iam.Role, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 bucket with server-side encryption using the provided KMS key
        self.bucket = s3.Bucket(
            self, 
            "GluePocBucket",
            bucket_name="glue-poc-bucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            bucket_key_enabled=True,
            )

        # Grant full access to the IAM role
        self.bucket.grant_read_write(iam_role)
        self.bucket.grant_delete(iam_role)