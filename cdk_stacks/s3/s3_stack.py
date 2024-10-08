from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_notifications as s3n,  # Add this import
    aws_lambda as lambda_,
    aws_kms as kms,
    aws_iam as iam,
    aws_ssm as ssm,
    aws_s3_deployment as s3deploy,
    RemovalPolicy
)
from constructs import Construct

class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, kms_key: kms.Key, iam_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket_name = ssm.StringParameter.from_string_parameter_name(
            self, "BucketName",
            string_parameter_name="/glue-poc/bucket-name"
        ).string_value

        # Create string variables for SSM parameters
        self.schema_folder = ssm.StringParameter.from_string_parameter_name(
            self, "SchemaFolder",
            string_parameter_name="/glue-poc/schema-folder"
        ).string_value

        self.source_folder = ssm.StringParameter.from_string_parameter_name(
            self, "SourceFolder",
            string_parameter_name="/glue-poc/source-folder"
        ).string_value

        self.destination_folder = ssm.StringParameter.from_string_parameter_name(
            self, "DestinationFolder",
            string_parameter_name="/glue-poc/destination-folder"
        ).string_value

        self.temp_folder = ssm.StringParameter.from_string_parameter_name(
            self, "TempFolder",
            string_parameter_name="/glue-poc/temp-folder"
        ).string_value

        self.etl_scripts_folder = ssm.StringParameter.from_string_parameter_name(
            self, "EtlScriptsFolder",
            string_parameter_name="/glue-poc/etl-scripts-folder"
        ).string_value

        self.failed_folder = ssm.StringParameter.from_string_parameter_name(
            self, "FailedFolder",
            string_parameter_name="/glue-poc/failed-folder"
        ).string_value

        # Create an S3 bucket with server-side encryption using the provided KMS key
        self.bucket = s3.Bucket(
            self, 
            "GluePocBucket",
            bucket_name=self.bucket_name,
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

        # Create an IAM role object from the provided ARN
        iam_role = iam.Role.from_role_arn(
            self,
            "ImportedIamRole",
            role_arn=iam_role_arn
        )

        self.bucket.grant_read(iam_role)

        # Create a schema folder inside the bucket
        self.schema_folder_deployment = s3deploy.BucketDeployment(
            self,
            "SchemaFolderDeployment",
            sources=[s3deploy.Source.asset("assets/schema")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.schema_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_read(iam_role, f"{self.schema_folder}")

        self.etl_scripts_folder_deployment = s3deploy.BucketDeployment(
            self,
            "EtlScriptsDeployment",
            sources=[s3deploy.Source.asset("assets/etl_scripts")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.etl_scripts_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_read(iam_role, f"{self.etl_scripts_folder}")

        self.source_folder_deployment  = s3deploy.BucketDeployment(
            self,
            "SourceFolderDeployment",
            sources=[s3deploy.Source.asset("assets/empty")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.source_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_read(iam_role, f"{self.source_folder}")

        self.destination_folder_deployment = s3deploy.BucketDeployment(
            self,
            "DestinationFolderDeployment",
            sources=[s3deploy.Source.asset("assets/empty")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.destination_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_write(iam_role, f"{self.destination_folder}")

        self.failed_folder_deployment = s3deploy.BucketDeployment(
            self,
            "FailedFolderDeployment",
            sources=[s3deploy.Source.asset("assets/empty")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.failed_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_write(iam_role, f"{self.failed_folder}")

        # Create a temp folder inside the bucket
        temp_folder = s3deploy.BucketDeployment(
            self,
            "TempFolderDeployment",
            sources=[s3deploy.Source.asset("assets/empty")],
            destination_bucket=self.bucket,
            destination_key_prefix=self.temp_folder,
            prune=False,
            retain_on_delete=False,
        )
        self.bucket.grant_read_write(iam_role, f"{self.temp_folder}")

        self.bucket_arn = self.bucket.bucket_arn