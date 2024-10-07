import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_stacks.kms.kms_stack import KmsStack
from cdk_stacks.s3.s3_stack import S3Stack

# resource in glue_cdk/cdk_stacks/s3/s3_stack.py
def test_s3_bucket_created():
    app = core.App()
    
    kms_stack = KmsStack(app, "KmsStack")
    stack = S3Stack(app, "S3Stack", kms_key=kms_stack.kms_key)
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName": "glue-poc-bucket",
        "VersioningConfiguration": {
            "Status": "Enabled"
        },
        "BucketEncryption": {
            "ServerSideEncryptionConfiguration": [{
                "ServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms"
                }
            }]
        }
    })

    template.resource_count_is("AWS::S3::Bucket", 1)

