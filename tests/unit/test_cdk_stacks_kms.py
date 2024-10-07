import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_stacks.kms.kms_stack import KmsStack

# resource in glue_cdk/glue_cdk_stack.py
def test_kms_key_created():
    app = core.App()
    stack = KmsStack(app, "KmsStack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::KMS::Key", {
        "Description": "Symmetric KMS key for Glue POC",
        "EnableKeyRotation": True,
        "KeySpec": "SYMMETRIC_DEFAULT"
    })

    template.has_resource_properties("AWS::KMS::Alias", {
        "AliasName": "alias/glue-poc-key"
    })

    template.resource_count_is("AWS::KMS::Key", 1)
    template.resource_count_is("AWS::KMS::Alias", 1)