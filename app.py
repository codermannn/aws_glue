#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk_stacks import s3
from cdk_stacks.dynamo.dynamo_stack import DynamoStack
from cdk_stacks.glue.glue_stack import GlueStack
from cdk_stacks.iam_role.iam_role_stack import IamRoleStack
from cdk_stacks.kms.kms_stack import KmsStack
from cdk_stacks.notification_lambda.lambda_stack import LambdaStack
from cdk_stacks.s3.s3_stack import S3Stack
from cdk_stacks.sns.sns_stack import SnsStack
from cdk_stacks.ssm.ssm_stack import SSMStack

app = cdk.App()


ssm_stack = SSMStack(scope=app, construct_id="SSMStack")
iam_role_stack = IamRoleStack(scope=app, construct_id="IamRoleStack")
kms_stack = KmsStack(scope=app, construct_id="KmsStack", iam_role_arn=iam_role_stack.iam_role_arn)
s3_stack = S3Stack(scope=app, construct_id="S3Stack", kms_key=kms_stack.kms_key, iam_role_arn=iam_role_stack.iam_role_arn)
notification_lambda_stack = LambdaStack(scope=app, construct_id="NotificationLambdaStack", iam_role_arn=iam_role_stack.iam_role_arn, s3_bucket_arn=s3_stack.bucket_arn)
sns_stack = SnsStack(scope=app, construct_id="SnsStack")
dynamo_stack = DynamoStack(scope=app, construct_id="DynamoStack", iam_role_arn=iam_role_stack.iam_role_arn)
glue_stack = GlueStack(
    scope=app,
    construct_id="GlueStack",
    bucket=s3_stack.bucket,
    dynamo_table=dynamo_stack.table,
    glue_role_arn=iam_role_stack.iam_role_arn
)

app.synth()
