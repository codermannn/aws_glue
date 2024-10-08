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


ssm_stack = SSMStack(app, "SSMStack")
iam_role_stack = IamRoleStack(app, "IamRoleStack")
kms_stack = KmsStack(app, "KmsStack", iam_role_stack.iam_role)
notification_lambda_stack = LambdaStack(app, "NotificationLambdaStack", iam_role_stack.iam_role)
s3_stack = S3Stack(app, "S3Stack", kms_key=kms_stack.kms_key, iam_role=iam_role_stack.iam_role, lambda_function_arn=notification_lambda_stack.lambda_function_arn)
sns_stack = SnsStack(app, "SnsStack")
dynamo_stack = DynamoStack(app, "DynamoStack", iam_role_stack.iam_role)
glue_stack = GlueStack(app, "GlueStack", s3_stack.bucket, dynamo_stack.table, iam_role_stack.iam_role)

# s3_stack.bucket.add_event_notification(
#     s3.EventType.OBJECT_CREATED,
#     s3.NotificationKeyFilter(prefix="my-folder/"),
#     cdk.aws_s3_notifications.LambdaDestination(notification_lambda_stack.lambda_function)
# )

app.synth()
