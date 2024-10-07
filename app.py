#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk_stacks.kms.kms_stack import KmsStack
from cdk_stacks.s3.s3_stack import S3Stack
from cdk_stacks.iam_role.iam_role_stack import IamRoleStack

app = cdk.App()

kms_stack = KmsStack(app, "KmsStack")
iam_role_stack = IamRoleStack(app, "IamRoleStack")
s3_stack = S3Stack(app, "S3Stack", kms_key=kms_stack.kms_key, iam_role=iam_role_stack.glue_iam_role)



app.synth()
