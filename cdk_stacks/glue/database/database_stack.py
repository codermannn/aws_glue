from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_ssm as ssm
)
from constructs import Construct

class DatabaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Glue Database
        self.glue_database = glue.CfnDatabase(
            self,
            "GluePocDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=ssm.StringParameter.from_string_parameter_name(
                    self, "DatabaseName",
                    string_parameter_name="/glue-poc/data-catalog-name"
                ).string_value,
                description="Database for Glue POC project",
            ),
        )
