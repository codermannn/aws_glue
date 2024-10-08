from aws_cdk import (
    Stack,
    aws_ssm as ssm
)
from constructs import Construct

class SSMStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.schema_folder = ssm.StringParameter(
            self, "SchemaFolder",
            parameter_name="/glue-poc/schema-folder",
            string_value="schema"
        )

        self.etl_scripts_folder = ssm.StringParameter(
            self, "EtlScriptsFolder",
            parameter_name="/glue-poc/etl-scripts-folder",
            string_value="etl_scripts"
        )

        # Create SSM Parameters
        self.source_folder = ssm.StringParameter(
            self, "SourceFolder",
            parameter_name="/glue-poc/source-folder",
            string_value="data"
        )

        self.destination_folder = ssm.StringParameter(
            self, "DestinationFolder",
            parameter_name="/glue-poc/destination-folder",
            string_value="archive"
        )

        self.temp_folder = ssm.StringParameter(
            self, "TempFolder",
            parameter_name="/glue-poc/temp-folder",
            string_value="temp"
        )

        self.data_catalog_name = ssm.StringParameter(
            self, "DataCatalogName",
            parameter_name="/glue-poc/data-catalog-name",
            string_value="glue-poc-catalog"
        )

        self.dynamodb_table_name = ssm.StringParameter(
            self, "DynamoDBTableName",
            parameter_name="/glue-poc/dynamodb-table-name",
            string_value="glue-poc-table"
        )
