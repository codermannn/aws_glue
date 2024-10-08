from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions
)
from constructs import Construct

class SnsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create SNS topic
        topic = sns.Topic(
            self,
            "GluePocTopic",
            topic_name="glue-poc-topic"
        )

        # Add email subscription
        topic.add_subscription(subscriptions.EmailSubscription("rahul@beigetech.com.au"))
