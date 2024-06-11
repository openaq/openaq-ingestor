from pathlib import Path
from typing import Dict

from aws_cdk import (
	Environment,
    aws_lambda,
    aws_s3,
	aws_ec2,
    Stack,
    Duration,
    aws_events,
    aws_events_targets,
    aws_sns,
    aws_sns_subscriptions,
)

from constructs import Construct
from utils import (
    stringify_settings,
    create_dependencies_layer,
)


class LambdaIngestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env: Environment,
        env_name: str,
        lambda_env: Dict,
        fetch_bucket: str,
        ingest_lambda_timeout: int,
        ingest_lambda_memory_size: int,
        ingest_rate_minutes: int = 15,
        topic_arn: str = None,
        vpc_id: str = None,
        **kwargs,
    ) -> None:
        """Lambda plus cronjob to ingest metadata,
        realtime and pipeline data"""
        super().__init__(scope, id, env=env,*kwargs)

        if vpc_id is not None:
            vpc_id = aws_ec2.Vpc.from_lookup(self, f"{id}-vpc", vpc_id=vpc_id)

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingestlambda",
            code=aws_lambda.Code.from_asset(
                path='..',
                exclude=[
                    'venv',
                    '__pycache__',
                    '.pytest_cache',
                    '.hypothesis',
                    'tests',
                    '.build',
                    'cdk',
                    'testdata2',
                    '*.pyc',
                    '*.md',
                    '.env*',
                    '.gitignore',
                ],
            ),
            handler="ingest.handler.handler",
			vpc=vpc_id,
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            allow_public_subnet=True,
            memory_size=ingest_lambda_memory_size,
            environment=stringify_settings(lambda_env),
            timeout=Duration.seconds(ingest_lambda_timeout),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'ingest',
                    Path('../requirements.txt')
                ),
            ],
        )

        # Provide access to the ingest file bucket
        openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
            self, "{env_name}-FETCH-BUCKET", fetch_bucket
        )
        openaq_fetch_bucket.grant_read(ingest_function)

        # Set how often the ingester will run
        # If 0 the ingester will not run automatically
        if ingest_rate_minutes > 0:
            aws_events.Rule(
                self,
                f"{id}-ingest-event-rule",
                schedule=aws_events.Schedule.cron(
                    minute=f"0/{ingest_rate_minutes}"
                ),
                targets=[
                    aws_events_targets.LambdaFunction(ingest_function),
                ],
            )

        # Subscribe to the file updated SNS event
        # without this the ingester will not log new files
        if topic_arn is not None:
            topic = aws_sns.Topic.from_topic_arn(
                self,
                f"{id}-ingest-topic",
                topic_arn=topic_arn
            )
            topic.add_subscription(
                aws_sns_subscriptions.LambdaSubscription(
                    ingest_function
                )
            )
