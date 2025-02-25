import aws_cdk
from aws_cdk import (
    Environment,
    Tags,
)
import os

from lambda_ingest_stack import LambdaIngestStack

from config import settings

# this is the only way that I can see to allow us to have
# one settings file and import it from there. I would recommend
# a better package structure in the future.
import os
import sys
p = os.path.abspath('../ingest')
sys.path.insert(1, p)
from settings import settings as lambda_env

app = aws_cdk.App()

env = Environment(
	account=os.environ['CDK_DEFAULT_ACCOUNT'],
	region=os.environ['CDK_DEFAULT_REGION']
	)

ingest = LambdaIngestStack(
    app,
    f"openaq-ingest-{settings.ENV}",
    env_name=settings.ENV,
    lambda_env=lambda_env,
    fetch_bucket=settings.FETCH_BUCKET,
	vpc_id=settings.VPC_ID,
    lambda_timeout=settings.LAMBDA_TIMEOUT,
    lambda_memory_size=settings.LAMBDA_MEMORY_SIZE,
    rate_minutes=settings.RATE_MINUTES,
    topic_arn=settings.TOPIC_ARN,
	env=env,
)

Tags.of(ingest).add("project", settings.PROJECT)
Tags.of(ingest).add("product", "ingest")
Tags.of(ingest).add("env", settings.ENV)

app.synth()
