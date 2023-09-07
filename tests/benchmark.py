import logging
import os
import sys
import argparse

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="""
Test benchmarks for ingestion
    """)

parser.add_argument(
	'--name',
	type=str,
	required=False,
	default="test",
	help='Name to use for the test'
	)
parser.add_argument(
	'--env',
	type=str,
	default='.env',
	required=False,
	help='The dot env file to use'
	)
parser.add_argument(
	'--debug',
	action="store_true",
	help='Output at DEBUG level'
	)
args = parser.parse_args()

from ingest.settings import settings

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)


print(args)
