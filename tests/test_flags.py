import os
import sys
import orjson
import psycopg2
import logging
from time import time
import csv

os.chdir(os.path.dirname(os.path.dirname(__file__)))

from ingest.lcsV2 import (
    IngestClient,
)


logger = logging.getLogger('handler')

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level='DEBUG',
    force=True,
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


## client based methods
## get a client
client = IngestClient()
## load all the data into the client
client.load_keys([[1, './tests/test_file1.json', '2024-01-01']])
## load the data
client.dump(load=True)
#client.dump_locations(load=True)
#client.dump_measurements(load=True)

client.reset()

client.load_keys([[2, './tests/test_file2.json', '2024-01-02']])
## load the data
client.dump(load=True)
