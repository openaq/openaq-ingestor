import os
import sys
import orjson
import psycopg2
import logging
from time import time
import csv


from ingest.lcsV2 import (
    IngestClient,
    load_measurements,
    load_measurements_db,
)

from ingest.utils import (
    select_object,
    get_file,
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




rows = [
    [3, '/home/christian/Downloads/habitatmap-1714036497-h84j.csv', '2024-01-01 00:00:00'],
    [4, '/home/christian/Downloads/airgradient-1714003639-h32tu.csv', '2024-01-05'],
    [5, '/home/christian/Downloads/senstate-1714007461-ivz5g.csv', '2021-02-01'],
    [1, '/home/christian/Downloads/1610335354.csv', '2022-01-01']
    [6, '/home/christian/Downloads/1722384430-2vfvm.json', '2024-07-30'],
    [7, '/home/christian/Downloads/1722384430-2vfvm_meas.json', '2024-07-30']
    ]


# local files
#load_measurements_db(pattern = '^/home/christian/.*\\.(csv|json)')
# remote files, make sure it can at least read it
#load_measurements_db()

## client based methods
client = IngestClient()
client.load_keys(rows)
client.dump()

# #client.load(data)
# client.load_metadata(data['meta'])
# client.load_locations(data['locations'])
# client.load_measurements(data['measures'])

# #client.dump()


# print(time() - start_time)
# print(f"measurements: {len(client.measurements)}")
# print(f"locations: {len(client.nodes)}")
