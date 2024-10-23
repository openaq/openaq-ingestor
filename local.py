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


# local files
#load_measurements_db(pattern = '^/home/christian/.*\\.(csv|json)')
# remote files, make sure it can at least read it
#load_measurements_db()

## client based methods
## get a client
client = IngestClient()
## load all the data into the client
client.load_keys([
    [8, '~/Downloads/cac-pipeline/measures/cac/2024-10-18/test_data.json.gz', '2024-10-09']
])

## dump just the locations
client.dump()

# rollups and cached tables
client.process_hourly_data()
client.process_daily_data()
client.process_annual_data()
client.refresh_cached_tables()

#client.dump_locations(load=False)
#client.dump_measurements(load=False)
## dump just the measurements
# client.dump_measurements
## Dump both
#client.dump()

# #client.load(data)
# client.load_metadata(data['meta'])
# client.load_locations(data['locations'])
# client.load_measurements(data['measures'])

# #client.dump()


# print(time() - start_time)
# print(f"measurements: {len(client.measurements)}")
# print(f"locations: {len(client.nodes)}")
