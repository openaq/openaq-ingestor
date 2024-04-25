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



rows = [
    [3, '/home/christian/Downloads/habitatmap-1714036497-h84j.csv', '2024-01-01 00:00:00'],
    [4, '/home/christian/Downloads/airgradient-1714003639-h32tu.csv', '2024-01-05'],
    [5, '/home/christian/Downloads/senstate-1714007461-ivz5g.csv', '2021-02-01'],
    [1, '/home/christian/Downloads/1610335354.csv', '2022-01-01']
    ]




#print(rows)
#load_measurements(rows)
#load_measurements_db()

#content = select_object(key)
#content = get_file(file).read()

#print(type(content))
#print(len(content))

# select object returns a string so we need to convert it
#data = orjson.loads(content)

#print(type(data))
#print(len(data))


# # load all the data
# start_time = time()

client = IngestClient()

client.load_keys(rows)
client.dump()

# #client.load(data)
# # client.load_metadata(data['meta'])
# #client.load_locations(data['locations'])
# client.load_measurements(data['measures'])

# #client.dump()


# print(time() - start_time)
# print(f"measurements: {len(client.measurements)}")
# print(f"locations: {len(client.nodes)}")
