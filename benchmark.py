import logging
import os
import sys
import argparse
from time import time
import re

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="""
Test benchmarks for ingestion
    """)

parser.add_argument(
	'--name',
	type=str,
	required=False,
	default="4xlarge",
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

if 'DOTENV' not in os.environ.keys() and args.env is not None:
    os.environ['DOTENV'] = args.env

if args.debug:
    os.environ['LOG_LEVEL'] = 'DEBUG'

from ingest.settings import settings
from fake import config, get_locations, as_realtime
from ingest.fetch import load_realtime

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)

f = open(f"benchmark_ingest_output_{args.name}.csv", "w")
f.writelines("name,key,locations,inserted_nodes,updated_nodes,total_meas,inserted_meas,ingest_time,process_time,log_time,copy_time,load_process_time\n")
n = 10
locations = [50, 250, 1000]
keys = []
ii = 1

## make a set of files
for r in locations:
	for i in range(n):
		config(source=f"benchmark-test-{r}-{i+1}", gz=True)
		l = get_locations(n=r)
		key = as_realtime(l["locations"], l["latitude"], l["longitude"])
		keys.append({ "key": key, "locations": len(l["locations"]) })
		ii=+1


## ingest each of the
for i, k in enumerate(keys):
	key = k["key"]
	locations = k["locations"]
	logger.info(f"Ingesting {i+1} of {len(keys)}: {key} with {locations} locations")

	start_time = time()
	copy_time, load_process_time, log_time, notice = load_realtime([
		(-1, key, None)
	  ])
	m = re.findall('([a-z-]+): (.+?),', notice)

	process_time = round(float(m[17][1]))
	total_meas = int(m[0][1])
	inserted_meas = int(m[9][1])
	updated_nodes = int(m[8][1])
	inserted_nodes = int(m[11][1])
	ingest_time = round((time() - start_time)*1000)
	f.writelines(f"'{args.name}','{key}',{locations},{inserted_nodes},{updated_nodes},{total_meas},{inserted_meas},{ingest_time},{process_time},{log_time},{copy_time},{load_process_time}\n")

	logger.info(
		"loaded realtime records, timer: %0.4f, process: %0.4f",
		ingest_time, process_time
		)


f.close()
