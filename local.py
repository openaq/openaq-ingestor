#!/usr/bin/env python

import argparse
import os
import sys
import orjson
import psycopg2
import logging
from time import time
import csv


from ingest.lcsV2 import (
    IngestClient,
)

from ingest.utils import (
    load_fetchlogs,
    list_objects,
    upsert_fetchlogs,
)


logger = logging.getLogger('local')

def load_localfile(path: str):
    print(filepath)

def main():
    """load files locally"""

    parser = argparse.ArgumentParser(
        description="Load files from S3 or local filesystem into database"
    )

    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, help='file limit to ingest', default=300)
    parser.add_argument('--prefix', type=str, help='file limit to ingest', default='')

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--id', type=int, help='id of the file to load')
    mode_group.add_argument('--key', type=str, help='key of the file to load')
    mode_group.add_argument('--file', type=str, help='path to the local file to load')
    mode_group.add_argument('--batch', type=str, help='batch uuid of files to load')
    mode_group.add_argument('--bucket', type=str, help='S3 location to lookup')


    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
        level=('DEBUG' if args.debug else 'INFO'),
        force=True,
    )

    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    rows = []

    if args.id is not None:
        # load via id
        rows = load_fetchlogs(id=args.id, limit=1, force=True)
    elif args.key is not None:
        # load via key
        rows = load_fetchlogs(pattern=args.key, limit=1, force=True)
    elif args.file is not None:
        # check for local file
        rows = upsert_fetchlogs([args.file])
    elif args.batch is not None:
        # load via batch
        rows = load_fetchlogs(batch=args.batch, limit=args.limit, force=True)
    elif args.bucket is not None:
        from ingest.settings import settings
        settings.FETCH_BUCKET = args.bucket
        # load directly from bucket
        # add it to fetchlogs and then ingest
        # "2026-03-28/"
        keys = list_objects(args.bucket, prefix=args.prefix, limit=args.limit)
        rows = upsert_fetchlogs(keys)


    logger.info(f"loading {len(rows)} files")
    if len(rows)>0:
        start_time = time()
        # get a client object to hold all the data
        client = IngestClient()
        # load all the keys
        client.load_keys(rows)
        # and finally we can dump it all into the db
        client.dump()
        # write to the log
        logger.info("load_measurements:get: %s keys; %s measurements; %s locations; %0.4f seconds",
                    len(client.keys), len(client.measurements), len(client.nodes), time() - start_time)




if __name__ == '__main__':
    main()
