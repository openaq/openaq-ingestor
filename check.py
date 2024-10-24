import argparse
import logging
import os
import sys
import orjson
import psycopg2


logger = logging.getLogger(__name__)

#os.chdir('/home/christian/git/caparker/openaq-ingestor/ingest')
#print(os.getcwd())

parser = argparse.ArgumentParser(
    description="""
    Do some basic checking against the database.
    Requires an env file with the basic database variables,
    the same that you would need to deploy.
    """)
parser.add_argument('--id', type=int, required=False,
                    help='The fetchlogs_id value')
parser.add_argument('--file', type=str, required=False,
                    help='A local file to load')
parser.add_argument('--batch', type=str, required=False,
                    help='The batch id value. Loads files based on batch uuid.')
parser.add_argument('--pattern', type=str, required=False,
                    help='A reqex to match keys for loading')
parser.add_argument('--env', type=str, required=False,
                    help='The dot env file to use')
parser.add_argument('--profile', type=str, required=False,
                    help='The AWS profile to use')
parser.add_argument('--n', type=int, required=False, default=30,
                    help="""Either the number of entries to list
                    (sorted by date) or the number of days to go
                    back if using the summary or rejects arguments""")
parser.add_argument('--pipeline', type=int, required=False, default=0,
                    help="""The number of pipeline files to load at a time""")
parser.add_argument('--metadata', type=int, required=False, default=0,
                    help="""The number of metadata files to load at a time""")
parser.add_argument('--realtime', type=int, required=False, default=0,
                    help="""The number of realtime files to load at a time""")
parser.add_argument('--fix', action="store_true",
                    help='Automatically attempt to fix the problem')
parser.add_argument('--load', action="store_true",
                    help='Attempt to load the file manually, outside the queue')
parser.add_argument('--download', action="store_true",
                    help='Attempt to download the file')
parser.add_argument('--dryrun', action="store_true",
                    help='Check to see if its fixable but dont actually save it')
parser.add_argument('--debug', action="store_true",
                    help='Output at DEBUG level')
parser.add_argument('--summary', action="store_true",
                    help='Summarize the fetchlog errors by type')
parser.add_argument('--rejects', action="store_true",
                    help='Show summary of the rejects errors')
parser.add_argument('--errors', action="store_true",
                    help='Show list of errors')
parser.add_argument('--resubmit', action="store_true",
                    help='Mark the fetchlogs file for resubmittal')
parser.add_argument('--keep', action="store_true",
                    help='Do not use TEMP tables for the ingest staging tables')
args = parser.parse_args()

if 'DOTENV' not in os.environ.keys() and args.env is not None:
    os.environ['DOTENV'] = args.env

if 'AWS_PROFILE' not in os.environ.keys() and args.profile is not None:
    os.environ['AWS_PROFILE'] = args.profile

if args.dryrun:
    os.environ['DRYRUN'] = 'True'

if args.debug:
    os.environ['LOG_LEVEL'] = 'DEBUG'

if args.keep:
    os.environ['USE_TEMP_TABLES'] = 'False'

from botocore.exceptions import ClientError
from ingest.handler import cronhandler, logger
from ingest.settings import settings

from ingest.lcs import (
    load_metadata,
    load_metadata_batch,
)

from ingest.lcsV2 import (
    load_measurements,
    load_measurements_batch,
)

from ingest.fetch import (
    load_realtime,
    create_staging_table,
    parse_json,
)

from ingest.utils import (
	load_fetchlogs,
    load_errors_list,
    load_errors_summary,
    load_rejects_summary,
    get_data,
    get_object,
    put_object,
    get_logs_from_ids,
    get_logs_from_pattern,
    mark_success,
    StringIteratorIO,
    deconstruct_path,
)


def check_realtime_key(key: str, fix: bool = False):
    """Check realtime file for common errors"""
    logger.debug(f"\n## Checking realtime for issues: {key}")
    # get text of object
    try:
        txt = get_object(key)
    except Exception as e:
        # these errors are not fixable so return
        logger.error(f"\t*** Error getting file: {e}")
        return;
    # break into lines
    lines = txt.split("\n")
    # check parse for each line
    n = len(lines)
    errors = []
    for jdx, line in enumerate(lines):
        if len(line) > 0:
            try:
                # first just try and load it
                obj = orjson.loads(line)
            except Exception as e:
                errors.append(jdx)
                print(f"*** Loading error on line #{jdx} (of {n}): {e}\n{line}")
            try:
                # then we can try to parse it
                parse_json(obj)
            except Exception as e:
                errors.append(jdx)
                print(f"*** Parsing error on line #{jdx} (of {n}): {e}\n{line}")

    if len(errors) > 0 and fix:
        # remove the bad rows and then replace the file
        nlines = [l for i, l in enumerate(lines) if i not in errors]
        message = f"Fixed: removed {len(errors)} and now have {len(nlines)} lines"
        print(message)
        ntext = "\n".join(nlines)
        put_object(
            data=ntext,
            key=key
        )
        mark_success(key=key, reset=True, message=message)
    elif len(errors) == 0 and fix:
        mark_success(key=key, reset=True)


logger.debug(settings)

if args.file is not None:
    # check if the files exists
    # is it a realtime file or a lcs file?
    # upload the file
    load_realtime([
        (-1, args.file, None)
    ])
    sys.exit()

# If we have passed an id than we check that
if args.id is not None:
    # get the details for that id
    logs = get_logs_from_ids(ids=[args.id])
    # get just the keys
    keys = [log[1] for log in logs]
    # loop through and check each
    for idx, key in enumerate(keys):
        if args.download:
            # we may be using the new source pat
            p = deconstruct_path(key)
            download_path = f'~/Downloads/{p["bucket"]}/{p["key"]}';
            logger.info(f'downloading to {download_path}')
            txt = get_object(**p)
            fpath = os.path.expanduser(download_path)
            os.makedirs(os.path.dirname(fpath), exist_ok=True)
            with open(fpath.replace('.gz', ''), 'w') as f:
                f.write(txt)
        # if we are resubmiting we dont care
        # what type of file it is
        elif args.resubmit:
            mark_success(key, reset=True, message='resubmitting')
        # figure out what type of file it is
        elif 'realtime' in key:
            if args.load:
                load_realtime([
                    (args.id, key, None)
                ])
            else:
                check_realtime_key(key, args.fix)
        elif 'stations' in key:
            load_metadata([
                {"id": args.id, "Key": key, "LastModified": None}
            ])
        else:
            load_measurements([
                (args.id, key, None)
            ])

elif args.batch is not None:
    # load_measurements_batch(args.batch)
    load_metadata_batch(args.batch)

elif args.pattern is not None:
	keys = load_fetchlogs(pattern=args.pattern, limit=25, ascending=True)
    # loop through and check each
	for row in keys:
		id = row[0]
		key = row[1]
		last = row[2]
		logger.debug(f"{key}: {id}")
		if args.load:
			if 'realtime' in key:
				load_realtime([
                    (id, key, last)
							  ])
			elif 'stations' in key:
				load_metadata([
					{"id": id, "Key": key, "LastModified": last}
				])
			else:
				load_measurements([
					(id, key, last)
				])



# Otherwise if we set the summary flag return a daily summary of errors
elif args.summary:
    rows = load_errors_summary(args.n)
    print("Type\t\tDay\t\tCount\tMin\t\tMax\t\tID")
    for row in rows:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}")
elif args.rejects:
    rows = load_rejects_summary(args.n)
    print("Provider\tSource\tLog\tNode\tRecords")
    for row in rows:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")
        if row[3] is None:
            # check for a station file
            station_keys = get_logs_from_pattern(f"{row[0]}/{row[1]}")
            for station in station_keys:
                print(f"station key: {station[1]}; log: {station[0]}")
# otherwise fetch a list of errors
elif args.errors:
    errors = load_errors_list(args.n)
    for error in errors:
        print(f"------------------\nDATE: {error[2]}\nKEY: {error[1]}\nID:{error[0]}\nERROR:{error[5]}")
        if 'realtime' in error[1]:
            check_realtime_key(error[1], args.fix)
else:
    cronhandler({
        "source": "check",
        "pipeline_limit": args.pipeline,
        "metadata_limit": args.metadata,
        "realtime_limit": args.realtime,
    }, {})
