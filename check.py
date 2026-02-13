#!/usr/bin/env python
"""
Load files from S3 or local filesystem into database.

Supports multiple operational modes:
- Default: Full load with ETL processing
- --download: Download file locally without DB operations
- --preview: Show file contents without DB changes
- --stage-only: Load to staging tables, skip ETL
- --test: Full workflow with verification and rollback
"""

import argparse
import logging
import os
import sys
from pathlib import Path
import psycopg2

from ingest.lcsV2 import IngestClient
from ingest.resources import Resources
from ingest.utils import deconstruct_path, get_object


def download_from_location(path: dict, output_path=None):
    """Download file from S3 to local filesystem."""
    if path.get('location') == 's3':
        bucket = path.get('bucket')
        key = path.get('key')
        content = get_object(key=key, bucket=bucket)
        if output_path:
            download_path = Path(output_path) / key
        else:
            download_path = Path.home() / 'Downloads/openaq-ingestor' / key

        # Make sure the directories exist
        download_path.parent.mkdir(parents=True, exist_ok=True)
        # Write file (remove .gz from name since get_object decompresses)
        output_file = str(download_path).replace('.gz', '')
        with open(output_file, 'w') as f:
            f.write(content)
        ## write out some details
        print(f"✓ Downloaded to: {output_file}")
        print(f"  Size: {len(content)} characters")
        path = deconstruct_path(output_file)
    elif path.get('location') == 'local':
        print(f"✗ Key appears to be a local file: {path}")
    elif path.get('location') is None:
        print(f"x Could not determine the location of that key")
        sys.exit(1)
    else:
        print(f"x Key appears to be in a location that does not support download - {path.get('location')}")

    return path

def print_header(header: str):
        print("\n" + "="*60)
        print(f"===  {header}")
        print("="*60)

def print_staging_summary(connection, id: int = -1):
    with connection.cursor() as cursor:
        # Staging counts
        print_header("Staging summary")
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes")
        staging_nodes = cursor.fetchone()[0]
        print(f"   • Staging nodes: {staging_nodes}")

        cursor.execute("SELECT COUNT(*) FROM staging_sensorsystems")
        staging_systems = cursor.fetchone()[0]
        print(f"   • Staging systems: {staging_systems}")

        cursor.execute("SELECT COUNT(*) FROM staging_sensors")
        staging_sensors = cursor.fetchone()[0]
        print(f"   • Staging sensors: {staging_sensors}")

        cursor.execute("SELECT COUNT(*) FROM staging_measurements")
        staging_measurements = cursor.fetchone()[0]
        print(f"   • Staging measurements: {staging_measurements}")

        # Check matched nodes
        cursor.execute("""
            SELECT COUNT(*) FROM staging_sensornodes
            WHERE sensor_nodes_id IS NOT NULL
        """)
        matched_nodes = cursor.fetchone()[0]
        print(f"   • Matched existing nodes: {matched_nodes}/{staging_nodes}")

        # Check measurements date range
        cursor.execute("""
            SELECT MIN(datetime), MAX(datetime)
            FROM staging_measurements
        """)
        date_range = cursor.fetchone()
        if date_range[0]:
            print(f"   • Staging measurement date range: {date_range[0]} to {date_range[1]}")

        # Check for rejects
        cursor.execute("""
            SELECT COUNT(*) FROM rejects
            WHERE fetchlogs_id = %s
        """, (id,))
        rejects = cursor.fetchone()[0]
        if rejects > 0:
            print(f"   ⚠ Rejected records: {rejects}")


def print_current_data_summary(connection):
    with connection.cursor() as cursor:
        # Current counts
        print_header("Current data summary")
        cursor.execute("SELECT COUNT(*) FROM sensor_nodes")
        nodes = cursor.fetchone()[0]
        print(f"   • Sensor nodes: {nodes}")

        cursor.execute("SELECT COUNT(*) FROM sensor_systems")
        systems = cursor.fetchone()[0]
        print(f"   • Sensor systems: {systems}")

        cursor.execute("SELECT COUNT(*) FROM sensors")
        sensors = cursor.fetchone()[0]
        print(f"   • Sensors: {sensors}")

        cursor.execute("SELECT COUNT(*) FROM measurements")
        measurements = cursor.fetchone()[0]
        print(f"   • Measurements: {measurements}")

        # Check measurements date range
        cursor.execute("""
            SELECT MIN(datetime), MAX(datetime)
            FROM measurements
        """)
        date_range = cursor.fetchone()
        if date_range[0]:
            print(f"   • Measurement date range: {date_range[0]} to {date_range[1]}")



def print_client_summary(client: IngestClient):
    print_header("Client summary")

    print(f"\nNodes (locations): {len(client.nodes)}")
    if client.nodes:
        print("\nSample Nodes:")
        for node in client.nodes[:5]:
            site_name = node.get('site_name', 'N/A')
            ingest_id = node.get('ingest_id', 'N/A')
            print(f"  • {ingest_id}: {site_name}")
        if len(client.nodes) > 5:
            print(f"  ... and {len(client.nodes) - 5} more")

    print(f"\nSystems: {len(client.systems)}")
    print(f"Sensors: {len(client.sensors)}")

    print(f"\nMeasurements: {len(client.measurements)}")
    if client.measurements:
        print("\nSample Measurements:")
        for i, meas in enumerate(client.measurements[:10]):
            # Measurements are arrays: [ingest_id, source_name, source_id, measurand, value, datetime, lon, lat]
            ingest_id = meas[0] if len(meas) > 0 else 'N/A'
            measurand = meas[3] if len(meas) > 3 else 'N/A'
            value = meas[4] if len(meas) > 4 else 'N/A'
            dt = meas[5] if len(meas) > 5 else 'N/A'
            print(f"  • {ingest_id}: {measurand}={value} at {dt}")
        if len(client.measurements) > 10:
            print(f"  ... and {len(client.measurements) - 10} more")

    print_header(f"Flags: {len(client.flags)} (more details to come)")



def main():
    """Main entry point."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Load files from S3 or local filesystem into database"
    )
    parser.add_argument('key', type=str, help='S3 key or local file path')

    # Mutually exclusive modes
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--stage-only', action='store_true',
                           help='Load to staging tables only, skip ETL')
    mode_group.add_argument('--preview', action='store_true',
                           help='Show file contents without loading to DB')
    mode_group.add_argument('--dryrun', action='store_true',
                           help='Load with verification and rollback')

    # Options
    parser.add_argument('--download', action='store_true',
                           help='Download file locally without loading to DB')
    parser.add_argument('--env', type=str, help='Path to .env file')
    parser.add_argument('--profile', type=str, help='AWS profile name')
    parser.add_argument('--output', type=str, help='Download output path')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--keep', action='store_true',
                       help='Do not use TEMP tables')
    parser.add_argument('--fetchlogs-id', type=int, default=-1,
                       help='Fetchlogs ID to use (default: -1)')

    args = parser.parse_args()

    # Set environment variables before importing ingest modules
    if args.env:
        os.environ['DOTENV'] = args.env
    if args.profile:
        os.environ['AWS_PROFILE'] = args.profile
    if args.debug:
        os.environ['LOG_LEVEL'] = 'DEBUG'
    if args.keep:
        os.environ['USE_TEMP_TABLES'] = 'False'

    # Set up logging
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
        force=True,
    )

    # Suppress noisy loggers
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Import after environment variables are set
    from ingest.settings import settings

    ## initialize our resource for starters
    key = args.key
    id = -1
    dryrun = not args.dryrun
    stage_only = args.stage_only
    # Get connection with autocommit disabled for rollback
    connection = psycopg2.connect(settings.DATABASE_WRITE_URL)
    connection.set_session(autocommit=False)
    ## use that connection moving forward
    resources = Resources(connection=connection)

    try:
        client = IngestClient(resources=resources)
        path = deconstruct_path(key)

        ## make sure the file exsits somewhere
        if path.get('location') is None:
            print(f"x Could not determine the location of that key")
            sys.exit(1)

        if args.download:
            if not path.get('location') == 'local':
                ## download and update the path
                print('download and update path')
                path = download_from_location(path, output_path=args.output)
                key = path.get('key')
            else:
                print('Key is already a local file')


        ## now we load the data and print out some details
        print_header(f"Loading {key}")
        client.load_keys([[id, key, None]])

        ## print out some information about what was just loaded
        print_client_summary(client)

        if args.preview:
            print_header('PREVIEW ONLY: Not loading data into the database')
        else:
            ## dump data into the database
            ## if not load we just insert to staging tables
            client.dump(load=(not stage_only))

            ## output what we have
            print_staging_summary(connection)

            if not stage_only:
                ## summarize the ingested data
                ## this may or may not include already ingested data
                print_current_data_summary(connection)


        if args.dryrun:
            print_header("Rolling back all changes")
            connection.rollback()
            connection.close()
            print("   ✓ Rollback complete")
        else:
            connection.commit()


    except Exception as e:

        print(f"✗ Error: {e}")
        sys.exit(1)

    finally:
        ## this will close the db connection
        resources.close()



if __name__ == '__main__':
    main()
