#!/usr/bin/env python
"""Unified CLI for loading, checking, and downloading fetchlog files.

Selectors (one required):
    --id ID             single fetchlog id
    --key KEY           single S3 key or local path
    --batch UUID        all keys in a batch
    --pattern REGEX     query fetchlogs by regex pattern
    --s3-prefix PREFIX  list S3 directly under prefix
    --from-file PATH    read keys from a file (one per line)
    KEY [KEY ...]       positional keys

Actions:
    (default)           process through IngestClient
    --preview           parse only; no DB writes
    --dry-run           resolve keys and print; no processing
    --download [PATH]   download files locally; no DB

Modifiers:
    --stage-only        dump to staging, skip ETL load
    --source-db DSN     alternate DB for fetchlog lookups
    --bucket NAME       override FETCH_BUCKET
    --limit N           cap on keys processed (default 300)

Reporting:
    --no-report         Do not print summary tables after processing
    --csv PATH          write per-key stats to CSV

Transaction:
    --commit            persist (default: rollback)
    --debug             verbose logging
"""

import argparse
import csv
import logging
import os
import sys
from time import time
from datetime import datetime, timezone

import psycopg2

from ingest.lcsV2 import IngestClient
from ingest.resources import Resources
from ingest.utils import (
    deconstruct_path,
    download_from_location,
    resolve_by_batch,
    resolve_by_id,
    resolve_by_keys,
    resolve_by_pattern,
    resolve_by_prefix,
    resolve_from_file,
)


logger = logging.getLogger('ingest')


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Ingest fetchlog files into the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Selectors (mutually exclusive)
    sel = p.add_mutually_exclusive_group(required=True)
    sel.add_argument('--id', type=int, help='fetchlog id')
    sel.add_argument('--batch', type=str, help='batch uuid')
    sel.add_argument('--pattern', type=str, help='regex against fetchlog keys')
    sel.add_argument('--s3-prefix', type=str, help='S3 key prefix')
    sel.add_argument('--from-file', type=str, help='file with one key per line')
    sel.add_argument('keys', nargs='*', default=[],
                     help='positional S3 keys or local paths')

    # Actions (mutually exclusive)
    act = p.add_mutually_exclusive_group()
    act.add_argument('--preview', action='store_true',
                     help='parse only; no DB writes')
    act.add_argument('--dry-run', action='store_true',
                     help='resolve keys and print; no processing')
    act.add_argument('--download', nargs='?', const='',
                     metavar='PATH',
                     help='download files locally; no DB')

    # Modifiers
    p.add_argument('--stage-only', action='store_true',
                   help='dump to staging, skip ETL load')
    p.add_argument('--source-db', type=str,
                   help='alt DB DSN for fetchlog lookups')
    p.add_argument('--bucket', type=str,
                   help='override FETCH_BUCKET')
    p.add_argument('--limit', type=int, default=300)

    # Reporting
    p.add_argument('--no-report', action='store_true',
                   help='suppress summary tables (default: show)')
    p.add_argument('--csv', type=str, metavar='PATH',
                   help='write per-key stats to CSV')

    # Transaction
    p.add_argument('--commit', action='store_true',
                   help='persist changes (default: rollback)')
    p.add_argument('--debug', action='store_true')

    p.add_argument('--keep-staging', action='store_true',
                   help='use permanent tables for staging (allows '
                        'post-run inspection; overrides USE_TEMP_TABLES)')

    p.add_argument('--diagnose', type=str, nargs='?', const='summary',
                   metavar='QUERY[,QUERY,...]',
                   help='run diagnostic queries; without argument runs '
                    '"summary" pack; "list" prints available names')

    p.add_argument('--diagnose-dir', type=str, metavar='DIR',
                   help='write diagnostic results to CSV files in this directory '
                        '(otherwise print to console)')

    args = p.parse_args()

    # Require at least one selector (positional keys count)
    if not any([args.id, args.batch, args.pattern, args.s3_prefix,
                args.from_file, args.keys]):
        p.error("provide a key selector")

    return args


def setup_logging(args):
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
        force=True,
    )
    for noisy in ('boto3', 'botocore', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Key resolution
# ---------------------------------------------------------------------------

def resolve_keys(args, *, connection=None) -> list:
    """Dispatch to the right resolver based on which selector was given."""
    from ingest.settings import settings

    if args.id is not None:
        return resolve_by_id(args.id, connection=connection)
    if args.batch is not None:
        return resolve_by_batch(args.batch, limit=args.limit,
                                connection=connection)
    if args.pattern is not None:
        return resolve_by_pattern(args.pattern, limit=args.limit,
                                  connection=connection)
    if args.s3_prefix is not None:
        return resolve_by_prefix(
            args.bucket or settings.FETCH_BUCKET,
            args.s3_prefix,
            limit=args.limit,
            connection=connection,
        )
    if args.from_file is not None:
        return resolve_from_file(args.from_file, connection=connection)
    if args.keys:
        return resolve_by_keys(args.keys, connection=connection)
    raise ValueError("no key selector provided")


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_dry_run(rows):
    """Print resolved keys and exit."""
    print(f"{'id':>8}  key")
    print("-" * 60)
    for row in rows:
        print(f"{row[0]:>8}  {row[1]}")
    print(f"\n{len(rows)} key(s)")


def cmd_download(rows, output_path):
    """Download files locally, no DB interaction."""
    for row in rows:
        _, key, _ = row
        path = deconstruct_path(key)
        download_from_location(path, output_path or None)


def cmd_process(rows, args):
    """Process rows through IngestClient."""
    connection = psycopg2.connect(_target_dsn())
    connection.set_session(autocommit=False)
    resources = Resources(connection=connection)

    try:
        results = []
        for row in rows:
            result = process_one(row, resources, args)
            results.append(result)

        if not args.no_report:
            print_report(results, committed=args.commit)
        if args.csv:
            write_csv_report(results, args.csv)

    finally:
        resources.close()


def process_one(row, resources, args) -> dict:
    """Process one fetchlog row. Returns stats dict."""
    fetchlogs_id, key, last_modified = row
    connection = resources.get_connection(autocommit=False)

    result = {
        "fetchlogs_id": fetchlogs_id,
        "key": key,
        "status": "ok",
        "error": "",
    }

    start = time()
    try:
        client = IngestClient(resources=resources,
                              fetchlogs_id=fetchlogs_id)
        client.load_key(key, fetchlogs_id, last_modified)

        if args.preview:
            _print_client_summary(client)
            result.update(client.summary())
            result["elapsed_sec"] = round(time() - start, 3)
            connection.rollback()
            return result

        client.dump_locations(load=not args.stage_only)
        client.dump_measurements(load=not args.stage_only)

        #[print(x) for x in client.systems.values()]
        # Stats before commit/rollback (staging still visible).
        elapsed = round(time() - start, 3)
        result.update(client.stats(connection, elapsed))

        # Diagnostics also before commit/rollback.
        if args.diagnose:
            run_diagnostics(connection, fetchlogs_id, args)

        if args.commit:
            connection.commit()
        else:
            connection.rollback()

    except Exception as e:
        connection.rollback()
        result["status"] = "error"
        result["error"] = str(e)
        result["elapsed_sec"] = round(time() - start, 3)
        logger.exception(f"Failed on {fetchlogs_id}: {key}")

    return result

def run_diagnostics(connection, fetchlogs_id, args):
    """Execute the requested diagnostic queries."""
    from ingest.diagnostics import QUERIES, resolve_names
    from ingest.utils import get_table
    import os

    names = [n.strip() for n in args.diagnose.split(',') if n.strip()]
    try:
        names = resolve_names(names)
    except ValueError as e:
        logger.error(str(e))
        return

    out_dir = args.diagnose_dir
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    for name in names:
        spec = QUERIES[name]
        sql = spec["sql"] % {"fetchlogs_id": fetchlogs_id}
        filename = None
        if out_dir:
            filename = os.path.join(
                out_dir, f"{fetchlogs_id}_{name}.csv"
            )
        get_table(connection, sql,
                  filename=filename,
                  head=10 if filename else None,
                  title=spec["title"])

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_client_summary(client):
    """Print what the client parsed (used by --preview)."""
    s = client.summary()
    print(f"\n{'=' * 60}")
    print(f"Client contents (fetchlogs_id={client.fetchlogs_id})")
    print(f"{'=' * 60}")
    print(f"  nodes:        {s['nodes']}")
    print(f"  systems:      {s['systems']}")
    print(f"  sensors:      {s['sensors']}")
    print(f"  measurements: {s['measurements']}")
    print(f"  flags:        {s['flags']}")


def print_report(results, *, committed):
    """Print per-key summary table + totals."""
    header = (f"{'ID':>8} {'Key':<40} "
              f"{'Nodes M/A':>11} {'Sys M/A':>11} {'Sens M/A':>11} "
              f"{'Meas':>8} {'Rej':>6} {'Time':>8} {'Status':>8}")
    print("\n" + "=" * len(header))
    print(header)
    print("=" * len(header))

    totals = {"nodes_matched": 0, "nodes_added": 0,
              "systems_matched": 0, "systems_added": 0,
              "sensors_matched": 0, "sensors_added": 0,
              "measurements_staged": 0, "rejects": 0}

    for r in results:
        key_short = r["key"] if len(r["key"]) <= 40 else "..." + r["key"][-37:]
        if r["status"] == "ok":
            for k in totals:
                totals[k] += r.get(k, 0)
            print(f"{r['fetchlogs_id']:>8} {key_short:<40} "
                  f"{r.get('nodes_matched', 0):>4}/{r.get('nodes_added', 0):<6} "
                  f"{r.get('systems_matched', 0):>4}/{r.get('systems_added', 0):<6} "
                  f"{r.get('sensors_matched', 0):>4}/{r.get('sensors_added', 0):<6} "
                  f"{r.get('measurements_staged', 0):>8} "
                  f"{r.get('rejects', 0):>6} "
                  f"{r.get('elapsed_sec', 0):>7.2f}s "
                  f"{r['status']:>8}")
        else:
            print(f"{r['fetchlogs_id']:>8} {key_short:<40} "
                  f"ERROR: {r['error'][:70]:<70} "
                  f"{r.get('elapsed_sec', 0):>7.2f}s")

    print("=" * len(header))
    print(f"{'':>8} {'TOTALS':<40} "
          f"{totals['nodes_matched']:>4}/{totals['nodes_added']:<6} "
          f"{totals['systems_matched']:>4}/{totals['systems_added']:<6} "
          f"{totals['sensors_matched']:>4}/{totals['sensors_added']:<6} "
          f"{totals['measurements_staged']:>8} "
          f"{totals['rejects']:>6}")

    ok = sum(1 for r in results if r["status"] == "ok")
    err = len(results) - ok
    print(f"\nProcessed: {len(results)} | OK: {ok} | Errors: {err}")
    if not committed:
        print("(Changes rolled back — use --commit to persist)")


def write_csv_report(results, path):
    fields = [
        "fetchlogs_id", "key", "status",
        "nodes_added", "nodes_matched", "nodes_unmatched",
        "systems_added", "systems_matched", "systems_unmatched",
        "sensors_added", "sensors_matched", "sensors_unmatched",
        "measurements_staged", "rejects",
        "client_nodes", "client_systems", "client_sensors",
        "client_measurements", "client_flags",
        "fetchlog_has_error", "fetchlog_message",
        "elapsed_sec", "error",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    logger.info(f"Wrote CSV report to {path}")


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def _target_dsn():
    """DSN for the ingest target (always the local/dev DB)."""
    from ingest.settings import settings
    return settings.DATABASE_WRITE_URL


def _source_connection(args):
    """Optional read-only connection for fetchlog lookups.

    Returns None if --source-db not given, meaning resolvers use the
    default (target) connection.
    """
    if not args.source_db:
        return None
    conn = psycopg2.connect(args.source_db)
    conn.set_session(autocommit=True)
    return conn


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    setup_logging(args)

    run_started_at = datetime.now(timezone.utc)
    logger.debug(f"Run started at {run_started_at.isoformat()}")

    if args.diagnose == 'list':
        from ingest.diagnostics import list_queries
        print(list_queries())
        return

    if args.keep_staging:
        from ingest.settings import settings
        settings.USE_TEMP_TABLES = False
        logger.info("Using permanent staging tables (--keep-staging)")

    if args.bucket:
        from ingest.settings import settings
        settings.FETCH_BUCKET = args.bucket
        logger.info(f"Using bucket override: {args.bucket}")

    source_conn = _source_connection(args)
    try:
        rows = resolve_keys(args, connection=source_conn)
    finally:
        if source_conn is not None:
            source_conn.close()

    if not rows:
        logger.warning("No keys resolved")
        return

    logger.info(f"Resolved {len(rows)} key(s)")

    if args.dry_run:
        cmd_dry_run(rows)
    elif args.download is not None:
        cmd_download(rows, args.download)
    else:
        cmd_process(rows, args)


if __name__ == '__main__':
    main()
