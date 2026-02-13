import io
import os
import sys
from pathlib import Path
import logging
from urllib.parse import unquote_plus
import gzip
import uuid
import csv

import boto3
import re
from io import StringIO
import psycopg2
# import typer

from .resources import Resources
from .settings import settings

# app = typer.Typer()

dir_path = os.path.dirname(os.path.realpath(__file__))


logger = logging.getLogger('utils')


class StringIteratorIO(io.TextIOBase):
    """Iterator wrapper for converting string iterators to file-like objects.

    This class wraps a string iterator to provide a file-like interface,
    allowing it to be used with functions that expect file objects (like csv.reader).

    Used by:
        - ingest/lcsV2.py (write_csv operations)

    Args:
        iter: An iterator that yields strings

    Example:
        >>> lines = ['line1\\n', 'line2\\n']
        >>> file_obj = StringIteratorIO(iter(lines))
        >>> data = file_obj.read()
    """
    def __init__(self, iter):
        self._iter = iter
        self._buff = ""

    def readable(self):
        return True

    def _read1(self, n=None):
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n=None):
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)





def clean_csv_value(value):
    """Clean and escape values for PostgreSQL COPY FROM CSV format.

    Converts None/empty values to PostgreSQL NULL representation (\\N) and
    escapes special characters (newlines, tabs) for CSV compatibility.

    Used by:
        - ingest/fetch.py
        - ingest/lcs.py
        - ingest/lcsV2.py

    Args:
        value: Any value to be written to CSV (str, int, float, None, etc.)

    Returns:
        str: Cleaned value safe for PostgreSQL COPY command
            - None or "" becomes r"\\N" (PostgreSQL NULL)
            - Newlines are escaped to \\n
            - Tabs are replaced with spaces

    Example:
        >>> clean_csv_value(None)
        '\\N'
        >>> clean_csv_value("line1\\nline2")
        'line1\\\\nline2'
    """
    if value is None or value == "":
        return r"\N"
    return str(value).replace("\n", "\\n").replace("\t", " ")


def get_query(file, **params):
    """Load a SQL query from a file and optionally format it with parameters.

    Reads SQL query files from the ingest directory and performs string formatting
    with provided keyword arguments. Useful for loading reusable SQL templates.

    Used by:
        - ingest/fetch.py
        - ingest/lcs.py
        - ingest/lcsV2.py

    Args:
        file (str): Relative path to SQL file within the ingest directory
        **params: Keyword arguments for string formatting the query

    Returns:
        str: The SQL query string, optionally formatted with parameters

    Example:
        >>> query = get_query('queries/select_data.sql', limit=100)
    """
    logger.debug(f"get_query: {file}, params: {params}")
    query = Path(os.path.join(dir_path, file)).read_text()
    if params is not None and len(params) >= 1:
        query = query.format(**params)
    return query




def get_logs_from_pattern(pattern: str, limit: int = 250, connection=None):
    r"""Retrieve fetchlog records matching a regex pattern.

    Queries the fetchlogs table using PostgreSQL regex matching (~*) to find
    entries whose keys match the provided pattern. Case-insensitive.

    Used by:
        - ingest/lcsV2.py
        - ingest/handler.py

    Args:
        pattern (str): PostgreSQL regex pattern to match against fetchlog keys
        limit (int, optional): Maximum number of records to return. Defaults to 250.

    Returns:
        list: List of tuples containing:
            - fetchlogs_id (int)
            - key (str)
            - init_datetime (datetime)
            - loaded_datetime (datetime)
            - completed_datetime (datetime)
            - last_message (str)
            - last_modified (datetime)

    Example:
        >>> rows = get_logs_from_pattern(r'^lcs-etl-pipeline/measures/.*\.csv', limit=10)
    """
    if connection is None:
        rs = Resources()
        connection = rs.get_connection(autocommit=True)

    with connection.cursor() as cursor:
        cursor.execute(
            """
              SELECT fetchlogs_id
              , key
              , init_datetime
              , loaded_datetime
              , completed_datetime
              , last_message
              , last_modified
              FROM fetchlogs
              WHERE key~*%s
              LIMIT %s
              """,
              (pattern, limit,),
            )
        return cursor.fetchall()


def fix_units(value: str):
    """Normalize measurement unit strings to canonical format.

    Standardizes various representations of microgram units (μ vs µ) and
    superscript variations (3 vs ³) to a consistent format: µg/m³.

    Used by:
        - ingest/lcs.py
        - ingest/lcsV2.py

    Args:
        value (str): Unit string to normalize

    Returns:
        str: Normalized unit string (µg/m³) or original value if no mapping exists

    Example:
        >>> fix_units("μg/m3")
        'µg/m³'
        >>> fix_units("ppm")
        'ppm'
    """
    units = {
        "μg/m3": "µg/m³",
        "µg/m3": "µg/m³",
        "μg/m³": "µg/m³",
    }
    if value in units.keys():
        return units[value]
    else:
        return value



def deconstruct_path(key: str):
    """Parse a file path and extract bucket/key information.

    Analyzes a file path string to determine if it's local or S3, then extracts
    the bucket name and key. Defaults to settings.FETCH_BUCKET if no bucket is specified.

    Used by:
        - Not currently imported by any modules (legacy utility function)

    Args:
        key (str): File path which may be:
            - Local file path (e.g., '/path/to/file.csv')
            - S3 URI (e.g., 's3://bucket-name/path/to/file.csv')
            - S3 key without URI (e.g., 'path/to/file.csv')

    Returns:
        dict: Dictionary containing:
            - 'local' (bool): True if local file, omitted for S3
            - 'bucket' (str): S3 bucket name (for S3 paths)
            - 'key' (str): File path/key

    Example:
        >>> deconstruct_path('s3://my-bucket/data/file.csv')
        {'bucket': 'my-bucket', 'key': 'data/file.csv'}
        >>> deconstruct_path('/local/file.csv')
        {'local': True, 'key': '/local/file.csv'}
    """
    is_local = os.path.isfile(key)
    is_s3 = bool(re.match(r"s3://[a-zA-Z]+[a-zA-Z0-9_-]+/[a-zA-Z]+", key))
    is_csv = bool(re.search(r"\.csv(.gz)?$", key))
    is_json = bool(re.search(r"\.(nd)?json(.gz)?$", key))
    is_compressed = bool(re.search(r"\.gz$", key))
    path = {}
    if is_local:
        path["location"] = 'local'
        path["key"] = key
    elif is_s3:
        # pull out the bucket name
        p = key.split("//")[1].split("/")
        path["location"] = "s3"
        path["bucket"] = p.pop(0)
        path["key"] = "/".join(p)
    else:
        # use the current bucket from settings
        path["bucket"] = settings.FETCH_BUCKET
        path["key"] = key

    return path

def get_data(key: str, resources=None):
    """Retrieve file data from local filesystem or S3.

    Automatically detects the source (local vs S3) from the key format and
    returns a file-like object. Handles gzip-compressed files transparently.

    Used by:
        - ingest/fetch.py

    Args:
        key (str): File path/key in one of these formats:
            - 's3://bucket/path/to/file.csv' - Full S3 URI
            - 'local:///path/to/file.csv' - Local file with URI prefix
            - '/path/to/file.csv' - Absolute local path
            - 'path/to/file.csv' - S3 key (uses settings.FETCH_BUCKET)

    Returns:
        file-like object: Readable stream of file contents
            - For local files: file handle from get_file()
            - For S3 files: boto3 StreamingBody or GzipFile

    Example:
        >>> data = get_data('s3://my-bucket/data.csv.gz')
        >>> content = data.read()
    """
    # if we have not provided a resource lets create one
    # check to see if we were provided with a path that includes the source
    # e.g.
    # s3://bucket/key
    # local://drive/key
    # /key (assume local)
    # or no source
    # key (no forward slash, assume etl bucket)
    if re.match(r"local://[a-zA-Z]+", key):
        key = key.replace("local://", "")

    is_local = os.path.isfile(key)
    is_s3 = bool(re.match(r"s3://[a-zA-Z]+[a-zA-Z0-9_-]+/[a-zA-Z]+", key))
    #is_csv = bool(re.search(r"\.csv(.gz)?$", key))
    #is_json = bool(re.search(r"\.(nd)?json(.gz)?$", key))
    is_compressed = bool(re.search(r"\.gz$", key))
    logger.debug(f"checking - {key}\ns3: {is_s3}; is_local: {is_local}")

    if is_local:
        return get_file(key)
    elif is_s3:
        # pull out the bucket name
        path = key.split("//")[1].split("/")
        bucket = path.pop(0)
        key = "/".join(path)
    else:
        # use the current bucket from settings
        bucket = settings.FETCH_BUCKET

    # stream the file
    logger.debug(f"streaming s3 file data from s3://{bucket}/{key}")
    rs = resources or Resources()
    obj = rs.s3.get_object(
        Bucket=bucket,
        Key=key,
        )
    f = obj["Body"]
    if is_compressed:
        return gzip.GzipFile(fileobj=obj["Body"])
    else:
        return obj["Body"]


def get_file(filepath: str):
    """Open a local file for reading, handling gzip compression.

      Opens local files and automatically decompresses if the file has a .gz extension.

      Used by:
      - ingest/lcsV2.py
      - ingest/utils.py (via get_data)

      Args:
      filepath (str): Path to local file

      Returns:
      file-like object: File handle for reading
      - GzipFile for .gz files (binary mode)
      - TextIOWrapper for regular files (utf-8 encoding)

      Example:
      >>> f = get_file('/path/to/data.csv.gz')
      >>> content = f.read()
      """
    is_compressed = bool(re.search(r"\.gz$", filepath))
    logger.debug(f"streaming local file data from {filepath}")
    if is_compressed:
        return gzip.open(filepath, 'rb')
    else:
        return io.open(filepath, "r", encoding="utf-8")


def get_object(
        key: str,
        bucket: str = settings.FETCH_BUCKET,
        resources = None,
):
    """Retrieve and decompress an S3 object as text.

    Downloads an S3 object and returns its contents as a string. Automatically
    decompresses gzip files and URL-decodes the key.

    Used by:
        - ingest/lcsV2.py

    Args:
        key (str): S3 object key (will be URL-decoded)
        bucket (str, optional): S3 bucket name. Defaults to settings.FETCH_BUCKET.

    Returns:
        str or StreamingBody: File contents
            - str (decoded utf-8) for .gz files
            - StreamingBody object for non-compressed files

    Example:
        >>> text = get_object('data/measurements.json.gz')
        >>> import json
        >>> data = json.loads(text)
    """
    key = unquote_plus(key)
    text = ''
    rs = resources or Resources()
    logger.debug(f"Getting {key} from {bucket}")
    obj = rs.s3.get_object(
        Bucket=bucket,
        Key=key,
    )
    body = obj['Body']
    if str.endswith(key, ".gz"):
        text = gzip.decompress(body.read()).decode('utf-8')
    else:
        text = body

    return text


def put_object(
        data: str,
        key: str,
        bucket: str = settings.FETCH_BUCKET,
        resources = None,
):
    """Upload a gzip-compressed string to S3 or local filesystem.

    Compresses the provided string data with gzip and uploads to S3. In DRYRUN
    mode, writes to local filesystem instead.

    Used by:
        - Not currently imported by any modules (utility function)

    Args:
        data (str): Text data to compress and upload
        key (str): S3 object key
        bucket (str, optional): S3 bucket name. Defaults to settings.FETCH_BUCKET.

    Returns:
        None

    Example:
        >>> put_object('{"data": "example"}', 'output/data.json.gz')
    """
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode='wb') as gz:
        with io.TextIOWrapper(gz, encoding='utf-8') as wrapper:
            wrapper.write(data)
    if settings.DRYRUN:
        filepath = os.path.join(bucket, key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.debug(f"Dry Run: Writing file to local file in {filepath}")
        txt = open(f"{filepath}", "wb")
        txt.write(out.getvalue())
        txt.close()
    else:
        logger.info(f"Uploading file to {bucket}/{key}")
        rs = resources or Resources()
        rs.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=out.getvalue(),
        )



def load_fail(cursor, fetchlogsId, e):
    """Mark a fetchlog as failed with error details.

    Updates a fetchlog record to indicate processing failure, storing the error
    message and setting completion timestamp. Used for error tracking and monitoring.

    Used by:
        - ingest/fetch.py
        - ingest/lcsV2.py

    Args:
        cursor: psycopg2 cursor object (active transaction)
        fetchlogsId (int): The fetchlogs_id to mark as failed
        e (Exception): The exception that caused the failure

    Returns:
        None: Updates are made via the cursor (commit separately)

    Example:
        >>> try:
        ...     process_file(key)
        ... except Exception as e:
        ...     load_fail(cursor, fetchlog_id, e)
        ...     connection.commit()
    """
    logger.warning(f"full copy of {fetchlogsId} failed: {e}")
    cursor.execute(
        """
        UPDATE fetchlogs
        SET last_message=%s
        , has_error = true
        , completed_datetime = clock_timestamp()
        WHERE fetchlogs_id=%s
        """,
        (
            str(e),
            fetchlogsId,
        ),
    )



def load_success(cursor, keys, message: str = 'success'):
    """Mark fetchlogs as successfully completed.

    Updates multiple fetchlog records to indicate successful processing, clearing
    error flags and setting completion timestamp.

    Used by:
        - ingest/fetch.py
        - ingest/lcsV2.py (via IngestClient)

    Args:
        cursor: psycopg2 cursor object (active transaction)
        keys (list): List of S3 keys to mark as successful
        message (str, optional): Success message to store. Defaults to 'success'.

    Returns:
        None: Updates are made via the cursor (commit separately)

    Example:
        >>> processed_keys = ['file1.json', 'file2.json']
        >>> load_success(cursor, processed_keys, message='Processed 100 records')
        >>> connection.commit()
    """
    cursor.execute(
        """
        UPDATE fetchlogs
        SET
        last_message=%s
        , completed_datetime=clock_timestamp()
        , has_error = false
        WHERE key=ANY(%s)
        """,
        (
            message,
            keys,
        ),
    )


def load_fetchlogs(
        pattern: str,
        limit: int = 250,
        ascending: bool = False,
        connection=None
):
    r"""Load and lock a batch of fetchlogs ready for processing.

    Atomically selects unprocessed fetchlog records matching a pattern, marks them
    as loaded, and returns them for processing. Uses row-level locking to prevent
    duplicate processing in concurrent environments.

    Used by:
        - ingest/fetch.py
        - ingest/lcs.py
        - ingest/lcsV2.py (via IngestClient)
        - ingest/handler.py (via load_measurements_db)

    Args:
        pattern (str): PostgreSQL regex pattern to match fetchlog keys
        limit (int, optional): Maximum number of records to load. Defaults to 250.
        ascending (bool, optional): If True, process oldest files first (by last_modified).
                                   If False, process newest first. Defaults to False.

    Returns:
        list: List of tuples containing:
            - fetchlogs_id (int)
            - key (str)
            - last_modified (datetime)

    Behavior:
        - Only selects records where:
            - key matches pattern
            - has_error = false
            - init_datetime is not null
            - completed_datetime is null
            - loaded_datetime is null OR > 30 minutes ago
        - Sets loaded_datetime to current timestamp
        - Increments jobs counter
        - Assigns a batch_uuid for tracking
        - Uses FOR UPDATE SKIP LOCKED to prevent concurrent processing

    Example:
        >>> rows = load_fetchlogs(r'^lcs-etl-pipeline/measures/.*\.csv', limit=100, ascending=True)
        >>> for fetchlog_id, key, modified in rows:
        ...     process_file(key)
    """
    order = 'ASC' if ascending else 'DESC'
    if connection is None:
        rs = Resources()
        connection = rs.get_connection(autocommit=True)

    batch_uuid = uuid.uuid4().hex
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH updated AS (
              UPDATE fetchlogs
              SET loaded_datetime = CURRENT_TIMESTAMP
              , jobs = jobs + 1
              , batch_uuid = %s
              FROM (
                SELECT fetchlogs_id
                FROM fetchlogs
                WHERE key~E'{pattern}'
                AND NOT has_error
                AND init_datetime is not null
                AND completed_datetime is null
                AND (
                   loaded_datetime IS NULL
                   OR loaded_datetime < now() - '30min'::interval
                )
                ORDER BY last_modified {order} nulls last
                LIMIT %s
                FOR UPDATE SKIP LOCKED
              ) as q
              WHERE q.fetchlogs_id = fetchlogs.fetchlogs_id
              RETURNING fetchlogs.fetchlogs_id
              , fetchlogs.key
              , fetchlogs.last_modified
            )
            SELECT * FROM updated
            ORDER BY last_modified {order} NULLS LAST;
            """,
            (batch_uuid, limit,),
        )
        rows = cursor.fetchall()
        logger.debug(f'Loaded {len(rows)} from fetchlogs using {pattern}/{order}')
        return rows


def write_csv(cursor, data, table, columns):
    """Bulk-insert rows into a PostgreSQL table using COPY FROM CSV.

    Efficiently writes multiple rows to a database table using PostgreSQL's
    COPY protocol, which is much faster than individual INSERT statements.

    Used by:
        - ingest/lcsV2.py (IngestClient for bulk data insertion)

    Args:
        cursor: psycopg2 cursor object
        data (list): List of dictionaries where each dict represents a row
        table (str): Target table name
        columns (list): List of column names to insert (must match dict keys)

    Returns:
        None: Data is inserted via cursor (commit separately)

    Behavior:
        - Skips operation if data is empty
        - Converts list of dicts to CSV format in memory
        - Uses PostgreSQL COPY FROM for high-performance bulk insert
        - Logs number of rows copied

    Example:
        >>> data = [
        ...     {'id': 1, 'value': 'A'},
        ...     {'id': 2, 'value': 'B'}
        ... ]
        >>> write_csv(cursor, data, 'measurements', ['id', 'value'])
        >>> connection.commit()
    """
    logger.debug(f"copying {len(data)} rows from table: {table}")
    if len(data)>0:
        fields = ",".join(columns)
        sio = StringIO()
        writer = csv.DictWriter(sio, columns)
        writer.writerows(data)
        sio.seek(0)
        cursor.copy_expert(
            f"""
            copy {table} ({fields}) from stdin with csv;
            """,
            sio,
        )
        logger.debug(f"copied {cursor.rowcount} rows from table: {table}")
