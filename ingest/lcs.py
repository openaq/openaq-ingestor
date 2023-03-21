import os
import logging
from datetime import datetime, timezone
import dateparser
import pytz
import orjson
import uuid
import csv
from time import time
from urllib.parse import unquote_plus
import warnings

import boto3
import psycopg2
import typer
from io import StringIO
from .settings import settings
from .utils import (
    get_query,
    clean_csv_value,
    StringIteratorIO,
    fix_units,
    load_fetchlogs,
)

s3 = boto3.resource("s3")
s3c = boto3.client("s3")

app = typer.Typer()
dir_path = os.path.dirname(os.path.realpath(__file__))

FETCH_BUCKET = settings.ETL_BUCKET

logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


class LCSData:
    def __init__(
        self, page=None, key=None, st=datetime.now().replace(tzinfo=pytz.UTC)
    ):
        logger.debug(f"Loading data with {len(page)} pages")
        self.sensors = []
        self.systems = []
        self.nodes = []
        self.keys = []
        self.page = page
        self.st = st
        if key is not None:
            self.page = [
                {
                    "Key": key,
                    "LastModified": datetime.now().replace(tzinfo=pytz.UTC),
                }
            ]

    def sensor(self, j, system_id, fetchlogsId):
        for s in j:
            sensor = {}
            metadata = {}
            sensor["ingest_sensor_systems_id"] = system_id
            sensor["fetchlogs_id"] = fetchlogsId
            for key, value in s.items():
                key = str.replace(key, "sensor_", "")
                if key == "id":
                    sensor["ingest_id"] = value
                elif key == "measurand_parameter":
                    sensor["measurand"] = value
                elif key == "measurand_unit":
                    sensor["units"] = fix_units(value)
                else:
                    metadata[key] = value
            sensor["metadata"] = orjson.dumps(metadata).decode()
            self.sensors.append(sensor)

    def system(self, j, node_id, fetchlogsId):
        for s in j:
            system = {}
            metadata = {}
            if "sensor_system_id" in s:
                id = s["sensor_system_id"]
            else:
                id = node_id
            system["ingest_sensor_nodes_id"] = node_id
            system["ingest_id"] = id
            system["fetchlogs_id"] = fetchlogsId
            for key, value in s.items():
                key = str.replace(key, "sensor_system_", "")
                if key == "sensors":
                    self.sensor(value, id, fetchlogsId)
                else:
                    metadata[key] = value
            system["metadata"] = orjson.dumps(metadata).decode()
            self.systems.append(system)

    def node(self, j):
        node = {"fetchlogs_id": None}
        metadata = {}
        if "sensor_node_id" in j:
            id = j["sensor_node_id"]
        else:
            return None
        # if we have passed the fetchlogs_id we should track it
        if "fetchlogs_id" in j:
            node["fetchlogs_id"] = j["fetchlogs_id"]

        for key, value in j.items():
            key = str.replace(key, "sensor_node_", "")
            if key == "id":
                node["ingest_id"] = value
            elif key in ["site_name", "source_name", "ismobile"]:
                node[key] = value
            elif key == "geometry":
                try:
                    lon = float(value[0])
                    lat = float(value[1])
                    if lon != 0 and lat != 0:
                        node[
                            "geom"
                        ] = f"SRID=4326;POINT({value[0]} {value[1]})"
                    else:
                        node["geom"] = None
                except Exception:
                    node["geom"] = None
            elif key == "sensor_systems":
                self.system(value, id, node["fetchlogs_id"])
            else:
                metadata[key] = value
        node["metadata"] = orjson.dumps(metadata).decode()
        self.nodes.append(node)

    def get_station(self, key, fetchlogsId):
        logger.debug(f"get_station - {key} - {fetchlogsId}")
        if str.endswith(key, ".gz"):
            compression = "GZIP"
        else:
            compression = "NONE"
        # Removed the try block because getting the data is the whole
        # purpose of this function and we should not continue without it
        # if we want to check for specific errors we could do that,
        # but than rethrow
        resp = s3c.select_object_content(
            Bucket=FETCH_BUCKET,
            Key=key,
            ExpressionType="SQL",
            Expression="SELECT * FROM s3object",
            InputSerialization={
                "JSON": {"Type": "Document"},
                "CompressionType": compression,
            },
            OutputSerialization={"JSON": {}},
        )
        for event in resp["Payload"]:
            if "Records" in event:
                records = event["Records"]["Payload"].decode("utf-8")
                obj = orjson.loads(records)
                obj['key'] = key
                if fetchlogsId is not None:
                    obj['fetchlogs_id'] = fetchlogsId
                self.node(obj)

    def load_data(self):
        logger.debug(f"load_data: {self.keys}, {self.nodes}")
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                start_time = time()
                self.create_staging_table(cursor)

                write_csv(
                    cursor,
                    self.keys,
                    "keys",
                    [
                        "key",
                        "last_modified",
                        "fetchlogs_id",
                    ],
                )
                # update by id instead of key due to matching issue
                cursor.execute(
                    """
                    UPDATE fetchlogs
                    SET loaded_datetime = clock_timestamp()
                    , last_message = 'load_data'
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM keys)
                    """
                )
                connection.commit()
                write_csv(
                    cursor,
                    self.nodes,
                    "ms_sensornodes",
                    [
                        "ingest_id",
                        "site_name",
                        "source_name",
                        "ismobile",
                        "geom",
                        "metadata",
                        "fetchlogs_id",
                    ],
                )
                write_csv(
                    cursor,
                    self.systems,
                    "ms_sensorsystems",
                    [
                        "ingest_id",
                        "ingest_sensor_nodes_id",
                        "metadata",
                        "fetchlogs_id",
                    ],
                )
                write_csv(
                    cursor,
                    self.sensors,
                    "ms_sensors",
                    [
                        "ingest_id",
                        "ingest_sensor_systems_id",
                        "measurand",
                        "units",
                        "metadata",
                        "fetchlogs_id",
                    ],
                )
                connection.commit()

                self.process_data(cursor)
                for notice in connection.notices:
                    print(notice)

                cursor.execute(
                    """
                    UPDATE fetchlogs
                    SET completed_datetime = clock_timestamp()
                    , last_message = NULL
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM keys)
                    """
                )

                connection.commit()
                logger.info("load_data: files: %s; time: %0.4f", len(self.keys), time() - start_time)
                for notice in connection.notices:
                    logger.debug(notice)

    def process_data(self, cursor):
        query = get_query("lcs_ingest_full.sql")
        cursor.execute(query)

    def create_staging_table(self, cursor):
        cursor.execute(get_query(
            "lcs_staging.sql",
            table="TEMP TABLE" if settings.USE_TEMP_TABLES else 'TABLE'
        ))

    def get_metadata(self):
        hasnew = False
        for obj in self.page:
            key = obj["Key"]
            id = obj["id"]
            last_modified = obj["LastModified"]
            try:
                logger.debug(f"Loading station file: {id}:{key}")
                self.get_station(key, id)
                self.keys.append(
                    {
                        "key": key,
                        "last_modified": last_modified,
                        "fetchlogs_id": id
                    }
                )
                hasnew = True
            except Exception as e:
                # catch and continue to next page
                logger.error(
                    f"Could not process file: {id}: {key}: {e}"
                )

        if hasnew:
            logger.debug(f"get_metadata:hasnew - {self.keys}")
            self.load_data()


def write_csv(cursor, data, table, columns):
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
    logger.debug(f"table: {table}; rowcount: {cursor.rowcount}")


def load_metadata_bucketscan(count=100):
    paginator = s3c.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=FETCH_BUCKET,
        Prefix="lcs-etl-pipeline/stations",
        PaginationConfig={"PageSize": count},
    ):
        try:
            contents = page["Contents"]
            data = LCSData(contents)
            data.get_metadata()
        except KeyError:
            break


def load_metadata_db(limit=250, ascending: bool = False):
    order = 'ASC' if ascending else 'DESC'
    pattern = 'lcs-etl-pipeline/stations/'
    rows = load_fetchlogs(pattern, limit, ascending)
    contents = []
    for row in rows:
        logger.debug(row)
        contents.append(
            {
                "Key": unquote_plus(row[1]),
                "LastModified": row[2],
                "id": row[0],
            }
        )    
    if len(contents) > 0:
        load_metadata(contents)
        # data = LCSData(contents)
        # data.get_metadata()
    return len(rows)


def load_metadata_batch(batch: str):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT key
                , last_modified
                , fetchlogs_id
                FROM fetchlogs
                WHERE batch_uuid = %s
                """,
                (batch,),
            )
            rows = cursor.fetchall()
            rowcount = cursor.rowcount
            contents = []
            for row in rows:
                contents.append(
                    {
                        "Key": unquote_plus(row[0]),
                        "LastModified": row[1],
                        "id": row[2],
                    }
                )
            for notice in connection.notices:
                logger.debug(notice)
    if len(contents) > 0:
        load_metadata(contents)
        # data = LCSData(contents)
        # data.get_metadata()
    return rowcount


def load_metadata(keys):
    logger.debug(f'Load metadata: {len(keys)}')
    data = LCSData(keys)
    try:
        data.get_metadata()
    except Exception as e:
        ids = ','.join([str(k['id']) for k in keys])
        logger.error(f'load error: {e} ids: {ids}')
        raise


def select_object(key):
    key = unquote_plus(key)
    if str.endswith(key, ".gz"):
        compression = "GZIP"
    else:
        compression = "NONE"
    try:
        content = ""
        resp = s3c.select_object_content(
            Bucket=settings.ETL_BUCKET,
            Key=key,
            ExpressionType="SQL",
            Expression="""
                SELECT
                *
                FROM s3object
                """,
            InputSerialization={
                "CSV": {"FieldDelimiter": ","},
                "CompressionType": compression,
            },
            OutputSerialization={"CSV": {}},
        )
        for event in resp["Payload"]:
            if "Records" in event:
                content += event["Records"]["Payload"].decode("utf-8")
    except Exception as e:
        submit_file_error(key, e)
    return content


def get_measurements(key, fetchlogsId):
    start = time()
    content = select_object(key)
    fetch_time = time() - start

    ret = []
    start = time()
    for row in csv.reader(content.split("\n")):
        if len(row) not in [3, 5]:
            continue
        if len(row) == 5:
            try:
                lon = float(row[3])
                lat = float(row[4])
                if not (
                    lon is None
                    or lat is None
                    or lat == ""
                    or lon == ""
                    or lon == 0
                    or lat == 0
                    or lon < -180
                    or lon > 180
                    or lat < -90
                    or lat > 90
                ):
                    row[3] = lon
                    row[4] = lat
                else:
                    row[3] = None
                    row[4] = None
            except Exception:
                row[3] = None
                row[4] = None
        else:
            row.insert(3, None)
            row.insert(4, None)
        if row[0] == "" or row[0] is None:
            continue
        dt = row[2]

        try:
            if dt.isnumeric():
                if len(dt) == 13:
                    dt = datetime.fromtimestamp(int(dt)/1000.0, timezone.utc)
                else:
                    dt = datetime.fromtimestamp(int(dt), timezone.utc)
                row[2] = dt.isoformat()
        except Exception:
            try:
                dt = dateparser.parse(dt).replace(tzinfo=timezone.utc)
            except Exception:
                logger.warning(f"Exception in parsing date for {dt} {Exception}")

        #row[2] = dt.isoformat()
        # addd the log id for tracing purposes
        row.insert(5, fetchlogsId)
        ret.append(row)
    logger.info("get_measurements:csv: %s; size: %s; rows: %s; fetching: %0.4f; reading: %0.4f", key, len(content)/1000, len(ret), fetch_time, time() - start)
    return ret


def submit_file_error(key, e):
    """Update the log to reflect the error and prevent a retry"""
    logger.error(f"{key}: {e}")
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fetchlogs
                SET completed_datetime = clock_timestamp()
                , last_message = %s
                WHERE key = %s
                """,
                (f"ERROR: {e}", key),
            )


def to_tsv(row):
    tsv = "\t".join(map(clean_csv_value, row)) + "\n"
    return tsv
    return ""


def load_measurements_file(fetchlogs_id: int):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetchlogs_id
                , key
                FROM fetchlogs
                WHERE fetchlogs_id = %s
                LIMIT 1
                ;
                """,
                (fetchlogs_id,),
            )
            rows = cursor.fetchall()
            load_measurements(rows)


def load_measurements_batch(batch: str):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetchlogs_id
                , key
                FROM fetchlogs
                WHERE batch_uuid = %s
                """,
                (batch,),
            )
            rows = cursor.fetchall()
            load_measurements(rows)


def load_measurements_db(limit=250, ascending: bool = False):
    pattern = '^lcs-etl-pipeline/measures/.*\\.csv'
    rows = load_fetchlogs(pattern, limit, ascending)
    load_measurements(rows)
    return len(rows)


def load_measurements(rows):
    logger.debug(f"loading {len(rows)} measurements")
    start_time = time()
    data = []
    new = []
    for row in rows:
        key = row[1]
        fetchlogsId = row[0]
        new.append({"key": key})
        newdata = get_measurements(key, fetchlogsId)
        if newdata is not None:
            data.extend(newdata)

    logger.info("load_measurements:get: %s keys; %s rows; %0.4f seconds",
                len(rows), len(data), time() - start_time)
    if len(data) > 0:
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:

                cursor.execute(get_query(
                    "lcs_staging.sql",
                    table="TEMP TABLE"
                ))

                write_csv(
                    cursor, new, "keys", ["key",],
                )

                iterator = StringIteratorIO(
                    (to_tsv(line) for line in data)
                )
                cursor.copy_expert(
                    """
                    COPY meas (ingest_id, value, datetime, lon, lat, fetchlogs_id)
                    FROM stdin;
                    """,
                    iterator,
                )
                mrows = cursor.rowcount
                status = cursor.statusmessage
                logger.debug(f"COPY Rows: {mrows} Status: {status}")

                cursor.execute(get_query("lcs_meas_ingest.sql"))
                for notice in connection.notices:
                    print(notice)

                logger.info(
                    "load_measurements: keys: %s; rows: %s; time: %0.4f",
                    len(rows), mrows, time() - start_time)
                connection.commit()

                for notice in connection.notices:
                    logger.debug(notice)
