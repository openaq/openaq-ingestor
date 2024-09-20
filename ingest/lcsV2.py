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
import re

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
    select_object,
    get_file,
)

s3 = boto3.resource("s3")
s3c = boto3.client("s3")

app = typer.Typer()
dir_path = os.path.dirname(os.path.realpath(__file__))

FETCH_BUCKET = settings.FETCH_BUCKET

logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


def to_geometry(key, data):
    # could be passed as lat/lng or coordinates
    if key in ['lat','lon']:
        lat = data.get('lat')
        lon = data.get('lon')
    elif key == 'coordinates':
        lat = data.get('coordinates', {}).get('lat')
        lon = data.get('coordinates', {}).get('lon')
    if None in [lat, lon]:
        raise Exception('Missing value for coordinates')
    # could add more checks
    return f"SRID=4326;POINT({lon} {lat})"

def to_timestamp(key, data):
    dt = data.get(key)
    value = None
    if dt in [None, '']:
        logger.warning('Passed none type value for timestamp')
        # no need for exception, we check for nones later
        return None;
    if dt.isnumeric():
        if len(dt) == 13:
            dt = datetime.fromtimestamp(int(dt)/1000.0, timezone.utc)
        else:
            dt = datetime.fromtimestamp(int(dt), timezone.utc)
    else:
        return dt
        dt = dateparser.parse(dt).replace(tzinfo=timezone.utc)

    return dt.isoformat()


class IngestClient:
    def __init__(
        self, key=None, fetchlogs_id=None, data=None
    ):
        self.key = key
        self.fetchlogs_id = fetchlogs_id
        self.keys = []
        self.st = datetime.now().replace(tzinfo=pytz.UTC)
        self.sensors = []
        self.systems = []
        self.nodes = []
        self.node_ids = {}
        self.measurements = []
        self.matching_method = 'ingest-id'
        self.source = None
        self.node_map = {
            "fetchlogs_id": {},
            "site_name": { "col":"site_name" },
            "source_name": {},
            "ismobile": {},
            "ingest_id": {},
            "matching_method": {},
            "location": {"col":"ingest_id"},
            "sensor_node_id": {"col":"ingest_id"},
            "label": {"col":"site_name"},
            "coordinates": {"col":"geom","func": to_geometry },
            "geometry": {"col":"geom", "func": to_geometry },
            "lat": {"col":"geom","func": to_geometry },
            "lon": {"col":"geom","func": to_geometry },
            }
        self.measurement_map = {
            "sensor_id": {"col": "ingest_id"},
            "ingest_id": {"col": "ingest_id"},
            "timestamp": {"col": "datetime", "func": to_timestamp },
            "datetime": {"col": "datetime", "func": to_timestamp },
            "measure": {"col": "value"},
            "value": {},
            "lat": {},
            "lon": {},
            }
        # if fetchlogs_id but no key or data
        # get key
        # if key, load data
        # if data
        if data is not None and isinstance(data, dict):
            self.load(data)

    def process(self, key, data, mp):
        col = None
        value = None
        m = mp.get(key)
        if m is not None:
            col = m.get('col', key)
            func = m.get('func')
            if func is None:
                # just return value
                value = data.get(key)
            else:
                # functions require key and data
                value = func(key, data)
        return col, value

    def dump(self):
        """
        Dump any data that is currenly loaded into the database
        We will dump if there is data OR if we have loaded any keys
        We do this because its possible that a file is empty but we
        need to run the dump method to get the file to be marked as finished
        """
        logger.debug(f"Dumping data from {len(self.keys)} files")
        if len(self.nodes)>0 or len(self.keys)>0:
            self.dump_locations()
        if len(self.measurements)>0 or len(self.keys)>0:
            self.dump_measurements()

    def dump_locations(self):
        """
        Dump the nodes into the temporary tables
        """
        logger.debug(f"Dumping {len(self.nodes)} nodes")
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                start_time = time()

                cursor.execute(get_query(
                    "temp_locations_dump.sql",
                    table="TEMP TABLE" if settings.USE_TEMP_TABLES else "TABLE"
                ))

                write_csv(
                    cursor,
                    self.keys,
                    f"staging_keys",
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
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM staging_keys)
                    """
                )
                connection.commit()

                write_csv(
                    cursor,
                    self.nodes,
                    "staging_sensornodes",
                    [
                        "ingest_id",
                        "site_name",
                        "matching_method",
                        "source_name",
                        "source_id",
                        "ismobile",
                        "geom",
                        "metadata",
                        "fetchlogs_id",
                    ],
                )

                write_csv(
                    cursor,
                    self.systems,
                    "staging_sensorsystems",
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
                    "staging_sensors",
                    [
                        "ingest_id",
                        "ingest_sensor_systems_id",
                        "measurand",
                        "units",
                        "logging_interval_seconds",
                        "averaging_interval_seconds",
                        "metadata",
                        "fetchlogs_id",
                    ],
                )
                connection.commit()

                # and now we load all the nodes,systems and sensors
                query = get_query("etl_process_nodes.sql")
                cursor.execute(query)

                for notice in connection.notices:
                    logger.debug(notice)

                cursor.execute(
                    """
                    UPDATE fetchlogs
                    SET completed_datetime = clock_timestamp()
                    , last_message = NULL
                    WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM staging_keys)
                    """
                )

                connection.commit()
                logger.info("dump_locations: locations: %s; time: %0.4f", len(self.nodes), time() - start_time)
                for notice in connection.notices:
                    logger.debug(notice)


    def dump_measurements(self):
        logger.debug(f"Dumping {len(self.measurements)} measurements")
        with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                start_time = time()

                cursor.execute(get_query(
                    "temp_measurements_dump.sql",
                    table="TEMP TABLE" if settings.USE_TEMP_TABLES else 'TABLE'
                ))

                iterator = StringIteratorIO(
                    (to_tsv(line) for line in self.measurements)
                )
                cursor.copy_expert(
                    """
                    COPY staging_measurements (ingest_id, source_name, source_id, measurand, value, datetime, lon, lat, fetchlogs_id)
                    FROM stdin;
                    """,
                    iterator,
                )

                # process the measurements
                logger.info(f'processing {len(self.measurements)} measurements');
                query = get_query("etl_process_measurements.sql")
                try:
                    cursor.execute(query)
                    connection.commit()
                    logger.info("dump_measurements: measurements: %s; time: %0.4f", len(self.measurements), time() - start_time)
                    for notice in connection.notices:
                        logger.debug(notice)

                except Exception as err:
                    logger.error(err)

    def load(self, data = {}):
        if "meta" in data.keys():
            self.load_metadata(data.get('meta'))
        if "locations" in data.keys():
            self.load_locations(data.get('locations'))
        if "measures" in data.keys():
            self.load_measurements(data.get('measures'))

    def load_keys(self, rows):
        # for each fetchlog we need to read and load
        for row in rows:
            key = row[1]
            fetchlogs_id = row[0]
            last_modified = row[2]
            self.load_key(key, fetchlogs_id, last_modified)


    def load_key(self, key, fetchlogs_id, last_modified):
        logger.debug(f"Loading key: {fetchlogs_id}//:{key}")
        is_csv = bool(re.search(r"\.csv(.gz)?$", key))
        is_json = bool(re.search(r"\.(nd)?json(.gz)?$", key))
        self.fetchlogs_id = fetchlogs_id

        # is it a local file? This is used for dev
        # but likely fine to leave in
        if os.path.exists(os.path.expanduser(key)):
            content = get_file(os.path.expanduser(key)).read()
        else:
            content = select_object(key)

        logger.debug(f"Read content containing {len(content)} lines: {is_json}")

        if is_csv:
            # all csv data will be measurements
            for rw in csv.reader(content.split("\n")):
                self.add_measurement(rw)
        elif is_json:
            # all json data should just be parsed and loaded
            data = orjson.loads(content)
            self.load(data)
        else:
            raise Exception('No idea what to do')

        # add the key to the table to update
        self.keys.append({"key": key, "last_modified": last_modified, "fetchlogs_id": fetchlogs_id})


    def load_metadata(self, meta):
        if "source" in meta.keys():
            self.source = meta.get('source')
        if "matching_method" in meta.keys():
            self.matching_method = meta.get('matching_method')
        if "schema" in meta.keys():
            self.schema = meta.get('schema')

    def load_locations(self, locations):
        for loc in locations:
            self.add_node(loc)

    def load_measurements(self, measurements):
        logger.debug(f'Loading {len(measurements)} measurements')
        for meas in measurements:
            self.add_measurement(meas)
        logger.debug(f'Loaded measurements')


    def add_sensor(self, j, system_id, fetchlogsId):
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
                elif key == "interval_seconds":
                    sensor["logging_interval_seconds"] = value
                    sensor["averaging_interval_seconds"] = value
                else:
                    metadata[key] = value
            if not sensor.get('measurand'):
                # get it from the ingest id
                ingest_arr = sensor.get('ingest_id').split('-')
                sensor['measurand'] = ingest_arr[-1]
            sensor["metadata"] = orjson.dumps(metadata).decode()
            self.sensors.append(sensor)

    def add_systems(self, j, node_id, fetchlogsId):
        for s in j:
            system = {}
            metadata = {}
            if "sensor_system_id" in s:
                id = s.get("sensor_system_id")
            elif "system_id" in s:
                id = s.get("system_id")
            else:
                id = node_id
            system["ingest_sensor_nodes_id"] = node_id
            system["ingest_id"] = id
            system["fetchlogs_id"] = fetchlogsId
            for key, value in s.items():
                key = str.replace(key, "sensor_system_", "")
                if key == "sensors":
                    self.add_sensor(value, id, fetchlogsId)
                else:
                    metadata[key] = value
            system["metadata"] = orjson.dumps(metadata).decode()
            self.systems.append(system)

    def add_node(self, j):
        fetchlogs_id = j.get('fetchlogs_id', self.fetchlogs_id)
        node = { "fetchlogs_id": fetchlogs_id }
        metadata = {}
        mp = self.node_map

        for k, v in j.items():
            # pass the whole measure
            col, value = self.process(k, j, self.node_map)
            if col is not None:
                node[col] = value
            else:
                if not k in ['systems','sensor_system']:
                    metadata[k] = v

        # make sure we actually have data to add
        if len(node.keys())>0:
            # check for id
            ingest_id = node.get('ingest_id')
            if ingest_id is None:
                raise Exception('Missing ingest id')

            ingest_arr = ingest_id.split('-')
            # source name could be set explicitly
            # or in the ingest id
            # or in the metadata
            if node.get('source_name') is None:
                if len(ingest_arr)>1:
                    node['source_name'] = ingest_arr[0]
                elif self.source is not None:
                    node['source_name'] = self.source
                else:
                    raise Exception('Could not find source name')

            # support ingest id that is just the source id
            if node.get('source_id') is None:
                if len(ingest_arr)>1:
                    node['source_id'] = ingest_arr[1]
                else:
                    node['source_id'] = ingest_arr[0]

            if node.get('matching_method') is None:
                node['matching_method'] = self.matching_method

            # prevent adding the node more than once
            # this does not save processing time of course
            if ingest_id not in self.node_ids:
                node["metadata"] = orjson.dumps(metadata).decode()
                self.node_ids[ingest_id] = True
                self.nodes.append(node)
            # now look for systems
            if "sensor_system" in j.keys():
                self.add_systems(j.get('sensor_system'), node.get('ingest_id'), node.get('fetchlogs_id'))
            elif "systems" in j.keys():
                self.add_systems(j.get("systems"), node.get('ingest_id'), node.get('fetchlogs_id'))
            else:
                # no systems
                logger.debug(j.keys())
        else:
            logger.warning('nothing mapped to node')


    def add_measurement(self, m):
        # create a row with
        # ingest_id,datetime,value,lon,lat
        # where ingest id will be what links to the sensor
        meas = {}
        lat = None
        lon = None

        # csv method
        if isinstance(m, list):
            if len(m) < 3:
                logger.warning(f'Not enough data in list value: {m}')
                return

            fetchlogs_id = self.fetchlogs_id
            ingest_id = m[0]
            value = m[1]
            # using the same key/data format as below
            datetime = to_timestamp('dt', {"dt": m[2]})
            if len(m) == 5:
                lat = m[3]
                lon = m[4]

        elif isinstance(m, dict):
            for k, v in m.items():
                # pass the whole measure
                col, value = self.process(k, m, self.measurement_map)
                if col is not None:
                    meas[col] = value

            ingest_id = meas.get('ingest_id')
            datetime = meas.get('datetime')
            value = meas.get('value')
            lon = meas.get('lon', None)
            lat = meas.get('lat', None)
            fetchlogs_id = m.get('fetchlogs_id', self.fetchlogs_id)

        # parse the ingest id here
        ingest_arr = ingest_id.split('-')
        if len(ingest_arr) < 3:
            logger.warning(f'Not enough information in ingest-id: `{ingest_id}`')
            return

        source_name = ingest_arr[0]
        source_id = ingest_arr[1]
        measurand = ingest_arr[2]

        if not None in [ingest_id, datetime, source_name, source_id, measurand]:
            self.measurements.append([ingest_id, source_name, source_id, measurand, value, datetime, lon, lat, fetchlogs_id])


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






def create_staging_table(cursor):
	# table and batch are used primarily for testing
	cursor.execute(get_query(
		"etl_staging_v2.sql",
		table="TEMP TABLE" if settings.USE_TEMP_TABLES else 'TABLE'
	))

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


def load_measurements_db(
    limit=250,
    ascending: bool = False,
    pattern = '^lcs-etl-pipeline/measures/.*\\.(csv|json)'
    ):
    rows = load_fetchlogs(pattern, limit, ascending)
    load_measurements(rows)
    return len(rows)


# Keep seperate from above so we can test rows not from the database
def load_measurements(rows):
    logger.debug(f"loading {len(rows)} measurements")
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
