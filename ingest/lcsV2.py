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
from .resources import Resources
from .utils import (
    get_query,
    clean_csv_value,
    StringIteratorIO,
    fix_units,
    get_logs_from_pattern,
    load_fetchlogs,
    get_object,
    get_file,
    write_csv,
    load_fail,
)

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
    # initialize for later checks
    lat = lon = None

    if key == 'coordinates':
        data = data.get(key)

    if 'lat' in data:
        lat = data.get('lat')
    elif 'latitude' in data:
        lat = data.get('latitude')

    if 'lon' in data:
        lon = data.get('lon')
    elif 'longitude' in data:
        lon = data.get('longitude')

    srid = data.get('srid', '4326')

    if None in [lat, lon]:
        raise Exception('Missing value for coordinates')

    return f"SRID={srid};POINT({lon} {lat})"

def to_timestamp(key, data):
    dt = data.get(key)
    value = None

    ## to handle the old realtime methods which passed a dict with utc/local back
    if isinstance(dt, dict) and 'utc' in dt.keys():
        dt = dt.get('utc')

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

    return dt.isoformat()

def to_sensorid(key, data):
    param = data.get(key)
    location = data.get('location')
    source = data.get('sourceName')
    return f"{source}-{location}-{param}"

def to_nodeid(key, data):
    location = data.get(key)
    source = data.get('sourceName')
    return f"{source}-{location}"


class IngestClient:
    def __init__(
        self, key=None, fetchlogs_id=None, resources=None
    ):
        self.key = key
        self.fetchlogs_id = fetchlogs_id
        self.keys = []
        self.st = datetime.now().replace(tzinfo=pytz.UTC)
        self.sensors = []
        self.systems = []
        self.flags = []
        self.nodes = []
        self.node_ids = []
        self.system_ids = []
        self.sensor_ids = []
        self.measurements = []
        self.matching_method = 'ingest-id'
        self.source = None

        # Resource management via resources
        if resources:
            self.resources = resources
            self._owns_resources = False
        else:
            self.resources = Resources()
            self._owns_resources = True

        self.node_map = {
            "fetchlogs_id": {},
            "site_name": { "col":"site_name" },
            "source_name": {},
            "ismobile": {},
            "key": {"col":"ingest_id"},
            "ingest_id": {},
            "location": {"col":"ingest_id"},
            "sensor_node_id": {"col":"ingest_id"},
            "ingestMatchingMethod": {"col":"matching_method"},
            "matching_method": {},
            "label": {"col":"site_name"},
            "coordinates": {"col":"geom","func": to_geometry },
            "geometry": {"col":"geom", "func": to_geometry },
            "lat": {"col":"geom","func": to_geometry },
            "lon": {"col":"geom","func": to_geometry },
            }
        self.measurement_map = {
            "sensor_id": {"col": "ingest_id"},
            "ingest_id": {},
            "parameter": {"col": "ingest_id", "func": to_sensorid },
            "timestamp": {"col": "datetime", "func": to_timestamp },
            "datetime": {"col": "datetime", "func": to_timestamp },
            "date": {"col": "datetime", "func": to_timestamp },
            "coordinates": {"col":"geom","func": to_geometry },
            "measure": {"col": "value"},
            "value": {},
            "lat": {},
            "lon": {},
            "key":{"col": "ingest_id"},
            }


    def get_connection(self, autocommit: bool = True):
      """Get database connection via resources."""
      return self.resources.get_connection(autocommit=autocommit)

    @property
    def connection(self):
      """Get connection from resources."""
      return self.resources._connection

    def close(self):
      """Close resources if we own it."""
      if self._owns_resources:
          self.resources.close()

    def commit(self):
      """Commit resources if we own it."""
      if self._owns_resources:
          self.resources.commit()


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

    def dump(self, load: bool = True):
        """
        Dump any data that is currenly loaded into the database
        We will dump if there is data OR if we have loaded any keys
        We do this because its possible that a file is empty but we
        need to run the dump method to get the file to be marked as finished
        """
        logger.debug(f"Dumping data from {len(self.keys)} files")
        if len(self.nodes)>0 or len(self.keys)>0:
            self.dump_locations(load)
        if len(self.measurements)>0 or len(self.keys)>0:
            self.dump_measurements(load)

    def dump_locations(self, load: bool = True):
        """
        Dump the nodes into the temporary tables
        """
        db_table = "TEMP TABLE" if (settings.USE_TEMP_TABLES and load) else "TABLE"
        logger.debug(f"Dumping {len(self.nodes)} nodes using {db_table} ({settings.USE_TEMP_TABLES}|{load})")
        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            start_time = time()
            cursor.execute(get_query(
                "temp_locations_dump.sql",
                table=db_table
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

            logger.debug(f"Adding {len(self.nodes)} nodes to staging")
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

            logger.debug(f"Adding {len(self.systems)} systems to staging")
            write_csv(
                cursor,
                self.systems,
                "staging_sensorsystems",
                [
                    "ingest_id",
                    "instrument_ingest_id",
                    "ingest_sensor_nodes_id",
                    "metadata",
                    "fetchlogs_id",
                ],
            )

            logger.debug(f"Adding {len(self.sensors)} sensors to staging")
            write_csv(
                cursor,
                self.sensors,
                "staging_sensors",
                [
                    "ingest_id",
                    "ingest_sensor_systems_id",
                    "measurand",
                    "units",
                    "status",
                    "logging_interval_seconds",
                    "averaging_interval_seconds",
                    "metadata",
                    "fetchlogs_id",
                ],
            )

            logger.debug(f"Adding {len(self.flags)} flags to staging")
            write_csv(
                cursor,
                self.flags,
                "staging_flags",
                [
                    "ingest_id",
                    "sensor_ingest_id",
                    "datetime_from",
                    "datetime_to",
                    "note",
                    "metadata",
                    "fetchlogs_id",
                ],
            )

            # and now we load all the nodes,systems and sensors
            if load:
                query = get_query("etl_process_nodes.sql")
                cursor.execute(query)

            for notice in connection.notices:
                logger.debug(f"etl_process_nodes {notice.rstrip()}")

            cursor.execute(
                """
                UPDATE fetchlogs
                SET completed_datetime = clock_timestamp()
                , last_message = NULL
                WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM staging_keys)
                """
            )

            logger.info("dump_locations: locations: %s; time: %0.4f, %s fetchlog(s)", len(self.nodes), time() - start_time, cursor.rowcount)

        self.close()



    def dump_measurements(self, load: bool = True):
        db_table = "TEMP TABLE" if (settings.USE_TEMP_TABLES and load) else "TABLE"
        logger.debug(f"Dumping {len(self.measurements)} measurements using {db_table} ({settings.USE_TEMP_TABLES}|{load})")
        connection = self.get_connection(True)

        with connection.cursor() as cursor:
            start_time = time()

            cursor.execute(get_query(
                "temp_measurements_dump.sql",
                table=db_table
            ))

            iterator = StringIteratorIO(
                ("\t".join(map(clean_csv_value, line)) + "\n" for line in self.measurements)
            )
            cursor.copy_expert(
                """
                COPY staging_measurements (ingest_id, source_name, source_id, measurand, value, datetime, lon, lat, fetchlogs_id)
                FROM stdin;
                """,
                iterator,
            )

            if load:
                logger.info(f'processing {len(self.measurements)} measurements');
                query = get_query("etl_process_measurements.sql")

                cursor.execute(query)
                logger.info("dump_measurements: measurements: %s; time: %0.4f", len(self.measurements), time() - start_time)

                for notice in connection.notices:
                    logger.debug(f"etl_process_measurements {notice.rstrip()}")

        self.close()


    def load(self, data = {}):
        if "meta" in data.keys():
            logger.debug("loading metada")
            self.load_metadata(data.get('meta'))
        if "locations" in data.keys():
            logger.debug("loading locations")
            self.load_locations(data.get('locations'))
        if "measures" in data.keys():
            logger.debug("loading measurements")
            self.load_measurements(data.get('measures'))


    # def reset(self):
    #     """
    #     Reset the client to the new state. Mostly for testing purposes
    #     """
    #     logger.debug("Reseting the client data")
    #     self.measurements = []
    #     self.nodes = []
    #     self.systems = []
    #     self.sensors = []
    #     self.flags = []
    #     self.keys = []
    #     self.key = None
    #     self.fetchlogs_id = None
    #     self.node_ids = []
    #     self.system_ids = []
    #     self.sensor_ids = []


    def load_keys(self, rows):
        # for each fetchlog we need to read and load
        for row in rows:
            key = row[1]
            fetchlogs_id = row[0]
            last_modified = row[2]
            try:
                self.load_key(key, fetchlogs_id, last_modified)
            except Exception as e:
                with self.get_connection(True).cursor() as cursor:
                    load_fail(cursor, fetchlogs_id, e);


    def load_key(self, key, fetchlogs_id, last_modified):
        logger.debug(f"Loading key: {fetchlogs_id}//:{key}")
        is_csv = bool(re.search(r"\.csv(.gz)?$", key))
        is_json = bool(re.search(r"\.json(.gz)?$", key))
        is_ndjson = bool(re.search(r"\.ndjson(.gz)?$", key))
        self.fetchlogs_id = fetchlogs_id

        # is it a local file? This is used for dev
        # but likely fine to leave in
        logger.info(os.path.expanduser(key))
        if os.path.exists(os.path.expanduser(key)):
            content = get_file(os.path.expanduser(key)).read()
        else:
            content = get_object(key)

        if is_json:
            logger.debug(f"Read JSON containing {len(content)} characters")
        else:
            logger.debug(f"Read CSV containing {len(content)} lines")

        if is_csv:
            # all csv data will be measurements
            for rw in csv.reader(content.split("\n")):
                self.add_measurement(rw)
        elif is_ndjson:
            logger.debug(len(content.split('\n')))
            measures = []
            for obj in content.split('\n'):
                if obj != "":
                    measures.append(orjson.loads(obj))
            data = { "measures": measures }
            self.load(data)
        elif is_json:
            # all json data should just be parsed and loaded
            data = orjson.loads(content)
            self.load(data)
        else:
            raise Exception('Not sure how to read file')

        # add the key to the table to update
        self.keys.append({"key": key, "last_modified": last_modified, "fetchlogs_id": fetchlogs_id})


    def load_metadata(self, meta):
        if "source" in meta.keys():
            self.source = meta.get('sourceName')
        if "ingestMatchingMethod" in meta.keys():
            self.matching_method = meta.get('ingestMatchingMethod')
        if "schema" in meta.keys():
            self.schema = meta.get('schema')

    def load_locations(self, locations):
        for loc in locations:
            self.add_node(loc)

    def load_measurements(self, measurements):
        logger.debug(f'Loading {len(measurements)} measurements')
        for meas in measurements:
            self.add_measurement(meas)


    def add_sensors(self, j, system_id, fetchlogsId):
        for s in j:
            sensor = {}
            metadata = {}
            sensor["ingest_sensor_systems_id"] = system_id
            sensor["fetchlogs_id"] = fetchlogsId

            if "sensor_id" in s:
                id = s.get("sensor_id")
            elif "key" in s:
                id = s.get("key")
            elif "id" in s:
                id = s.get("id")
            else:
                id = system_id

            if id in self.sensor_ids:
                # would it make more sense to merge or skip or throw error?
                # merge and submit a warning maybe?
                continue

            sensor["ingest_id"] = id

            logger.debug(f"Adding sensor {s.get('key')}")
            for key, value in s.items():
                key = str.replace(key, "sensor_", "")
                if key == "flags":
                    self.add_flags(value, id, fetchlogsId)
                elif key == "measurand_parameter":
                    sensor["measurand"] = value
                elif key == "measurand_unit":
                    sensor["units"] = fix_units(value)
                elif key == "status":
                    sensor["status"] = value
                elif key == "interval_seconds":
                    sensor["logging_interval_seconds"] = value
                    sensor["averaging_interval_seconds"] = value
                else:
                    metadata[key] = value
            if not sensor.get('measurand'):
                # get it from the ingest id
                ingest_arr = sensor.get('ingest_id').split('-')
                sensor['measurand'] = ingest_arr[-1] # take the last one
            sensor["metadata"] = orjson.dumps(metadata).decode()
            self.sensors.append(sensor)
            self.sensor_ids.append(id)

    def add_flags(self, flags, sensor_id, fetchlogsId):
        for f in flags:
            flag = {}
            metadata = {}
            flag["sensor_ingest_id"] = sensor_id
            flag["fetchlogs_id"] = fetchlogsId
            for key, value in f.items():
                key = str.replace(key, "flag_", "")
                if key == "id":
                    v = str.replace(value, f"{sensor_id}-", "")
                    flag["ingest_id"] = v

                elif key == 'datetime_from':
                    flag["datetime_from"] = value
                elif key == 'datetime_to':
                    flag["datetime_to"] = value
                elif key == 'note':
                    flag["note"] = value
                else:
                    metadata[key] = value

            flag["metadata"] = orjson.dumps(metadata).decode()
            self.flags.append(flag)

    def add_systems(self, j, node_id, fetchlogsId):
        logger.debug(f'adding system')
        for s in j:
            system = {}
            metadata = {}
            if "sensor_system_id" in s:
                id = s.get("sensor_system_id")
            elif "system_id" in s:
                id = s.get("system_id")
            elif "key" in s:
                id = s.get("key")
            else:
                id = node_id

            if id in self.system_ids:
                # would it make more sense to merge or skip or throw error?
                continue

            ingest_arr = id.split('-')
            # this will not work with a uuid passed as a site id
            if len(ingest_arr) == 3:
                system["instrument_ingest_id"] = ingest_arr[-1];

            system["ingest_sensor_nodes_id"] = node_id
            system["ingest_id"] = id
            system["fetchlogs_id"] = fetchlogsId
            for key, value in s.items():
                key = str.replace(key, "sensor_system_", "")
                if key == "sensors":
                    self.add_sensors(value, id, fetchlogsId)
                else:
                    metadata[key] = value
            system["metadata"] = orjson.dumps(metadata).decode()

            self.systems.append(system)
            self.system_ids.append(id)

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
                logger.error(f'Missing ingest id {node}')
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
                    # updated to handle uuid
                    node['source_id'] = '-'.join(ingest_arr[1:len(ingest_arr)])
                else:
                    node['source_id'] = ingest_arr[0]

            if node.get('ingestMatchingMethod') is None:
                node['matching_method'] = self.matching_method

            # prevent adding the node more than once
            # this does not save processing time of course
            if ingest_id not in self.node_ids:
                node["metadata"] = orjson.dumps(metadata).decode()
                self.node_ids.append(ingest_id)
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
                logger.debug(f"Mapping data: {k}/{v} = {col}/{value}")
                if col is not None:
                    meas[col] = value

            ingest_id = meas.get('ingest_id')
            datetime = meas.get('datetime')
            value = meas.get('value')
            lon = meas.get('lon', None)
            lat = meas.get('lat', None)
            fetchlogs_id = m.get('fetchlogs_id', self.fetchlogs_id)

        # parse the ingest id here
        if ingest_id is None:
            raise Exception(f"Could not find ingest id in {meas}")

        ingest_arr = ingest_id.split('-')
        if len(ingest_arr) < 3:
            logger.warning(f'Not enough information in ingest-id: `{ingest_id}`')
            return

        source_name = ingest_arr[0]
        source_id = '-'.join(ingest_arr[1:len(ingest_arr)-1])
        measurand = ingest_arr[-1]

        if not None in [ingest_id, datetime, source_name, source_id, measurand]:
            self.measurements.append([ingest_id, source_name, source_id, measurand, value, datetime, lon, lat, fetchlogs_id])



    def refresh_cached_tables(self):
        """
        Refresh the cached tables that we use for most production endpoints.
        Right now this is just for testing purposes
        """
        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            logger.debug("Refreshing the cached tables")
            cursor.execute("REFRESH MATERIALIZED VIEW locations_view_cached;")
            cursor.execute("REFRESH MATERIALIZED VIEW locations_manufacturers_cached;")
            cursor.execute("REFRESH MATERIALIZED VIEW locations_latest_measurements_cached;")
            cursor.execute("REFRESH MATERIALIZED VIEW providers_view_cached;")
            cursor.execute("REFRESH MATERIALIZED VIEW countries_view_cached;")
            cursor.execute("REFRESH MATERIALIZED VIEW parameters_view_cached;")

        self.close()


    def process_hourly_data(self,n: int = 1000):
        """
        Process any pending hourly data rollups.
        Right now this is just for testing purposes
        """
        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT datetime, tz_offset FROM fetch_hourly_data_jobs(%s)", (n,))
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute("SELECT update_hourly_data(%s, %s)", row)

        self.close()


    def process_daily_data(self,n: int = 500):
        """
        Process any pending daily data rollups.
        Right now this is just for testing purposes
        """
        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT datetime, tz_offset FROM fetch_daily_data_jobs(%s)", (n,))
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute("SELECT update_daily_data(%s, %s)", row)

        self.close()


    def process_annual_data(self,n: int = 25):
        """
        Process any pending annual data rollups.
        Right now this is just for testing purposes
        """
        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT datetime, tz_offset FROM fetch_annual_data_jobs(%s)", (n,))
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute("SELECT update_annual_data(%s, %s)", row)

        self.close()


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


#################################################################################################
############################## END OF IngestClient ##############################################
#################################################################################################
