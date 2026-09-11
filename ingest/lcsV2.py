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
import geohash
import traceback

from psycopg2.extras import Json
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    )

from typing import (
    Any,
    )

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


## app = typer.Typer()
## dir_path = os.path.dirname(os.path.realpath(__file__))
VERBOSE_LEVEL = 5
logging.addLevelName(VERBOSE_LEVEL, "VERBOSE")

logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)

MULTIPLIERS = {
    "seconds": 1,
    "minutes": 60,
    "hours": 3600,
    "days": 86400,
}

class SourceResponse(BaseModel):
    source_name: str
    message: str | None = None
    records: int = 0
    fetchlogs_id: int | None = None
    locations: int | None = None
    systems: int | None = None
    sensors: int | None = None
    flags: int | None = None
    datetime_from: datetime | None = None
    datetime_to: datetime | None = None
    duration_seconds: float | None = None
    started_on: datetime | None = None
    finished_on: datetime | None = None
    exported_on: datetime | None = None
    errors: dict | None = None
    parameters: list | None = None
    boundary: list | None = None

    @field_validator('duration_seconds')
    @classmethod
    def replace_empty_string(cls, v: Any):
        if v == '':
            v = None
        return v

    @field_validator('boundary')
    @classmethod
    def validate_boundary(cls, v: Any):
        if v is None:
            return None
        if len(v) != 4:
            raise ValueError("boundary must be [west, south, east, north]")
        return v  # keep as list; convert at DB boundary



def to_geometry(key, data):
    # could be passed as lat/lng or coordinates
    # initialize for later checks
    lat = lon = None

    if key == 'coordinates':
        data = data.get(key)
        if data is None:
            raise Exception('Missing value for coordinates')

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

def to_seconds(key, data):
    param = data.get(key)
    if param is None:
        return
    unit = param.get('unit', '')
    multiplier = MULTIPLIERS.get(unit)
    if param and multiplier:
        value = param.get('value')
        return int(value * multiplier)

def to_sensorid(key, data):
    param = data.get(key)
    delim = "-"
    location = data.get('location')
    source = data.get('sourceName')
    return delim.join([source, location, param])

def to_nodeid(key, data):
    delim = "-"
    location = data.get(key)
    source = data.get('sourceName')
    return delim.join([source, location])


class IngestClient:
    def __init__(
        self, key=None, fetchlogs_id=None, resources=None
    ):
        self.key = key
        self.fetchlogs_id = fetchlogs_id
        self.keys = []
        self.st = datetime.now().replace(tzinfo=pytz.UTC)

        #self.systems = []
        self.flags = []

        self.nodes: dict[str, dict] = {}
        self.systems: dict[str, dict] = {}
        self.sensors: dict[str, dict] = {}

        self.measurements = []
        self.matching_method = 'ingest-id'
        self.source = None
        self.schema = None
        self.delim = "-"

        # Resource management via resources
        if resources:
            self.resources = resources
            self._owns_resources = False
        else:
            self.resources = Resources()
            self._owns_resources = True


        self.node_map = {
            "fetchlogs_id": {},
            "site_name": {},
            "source_name": {},
            "source_id": {}, ## ADDED
            "site_id": { "col":"source_id" },
            "ismobile": {},
            "key": {"col":"ingest_id"},
            "ingest_id": {},
            "location": {"col":"ingest_id"},
            "sensor_node_id": {"col":"ingest_id"},
            "interval_seconds": {},
            "logging_interval_secs": {},
            "averaging_interval_secs": {},
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
            "source_name": {},
            "source_id": {},
            "measurand": {},
            "parameter": {"col": "ingest_id", "func": to_sensorid },
            "timestamp": {"col": "datetime", "func": to_timestamp },
            "datetime": {"col": "datetime", "func": to_timestamp },
            "date": {"col": "datetime", "func": to_timestamp },
            "coordinates": {"col":"geom","func": to_geometry },
            "averagingPeriod": {"col":"data_averaging_period_seconds", "func": to_seconds},
            "measure": {"col": "value"},
            "value": {},
            "units": {},
            "unit": {"col": "units"},
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
        logger.debug(f"Dumping {len(self.nodes)} nodes using {db_table} (TEMP: {settings.USE_TEMP_TABLES}, LOADING: {load})")
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
                self.nodes.values(),
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
                self.systems.values(),
                "staging_sensorsystems",
                [
                    "ingest_id",
                    "manufacturer_key",
                    "model_key",
                    "instrument_ingest_id",
                    "ingest_sensor_nodes_id",
                    "metadata",
                    "fetchlogs_id",
                ],
            )

            logger.debug(f"Adding {len(self.sensors)} sensors to staging")
            write_csv(
                cursor,
                self.sensors.values(),
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
                COPY staging_measurements (ingest_id, source_name, node_source_id, system_source_id, measurand, units, value, datetime, lon, lat, fetchlogs_id)
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
                    if notice.startswith("WARNING"):
                        logger.warn(f"etl_process_measurements {notice.rstrip()}")
                    else:
                        logger.info(f"etl_process_measurements {notice.rstrip()}")

        self.close()


    def load(self, data = {}):
        if "meta" in data.keys():
            logger.debug("loading metada")
            self.load_metadata(data.get('meta'), data.get('errors'))
        if "locations" in data.keys():
            logger.debug("loading locations")
            self.load_locations(data.get('locations'))
        if "measures" in data.keys():
            logger.debug("loading measurements")
            self.load_measurements(data.get('measures'))
        # transform is currently using the measurements key
        if "measurements" in data.keys():
            logger.debug("loading measurements")
            self.load_measurements(data.get('measurements'))


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
        # logger.info(os.path.expanduser(key))
        if os.path.exists(os.path.expanduser(key)):
            content = get_file(os.path.expanduser(key)).read()
        else:
            content = get_object(key)


        if is_csv:
            # all csv data will be measurements
            self.schema = None
            self.delim = "-"
            for rw in csv.reader(content.split("\n")):
                self.add_measurement(rw)
        elif is_ndjson:
            ## instead of reshaping the data lets just loop through and add
            ## each node, system and sensor as we go
            measures = []
            locations = []
            ingest_ids = []
            self.schema = None
            self.delim = "-"
            for idx, obj in enumerate(content.split('\n')):
                try:
                    if obj != "":
                        nd = orjson.loads(obj)
                        ## this will be used as the node and system ingest id
                        coords = nd.get('coordinates', {})
                        if None in [coords.get('latitude'), coords.get('longitude')]:
                            logger.debug('Missing coordinates')
                            continue
                        geo = geohash.encode(coords.get('latitude'), coords.get('longitude'), 9)
                        source_id = nd.get("id", geo)
                        source_name = nd.get("sourceName")
                        ingest_id = self.delim.join([source_name, source_id])
                        sensor_ingest_id = self.delim.join([ingest_id, nd.get('parameter')])
                        interval_seconds = to_seconds('averagingPeriod', nd)
                        units = nd.get("unit", "")
                        parameter = f"{nd.get("parameter", "")}"
                        if ingest_id not in self.nodes:
                            attributes = nd.get('attribution', [{}])[0]
                            self.add_node({
                                "ingestMatchingMethod": "source-spatial",
                                "source_name": source_name,
                                "source_id": source_id,
                                "site_name": nd.get("location"),#attributes.get("name"),
                                "coordinates": coords,
                                "ismobile": nd.get("mobile"),
                                "ingest_id": ingest_id,
                                "systems": [{
                                    "key":  ingest_id,
                                    "sensors": [{
                                        "key": sensor_ingest_id,
                                        "units": units,
                                        "parameter": f"{parameter}",
                                        "interval_seconds": interval_seconds
                                    }]
                                }]
                            })
                        ## Systems will be the same for all sensors for one node
                        ## but its possible that we may have already added the node but
                        ## not this specific parameter/sensor
                        if sensor_ingest_id not in self.sensors:
                            self.add_sensors([{
                                "key": sensor_ingest_id,
                                "units": units,
                                "parameter": parameter,
                                "interval_seconds": interval_seconds
                            }], ingest_id, fetchlogs_id)
                        ## all measurements should be added
                        self.add_measurement({
                            "ingest_id": sensor_ingest_id,
                            "source_name": source_name,
                            "node_source_id": source_id,
                            "system_source_id": source_id,
                            "date": nd.get("date"),
                            "measurand": parameter,
                            "unit": units,
                            "value": nd.get("value"),
                            "averagingPeriod": nd.get("averagingPeriod"),
                        })
                except Exception as e:
                    logger.error(f"LOADING NDJSON: {ingest_id} - {e}")
                    logger.error(nd)
                    logger.error(traceback.format_exc())
                    raise

        elif is_json:
            # all json data should just be parsed and loaded
            data = orjson.loads(content)
            if isinstance(data, dict):
                self.load(data)
            elif isinstance(data, list):
                for d in data:
                    self.load(d)
            else:
                raise Exception(f'Not sure what to do with json data of type {type(data)}')
        else:
            raise Exception('Not sure how to read file')

        # add the key to the table to update
        self.keys.append({"key": key, "last_modified": last_modified, "fetchlogs_id": fetchlogs_id})


    def load_metadata(self, meta, errors):
        if "source" in meta.keys():
            self.source = meta.get('source')
        if "sourceName" in meta.keys():
            self.source = meta.get('sourceName')
        if "ingestMatchingMethod" in meta.keys():
            self.matching_method = meta.get('ingestMatchingMethod')
        if "matching_method" in meta.keys():
            self.matching_method = meta.get('matching_method')
        if "schema" in meta.keys():
            self.schema = meta.get('schema')
            if self.schema == "v0.1":
                self.delim = "/"

        self.insert_metadata(meta, errors)


    def insert_metadata(self, meta, errors):
        sql = """
            INSERT INTO fetcher_responses (
              source_name
            , fetchlogs_id
            , message
            , records
            , locations
            , sensors
            , systems
            , flags
            , started_on
            , finished_on
            , exported_on
            , datetime_from
            , datetime_to
            , duration_seconds
            , errors
            , parameters
            , boundary
            )
            VALUES(
              %(source_name)s
            , %(fetchlogs_id)s
            , %(message)s
            , %(records)s
            , %(locations)s
            , %(sensors)s
            , %(systems)s
            , %(flags)s
            , %(started_on)s
            , %(finished_on)s
            , %(exported_on)s
            , %(datetime_from)s
            , %(datetime_to)s
            , %(duration_seconds)s
            , %(errors)s
            , %(parameters)s
            , CASE WHEN %(boundary)s IS NULL THEN NULL
                 ELSE ST_MakeEnvelope(
                     (%(boundary)s::float[])[1],  -- west
                     (%(boundary)s::float[])[2],  -- south
                     (%(boundary)s::float[])[3],  -- east
                     (%(boundary)s::float[])[4],  -- north
                     4326
                 )
            END
            );
            """

        data = meta.get("fetchSummary", {})
        mdl = SourceResponse(
            source_name = self.source or meta.get('sourceId'),
            fetchlogs_id = self.fetchlogs_id,
            message = meta.get("schema","transform"),
            locations = data.get("locations"),
            sensors = data.get("sensors"),
            systems = data.get("systems"),
            flags = data.get("flags"),
            records = data.get("measurements",0),
            datetime_from = data.get("datetimeFrom"),
            datetime_to = data.get("datetimeTo"),
            started_on = data.get("startedOn"),
            finished_on = data.get("finishedOn"),
            exported_on = data.get("exportedOn"),
            boundary = data.get("bounds"),
            errors = errors,
        )

        connection = self.get_connection(True)
        with connection.cursor() as cursor:
            params = mdl.model_dump()
            for k in ("errors", "parameters"):
                if params[k] is not None:
                    params[k] = Json(params[k])
            cursor.execute(sql, params)

        self.close()


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
            sensor["status"] = 'active' ## default to active

            if "sensor_id" in s:
                id = s.get("sensor_id")
            elif "key" in s:
                id = s.get("key")
            elif "id" in s:
                id = s.get("id")
            else:
                id = system_id

            if id in self.sensors:
                # would it make more sense to merge or skip or throw error?
                # merge and submit a warning maybe?
                continue

            sensor["ingest_id"] = id

            logger.debug(f"Adding sensor {id}")
            for key, value in s.items():
                key = str.replace(key, "sensor_", "")
                if key == "flags":
                    self.add_flags(value, id, fetchlogsId)
                elif key in ["measurand_parameter", "parameter"]:
                    sensor["measurand"] = value
                elif key in ["units", "measurand_unit"]:
                    sensor["units"] = fix_units(value)
                elif key == "status":
                    sensor["status"] = value
                elif key == "averaging_interval_secs" and value:
                    sensor["averaging_interval_seconds"] = value
                elif key == "logging_interval_secs" and value:
                    sensor["logging_interval_seconds"] = value
                elif key == "interval_seconds" and value:
                    sensor["logging_interval_seconds"] = value
                    sensor["averaging_interval_seconds"] = value
                elif key not in ["sensor_id","key","id"]:
                    metadata[key] = value

            if not sensor.get('measurand'):
                # get it from the ingest id
                ingest_arr = sensor.get('ingest_id').split(self.delim)
                sensor['measurand'] = ingest_arr[-1] # take the last one
            sensor["metadata"] = orjson.dumps(metadata).decode()
            if id not in self.sensors:
                self.sensors[id] = sensor


    def add_flags(self, flags, sensor_id, fetchlogsId, dt=None):
        for f in flags:
            flag = {}
            metadata = {}
            flag["sensor_ingest_id"] = sensor_id
            flag["fetchlogs_id"] = fetchlogsId

            # normalize string flags to dict
            if isinstance(f, str):
                f = {
                    "flag_id": f"{f}",
                    "note": f,
                }
            elif isinstance(f, dict):
                pass
            else:
                continue

            for key, value in f.items():
                key = str.replace(key, "flag_", "")
                if key == "id":
                    flag["ingest_id"] = value
                elif key == "flag":
                    flag["ingest_id"] = f"{value}"
                    if "note" not in f:
                        flag["note"] = value
                elif key == "starts":
                    flag["datetime_from"] = value
                elif key == "ends":
                    flag["datetime_to"] = value
                elif key == "datetime_from":
                    flag["datetime_from"] = value
                elif key == "datetime_to":
                    flag["datetime_to"] = value
                elif key == "note":
                    flag["note"] = value
                else:
                    metadata[key] = value

            # fall back to the passed datetime if not set by the flag itself
            if "datetime_from" not in flag:
                flag["datetime_from"] = dt
            if "datetime_to" not in flag:
                flag["datetime_to"] = dt

            flag["metadata"] = orjson.dumps(metadata).decode()
            self.flags.append(flag)

    # def add_flags(self, flags, sensor_id, fetchlogsId, sensor_nodes_id: int = None):
    #     for f in flags:
    #         flag = {}
    #         metadata = {}
    #         flag["sensor_ingest_id"] = sensor_id
    #         flag["sensor_node_ingest_id"] = sensor_nodes_id
    #         flag["fetchlogs_id"] = fetchlogsId
    #         for key, value in f.items():
    #             key = str.replace(key, "flag_", "")
    #             if key == "id":
    #                 v = str.replace(value, f"{sensor_id}-", "")
    #                 flag["ingest_id"] = v

    #             elif key == 'datetime_from':
    #                 flag["datetime_from"] = value
    #             elif key == 'datetime_to':
    #                 flag["datetime_to"] = value
    #             elif key == 'note':
    #                 flag["note"] = value
    #             else:
    #                 metadata[key] = value

    #         flag["metadata"] = orjson.dumps(metadata).decode()
    #         self.flags.append(flag)

    def add_systems(self, j, node_id, fetchlogsId):
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

            if id in self.systems:
                # would it make more sense to merge or skip or throw error?
                continue

            ingest_arr = id.split(self.delim)
            # this will not work with a uuid passed as a site id
            if len(ingest_arr) == 3:
                instrument = (ingest_arr[-1]).split('::')
                system['manufacturer_key'] = instrument[0]
                system['model_key'] = instrument[1] if len(instrument)>1 else 'default'
                system["instrument_ingest_id"] = ingest_arr[-1];

            system["ingest_sensor_nodes_id"] = node_id

            system["ingest_id"] = id
            system["fetchlogs_id"] = fetchlogsId
            for key, value in s.items():
                key = str.replace(key, "sensor_system_", "")
                if key == "sensors":
                    self.add_sensors(value, id, fetchlogsId)
                elif key == 'manufacturer_name':
                    system['manufacturer_key'] = s.get('manufacturer_name')
                elif key == 'model_name':
                    system['model_key'] = s.get('model_name')
                elif key == 'instrument':
                    instrument = s.get('instrument').split('::')
                    system['manufacturer_key'] = instrument[0]
                    system['model_key'] = instrument[1] if len(instrument)>1 else 'default'
                elif key not in ["sensor_system_id","key","system_id"]:
                    metadata[key] = value

            system["metadata"] = orjson.dumps(metadata).decode()

            if system.get('manufacturer_key') in (None,'default'):
                system['manufacturer_key'] = ingest_arr[0]

            if 'model_key' not in system.keys():
                system['model_key'] = 'default'

            logger.debug(f"Adding system {id}")

            self.systems[id] = system


    def add_node(self, j):
        fetchlogs_id = j.get('fetchlogs_id', self.fetchlogs_id)
        node = { "fetchlogs_id": fetchlogs_id }
        metadata = {}
        #mp = self.node_map
        for k, v in j.items():
            # pass the whole measure
            if k not in ['systems','sensor_system','flags']:
                col, value = self.process(k, j, self.node_map)
                if col is not None:
                    node[col] = value
                else:
                    metadata[k] = v

        # make sure we actually have data to add
        if len(node.keys())>0:
            # check for id
            ingest_id = node.get('ingest_id')
            if ingest_id is None:
                logger.error(f'Missing ingest id {node}')
                raise Exception('Missing ingest id')

            ingest_arr = ingest_id.split(self.delim)

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
                    node['source_id'] = self.delim.join(ingest_arr[1:len(ingest_arr)])
                else:
                    node['source_id'] = ingest_arr[0]

            if node.get('matching_method') is None:
                node['matching_method'] = self.matching_method

            # check for flags
            self.add_flags(j.get('flags',[]), ingest_id, fetchlogs_id)

            # prevent adding the node more than once
            # this does not save processing time of course
            if ingest_id not in self.nodes:
                logger.debug(f"Adding node {ingest_id} / {node.get('geom')}")
                node["metadata"] = orjson.dumps(metadata).decode()
                self.nodes[ingest_id] = node
            # now look for systems
            if "sensor_system" in j.keys():
                self.add_systems(j.get('sensor_system'), node.get('ingest_id'), node.get('fetchlogs_id'))
            elif "systems" in j.keys():
                self.add_systems(j.get("systems"), node.get('ingest_id'), node.get('fetchlogs_id'))
            else:
                # no systems
                self.add_systems([{}], node.get('ingest_id'), node.get('fetchlogs_id'))


        else:
            logger.warning('nothing mapped to node')


    def add_measurement(self, m):
        # create a row with
        # ingest_id,datetime,value,lon,lat
        # where ingest id will be what links to the sensor
        meas = {}
        lat = None
        lon = None
        units = None
        source_name = None
        node_source_id = None
        system_source_id = None
        measurand = None

        # csv method
        if isinstance(m, list):
            if len(m) < 3:
                logger.warning(f'Not enough data in list value - {self.fetchlogs_id}: {m}')
                return

            fetchlogs_id = self.fetchlogs_id
            ingest_id = m[0]
            value = m[1]
            # using the same key/data format as below
            datetime = to_timestamp('dt', {"dt": m[2]})
            if len(m) == 5:
                lon = m[3]
                lat = m[4]

        elif isinstance(m, dict):
            for k, v in m.items():
                # pass the whole measure
                col, value = self.process(k, m, self.measurement_map)
                logger.log(VERBOSE_LEVEL, f"Mapping data: {k}/{v} = {col}/{value}")
                ## Do not overwrite something that already exists
                if col is not None and meas.get(col) is None:
                    meas[col] = value

            ingest_id = meas.get('ingest_id')
            datetime = meas.get('datetime')
            value = meas.get('value')
            measurand = meas.get('measurand')
            units = meas.get('units')
            source_name = meas.get('source_name')
            node_source_id = meas.get('node_source_id')
            system_source_id = meas.get('system_source_id')

            if units is None:
                ## if the data is new and the sensor exists
                ## not sure its worth doing this, lets revisit after the etl updates
                units = self.sensors.get(ingest_id, {}).get('units')

            lon = meas.get('lon', None)
            lat = meas.get('lat', None)
            fetchlogs_id = m.get('fetchlogs_id', self.fetchlogs_id)

            self.add_flags(m.get('flags', []), ingest_id, fetchlogs_id, datetime)

        if ingest_id is None:
            raise Exception(f"Could not find ingest id in {meas}")

        # parse the ingest id here only if we need it
        if None in [source_name, node_source_id, system_source_id, measurand]:
            ingest_arr = ingest_id.split(self.delim)
            if len(ingest_arr) < 3:
                logger.warning(f'Not enough information in ingest-id: `{ingest_id}`')
                return

            if source_name is None:
                source_name = ingest_arr[0] ## first one
            if measurand is None:
                measurand = ingest_arr[-1]  ## last one
            if system_source_id is None: ## this is the system source id
                system_source_id = self.delim.join(ingest_arr[1:len(ingest_arr)-1])  ## all the middle ones
            if node_source_id is None:
                if self.schema is None:
                    node_source_id = self.delim.join(ingest_arr[1:len(ingest_arr)-1])  ## all the middle ones
                else:
                    node_source_id = ingest_arr[1]

        if not None in [ingest_id, datetime, source_name, node_source_id, system_source_id,  measurand]:
            ## this is to solve a realtime issue
            if ingest_id not in self.sensors:
                ## I need to look up the node
                node_ingest_id = self.delim.join([source_name, node_source_id])
                node = self.nodes.get(node_ingest_id, {})
                self.add_sensors([{
                    "key": ingest_id,
                    "averaging_interval_secs": node.get("averaging_interval_secs"),
                    "logging_interval_secs": node.get("logging_interval_secs")
                }], node_ingest_id, fetchlogs_id)
            logger.debug(f"Adding measurement: {source_name}|{system_source_id}|{measurand}|{units}")
            self.measurements.append([ingest_id, source_name, node_source_id, system_source_id, measurand, units, value, datetime, lon, lat, fetchlogs_id])
        else:
            logger.warning(f"Something was not set {[ingest_id, datetime, source_name, node_source_id, system_source_id, measurand]} - {ingest_arr} - {m}")


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


    def summary(self) -> dict:
        """Return a summary of what's currently loaded in the client.

        Useful for reporting before/after dump operations, in tests,
        or in any tool that wants to inspect client state.

        Returns:
            dict with counts and sample rows for nodes, systems, sensors,
            measurements, and flags.
        """
        return {
            "keys": len(self.keys),
            "nodes": len(self.nodes),
            "systems": len(self.systems),
            "sensors": len(self.sensors),
            "measurements": len(self.measurements),
            "flags": len(self.flags),
            "sample_nodes": list(self.nodes.values())[:5],
            "sample_measurements": self.measurements[:10],
        }

    def staging_counts(self, connection=None) -> dict:
        """Return counts from staging tables.

        Args:
            connection: optional psycopg2 connection; defaults to client's own.

        Returns:
            dict with counts for staging_sensornodes, staging_sensorsystems,
            staging_sensors, staging_measurements, plus matched_nodes and
            rejects for this client's fetchlogs_id.
        """
        conn = connection or self.get_connection(True)
        counts = {}
        with conn.cursor() as cursor:
            for table in ("staging_sensornodes", "staging_sensorsystems",
                          "staging_sensors", "staging_measurements"):
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM staging_sensornodes
                WHERE sensor_nodes_id IS NOT NULL AND NOT is_new
            """)
            counts["matched_nodes"] = cursor.fetchone()[0]

            cursor.execute("""
                SELECT MIN(datetime), MAX(datetime)
                FROM staging_measurements
            """)
            counts["staging_date_range"] = cursor.fetchone()

            if self.fetchlogs_id is not None:
                cursor.execute(
                    "SELECT COUNT(*) FROM rejects WHERE fetchlogs_id = %s",
                    (self.fetchlogs_id,),
                )
                counts["rejects"] = cursor.fetchone()[0]

        return counts


    def stats(self, connection=None, elapsed_sec=None) -> dict:
        """Return per-fetchlog stats from staging + fetchlog row.

        Must be called before staging tables are dropped or the
        transaction is closed. All counts are scoped to this
        client's fetchlogs_id.

        Args:
            connection: optional psycopg2 connection; defaults to client's own.
            elapsed_sec: optional processing time to include in the result.

        Returns:
            dict with client counts, staging counts (added/matched/unmatched
            for nodes, systems, sensors), reject count, fetchlog row status,
            and (if provided) elapsed time.
        """
        conn = connection or self.get_connection(True)
        result = {
            "fetchlogs_id": self.fetchlogs_id,
            "client_nodes": len(self.nodes),
            "client_systems": len(self.systems),
            "client_sensors": len(self.sensors),
            "client_measurements": len(self.measurements),
            "client_flags": len(self.flags),
            "elapsed_sec": elapsed_sec,
        }

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE is_new) AS added,
                    COUNT(*) FILTER (WHERE NOT is_new
                                     AND sensor_nodes_id IS NOT NULL) AS matched,
                    COUNT(*) FILTER (WHERE sensor_nodes_id IS NULL) AS unmatched
                FROM staging_sensornodes
                WHERE fetchlogs_id = %s
            """, (self.fetchlogs_id,))
            row = cursor.fetchone()
            result["nodes_added"] = row[0]
            result["nodes_matched"] = row[1]
            result["nodes_unmatched"] = row[2]

            cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE is_new) AS added,
                    COUNT(*) FILTER (WHERE NOT is_new
                                     AND sensor_systems_id IS NOT NULL) AS matched,
                    COUNT(*) FILTER (WHERE sensor_systems_id IS NULL) AS unmatched
                FROM staging_sensorsystems
                WHERE fetchlogs_id = %s
            """, (self.fetchlogs_id,))
            row = cursor.fetchone()
            result["systems_added"] = row[0]
            result["systems_matched"] = row[1]
            result["systems_unmatched"] = row[2]

            cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE is_new) AS added,
                    COUNT(*) FILTER (WHERE NOT is_new
                                     AND sensors_id IS NOT NULL) AS matched,
                    COUNT(*) FILTER (WHERE sensors_id IS NULL) AS unmatched
                FROM staging_sensors
                WHERE fetchlogs_id = %s
            """, (self.fetchlogs_id,))
            row = cursor.fetchone()
            result["sensors_added"] = row[0]
            result["sensors_matched"] = row[1]
            result["sensors_unmatched"] = row[2]

            cursor.execute("""
                SELECT COUNT(*), MIN(datetime), MAX(datetime)
                FROM staging_measurements
                WHERE fetchlogs_id = %s
            """, (self.fetchlogs_id,))
            row = cursor.fetchone()
            result["measurements_staged"] = row[0]
            result["measurement_date_range"] = (row[1], row[2])

            cursor.execute(
                "SELECT COUNT(*) FROM rejects WHERE fetchlogs_id = %s",
                (self.fetchlogs_id,),
            )
            result["rejects"] = cursor.fetchone()[0]

            cursor.execute("""
                SELECT last_message, has_error, completed_datetime, loaded_datetime
                FROM fetchlogs WHERE fetchlogs_id = %s
            """, (self.fetchlogs_id,))
            row = cursor.fetchone()
            if row:
                result["fetchlog_message"] = row[0]
                result["fetchlog_has_error"] = row[1]
                result["fetchlog_completed"] = row[2]
                result["fetchlog_loaded"] = row[3]

        return result

#################################################################################################
############################## END OF IngestClient ##############################################
#################################################################################################
