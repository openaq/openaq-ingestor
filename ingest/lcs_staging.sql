DROP TABLE IF EXISTS
  ms_sensornodes
, ms_sensorsystems
, ms_sensors
, ms_versions
, meas
, keys
, temp_inserted_measurements;

CREATE {table} IF NOT EXISTS ms_sensornodes (
    sensor_nodes_id int,
    ingest_id text,
    site_name text,
    source_name text,
    source_id text,
    ismobile boolean,
    geom geometry,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS ms_sensorsystems (
    sensor_systems_id int,
    ingest_id text,
    ingest_sensor_nodes_id text,
    sensor_nodes_id int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS ms_sensors (
    ingest_id text,
    sensors_id int,
    sensor_systems_id int,
    ingest_sensor_systems_id text,
    measurand text,
    units text,
    measurands_id int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS meas (
    ingest_id text,
    sensors_id int,
    value float,
    datetime timestamptz,
    lon float,
    lat float,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS keys (
    fetchlogs_id int
    , key text
    , last_modified timestamptz
    );

-- This table will hold measurements that have
-- actually been inserted into the measurements table
-- this is to deal with the overlap that we see in the
-- incoming files
CREATE {table} IF NOT EXISTS temp_inserted_measurements (
  sensors_id int
  , datetime timestamptz
  , value double precision
  , lat double precision
  , lon double precision
  , fetchlogs_id int
);
