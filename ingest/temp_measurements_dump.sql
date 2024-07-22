DROP TABLE IF EXISTS
  staging_sensors
, staging_measurements
, staging_inserted_measurements;


CREATE {table} IF NOT EXISTS staging_sensors (
    ingest_id text NOT NULL,
    is_new boolean DEFAULT true,
    source_name text NOT NULL,
    source_id text NOT NULL,
    measurand text NOT NULL,
    sensors_id int,
    sensor_systems_id int,
    ingest_sensor_systems_id text,
    units text,
    measurands_id int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS staging_measurements (
    ingest_id text NOT NULL,
    source_name text NOT NULL,
    source_id text NOT NULL,
    measurand text NOT NULL,
    sensors_id int,
    value float,
    datetime timestamptz,
    lon float,
    lat float,
    fetchlogs_id int
);

--This table will hold measurements that have
--actually been inserted into the measurements table
--this is to deal with the overlap that we see in the
--incoming files
CREATE {table} IF NOT EXISTS staging_inserted_measurements (
  sensors_id int
  , datetime timestamptz
  , value double precision
  , lat double precision
  , lon double precision
  , fetchlogs_id int
);
