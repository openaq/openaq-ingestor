
CREATE {table} IF NOT EXISTS staging_keys (
  fetchlogs_id int,
  key text,
  last_modified timestamptz
);

CREATE {table} IF NOT EXISTS staging_sensornodes (
    sensor_nodes_id int,
    is_new boolean DEFAULT true,
    is_moved boolean DEFAULT false,
    ingest_id text NOT NULL UNIQUE,
    source_name text NOT NULL,
    source_id text NOT NULL,
    matching_method text NOT NULL DEFAULT 'ingest-id',
    site_name text,
    ismobile boolean,
    geom geometry,
    timezones_id int,
    countries_id int,
    metadata jsonb,
    fetchlogs_id int,
    UNIQUE (source_name, source_id)
);

CREATE {table} IF NOT EXISTS staging_sensorsystems (
    sensor_systems_id int,
    is_new boolean DEFAULT true,
    ingest_id text NOT NULL UNIQUE,
    manufacturer_key text,
    model_key text,
    instrument_ingest_id text,
    ingest_sensor_nodes_id text,
    sensor_nodes_id int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS staging_sensors (
    ingest_id text,
    is_new boolean DEFAULT true,
   -- source_name text NOT NULL,
   -- source_id text NOT NULL,
    sensors_id int,
    sensor_systems_id int,
    ingest_sensor_systems_id text,
    status text,
    measurand text,
    units text,
    measurands_id int,
    averaging_interval_seconds int,
    logging_interval_seconds int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS staging_flags (
    ingest_id text, --NOT NULL,
    sensor_ingest_id text NOT NULL,
    flags_id int,
    sensor_nodes_id int,
    sensors_id int,
    flag_types_id int,
    datetime_from timestamptz,
    datetime_to timestamptz,
    period tstzrange,
    note text,
    metadata jsonb,
    fetchlogs_id int
);


CREATE {table} IF NOT EXISTS staging_measurements (
    ingest_id text NOT NULL,
    source_name text NOT NULL,
    node_source_id text NOT NULL,
    system_source_id text NOT NULL,
    measurand text NOT NULL,
    units text,   -- the current units of the measurement
    units_id int, -- the current units_id for the measurement
    sensors_id int,
    sensor_averaging_interval interval,
    measurands_id int,
    value float,
    value_original float,
    datetime_from timestamptz,
    datetime timestamptz,
    lon float,
    lat float,
    fetchlogs_id int,
    note text
);

--This table will hold measurements that have
--actually been inserted into the measurements table
--this is to deal with the overlap that we see in the
--incoming files
CREATE {table} IF NOT EXISTS staging_inserted_measurements (
  sensors_id int
  , datetime timestamptz
  , value double precision
  , value_original double precision
  , lat double precision
  , lon double precision
  , fetchlogs_id int
);
