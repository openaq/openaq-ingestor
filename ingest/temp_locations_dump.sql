DROP TABLE IF EXISTS
  staging_sensornodes
, staging_sensorsystems
, staging_sensors
, staging_keys;

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
    ingest_sensor_nodes_id text,
    sensor_nodes_id int,
    metadata jsonb,
    fetchlogs_id int
);

CREATE {table} IF NOT EXISTS staging_sensors (
    ingest_id text,
    is_new boolean DEFAULT true,
    sensors_id int,
    sensor_systems_id int,
    ingest_sensor_systems_id text,
    measurand text,
    units text,
    measurands_id int,
    metadata jsonb,
    fetchlogs_id int
);
