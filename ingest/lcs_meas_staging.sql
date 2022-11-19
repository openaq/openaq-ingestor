CREATE TEMP TABLE meas (
    ingest_id text,
    sensors_id int,
    value float,
    --datetime timestamptz,
    datetime text,
    lon float,
    lat float,
    fetchlogs_id int
);
CREATE TEMP TABLE keys (key text, last_modified timestamptz);
