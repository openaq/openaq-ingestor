-- DROP TABLE IF EXISTS tempfetchdata
-- , temp_inserted_measurements
-- , tempfetchdata_nodes
-- , tempfetchdata_sensors
-- , tempfetchdata_sensors_clean;

CREATE {table} IF NOT EXISTS tempfetchdata (
    fetchlogs_id int,
    location text,
    value float,
    unit text,
    parameter text,
    country text,
    city text,
    data jsonb,
    source_name text,
    datetime timestamptz,
    coords geography,
    source_type text,
    mobile boolean,
    avpd_unit text,
    avpd_value float,
    tfdid int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sensors_id int
);

--CREATE {table} IF NOT EXISTS ingestfiles(
--    key text
--);

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
