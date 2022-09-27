CREATE TEMP TABLE IF NOT EXISTS tempfetchdata (
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

CREATE TEMP TABLE IF NOT EXISTS ingestfiles(
    key text
);

-- This table will hold measurements that have
-- actually been inserted into the measurements table
-- this is to deal with the overlap that we see in the
-- incoming files
CREATE TEMP TABLE IF NOT EXISTS temp_inserted_measurements (
  sensors_id int,
  datetime timestamptz
);
