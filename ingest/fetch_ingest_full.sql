-- fetch_ingest_full
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__min_measurement_date date := '1970-01-01'::date;
__max_measurement_date date := current_date + 1;
__total_measurements int;
__total_nodes int;
__updated_nodes int;
__inserted_nodes int;
__inserted_sensors int;
__inserted_measurements int;
__inserted_measurands int;
__rejected_nodes int := 0;
__rejected_systems int;
__rejected_sensors int;
__rejected_measurements int := 0;
__start_datetime timestamptz;
__end_datetime timestamptz;
__inserted_start_datetime timestamptz;
__inserted_end_datetime timestamptz;
__deleted_timescaledb int;
__deleted_future_measurements int;
__deleted_past_measurements int;
__exported_days int;
__process_time_ms int;
__insert_time_ms int;
__node_time_ms int;
__cache_time_ms int;
__ingest_method text := 'realtime';
__inserted_spatial_rollups int := 0;
BEGIN

-- REQUIRED
-- {table} should be `TEMP TABLE` in production but could be changed to
-- just `TABLE` if you are debugging and want the temp tables to persist

---------------------------
-- File fetch_filter.sql --
---------------------------

-- this seems questionable, I dont want to pass data to this
-- process only to have some of it filtered out because its too old
-- Commenting this out because it will prevent us from submitting patch
-- data that spans more than two days.
-- WITH deletes AS (
--   DELETE
--   FROM tempfetchdata
--   WHERE datetime < (SELECT max(datetime) - '2 days'::interval from tempfetchdata)
--   RETURNING 1)
-- SELECT COUNT(1) INTO __deleted_past_measurements
-- FROM deletes;

-- use the partitions to determine start and end date
SELECT partition_start_date
	, partition_end_date
INTO __min_measurement_date
	, __max_measurement_date
FROM data_table_stats
WHERE table_name = 'public.measurements';

---------------------------------
-- start with simple count
SELECT COUNT(1)
, MIN(datetime)
, MAX(datetime)
INTO __total_measurements
, __start_datetime
, __end_datetime
FROM tempfetchdata
WHERE datetime <= now();

-- Now we start the old fetch_ingest#.sql files
-------------
-- File #1 --
-------------
CREATE {table} IF NOT EXISTS tempfetchdata_sensors AS
WITH t AS (
SELECT DISTINCT
    location as site_name,
    unit as units,
    parameter as measurand,
    country,
    city,
    jsonb_merge_agg(data) as data,
    source_name,
    coords::geometry as geom,
    source_type,
    mobile as ismobile,
    avpd_unit,
    avpd_value,
--    coords::geometry as cgeom,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::int as measurands_id,
    null::int as sensors_id,
    null::jsonb as node_metadata,
    null::jsonb as sensor_metadata,
    array_agg(tfdid) as tfdids,
    fetchlogs_id
FROM tempfetchdata
GROUP BY
    location,
    unit,
    parameter,
    country,
    city,
    coords,
    source_type,
    source_name,
    mobile,
    avpd_unit,
    avpd_value,
    sensor_nodes_id,
    sensor_systems_id,
    measurands_id,
    sensors_id,
    node_metadata,
    sensor_metadata,
    fetchlogs_id
)
SELECT row_number() over () as tfsid, *
FROM t;

CREATE INDEX ON tempfetchdata_sensors (tfsid);
-------------
-- File #2 --
-------------

-- Cleanup fields

UPDATE tempfetchdata_sensors t
SET geom = NULL
WHERE st_x(geom) = 0
AND st_y(geom) = 0;

UPDATE tempfetchdata_sensors
SET units  = 'µg/m³'
WHERE units IN ('µg/m��','��g/m³', 'ug/m3');

UPDATE tempfetchdata_sensors
SET node_metadata =
    jsonb_strip_nulls(
        COALESCE(data, '{{}}'::jsonb)
        ||
        jsonb_build_object(
            'source_type', 'government',
            'origin','openaq',
            'fetchlogs_id', fetchlogs_id
            )
    ),
    -- the following assumes that avpd_unit is always hours
    -- which at the last check (2022-12-07) it was
sensor_metadata = jsonb_strip_nulls(jsonb_build_object(
    'data_averaging_period_seconds', avpd_value * 3600
    ))
;

-------------
-- File #3 --
-------------

CREATE {table} IF NOT EXISTS tempfetchdata_nodes AS
SELECT * FROM (SELECT
    site_name,
    source_name,
    country,
    city,
    node_metadata::jsonb as metadata,
    ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::boolean as added,
    null::text as method,
    st_centroid(st_collect(geom)) as geom,
    array_agg(tfsid) as tfsids
    , array_agg(st_astext(geom)) as points
    , COUNT(DISTINCT st_astext(geom)) as n_points
FROM tempfetchdata_sensors
WHERE geom IS NOT NULL
GROUP BY
    1,2,3,4,5,6,7,8,9,st_snaptogrid(geom, .00001)
) AS wgeom
UNION ALL
SELECT * FROM
(SELECT
    site_name,
    source_name,
    country,
    city,
    node_metadata::jsonb as metadata,
    ismobile,
    null::int as sensor_nodes_id,
    null::int as sensor_systems_id,
    null::boolean as added,
    null::text as method,
    null::geometry as geom,
    array_agg(tfsid) as tfsids
    , null::text[] as points
    , 0 as n_points
FROM tempfetchdata_sensors
WHERE geom IS NULL
AND site_name IS NOT NULL
AND source_name IS NOT NULL
GROUP BY
    1,2,3,4,5,6,7,8,9,10
) as nogeom
;

SELECT COUNT(1)
INTO __total_nodes
FROM tempfetchdata_nodes;

-------------
-- File #4 --
-------------

-- Lookup Node Ids

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id
, added = FALSE
, method = 'spatial'
FROM sensor_nodes sn
WHERE t.geom IS NOT NULL
AND st_dwithin(sn.geom, t.geom, .0001)
AND origin='OPENAQ';

UPDATE tempfetchdata_nodes t
SET sensor_nodes_id = sn.sensor_nodes_id
, added = FALSE
, method = 'source_id'
FROM sensor_nodes sn
WHERE t.sensor_nodes_id is null
AND t.site_name is not null
AND t.source_name is not null
AND t.site_name = sn.site_name
AND t.source_name=sn.source_name
AND t.geom IS NULL
AND origin='OPENAQ';


__process_time_ms := 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

-------------
-- File #5 --
-------------

--DROP TABLE IF EXISTS checkrealtime_matched;
-- CREATE TABLE IF NOT EXISTS checkrealtime_matched (
--   sensor_nodes_id int
-- , site_name text
-- , source_name text
-- , city text
-- , country text
-- , origin text
-- , method text
-- , geom_old geometry
-- , geom_new geometry
-- , added_on timestamptz DEFAULT now()
-- );


-- INSERT INTO checkrealtime_matched
-- SELECT t.sensor_nodes_id
-- , format('%s -> %s', s.site_name, t.site_name)
-- , format('%s -> %s', s.source_name, t.source_name)
-- , format('%s -> %s', s.city, t.city)
-- , format('%s -> %s', s.country, t.country)
-- , origin
-- , method
-- , s.geom
-- , t.geom
-- FROM tempfetchdata_nodes t
-- JOIN sensor_nodes s ON (t.sensor_nodes_id = s.sensor_nodes_id)
-- WHERE ROW(
--         t.site_name,
--         t.source_name,
--         t.city,
--         t.country,
--         t.metadata
--     ) IS DISTINCT FROM (
--         s.site_name,
--         s.source_name,
--         s.city,
--         s.country,
--         s.metadata - 'timezone'
--     );

--  SELECT sensor_nodes_id
--  , method
--  , site_name
--  , source_name
--  , city
--  , country
--  , ROUND(st_distancesphere(geom_new, geom_old)::numeric, 1) as distance
--  FROM checkrealtime_matched
-- WHERE st_distancesphere(geom_new, geom_old) > 0
-- GROUP BY 1,2,3,4,5,6, 7
-- LIMIT 100;

-- Update any records that have changed
WITH updates AS (
UPDATE sensor_nodes s SET
    site_name = COALESCE(t.site_name, s.site_name),
    source_name = COALESCE(t.source_name, s.source_name),
    city = COALESCE(t.city, s.city),
    country = COALESCE(t.country, s.country),
    ismobile = COALESCE(t.ismobile, s.ismobile),
    metadata = COALESCE(s.metadata, '{{}}'::jsonb) || t.metadata,
    geom = COALESCE(t.geom, s.geom)
    --, timezones_id = get_timezones_id(COALESCE(t.geom, s.geom))
    , providers_id = get_providers_id(COALESCE(t.source_name, s.source_name))
    , modified_on = now()
FROM tempfetchdata_nodes t
WHERE t.sensor_nodes_id = s.sensor_nodes_id
AND
(
    (s.geom IS NULL and t.geom IS NOT NULL)
OR

    ROW(
        t.sensor_nodes_id,
        t.ismobile,
        t.site_name,
        t.source_name,
        t.city,
        t.country,
        t.metadata - ARRAY['imported','fetchlogs_id']::text[]
    ) IS DISTINCT FROM (
        s.sensor_nodes_id,
        s.ismobile,
        s.site_name,
        s.source_name,
        s.city,
        s.country,
        s.metadata - ARRAY['imported','fetchlogs_id']::text[]
    )
)
RETURNING 1)
SELECT COUNT(1) INTO __updated_nodes
FROM updates;


-- SELECT s.sensor_nodes_id
-- , t.site_name
-- , s.site_name
-- , t.metadata - ARRAY['imported','fetchlogs_id']::text[] as temp
-- , s.metadata - ARRAY['imported','fetchlogs_id']::text[] as node
-- FROM tempfetchdata_nodes t
-- JOIN sensor_nodes s ON (t.sensor_nodes_id = s.sensor_nodes_id)
-- WHERE (s.geom IS NULL and t.geom IS NOT NULL)
-- OR
--     ROW (
--        t.sensor_nodes_id,
--       --  t.ismobile,
--       --  t.site_name,
--       --  t.source_name,
--       --  t.city,
--        -- t.country,
--         t.metadata - ARRAY['imported','fetchlogs_id']::text[]
--     ) IS DISTINCT FROM (
--        s.sensor_nodes_id,
--       --  s.ismobile,
--       --  s.site_name,
--       --  s.source_name,
--       --  s.city,
--       -- s.country,
--         s.metadata - ARRAY['imported','fetchlogs_id']::text[]
--     )
-- LIMIT 20;

-- SELECT h.site_name
-- , n.site_name
-- , st_astext(h.geom)
-- , st_astext(n.geom)
-- , h.origin
-- , n.origin
-- , h.metadata - ARRAY['imported','fetchlogs_id']::text[] as history
-- , n.metadata - ARRAY['imported','fetchlogs_id']::text[] as current
-- FROM sensor_nodes_history h
-- JOIN sensor_nodes n USING (sensor_nodes_id)
-- WHERE created > now() - '2min'::interval;

-- SELECT source_name
-- , COALESCE(jsonb_array_length(metadata->'attribution'), 0) as attributes
-- , COUNT(1) as n
-- FROM sensor_nodes
-- GROUP BY 1,2
-- ORDER BY 2 DESC
-- LIMIT 500;

-------------
-- File #6 --
-------------

-- Create new nodes where they don't exist
WITH sn AS (
INSERT INTO sensor_nodes (
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile,
    origin,
    timezones_id,
    providers_id,
    countries_id
)
SELECT
    site_name,
    metadata,
    geom,
    source_name,
    city,
    country,
    ismobile,
    'OPENAQ'
    , get_timezones_id(geom)
    , get_providers_id(source_name)
    , get_countries_id(geom)
FROM tempfetchdata_nodes t
WHERE t.sensor_nodes_id is NULL
RETURNING *
), inserted AS (
UPDATE tempfetchdata_nodes tf SET
 sensor_nodes_id = sn.sensor_nodes_id
 , added = TRUE
FROM sn
WHERE tf.sensor_nodes_id is null
and row(tf.site_name, tf.geom, tf.source_name) is not distinct from row(sn.site_name, sn.geom, sn.source_name)
)
SELECT COUNT(1) INTO __inserted_nodes
FROM sn;

-------------
-- File #7 --
-------------

UPDATE tempfetchdata_nodes t
SET sensor_systems_id = ss.sensor_systems_id
FROM sensor_systems ss
WHERE t.sensor_nodes_id = ss.sensor_nodes_id;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp(), 'sensor_nodes', to_jsonb(tf)
  FROM tempfetchdata_nodes tf
  WHERE sensor_nodes_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_nodes
FROM inserts;

DELETE
FROM tempfetchdata_nodes
WHERE sensor_nodes_id IS NULL;

-- create sensor systems that don't exist
WITH ss AS (
INSERT INTO sensor_systems (sensor_nodes_id)
SELECT DISTINCT sensor_nodes_id FROM tempfetchdata_nodes t
WHERE t.sensor_systems_id is NULL AND t.sensor_nodes_id IS NOT NULL
RETURNING *
) UPDATE tempfetchdata_nodes tf
SET sensor_systems_id = ss.sensor_systems_id
FROM ss WHERE tf.sensor_nodes_id=ss.sensor_nodes_id
and tf.sensor_systems_id is null;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp(), 'sensor_systems', to_jsonb(tf)
  FROM tempfetchdata_nodes tf
  WHERE sensor_systems_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM inserts;

DELETE
FROM tempfetchdata_nodes
WHERE sensor_systems_id IS NULL;

-- merge sensor node / system ids back to sensors table
UPDATE tempfetchdata_sensors ts SET
    sensor_nodes_id = tn.sensor_nodes_id,
    sensor_systems_id = tn.sensor_systems_id
FROM
    tempfetchdata_nodes tn
WHERE
    ts.tfsid = ANY(tn.tfsids);


-- add any measurands that don't exist
UPDATE tempfetchdata_sensors t
SET measurands_id= m.measurands_id
FROM measurands m
WHERE t.measurand = m.measurand
AND t.units = m.units;

WITH inserts AS (
  INSERT INTO measurands (measurand, units)
  SELECT DISTINCT measurand, units FROM tempfetchdata_sensors t
  WHERE t.measurands_id is NULL
  RETURNING *
), m AS (
  UPDATE tempfetchdata_sensors tf
  SET measurands_id = inserts.measurands_id
  FROM inserts
  WHERE tf.measurand=inserts.measurand
  AND tf.units=inserts.units
  AND tf.measurands_id is null)
SELECT COUNT(1) INTO __inserted_measurands
FROM inserts;

-- get cleaned sensors table
CREATE {table} IF NOT EXISTS tempfetchdata_sensors_clean AS
SELECT
    null::int as sensors_id,
    sensor_nodes_id,
    sensor_systems_id,
    measurands_id,
    jsonb_merge_agg(sensor_metadata) as metadata,
    array_merge_agg(tfdids) as tfdids,
    null::boolean as added
FROM tempfetchdata_sensors
GROUP BY 1,2,3,4;


-- get sensor id
UPDATE tempfetchdata_sensors_clean t
SET sensors_id = s.sensors_id
, added = FALSE
FROM sensors s
WHERE t.sensor_systems_id = s.sensor_systems_id
AND t.measurands_id = s.measurands_id
;

-- Add any rows that did not get an id
-- into the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp()
  , 'systems'
  , to_jsonb(tf)
  FROM tempfetchdata_sensors_clean tf
  WHERE sensor_systems_id IS NULL
  OR measurands_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM inserts;

DELETE
FROM tempfetchdata_sensors_clean
WHERE sensor_systems_id IS NULL
OR measurands_id IS NULL;

-- add any sensors that don't exist
WITH s AS (
    INSERT INTO sensors (
        sensor_systems_id,
        measurands_id,
        metadata
    )
    SELECT
        sensor_systems_id,
        measurands_id,
        metadata
    FROM
        tempfetchdata_sensors_clean tf
    WHERE
        tf.sensors_id IS NULL
    RETURNING *
), u AS (
    UPDATE tempfetchdata_sensors_clean tfc
    SET
        sensors_id = s.sensors_id,
        added = TRUE
    FROM s
    WHERE
        tfc.sensors_id IS NULL
        AND
        s.sensor_systems_id = tfc.sensor_systems_id
        AND
        s.measurands_id = tfc.measurands_id)
 SELECT COUNT(1) INTO __inserted_sensors
 FROM s;

UPDATE tempfetchdata t
SET sensors_id = ts.sensors_id
FROM tempfetchdata_sensors_clean ts
WHERE t.tfdid = ANY(ts.tfdids);

-- Add any rows that did not get an id into
-- the rejects table and then delete
WITH inserts AS (
  INSERT INTO rejects
  SELECT clock_timestamp()
  , 'sensors'
  , to_jsonb(tf)
  FROM tempfetchdata tf
  WHERE sensors_id IS NULL
  RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM inserts;

DELETE
FROM tempfetchdata
WHERE sensors_id IS NULL;

--DELETE
--FROM measurements m
--USING tempfetchdata t
--WHERE m.datetime = t.datetime
--AND m.sensors_id = t.sensors_id;

__node_time_ms := 1000 * (extract(epoch FROM clock_timestamp() - __process_start));
-- restart the clock to measure just inserts
__process_start := clock_timestamp();


-- moved down
-- count the future measurements
SELECT COUNT(1) INTO __deleted_future_measurements
FROM tempfetchdata
WHERE datetime > __max_measurement_date
;

	SELECT COUNT(1) INTO __deleted_past_measurements
FROM tempfetchdata
WHERE datetime < __min_measurement_date
;


WITH inserts AS (
  INSERT INTO measurements (sensors_id, datetime, value)
  SELECT sensors_id
  , datetime
  , value
  FROM tempfetchdata
  WHERE datetime > __min_measurement_date
	AND datetime < __max_measurement_date
  ON CONFLICT DO NOTHING
  RETURNING sensors_id, datetime, value
), inserted as (
   INSERT INTO temp_inserted_measurements (sensors_id, datetime, value)
   SELECT sensors_id
   , datetime
   , value
   FROM inserts
   RETURNING sensors_id, datetime
)
SELECT MIN(datetime)
, MAX(datetime)
, COUNT(1)
INTO __inserted_start_datetime
, __inserted_end_datetime
, __inserted_measurements
FROM inserted;

__insert_time_ms := 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

-- mark the fetchlogs as done
WITH inserted AS (
  SELECT m.fetchlogs_id
  , COUNT(m.*) as n_records
  , COUNT(t.*) as n_inserted
  , MIN(m.datetime) as fr_datetime
  , MAX(m.datetime) as lr_datetime
  , MIN(t.datetime) as fi_datetime
  , MAX(t.datetime) as li_datetime
  FROM tempfetchdata m
  LEFT JOIN temp_inserted_measurements t ON (t.sensors_id = m.sensors_id AND t.datetime = m.datetime AND t.fetchlogs_id = m.fetchlogs_id)
  GROUP BY m.fetchlogs_id)
UPDATE fetchlogs
SET completed_datetime = CURRENT_TIMESTAMP
, inserted = COALESCE(n_inserted, 0)
, records = COALESCE(n_records, 0)
, first_recorded_datetime = fr_datetime
, last_recorded_datetime = lr_datetime
, first_inserted_datetime = fi_datetime
, last_inserted_datetime = li_datetime
FROM inserted
WHERE inserted.fetchlogs_id = fetchlogs.fetchlogs_id;

-- track the time required to update cache tables
__process_start := clock_timestamp();

-- -- Now we can use those temp_inserted_measurements to update the cache tables
-- INSERT INTO sensors_latest (
--   sensors_id
--   , datetime
--   , value
--   )
-- ---- identify the row that has the latest value
-- WITH numbered AS (
--   SELECT sensors_id
--    , datetime
--    , value
--    , row_number() OVER (PARTITION BY sensors_id ORDER BY datetime DESC) as rn
--   FROM temp_inserted_measurements
-- ), latest AS (
-- ---- only insert those rows
--   SELECT sensors_id
--    , datetime
--    , value
--   FROM numbered
--   WHERE rn = 1
-- )
-- SELECT l.sensors_id
-- , l.datetime
-- , l.value
-- FROM latest l
-- LEFT JOIN sensors_latest sl ON (l.sensors_id = sl.sensors_id)
-- WHERE sl.sensors_id IS NULL
-- OR l.datetime > sl.datetime
-- ON CONFLICT (sensors_id) DO UPDATE
-- SET datetime = EXCLUDED.datetime
-- , value = EXCLUDED.value
-- , modified_on = now()
-- --, fetchlogs_id = EXCLUDED.fetchlogs_id
-- ;

-- update the exceedances
INSERT INTO sensor_exceedances (sensors_id, threshold_value, datetime_latest)
  SELECT
  m.sensors_id
  , t.value
  , MAX(datetime)
  FROM temp_inserted_measurements m
  JOIN sensors s ON (m.sensors_id = s.sensors_id)
  JOIN thresholds t ON (s.measurands_id = t.measurands_id)
  AND m.value > t.value
  GROUP BY 1, 2
  ON CONFLICT (sensors_id, threshold_value) DO UPDATE SET
  datetime_latest = GREATEST(sensor_exceedances.datetime_latest, EXCLUDED.datetime_latest)
  , updated_on = now();

INSERT INTO sensors_rollup (
  sensors_id
  , datetime_first
  , datetime_last
  , value_latest
  , value_count
  , value_avg
  , value_min
  , value_max
  )
---- identify the row that has the latest value
WITH numbered AS (
  SELECT sensors_id
   , datetime
   , value
   , sum(1) OVER (PARTITION BY sensors_id) as value_count
   , min(datetime) OVER (PARTITION BY sensors_id) as datetime_min
   , avg(value) OVER (PARTITION BY sensors_id) as value_avg
   , row_number() OVER (PARTITION BY sensors_id ORDER BY datetime DESC) as rn
  FROM temp_inserted_measurements
), latest AS (
---- only insert those rows
  SELECT sensors_id
   , datetime
   , value
   , value_count
   , value_avg
   , datetime_min
  FROM numbered
  WHERE rn = 1
)
SELECT l.sensors_id
, l.datetime_min -- first
, l.datetime -- last
, l.value -- last value
, l.value_count
, l.value_avg
, l.value -- min
, l.value -- max
FROM latest l
LEFT JOIN sensors_rollup sr ON (l.sensors_id = sr.sensors_id)
WHERE sr.sensors_id IS NULL
OR l.datetime > sr.datetime_last
OR l.datetime_min < sr.datetime_first
ON CONFLICT (sensors_id) DO UPDATE
SET datetime_last = GREATEST(sensors_rollup.datetime_last, EXCLUDED.datetime_last)
, value_latest = CASE WHEN EXCLUDED.datetime_last > sensors_rollup.datetime_last
                 THEN EXCLUDED.value_latest
                 ELSE sensors_rollup.value_latest
                 END
, value_count = sensors_rollup.value_count + EXCLUDED.value_count
, value_min = LEAST(sensors_rollup.value_min, EXCLUDED.value_latest)
, value_max = GREATEST(sensors_rollup.value_max, EXCLUDED.value_latest)
, datetime_first = LEAST(sensors_rollup.datetime_first, EXCLUDED.datetime_first)
, modified_on = now()
--, fetchlogs_id = EXCLUDED.fetchlogs_id
;


-- WITH spatial_inserts AS (
-- INSERT INTO sensor_nodes_spatial_rollup (
-- sensor_nodes_id
-- , geom
-- , cell_size
-- , start_datetime
-- , end_datetime
-- , measurements_count
-- , added_on)
-- SELECT sensor_nodes_id
-- , st_snaptogrid(s.geom, 250)
-- , 250
-- , MIN(datetime) as start_datetime
-- , MAX(datetime) as end_datetime
-- , COUNT(DISTINCT datetime) as measurements
-- , now()
-- FROM temp_inserted_measurements
-- JOIN tempfetchdata_sensors s USING (sensors_id)
-- JOIN sensor_systems ss USING (sensor_systems_id)
-- WHERE lat IS NOT NULL
-- AND lon IS NOT NULL
-- GROUP BY 1,2
-- ON CONFLICT (sensor_nodes_id, geom) DO UPDATE SET
--   start_datetime = LEAST(sensor_nodes_spatial_rollup.start_datetime, EXCLUDED.start_datetime)
-- , end_datetime = GREATEST(sensor_nodes_spatial_rollup.end_datetime, EXCLUDED.end_datetime)
-- , measurements_count = sensor_nodes_spatial_rollup.measurements_count + EXCLUDED.measurements_count
-- , modified_on = now()
-- RETURNING 1)
-- SELECT COUNT(1) INTO __inserted_spatial_rollups
-- FROM spatial_inserts;


-- Update the table that will help to track hourly rollups
INSERT INTO hourly_stats (datetime)
  SELECT date_trunc('hour', datetime)
  FROM temp_inserted_measurements
  GROUP BY 1
ON CONFLICT (datetime) DO UPDATE
SET modified_on = now();

-- update the table that will track the daily exports
WITH e AS (
INSERT INTO open_data_export_logs (sensor_nodes_id, day, records, measurands, modified_on)
SELECT sn.sensor_nodes_id
, ((m.datetime - '1sec'::interval) AT TIME ZONE (COALESCE(sn.metadata->>'timezone', 'UTC'))::text)::date as day
, COUNT(1)
, COUNT(DISTINCT p.measurands_id)
, MAX(now())
FROM temp_inserted_measurements m --tempfetchdata m
JOIN sensors s ON (m.sensors_id = s.sensors_id)
JOIN measurands p ON (s.measurands_id = p.measurands_id)
JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
GROUP BY sn.sensor_nodes_id
, ((m.datetime - '1sec'::interval) AT TIME ZONE (COALESCE(sn.metadata->>'timezone', 'UTC'))::text)::date
ON CONFLICT (sensor_nodes_id, day) DO UPDATE
SET records = EXCLUDED.records
, measurands = EXCLUDED.measurands
, modified_on = EXCLUDED.modified_on
RETURNING 1)
SELECT COUNT(1) INTO __exported_days
FROM e;


__cache_time_ms := 1000 * (extract(epoch FROM clock_timestamp() - __process_start));


INSERT INTO ingest_stats (
    ingest_method
    -- total
  , total_measurements_processed
  , total_measurements_inserted
  , total_measurements_rejected
  , total_nodes_processed
  , total_nodes_inserted
  , total_nodes_updated
  , total_nodes_rejected
  -- total times
  , total_process_time_ms
  , total_insert_time_ms
  , total_cache_time_ms
  -- latest
  , latest_measurements_processed
  , latest_measurements_inserted
  , latest_measurements_rejected
  , latest_nodes_processed
  , latest_nodes_inserted
  , latest_nodes_updated
  , latest_nodes_rejected
  -- times
  , latest_process_time_ms
  , latest_insert_time_ms
  , latest_cache_time_ms
  ) VALUES (
  -- totals
    __ingest_method
  , __total_measurements
  , __inserted_measurements
  , __rejected_measurements
  , __total_nodes
  , __inserted_nodes
  , __updated_nodes
  , __rejected_nodes
  -- times
  , __process_time_ms
  , __insert_time_ms
  , __cache_time_ms
  -- latest
  , __total_measurements
  , __inserted_measurements
  , __rejected_measurements
  , __total_nodes
  , __inserted_nodes
  , __updated_nodes
  , __rejected_nodes
  -- times
  , __process_time_ms
  , __insert_time_ms
  , __cache_time_ms
) ON CONFLICT (ingest_method) DO UPDATE SET
  -- totals
   total_measurements_processed = ingest_stats.total_measurements_processed + EXCLUDED.total_measurements_processed
 , total_measurements_inserted = ingest_stats.total_measurements_inserted + EXCLUDED.total_measurements_inserted
 , total_measurements_rejected = ingest_stats.total_measurements_rejected + EXCLUDED.total_measurements_rejected
 , total_nodes_processed = ingest_stats.total_nodes_processed + EXCLUDED.total_nodes_processed
 , total_nodes_inserted = ingest_stats.total_nodes_inserted + EXCLUDED.total_nodes_inserted
 , total_nodes_updated = ingest_stats.total_nodes_updated + EXCLUDED.total_nodes_updated
 , total_nodes_rejected = ingest_stats.total_nodes_rejected + EXCLUDED.total_nodes_rejected
 , total_process_time_ms = ingest_stats.total_process_time_ms + EXCLUDED.total_process_time_ms
 , total_insert_time_ms = ingest_stats.total_insert_time_ms + EXCLUDED.total_insert_time_ms
 , total_cache_time_ms = ingest_stats.total_cache_time_ms + EXCLUDED.total_cache_time_ms
 -- latest
 , latest_measurements_processed = EXCLUDED.latest_measurements_processed
 , latest_measurements_inserted = EXCLUDED.latest_measurements_inserted
 , latest_measurements_rejected = EXCLUDED.latest_measurements_rejected
 , latest_nodes_processed = EXCLUDED.latest_nodes_processed
 , latest_nodes_inserted = EXCLUDED.latest_nodes_inserted
 , latest_nodes_updated = EXCLUDED.latest_nodes_updated
 , latest_nodes_rejected = EXCLUDED.latest_nodes_rejected
 -- times
 , latest_process_time_ms = EXCLUDED.latest_process_time_ms
 , latest_insert_time_ms = EXCLUDED.latest_insert_time_ms
 , latest_cache_time_ms = EXCLUDED.latest_cache_time_ms
 , ingest_count = ingest_stats.ingest_count + 1
 , ingested_on = EXCLUDED.ingested_on;



RAISE NOTICE 'total-measurements: %, deleted-timescaledb: %, deleted-future-measurements: %, deleted-past-measurements: %, from: %, to: %, inserted-from: %, inserted-to: %, updated-nodes: %, inserted-measurements: %, inserted-measurands: %, inserted-nodes: %, rejected-nodes: %, rejected-systems: %, rejected-sensors: %, exported-sensor-days: %, inserted-spatial-rollups: %, process-time-ms: %, insert-time-ms: %, cache-time-ms: %, source: fetch'
      , __total_measurements
      , __deleted_timescaledb
      , __deleted_future_measurements
      , __deleted_past_measurements
      , __start_datetime
      , __end_datetime
      , __inserted_start_datetime
      , __inserted_end_datetime
      , __updated_nodes
      , __inserted_measurements
      , __inserted_measurands
      , __inserted_nodes
      , __rejected_nodes
      , __rejected_systems
      , __rejected_sensors
      , __exported_days
      , __inserted_spatial_rollups
		  , __process_time_ms
			, __insert_time_ms
	    , __cache_time_ms;

END $$;

--SELECT min(datetime), max(datetime) FROM tempfetchdata;
