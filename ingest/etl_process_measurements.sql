-- lcs_meas_ingest
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__total_measurements int;
__inserted_measurements int;
__rejected_measurements int := 0;
__rejected_nodes int := 0;
__total_nodes int := 0;
__updated_nodes int := 0;
__inserted_nodes int := 0;
__exported_days int;
__start_datetime timestamptz;
__end_datetime timestamptz;
__inserted_start_datetime timestamptz;
__inserted_end_datetime timestamptz;
__process_time_ms int;
__insert_time_ms int;
__cache_time_ms int;
__error_context text;
__ingest_method text := 'lcs';
BEGIN


DELETE
FROM staging_measurements
WHERE ingest_id IS NULL
OR datetime is NULL
OR value IS NULL;

--DELETE
--FROM staging_measurements
--WHERE datetime < '2018-01-01'::timestamptz
--OR datetime>now();

DELETE
FROM rejects
WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM staging_measurements)
AND tbl ~* '^meas';


SELECT COUNT(1)
, MIN(datetime)
, MAX(datetime)
INTO __total_measurements
, __start_datetime
, __end_datetime
FROM staging_measurements;


-- 	The ranking is to deal with the current possibility
-- that duplicate sensors with the same ingest/source id are created
	-- this is a short term fix
	-- a long term fix would not allow duplicate source_id's
WITH ranked_sensors AS (
  SELECT s.sensors_id
	, s.source_id
	, RANK() OVER (PARTITION BY s.source_id ORDER BY added_on ASC) as rnk
	FROM sensors s
	JOIN staging_measurements m ON (s.source_id = m.ingest_id)
), active_sensors AS (
	SELECT source_id
	, sensors_id
	FROM ranked_sensors
	WHERE rnk = 1)
	UPDATE staging_measurements
	SET sensors_id=s.sensors_id
	FROM active_sensors s
	WHERE s.source_id=ingest_id;

-- Now we have to fill in any missing information
-- first add the nodes and systems that dont exist
-- add just the bare minimum amount of data to the system
-- we assume that the node information will be added later
WITH nodes AS (
INSERT INTO sensor_nodes (
  source_name
, site_name
, source_id
, metadata)
SELECT source_name
, source_name
, source_id
, jsonb_build_object('fetchlogs_id', MIN(fetchlogs_id))
FROM staging_measurements
WHERE sensors_id IS NULL
GROUP BY 1,2,3
ON CONFLICT (source_name, source_id) DO UPDATE
SET source_id = EXCLUDED.source_id
, metadata = EXCLUDED.metadata||COALESCE(sensor_nodes.metadata, '{}'::jsonb)
RETURNING sensor_nodes_id, source_id)
INSERT INTO sensor_systems (
  sensor_nodes_id
, source_id)
SELECT sensor_nodes_id
, source_id
FROM nodes
ON CONFLICT DO NOTHING;

-- now create a sensor for each
-- this method depends on us having a match for the parameter
WITH sen AS (
  SELECT ingest_id
  , source_name
  , source_id
  , measurand as parameter
  FROM staging_measurements
  WHERE sensors_id IS NULL
  GROUP BY 1,2,3,4
), inserts AS (
INSERT INTO sensors (sensor_systems_id, measurands_id, source_id)
SELECT sy.sensor_systems_id
, m.measurands_id
, ingest_id
FROM sen s
JOIN measurands_map_view m ON (s.parameter = m.key)
JOIN sensor_nodes n ON (s.source_name = n.source_name AND s.source_id = n.source_id)
JOIN sensor_systems sy ON (sy.sensor_nodes_id = n.sensor_nodes_id AND s.source_id = sy.source_id)
ON CONFLICT DO NOTHING
RETURNING sensor_systems_id)
SELECT COUNT(DISTINCT sensor_systems_id) INTO __inserted_nodes
FROM inserts;

-- try again to find the sensors
UPDATE staging_measurements
SET sensors_id=s.sensors_id
FROM sensors s
WHERE s.source_id=ingest_id
AND staging_measurements.sensors_id IS NULL;


SELECT COUNT(DISTINCT sensors_id)
INTO __total_nodes
FROM staging_measurements;


__process_time_ms := 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

-- reject any missing. Most likely due to issues
-- with the measurand
WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
    current_timestamp
    , 'meas-missing-sensors-id'
    , to_jsonb(staging_measurements)
    , fetchlogs_id
FROM staging_measurements
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_measurements
FROM r;

-- restart the clock to measure just inserts
__process_start := clock_timestamp();

WITH inserts AS (
INSERT INTO measurements (
    sensors_id,
    datetime,
    value,
    lon,
    lat
) SELECT
    --DISTINCT
    sensors_id,
    datetime,
    value,
    lon,
    lat
FROM staging_measurements
WHERE sensors_id IS NOT NULL
ON CONFLICT DO NOTHING
RETURNING sensors_id, datetime, value, lat, lon
), inserted as (
   INSERT INTO staging_inserted_measurements (sensors_id, datetime, value, lat, lon)
   SELECT sensors_id
   , datetime
   , value
   , lat
   , lon
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
  FROM staging_measurements m
  LEFT JOIN staging_inserted_measurements t ON (t.sensors_id = m.sensors_id AND t.datetime = m.datetime)
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

-- -- Now we can use those staging_inserted_measurements to update the cache tables
-- INSERT INTO sensors_latest (
--   sensors_id
--   , datetime
--   , value
--   , lat
--   , lon
--   )
-- ---- identify the row that has the latest value
-- WITH numbered AS (
--   SELECT sensors_id
--    , datetime
--    , value
--    , lat
--    , lon
--    , row_number() OVER (PARTITION BY sensors_id ORDER BY datetime DESC) as rn
--   FROM staging_inserted_measurements
-- ), latest AS (
-- ---- only insert those rows
--   SELECT sensors_id
--    , datetime
--    , value
--    , lat
--    , lon
--   FROM numbered
--   WHERE rn = 1
-- )
-- SELECT l.sensors_id
-- , l.datetime
-- , l.value
-- , l.lat
-- , l.lon
-- FROM latest l
-- LEFT JOIN sensors_latest sl ON (l.sensors_id = sl.sensors_id)
-- WHERE sl.sensors_id IS NULL
-- OR l.datetime > sl.datetime
-- ON CONFLICT (sensors_id) DO UPDATE
-- SET datetime = EXCLUDED.datetime
-- , value = EXCLUDED.value
-- , lat = EXCLUDED.lat
-- , lon = EXCLUDED.lon
-- , modified_on = now()
-- --, fetchlogs_id = EXCLUDED.fetchlogs_id
-- ;
-- update the exceedances
INSERT INTO sensor_exceedances (sensors_id, threshold_value, datetime_latest)
  SELECT
  m.sensors_id
  , t.value
  , MAX(datetime)
  FROM staging_inserted_measurements m
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
  , geom_latest
  )
---- identify the row that has the latest value
WITH numbered AS (
  SELECT sensors_id
   , datetime
   , value
   , lat
   , lon
   , sum(1) OVER (PARTITION BY sensors_id) as value_count
   , min(datetime) OVER (PARTITION BY sensors_id) as datetime_min
   , avg(value) OVER (PARTITION BY sensors_id) as value_avg
   , row_number() OVER (PARTITION BY sensors_id ORDER BY datetime DESC) as rn
  FROM staging_inserted_measurements
), latest AS (
---- only insert those rows
  SELECT sensors_id
   , datetime
   , value
   , value_count
   , value_avg
   , datetime_min
   , lat
   , lon
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
, public.pt3857(lon, lat)
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
, geom_latest = CASE WHEN EXCLUDED.datetime_last > sensors_rollup.datetime_last
                 THEN EXCLUDED.geom_latest
                 ELSE sensors_rollup.geom_latest
                 END
, value_count = sensors_rollup.value_count + EXCLUDED.value_count
, value_min = LEAST(sensors_rollup.value_min, EXCLUDED.value_latest)
, value_max = GREATEST(sensors_rollup.value_max, EXCLUDED.value_latest)
, datetime_first = LEAST(sensors_rollup.datetime_first, EXCLUDED.datetime_first)
, modified_on = now()
--, fetchlogs_id = EXCLUDED.fetchlogs_id
;


-- Update the table that will help to track hourly rollups
INSERT INTO hourly_stats (datetime)
  SELECT date_trunc('hour', datetime)
  FROM staging_inserted_measurements
  GROUP BY 1
ON CONFLICT (datetime) DO UPDATE
SET modified_on = now();


--Update the export queue/logs to export these records
--wrap it in a block just in case the database does not have this module installed
--we subtract the second because the data is assumed to be time ending
WITH e AS (
INSERT INTO open_data_export_logs (sensor_nodes_id, day, records, measurands, modified_on)
SELECT sn.sensor_nodes_id
, ((m.datetime - '1sec'::interval) AT TIME ZONE (COALESCE(sn.metadata->>'timezone', 'UTC'))::text)::date as day
, COUNT(1)
, COUNT(DISTINCT p.measurands_id)
, MAX(now())
FROM staging_inserted_measurements m -- meas m
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


RAISE NOTICE 'inserted-measurements: %, inserted-from: %, inserted-to: %, rejected-measurements: %, exported-sensor-days: %, process-time-ms: %, insert-time-ms: %, cache-time-ms: %, source: lcs'
      , __inserted_measurements
      , __inserted_start_datetime
      , __inserted_end_datetime
      , __rejected_measurements
      , __exported_days
      , __process_time_ms
      , __insert_time_ms
      , __cache_time_ms;


EXCEPTION WHEN OTHERS THEN
 GET STACKED DIAGNOSTICS __error_context = PG_EXCEPTION_CONTEXT;
 RAISE NOTICE 'Failed to ingest measurements: %, %', SQLERRM, __error_context;

END $$;
