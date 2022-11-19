-- Get sensor systems
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__inserted_measurements int;
__rejected_measurements int;
__exported_days int;
__inserted_start_datetime timestamptz;
__inserted_end_datetime timestamptz;
BEGIN

DELETE
FROM meas
WHERE ingest_id IS NULL
OR datetime is NULL
OR value IS NULL;

--DELETE
--FROM meas
--WHERE datetime < '2018-01-01'::timestamptz
--OR datetime>now();

DELETE
FROM rejects
WHERE fetchlogs_id IN (SELECT fetchlogs_id FROM meas)
AND tbl ~* '^meas';

UPDATE meas
SET sensors_id=s.sensors_id
FROM sensors s
WHERE s.source_id=ingest_id;


-- first the sensor nodes
WITH nodes AS (
INSERT INTO sensor_nodes (
  source_name
, source_id )
SELECT split_ingest_id(ingest_id, 1) as source_name
, split_ingest_id(ingest_id, 2) as source_id
FROM meas
WHERE sensors_id IS NULL
GROUP BY 1,2
ON CONFLICT (source_name, source_id) DO UPDATE
SET source_id = EXCLUDED.source_id
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
, split_ingest_id(ingest_id, 1) as source_name
, split_ingest_id(ingest_id, 2) as source_id
, split_ingest_id(ingest_id, 3) as parameter
FROM meas
WHERE sensors_id IS NULL
GROUP BY 1,2,3,4)
INSERT INTO sensors (sensor_systems_id, measurands_id, source_id)
SELECT sy.sensor_systems_id
, m.measurands_id
, ingest_id
FROM sen s
JOIN measurands_map_view m ON (s.parameter = m.key)
JOIN sensor_nodes n ON (s.source_name = n.source_name AND s.source_id = n.source_id)
JOIN sensor_systems sy ON (sy.sensor_nodes_id = n.sensor_nodes_id AND s.source_id = sy.source_id)
ON CONFLICT DO NOTHING;

-- try again to find the sensors
UPDATE meas
SET sensors_id=s.sensors_id
FROM sensors s
WHERE s.source_id=ingest_id
AND meas.sensors_id IS NULL;

-- reject any missing. Most likely due to issues
-- with the measurand
WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
    current_timestamp
    , 'meas-missing-sensors-id'
    , to_jsonb(meas)
    , fetchlogs_id
FROM meas
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_measurements
FROM r;


--DELETE
--FROM meas
--WHERE sensors_id IS NULL;

-- --Some fake data to make it easier to test this section
-- TRUNCATE meas;
-- INSERT INTO meas (ingest_id, sensors_id, value, datetime)
-- SELECT 'fake-ingest'
-- , (SELECT sensors_id FROM sensors ORDER BY random() LIMIT 1)
-- , -99
-- , generate_series(now() - '3day'::interval, current_date, '1hour'::interval);

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
    datetime::timestamptz,
    value,
    lon,
    lat
FROM meas
WHERE sensors_id IS NOT NULL
ON CONFLICT DO NOTHING
RETURNING sensors_id, datetime, value, lat, lon
), inserted as (
   INSERT INTO temp_inserted_measurements (sensors_id, datetime, value, lat, lon)
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

-- Now we can use those temp_inserted_measurements to update the cache tables
INSERT INTO sensors_latest (
  sensors_id
  , datetime
  , value
  , lat
  , lon
  )
---- identify the row that has the latest value
WITH numbered AS (
  SELECT sensors_id
   , datetime
   , value
   , lat
   , lon
   , row_number() OVER (PARTITION BY sensors_id ORDER BY datetime DESC) as rn
  FROM temp_inserted_measurements
), latest AS (
---- only insert those rows
  SELECT sensors_id
   , datetime
   , value
   , lat
   , lon
  FROM numbered
  WHERE rn = 1
)
SELECT l.sensors_id
, l.datetime
, l.value
, l.lat
, l.lon
FROM latest l
LEFT JOIN sensors_latest sl ON (l.sensors_id = sl.sensors_id)
WHERE sl.sensors_id IS NULL
OR l.datetime > sl.datetime
ON CONFLICT (sensors_id) DO UPDATE
SET datetime = EXCLUDED.datetime
, value = EXCLUDED.value
, lat = EXCLUDED.lat
, lon = EXCLUDED.lon
, modified_on = now()
--, fetchlogs_id = EXCLUDED.fetchlogs_id
;


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
FROM temp_inserted_measurements m -- meas m
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

RAISE NOTICE 'inserted-measurements: %, inserted-from: %, inserted-to: %, rejected-measurements: %, exported-sensor-days: %, process-time-ms: %, source: lcs'
      , __inserted_measurements
      , __inserted_start_datetime
      , __inserted_end_datetime
      , __rejected_measurements
      , __exported_days
      , 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'Failed to export to logs: %', SQLERRM
 USING HINT = 'Make sure that the open data module is installed';

END $$;
