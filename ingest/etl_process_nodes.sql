-- lcs_ingest_full
DO $$
DECLARE
__process_start timestamptz := clock_timestamp();
__inserted_nodes int;
__inserted_sensors int;
__rejected_nodes int;
__rejected_systems int;
__rejected_sensors int;
__rejected_measurands int;

BEGIN

--------------------------
-- lcs_ingest_nodes.sql --
--------------------------

DELETE
FROM staging_sensornodes
WHERE staging_sensornodes.ingest_id IS NULL;

DELETE
FROM staging_sensorsystems
WHERE staging_sensorsystems.ingest_id IS NULL
OR ingest_sensor_nodes_id IS NULL;

DELETE
FROM staging_sensors
WHERE staging_sensors.ingest_id IS NULL
OR ingest_sensor_systems_id IS NULL;

UPDATE staging_sensors
SET units  = 'µg/m³'
WHERE units IN ('µg/m��','��g/m³', 'ug/m3');



-- match the locations to existing nodes using the source_name/id combo
UPDATE staging_sensornodes
SET sensor_nodes_id = s.sensor_nodes_id
, timezones_id = s.timezones_id
, countries_id = s.countries_id
, is_new = false
, is_moved = st_astext(s.geom) != st_astext(staging_sensornodes.geom)
FROM sensor_nodes s
WHERE s.source_name = staging_sensornodes.source_name
AND s.source_id = staging_sensornodes.source_id
AND ( staging_sensornodes.matching_method IS NULL
 OR staging_sensornodes.matching_method = 'ingest-id');


-- now update them using the source + spatial method
UPDATE staging_sensornodes
SET sensor_nodes_id = s.sensor_nodes_id
, timezones_id = s.timezones_id
, countries_id = s.countries_id
, is_new = false
, is_moved = st_astext(s.geom) != st_astext(staging_sensornodes.geom)
FROM sensor_nodes s
WHERE s.source_name = staging_sensornodes.source_name
AND st_distance(staging_sensornodes.geom, s.geom) < 0.00001 -- about 1.11 meters difference
AND staging_sensornodes.matching_method = 'source-spatial';


-- only update the nodes where the geom has changed
-- the geom queries are really slow so we dont want to be doing that all the time
-- ~18 locations per second
UPDATE staging_sensornodes SET
  timezones_id = get_timezones_id(geom)
, countries_id = get_countries_id(geom)
WHERE is_new
  OR is_moved
  OR timezones_id IS NULL
  OR countries_id IS NULL;


-- we are going to update the source_id  where we are matching via geometry
-- for ingest-id matches this should not matter.
UPDATE sensor_nodes
SET source_id = COALESCE(s.source_id, sensor_nodes.source_id)
  , geom = COALESCE(s.geom, sensor_nodes.geom)
  , site_name = COALESCE(s.site_name, sensor_nodes.site_name)
  , timezones_id = COALESCE(s.timezones_id, sensor_nodes.timezones_id)
  , countries_id = COALESCE(s.countries_id, sensor_nodes.countries_id)
  , ismobile = COALESCE(s.ismobile, sensor_nodes.ismobile)
  , metadata = COALESCE(s.metadata, '{}') || COALESCE(sensor_nodes.metadata, '{}')
  , modified_on = now()
FROM staging_sensornodes s
WHERE sensor_nodes.sensor_nodes_id = s.sensor_nodes_id;


-- And now we insert those into the sensor nodes table
WITH inserts AS (
INSERT INTO sensor_nodes (
  site_name
, source_name
, ismobile
, geom
, metadata
, source_id
, timezones_id
, providers_id
, countries_id
)
SELECT site_name
, source_name
, ismobile
, geom
, metadata
, source_id
, timezones_id
-- default to the unknown provider
-- just to make sure we have one set
, COALESCE(get_providers_id(source_name), 1)
, countries_id
FROM staging_sensornodes
WHERE sensor_nodes_id IS NULL
ON CONFLICT (source_name, source_id) DO UPDATE
SET
    site_name=coalesce(EXCLUDED.site_name,sensor_nodes.site_name)
    , source_id=COALESCE(EXCLUDED.source_id, sensor_nodes.source_id)
    , ismobile=coalesce(EXCLUDED.ismobile,sensor_nodes.ismobile)
    , geom=coalesce(EXCLUDED.geom,sensor_nodes.geom)
    , metadata=COALESCE(sensor_nodes.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
    , timezones_id = COALESCE(EXCLUDED.timezones_id, sensor_nodes.timezones_id)
    , providers_id = COALESCE(EXCLUDED.providers_id, sensor_nodes.providers_id)
    , modified_on = now()
RETURNING 1)
SELECT COUNT(1) INTO __inserted_nodes
FROM inserts;

----------------------------
-- lcs_ingest_systems.sql --
----------------------------

-- fill in any new sensor_nodes_id
UPDATE staging_sensornodes
SET sensor_nodes_id = sensor_nodes.sensor_nodes_id
FROM sensor_nodes
WHERE staging_sensornodes.sensor_nodes_id is null
AND sensor_nodes.source_name = staging_sensornodes.source_name
AND sensor_nodes.source_id = staging_sensornodes.source_id;

-- log anything we were not able to get an id for
WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT now()
, 'staging_sensornodes-missing-nodes-id'
, to_jsonb(staging_sensornodes)
, fetchlogs_id
FROM staging_sensornodes
WHERE sensor_nodes_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_nodes
FROM r;

--------------------
-- Sensor Systems --
--------------------


-- make sure that we have a system entry for every ingest_id
-- this is to deal with fetchers that do not add these data
INSERT INTO staging_sensorsystems (sensor_nodes_id, ingest_id, fetchlogs_id, metadata)
SELECT sensor_nodes_id
--, source_id -- the ingest_id has the source_name in it and we dont need/want that
, ingest_id
, fetchlogs_id
, '{"note":"automatically added for sensor node"}'
FROM staging_sensornodes
WHERE is_new AND ingest_id NOT IN (SELECT ingest_sensor_nodes_id FROM staging_sensorsystems)
ON CONFLICT (ingest_id) DO UPDATE
  SET sensor_nodes_id = EXCLUDED.sensor_nodes_id
  ;

-- Now match the sensor nodes to the system
UPDATE staging_sensorsystems
SET sensor_nodes_id = staging_sensornodes.sensor_nodes_id
FROM staging_sensornodes
WHERE staging_sensorsystems.ingest_sensor_nodes_id = staging_sensornodes.ingest_id;

-- And match to any existing sensor systems
UPDATE staging_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
, is_new = false
FROM sensor_systems
WHERE sensor_systems.sensor_nodes_id = staging_sensorsystems.sensor_nodes_id
AND sensor_systems.source_id = staging_sensorsystems.ingest_id;


-- log anything we were not able to get an id for
WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT now()
, 'staging_sensorsystems-missing-nodes-id'
,  to_jsonb(staging_sensorsystems)
,  fetchlogs_id
FROM staging_sensorsystems
WHERE sensor_nodes_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM r;

-- And finally we add/update the sensor systems
INSERT INTO sensor_systems (sensor_nodes_id, source_id, instruments_id, metadata)
SELECT sensor_nodes_id
, s.ingest_id
, i.instruments_id
, metadata
FROM staging_sensorsystems s
LEFT JOIN instruments i ON (s.instrument_ingest_id = i.ingest_id)
WHERE sensor_nodes_id IS NOT NULL
GROUP BY sensor_nodes_id, s.ingest_id, instruments_id, metadata
ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE SET
    metadata=COALESCE(sensor_systems.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
    , instruments_id = EXCLUDED.instruments_id
    , modified_on = now();

----------------------------
-- lcs_ingest_sensors.sql --
----------------------------

-- Match the sensor system data
UPDATE staging_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE staging_sensorsystems.sensor_systems_id IS NULL
AND staging_sensorsystems.sensor_nodes_id=sensor_systems.sensor_nodes_id
AND staging_sensorsystems.ingest_id=sensor_systems.source_id
;

WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT
  now()
, 'staging_sensorsystems-missing-systems-id'
, to_jsonb(staging_sensorsystems)
, fetchlogs_id
FROM staging_sensorsystems
WHERE sensor_systems_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_systems
FROM r;

-------------
-- SENSORS --
-------------

 -- We do not want to create default sensors because we are not dealling with measurements here
UPDATE staging_sensors
SET sensor_systems_id = staging_sensorsystems.sensor_systems_id
FROM staging_sensorsystems
WHERE staging_sensors.ingest_sensor_systems_id = staging_sensorsystems.ingest_id;

WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
  now()
, 'staging_sensors-missing-systems-id'
, to_jsonb(staging_sensors)
, fetchlogs_id
FROM staging_sensors
WHERE sensor_systems_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM r;


UPDATE staging_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE sensors.sensor_systems_id=staging_sensors.sensor_systems_id
AND sensors.source_id = staging_sensors.ingest_id;


UPDATE staging_sensors
SET measurands_id = m.measurands_id
FROM (SELECT measurand, MIN(measurands_id) AS measurands_id FROM measurands GROUP BY measurand) as m
WHERE staging_sensors.measurand=m.measurand
--AND staging_sensors.units=measurands.units
;


WITH r AS (
INSERT INTO rejects (t, tbl,r,fetchlogs_id)
SELECT
 now()
, 'staging_sensors-missing-measurands-id'
, to_jsonb(staging_sensors)
, fetchlogs_id
FROM staging_sensors
WHERE measurands_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_measurands
FROM r;

WITH inserts AS (
INSERT INTO sensors (
  source_id
, sensor_systems_id
, measurands_id
, data_logging_period_seconds
, data_averaging_period_seconds
, sensor_statuses_id
, metadata)
SELECT ingest_id
, sensor_systems_id
, measurands_id
, logging_interval_seconds
, averaging_interval_seconds
, COALESCE(ss.sensor_statuses_id, 1)
, metadata
FROM staging_sensors s
LEFT JOIN sensor_statuses ss ON (ss.short_code = s.status)
WHERE measurands_id is not null
AND sensor_systems_id is not null
GROUP BY ingest_id
, sensor_systems_id
, measurands_id
, logging_interval_seconds
, averaging_interval_seconds
, ss.sensor_statuses_id
, metadata
ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO UPDATE
SET metadata = COALESCE(sensors.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
  , data_logging_period_seconds = EXCLUDED.data_logging_period_seconds
  , data_averaging_period_seconds = EXCLUDED.data_averaging_period_seconds
  , sensor_statuses_id = EXCLUDED.sensor_statuses_id
  , modified_on = now()
RETURNING 1)
SELECT COUNT(1) INTO __inserted_sensors
FROM inserts;

UPDATE staging_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE sensors.sensor_systems_id=staging_sensors.sensor_systems_id
AND sensors.source_id = staging_sensors.ingest_id;

WITH r AS (
INSERT INTO rejects (t,tbl,r,fetchlogs_id)
SELECT
  now()
  , 'staging_sensors-missing-sensors-id'
  , to_jsonb(staging_sensors)
  , fetchlogs_id
FROM staging_sensors
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO __rejected_sensors
FROM r;


-- update the period so that we dont have to keep doing it later
-- we could do this on import as well if we feel this is slowing us down
UPDATE staging_flags
  SET period = tstzrange(COALESCE(datetime_from, '-infinity'::timestamptz),COALESCE(datetime_to, 'infinity'::timestamptz), '[]');

-- Now we have to match things
-- get the right node id and sensors id for the flags
UPDATE staging_flags
SET sensors_id = s.sensors_id
  , sensor_nodes_id = sy.sensor_nodes_id
FROM sensors s
JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
WHERE staging_flags.sensor_ingest_id = s.source_id;

-- and then get the right flags_id
UPDATE staging_flags
SET flag_types_id = ft.flag_types_id
FROM flag_types ft
WHERE split_part(staging_flags.ingest_id, '::', 1) = ft.ingest_id;

-- now we should look to see if we should be just extending a flag
UPDATE staging_flags sf
  SET flags_id = fm.flags_id
  FROM flags fm
  -- where the core information is the same (exactly)
  WHERE sf.sensor_nodes_id = fm.sensor_nodes_id
  AND sf.flag_types_id = fm.flag_types_id
  AND ((sf.note = fm.note) OR (sf.note IS NULL AND fm.note IS NULL))
  -- the periods touch or overlap
  AND fm.period && sf.period
  -- and the flagged record sensors contains the current sensors
  AND fm.sensors_ids @> ARRAY[sf.sensors_id];

-- and finally we will insert the new flags
INSERT INTO flags (flag_types_id, sensor_nodes_id, sensors_ids, period, note)
  SELECT flag_types_id
  , sensor_nodes_id
  , CASE WHEN sensors_id IS NOT NULL THEN ARRAY[sensors_id] ELSE NULL END
  , period
  , note
  FROM staging_flags
  WHERE flag_types_id IS NOT NULL
  AND sensor_nodes_id IS NOT NULL
  AND flags_id IS NULL;

-- And then update any that need to be updated
 UPDATE flags fm
  SET period = sf.period + fm.period
  , note = sf.note
  , modified_on = now()
  FROM staging_flags sf
  WHERE sf.flags_id = fm.flags_id;


------------------
-- Return stats --
------------------

RAISE NOTICE 'inserted-nodes: %, inserted-sensors: %, rejected-nodes: %, rejected-sensors: %, rejected-measurands: %, process-time-ms: %, source: lcs'
      , __inserted_nodes
      , __inserted_sensors
      , __rejected_nodes
      , __rejected_sensors
      , __rejected_measurands
      , 1000 * (extract(epoch FROM clock_timestamp() - __process_start));

END $$;
