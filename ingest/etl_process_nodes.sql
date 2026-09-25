-- etl_process_nodes.sql
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

-- DELETE
-- FROM staging_sensornodes
-- WHERE staging_sensornodes.ingest_id IS NULL;

-- DELETE
-- FROM staging_sensorsystems
-- WHERE staging_sensorsystems.ingest_id IS NULL
-- OR ingest_sensor_nodes_id IS NULL;

-- DELETE
-- FROM staging_sensors
-- WHERE staging_sensors.ingest_id IS NULL
-- OR ingest_sensor_systems_id IS NULL;

UPDATE staging_sensors
SET units  = 'µg/m³'
WHERE units IN ('µg/m��','��g/m³', 'ug/m3');


-- For measurements only files without locations
UPDATE staging_sensornodes sn
SET sensor_nodes_id = n.sensor_nodes_id
  , timezones_id = n.timezones_id
  , countries_id = n.countries_id
  , is_new = false
FROM sensor_nodes n
WHERE sn.source_name = n.source_name
  AND sn.source_id = n.source_id
  AND sn.geom IS NULL;

-- now update them using the source + spatial method
-- makeing sure to only pick one match for all nodes

UPDATE staging_sensornodes sn
SET sensor_nodes_id = best.sensor_nodes_id
  , timezones_id = best.timezones_id
  , countries_id = best.countries_id
  , is_new = false
FROM (
    SELECT DISTINCT ON (sn.ingest_id)
           sn.ingest_id,
           s.sensor_nodes_id,
           s.timezones_id,
           s.countries_id
    FROM staging_sensornodes sn
    JOIN sensor_nodes s ON s.source_name = sn.source_name
    JOIN providers p ON s.providers_id = p.providers_id
    WHERE ST_DWithin(sn.geom, s.geom, 0.0002) -- -- Cheap prefliter
    AND st_distance(sn.geom::geography, s.geom::geography) <= 20 -- Exact check only on candidates
      AND (
        sn.source_id IS NULL
        OR s.source_id IS NULL
        OR s.source_id = s.sensor_nodes_id::text
        OR s.source_id = sn.source_id
      )
    ORDER BY sn.ingest_id,
             -- Rank 1: real source_id on both sides that match exactly
             (sn.source_id IS NOT NULL
              AND s.source_id IS NOT NULL
              AND s.source_id != s.sensor_nodes_id::text
              AND s.source_id = sn.source_id) DESC,
             -- Rank 2: closest spatial match
             st_distance(sn.geom, s.geom) ASC
) best
  WHERE sn.ingest_id = best.ingest_id
  AND sn.sensor_nodes_id IS NULL;

  -- only update the nodes where the geom has changed
-- the per row geom queries are really slow so we dont want to be doing that all the time
-- ~18 locations per second
-- UPDATE staging_sensornodes SET
--   timezones_id = get_timezones_id(geom)
-- , countries_id = get_countries_id(geom)
-- WHERE geom IS NOT NULL AND (
--     is_new --OR is_moved
--     OR timezones_id IS NULL
--     OR countries_id IS NULL
--   );

-- Update timezones_id via spatial join
UPDATE staging_sensornodes s
SET timezones_id = t.timezones_id
FROM timezones t
WHERE ST_Intersects(s.geom, t.geog::geometry)
  AND s.geom IS NOT NULL
  AND (
    s.is_new
    OR s.timezones_id IS NULL
  );

-- Update countries_id via spatial join
UPDATE staging_sensornodes s
SET countries_id = c.countries_id
FROM countries c
WHERE ST_Intersects(s.geom, c.geog::geometry)
  AND s.geom IS NOT NULL
  AND (
    s.is_new
    OR s.countries_id IS NULL
  );



-- Update the matched nodes if anything has changed
UPDATE sensor_nodes
SET site_name = COALESCE(s.site_name, sensor_nodes.site_name)
  , source_id = COALESCE(s.source_id, sensor_nodes.source_id)
  , geom = COALESCE(s.geom, sensor_nodes.geom)
  , timezones_id = COALESCE(s.timezones_id, sensor_nodes.timezones_id)
  , countries_id = COALESCE(s.countries_id, sensor_nodes.countries_id)
  , ismobile = COALESCE(s.ismobile, sensor_nodes.ismobile)
  , metadata = sensor_nodes.metadata || jsonb_modified('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
  , modified_on = clock_timestamp()
FROM staging_sensornodes s
WHERE sensor_nodes.sensor_nodes_id = s.sensor_nodes_id
AND (
     COALESCE(s.geom, sensor_nodes.geom) IS DISTINCT FROM sensor_nodes.geom
  OR COALESCE(s.source_id, sensor_nodes.source_id) IS DISTINCT FROM sensor_nodes.source_id
  OR COALESCE(s.geom, sensor_nodes.geom) IS DISTINCT FROM sensor_nodes.geom
  OR COALESCE(s.site_name, sensor_nodes.site_name) IS DISTINCT FROM sensor_nodes.site_name
  OR COALESCE(s.timezones_id, sensor_nodes.timezones_id) IS DISTINCT FROM sensor_nodes.timezones_id
  OR COALESCE(s.countries_id, sensor_nodes.countries_id) IS DISTINCT FROM sensor_nodes.countries_id
  OR COALESCE(s.ismobile, sensor_nodes.ismobile) IS DISTINCT FROM sensor_nodes.ismobile
  OR (COALESCE(s.metadata, '{}') || COALESCE(sensor_nodes.metadata, '{}'))
       IS DISTINCT FROM sensor_nodes.metadata
);


-- And now we insert any new nodes into our sensor table
-- currently has a bug where the source_name (provider) and the source_id
  -- contstraint results in a moved (lat/long changed) node fails to insert
  -- and instead updates the geometry of existing node. The only way I can
  -- think to solve this issue is to not allow source_ids for spatial-source matches
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
, metadata || jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
, source_id
, timezones_id
-- default to the unknown provider
-- just to make sure we have one set
, COALESCE(get_providers_id(source_name), 1)
, countries_id
FROM staging_sensornodes
WHERE sensor_nodes_id IS NULL
-- should rethink this ON CONFLICT clause
--ON CONFLICT (source_name, source_id, geom) DO UPDATE
ON CONFLICT ON CONSTRAINT sensor_nodes_deployment_key DO UPDATE
SET
    site_name=coalesce(EXCLUDED.site_name,sensor_nodes.site_name)
    , source_id=COALESCE(EXCLUDED.source_id, sensor_nodes.source_id)
    , ismobile=coalesce(EXCLUDED.ismobile,sensor_nodes.ismobile)
    , geom=coalesce(EXCLUDED.geom,sensor_nodes.geom)
    , metadata=COALESCE(sensor_nodes.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
    , timezones_id = COALESCE(EXCLUDED.timezones_id, sensor_nodes.timezones_id)
    , providers_id = COALESCE(EXCLUDED.providers_id, sensor_nodes.providers_id)
    , modified_on = clock_timestamp()
WHERE COALESCE(EXCLUDED.site_name, sensor_nodes.site_name) IS DISTINCT FROM sensor_nodes.site_name
   OR COALESCE(EXCLUDED.source_id, sensor_nodes.source_id) IS DISTINCT FROM sensor_nodes.source_id
   OR COALESCE(EXCLUDED.ismobile, sensor_nodes.ismobile) IS DISTINCT FROM sensor_nodes.ismobile
   OR COALESCE(EXCLUDED.geom, sensor_nodes.geom) IS DISTINCT FROM sensor_nodes.geom
   OR COALESCE(EXCLUDED.timezones_id, sensor_nodes.timezones_id) IS DISTINCT FROM sensor_nodes.timezones_id
   OR COALESCE(EXCLUDED.providers_id, sensor_nodes.providers_id) IS DISTINCT FROM sensor_nodes.providers_id
   OR (COALESCE(sensor_nodes.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}'))
        IS DISTINCT FROM sensor_nodes.metadata
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


-- -- make sure that we have a system entry for every ingest_id
-- -- this is to deal with fetchers that do not add these data
-- -- however, for transoform we dont want to do this because it can create empty systems
-- INSERT INTO staging_sensorsystems (sensor_nodes_id, ingest_id, fetchlogs_id, metadata)
-- SELECT sensor_nodes_id
-- --, source_id -- the ingest_id has the source_name in it and we dont need/want that
-- , ingest_id
-- , fetchlogs_id
-- , '{"note":"automatically added for sensor node"}'
-- FROM staging_sensornodes
-- WHERE is_new AND ingest_id NOT IN (SELECT ingest_sensor_nodes_id FROM staging_sensorsystems)
-- ON CONFLICT (ingest_id) DO UPDATE
--   SET sensor_nodes_id = EXCLUDED.sensor_nodes_id
--   ;

-- Now match the sensor nodes to the system
UPDATE staging_sensorsystems
SET sensor_nodes_id = staging_sensornodes.sensor_nodes_id
FROM staging_sensornodes
WHERE staging_sensorsystems.ingest_sensor_nodes_id = staging_sensornodes.ingest_id;

-- And match to any existing sensor systems
-- a null source_id or a source_id that is the same as the sensor_systems_id is considered a generic system
-- UPDATE staging_sensorsystems

UPDATE staging_sensorsystems ss
SET sensor_systems_id = m.sensor_systems_id
  , is_new = false
FROM (
  SELECT DISTINCT ON (ss2.ctid)
    ss2.ctid AS staging_ctid,
    s.sensor_systems_id
  FROM staging_sensorsystems ss2
  JOIN sensor_systems s
    ON s.sensor_nodes_id = ss2.sensor_nodes_id
   AND (
     s.source_id = ss2.ingest_id                                 -- exact match
     OR s.source_id ~* without_instrument_pattern(ss2.ingest_id) -- matches except for the instrument
     OR s.source_id IS NULL                                      -- no source_id in the existing system
     OR s.source_id = s.sensor_systems_id::text                  -- source_id matches its own id
   )
  ORDER BY ss2.ctid,
    CASE
      WHEN s.source_id = ss2.ingest_id THEN 1
      WHEN s.source_id ~* without_instrument_pattern(ss2.ingest_id) THEN 2
      WHEN s.source_id = s.sensor_systems_id::text THEN 3
      WHEN s.source_id IS NULL THEN 4
    END
) m
WHERE ss.ctid = m.staging_ctid;




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

-- add any missing sensors
  -- the goal is to only add what we need and do it before we need it
INSERT INTO entities (full_name, ingest_id, entity_type, metadata)
SELECT DISTINCT manufacturer_key
  , manufacturer_key
  , 'Organization'::entity_type
  , jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
FROM staging_sensorsystems
WHERE manufacturer_key IS NOT NULL
ON CONFLICT DO NOTHING;
-- and the instruments
INSERT INTO instruments (manufacturer_entities_id, label, description, is_monitor, ingest_id, metadata)
SELECT DISTINCT e.entities_id
  , s.model_key
  , 'Added automatically during ingest'
  , 'f'::boolean
  , s.instrument_ingest_id
  , jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
FROM staging_sensorsystems s
JOIN entities e ON s.manufacturer_key = e.ingest_id
ON CONFLICT DO NOTHING;


UPDATE sensor_systems p
  SET instruments_id = i.instruments_id
    , source_id = s.ingest_id
    , modified_on = now()
    , metadata = p.metadata || jsonb_modified('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
  FROM staging_sensorsystems s
  LEFT JOIN instruments i ON (s.instrument_ingest_id = i.ingest_id)
  WHERE p.sensor_systems_id = s.sensor_systems_id
  AND (
    (p.instruments_id IS NULL AND i.instruments_id IS NOT NULL OR p.instruments_id != i.instruments_id)
    OR
    (p.source_id IS NULL AND s.ingest_id IS NOT NULL OR p.source_id != s.ingest_id)
  );

-- And finally we add the sensor systems
--- FIX ME -- split up the add and update part of this
INSERT INTO sensor_systems (sensor_nodes_id, source_id, instruments_id, metadata)
SELECT sensor_nodes_id
, s.ingest_id
, i.instruments_id
, s.metadata || jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
FROM staging_sensorsystems s
LEFT JOIN instruments i ON (s.instrument_ingest_id = i.ingest_id)
WHERE sensor_nodes_id IS NOT NULL
  AND sensor_systems_id IS NULL
GROUP BY sensor_nodes_id, s.ingest_id, i.instruments_id, s.metadata, fetchlogs_id
  ON CONFLICT DO NOTHING;

-- ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE SET
--     metadata=COALESCE(sensor_systems.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}')
--     , instruments_id = EXCLUDED.instruments_id
--     , modified_on = clock_timestamp()
-- WHERE (COALESCE(sensor_systems.metadata, '{}') || COALESCE(EXCLUDED.metadata, '{}'))
--         IS DISTINCT FROM sensor_systems.metadata
--    OR EXCLUDED.instruments_id IS DISTINCT FROM sensor_systems.instruments_id;

----------------------------
-- lcs_ingest_sensors.sql --
----------------------------

-- Match the sensor system data
UPDATE staging_sensorsystems
SET sensor_systems_id = sensor_systems.sensor_systems_id
FROM sensor_systems
WHERE staging_sensorsystems.sensor_systems_id IS NULL
AND staging_sensorsystems.sensor_nodes_id = sensor_systems.sensor_nodes_id
AND staging_sensorsystems.ingest_id=sensor_systems.source_id;

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


--- Start with the old way to fix an LCS import issues
-- this is to deal with the fact that the old lcs import data does not include units
UPDATE staging_sensors
  SET measurands_id = lcs.measurands_id
  , units = lcs.units
  FROM (SELECT s.ingest_id
  , m.measurands_id
  , m2.units
  FROM staging_sensors s
  JOIN staging_sensorsystems y ON (s.ingest_sensor_systems_id = y.ingest_id)
  JOIN staging_sensornodes n ON (y.ingest_sensor_nodes_id = n.ingest_id)
  JOIN measurands_map m ON (n.source_name = m.source_name AND s.measurand = m.key) -- Source -> Measurands
  JOIN measurands m2 ON (m.measurands_id = m2.measurands_id)
  ) as lcs
  WHERE lcs.ingest_id = staging_sensors.ingest_id
  AND staging_sensors.units IS NULL;


-- deal with realtime data
UPDATE staging_sensors s
SET measurands_id = m.measurands_id
FROM (SELECT measurand, units, measurands_id FROM measurands) as m
WHERE m.measurand = s.measurand
AND m.units = s.units
AND s.measurands_id IS NULL;



-- Then apply the new way (transform)
UPDATE staging_sensors s
SET measurands_id = m.measurands_id
FROM (SELECT key,  measurands_id FROM active_measurands_view) as m
WHERE m.key = s.measurand
--WHERE m.key = format('%s%s', s.measurand, s.units)
AND s.measurands_id IS NULL;



  -- Find a matching sensor based on the same criteria as the systems
  -- this accounts for any bad sensors that were added in previous instances
  -- and will match a version that does not have the instrument added
UPDATE staging_sensors s
SET sensors_id = best.sensors_id
  , is_new = 'f'::boolean
FROM (
  SELECT DISTINCT ON (ss.ingest_id)
    ss.ingest_id
    , ss.sensor_systems_id
    , p.sensors_id
  FROM staging_sensors ss
  JOIN sensors p ON p.sensor_systems_id = ss.sensor_systems_id
  WHERE p.source_id = ss.ingest_id                                         -- best fit
     OR p.source_id ~* without_instrument_pattern(ss.ingest_id)            -- next best
     OR (p.source_id IS NULL AND p.measurands_id = ss.measurands_id)       -- backup
     OR (p.source_id = p.sensors_id::text AND p.measurands_id = ss.measurands_id) -- backup
  ORDER BY
    ss.ingest_id
    , CASE
        WHEN p.source_id = ss.ingest_id THEN 1
        WHEN p.source_id ~* without_instrument_pattern(ss.ingest_id) THEN 2
        WHEN p.source_id IS NULL AND p.measurands_id = ss.measurands_id THEN 3
        WHEN p.source_id = p.sensors_id::text AND p.measurands_id = ss.measurands_id THEN 4
      END
    , p.sensors_id  -- tiebreaker: prefer lowest id (or use added_on DESC, etc.)
) best
WHERE best.ingest_id = s.ingest_id
  AND best.sensor_systems_id = s.sensor_systems_id;



WITH deleted AS (
  DELETE FROM staging_sensors
  WHERE measurands_id IS NULL
  RETURNING *
), r AS (
  INSERT INTO rejects (t, tbl, r, fetchlogs_id)
  SELECT now(),
         'staging_sensors-missing-measurands-id',
         to_jsonb(deleted),
         fetchlogs_id
  FROM deleted
  RETURNING 1
)
SELECT COUNT(*) INTO __rejected_sensors
FROM r;


WITH inserts AS (
INSERT INTO sensors (
  source_id
, sensor_systems_id
, measurands_id
, data_logging_period_seconds
, data_averaging_period_seconds
, sensor_statuses_id
, metadata
  )
SELECT ingest_id
, sensor_systems_id
, measurands_id
, logging_interval_seconds
, averaging_interval_seconds
, COALESCE(ss.sensor_statuses_id, 1)
, s.metadata || jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
FROM staging_sensors s
LEFT JOIN sensor_statuses ss ON (ss.short_code = s.status)
WHERE measurands_id is not null
AND sensor_systems_id is not null
AND sensors_id IS NULL
GROUP BY ingest_id
, sensor_systems_id
, measurands_id
, logging_interval_seconds
, averaging_interval_seconds
, ss.sensor_statuses_id
, s.metadata
, fetchlogs_id
RETURNING 1)
SELECT COUNT(1) INTO __inserted_sensors
FROM inserts;


  --- Update any that need to be updated
  UPDATE sensors
  SET source_id = s.ingest_id
  , metadata = sensors.metadata || jsonb_modified('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
  , modified_on = now()
  FROM staging_sensors s
  WHERE sensors.sensors_id = s.sensors_id
  AND (
    sensors.source_id IS NULL
    OR sensors.source_id = sensors.sensors_id::text
  );

  -- update with the new ones
UPDATE staging_sensors
SET sensors_id = sensors.sensors_id
FROM sensors
WHERE staging_sensors.sensors_id IS NULL
  AND sensors.sensor_systems_id = staging_sensors.sensor_systems_id
  AND sensors.source_id = staging_sensors.ingest_id;


WITH deleted AS (
  DELETE FROM staging_sensors
  WHERE sensors_id IS NULL
  RETURNING *
), r AS (
  INSERT INTO rejects (t, tbl, r, fetchlogs_id)
  SELECT now(),
         'staging_sensors-missing-sensors-id',
         to_jsonb(deleted),
         fetchlogs_id
  FROM deleted
  RETURNING 1
)
SELECT COUNT(*) INTO __rejected_sensors
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

-- Now we match any of teh sensor node flags
UPDATE staging_flags
SET sensor_nodes_id = n.sensor_nodes_id
FROM staging_sensornodes n
WHERE staging_flags.sensor_ingest_id = n.ingest_id
AND staging_flags.sensor_nodes_id IS NULL;

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
  AND (
    (sf.sensors_id IS NULL AND fm.sensors_ids IS NULL) OR
    fm.sensors_ids @> ARRAY[sf.sensors_id]
  );

-- and finally we will insert the new flags
INSERT INTO flags (flag_types_id, sensor_nodes_id, sensors_ids, period, note, metadata)
  SELECT flag_types_id
  , sensor_nodes_id
  , CASE WHEN sensors_id IS NOT NULL THEN ARRAY[sensors_id] ELSE NULL END
  , period
  , note
  , metadata || jsonb_added('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
  FROM staging_flags
  WHERE flag_types_id IS NOT NULL
  AND sensor_nodes_id IS NOT NULL
  AND flags_id IS NULL;


-- And then update any that need to be updated
 UPDATE flags fm
  SET period = sf.period + fm.period
  , note = sf.note
  , metadata = fm.metadata || jsonb_modified('fetchlogs_id', fetchlogs_id, 'etl-process-nodes')
  , modified_on = clock_timestamp()
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
