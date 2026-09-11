"""Named diagnostic queries for post-ingest inspection.

Each query runs against the current transaction's state (staging
tables + committed tables). Intended to be run inside the same
transaction as an ingest, before commit/rollback.

To add a query: append to QUERIES with a unique name, SQL string,
and human-readable title. If the query needs the current
fetchlogs_id, use %(fetchlogs_id)s as a named parameter.
"""

QUERIES = {
    # ----- Staging inspection ---------------------------------------------
    "staged-nodes": {
        "title": "Nodes in staging for this fetchlog",
        "sql": """
            SELECT ingest_id, source_name, source_id,
                   st_astext(geom) AS coords,
                   sensor_nodes_id, is_new
            FROM staging_sensornodes
            WHERE fetchlogs_id = %(fetchlogs_id)s
            ORDER BY source_name, source_id
        """,
    },
    "added-summary": {
        "title": "Nodes added for this fetchlog",
        "sql": """
            SELECT source_name
            , n.metadata->>'added-from' as from
            , COUNT(DISTINCT n.sensor_nodes_id) as nodes
            , COUNT(DISTINCT y.sensor_systems_id) as systems
            , COUNT(DISTINCT s.sensors_id) as sensors
            , COUNT(datetime) as measurements
            FROM sensor_nodes n
            LEFT JOIN sensor_systems y ON (n.sensor_nodes_id = y.sensor_nodes_id)
            LEFT JOIN sensors s ON (y.sensor_systems_id = s.sensor_systems_id)
            LEFT JOIN measurements m ON (s.sensors_id = m.sensors_id)
            WHERE (n.metadata->>'fetchlogs_id')::int = %(fetchlogs_id)s
            GROUP BY source_name, 2
            ORDER BY source_name
        """,
    },
    "fetcher-response": {
        "title": "Fetcher response summary",
        "sql": """
            SELECT fetchlogs_id
            , source_name
            , locations
            , systems
            , sensors
            , flags
            , datetime_from
            , datetime_to
            --, st_astext(boundary) as bounds
            FROM fetcher_responses
            WHERE fetchlogs_id = %(fetchlogs_id)s
            ORDER BY source_name
        """,
    },
    "ingested-summary": {
        "title": "Data ingested for this fetchlog",
        "sql": """
            SELECT MIN(datetime) as first
            , MAX(datetime) as last
            , COUNT(datetime)
            , COUNT(DISTINCT s.ingest_id) FILTER (WHERE s.is_new) as sensors_a
            , COUNT(DISTINCT s.ingest_id) FILTER (WHERE NOT s.is_new) as sensors_m
            , COUNT(DISTINCT y.ingest_id) FILTER (WHERE y.is_new) as systems_a
            , COUNT(DISTINCT y.ingest_id) FILTER (WHERE NOT y.is_new) as systems_m
            , COUNT(DISTINCT n.ingest_id) FILTER (WHERE n.is_new) as nodes_a
            , COUNT(DISTINCT n.ingest_id) FILTER (WHERE NOT n.is_new) as nodes_m
            FROM staging_sensornodes n
            LEFT JOIN staging_sensorsystems y ON (n.sensor_nodes_id = y.sensor_nodes_id)
            LEFT JOIN staging_sensors s ON (y.sensor_systems_id = s.sensor_systems_id)
            LEFT JOIN staging_measurements m ON (s.sensors_id = m.sensors_id)
            WHERE m.fetchlogs_id = %(fetchlogs_id)s
            OR n.fetchlogs_id = %(fetchlogs_id)s
        """,
    },
    "added-details": {
        "title": "Nodes, systems and sensors",
        "sql": """
            SELECT source_name
            , n.metadata->>'added-from' as from
            , n.source_id as node_source_id
            , y.source_id as system_source_id
            , s.source_id as sensor_source_id
            , COUNT(datetime) as measurements
            FROM sensor_nodes n
            LEFT JOIN sensor_systems y ON (n.sensor_nodes_id = y.sensor_nodes_id)
            LEFT JOIN sensors s ON (y.sensor_systems_id = s.sensor_systems_id)
            LEFT JOIN measurements m ON (s.sensors_id = m.sensors_id)
            WHERE (n.metadata->>'fetchlogs_id')::int = %(fetchlogs_id)s
            GROUP BY 1,2,3,4,5
            ORDER BY 1,2,3,4
        """,
    },
    "staged-systems": {
        "title": "Staging sensor systems table",
        "sql": """
            SELECT *
            FROM staging_sensorsystems s
            WHERE s.fetchlogs_id = %(fetchlogs_id)s
        """,
    },
    "staged-sensors": {
        "title": "Sensors in staging for this fetchlog",
        "sql": """
            SELECT s.ingest_id,
                   s.measurand,
                   s.units,
                   s.logging_interval_seconds AS logging_s,
                   s.averaging_interval_seconds AS averaging_s,
                   s.sensor_systems_id,
                   s.sensors_id
            FROM staging_sensors s
            WHERE s.fetchlogs_id = %(fetchlogs_id)s
            ORDER BY ingest_id
        """,
    },
    "staged-measurements": {
        "title": "Measurements in staging (sample)",
        "sql": """
            SELECT ingest_id
        , source_id
        , measurand
        , units
        , value
        , datetime
        , sensors_id
            FROM staging_measurements
            WHERE fetchlogs_id = %(fetchlogs_id)s
            ORDER BY datetime DESC
            LIMIT 25
        """,
    },
    # ----- Match analysis -------------------------------------------------
    "new-nodes": {
        "title": "New nodes added by this fetchlog",
        "sql": """
            SELECT n.source_name, n.source_id, n.site_name,
                   st_astext(n.geom) AS coords, n.added_on
            FROM sensor_nodes n
            JOIN staging_sensornodes s
              ON s.sensor_nodes_id = n.sensor_nodes_id
             AND s.fetchlogs_id = %(fetchlogs_id)s
            WHERE s.is_new
            ORDER BY n.source_name, n.source_id
        """,
    },
    "new-sensors": {
        "title": "New sensors added by this fetchlog",
        "sql": """
            WITH sensors_used AS (
                SELECT sensor
            SELECT p.source_id, m.measurand, m.units_id
            FROM sensors p
            JOIN staging_sensors s ON s.sensors_id = p.sensors_id AND s.fetchlogs_id = %(fetchlogs_id)s
            JOIN measurands m ON (p.measurands_id = m.measurands_id)
            WHERE s.is_new
            LIMIT 10
        """,
    },
    "new-nodes-with-nearby": {
        "title": "New nodes that have a pre-existing node within 0.0001°",
        "sql": """
            SELECT n.source_name
                   , n.source_id,
                   st_astext(n.geom) AS coords,
                   n2.source_name AS existing_source,
                   n2.source_id AS existing_source_id,
                   n2.sensor_nodes_id AS existing_node_id,
        st_distance(n.geom::geography, n2.geom::geography) as distance_m
            FROM sensor_nodes n
            JOIN staging_sensornodes s
              ON s.sensor_nodes_id = n.sensor_nodes_id
             AND s.fetchlogs_id = %(fetchlogs_id)s
             AND s.is_new
            JOIN sensor_nodes n2
              ON st_distance(n.geom, n2.geom) < 0.0001
             AND n2.sensor_nodes_id != n.sensor_nodes_id
            ORDER BY n.source_name, n.source_id
        """,
    },
    "matched-nodes": {
        "title": "Nodes matched to existing records",
        "sql": """
            SELECT s.ingest_id,
                   n.source_name, n.source_id, n.site_name,
                   n.sensor_nodes_id
            FROM staging_sensornodes s
            JOIN sensor_nodes n USING (sensor_nodes_id)
            WHERE s.fetchlogs_id = %(fetchlogs_id)s
              AND NOT s.is_new
            ORDER BY n.source_name, n.source_id
        """,
    },

    # ----- Rejects --------------------------------------------------------
    "rejects": {
        "title": "Rejected records for this fetchlog",
        "sql": """
            SELECT tbl,
                   r->>'ingest_id' AS ingest_id,
                   r->>'measurand' AS measurand,
                   r->>'units' AS units,
                   r->>'sensors_id' AS sensors_id,
                   r->>'measurands_id' AS measurands_id,
                   r->>'units_id' AS units_id
            FROM rejects
            WHERE fetchlogs_id = %(fetchlogs_id)s
            LIMIT 50
        """,
    },
    "rejects-unit-mismatch": {
        "title": "Rejects with unit mismatches (measurement rejects)",
        "sql": """
            SELECT tbl,
                   r->>'ingest_id' AS ingest_id,
                   r->>'measurand' AS measurand,
                   r->>'units' AS staged_units,
                   m.units AS expected_units,
                   r->>'measurands_id' AS measurands_id
            FROM rejects
            JOIN measurands m
              ON (r->>'measurands_id')::int = m.measurands_id
            WHERE fetchlogs_id = %(fetchlogs_id)s
            LIMIT 50
        """,
    },
    # ----- Cross-cutting --------------------------------------------------
    "providers-in-batch": {
        "title": "Provider IDs across all nodes matched or added in this fetchlog",
        "sql": """
            SELECT DISTINCT n.providers_id, p.label
            FROM sensor_nodes n
            LEFT JOIN providers p USING (providers_id)
            WHERE n.sensor_nodes_id IN (
                SELECT sensor_nodes_id
                FROM staging_sensornodes
                WHERE fetchlogs_id = %(fetchlogs_id)s
                  AND sensor_nodes_id IS NOT NULL
            )
            ORDER BY n.providers_id
        """,
    },
    "sources-added-count": {
        "title": "Count of nodes added per source (this fetchlog)",
        "sql": """
            SELECT n.source_name
            , COUNT(*) AS added
            FROM sensor_nodes n
            JOIN staging_sensornodes s
              ON s.sensor_nodes_id = n.sensor_nodes_id
             AND s.fetchlogs_id = %(fetchlogs_id)s
             AND s.is_new
            GROUP BY n.source_name
            ORDER BY added DESC
        """,
    },
    "rejects-by-reason": {
        "title": "Reject counts grouped by table/reason",
        "sql": """
        SELECT tbl, COUNT(*) AS n
        FROM rejects
        WHERE fetchlogs_id = %(fetchlogs_id)s
        GROUP BY tbl
        ORDER BY n DESC
    """,
    },
    "rejects-missing-measurand": {
        "title": "Sensors rejected because measurand didn't match active_measurands_view",
        "sql": """
        SELECT r->>'ingest_id' AS ingest_id,
               r->>'measurand' AS measurand,
               r->>'units' AS units
        FROM rejects
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND tbl = 'staging_sensors-missing-measurands-id'
        LIMIT 50
    """,
    },
    "rejects-unsupported-measurand": {
        "title": "Measurements rejected: sensor doesn't exist for the ingest_id",
        "sql": """
        SELECT r->>'ingest_id' AS ingest_id,
               r->>'measurand' AS measurand,
               r->>'source_name' AS source_name,
               r->>'source_id' AS source_id,
               COUNT(*) AS n
        FROM rejects
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND tbl = 'meas-unsupported-measurand'
        GROUP BY 1, 2, 3, 4
        ORDER BY n DESC
        LIMIT 25
    """,
    },
    "rejects-unit-conversion": {
        "title": "Measurements rejected: no unit conversion available",
        "sql": """
        SELECT r->>'measurand' AS measurand,
               r->>'measurands_id' AS measurands_id,
               r->>'units' AS staged_units,
               (r->>'units_id')::int AS staged_units_id,
               m.units AS expected_units,
               m.units_id AS expected_units_id,
               r->>'source_name' AS source_name,
               COUNT(1) as n
        FROM rejects
        JOIN measurands m ON (r->>'measurands_id')::int = m.measurands_id
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND tbl = 'meas-no-unit-conversion'
        GROUP BY 1,2,3,4,5,6,7
        LIMIT 50
    """,
    },
    "unmatched-nodes": {
        "title": "Nodes in staging that failed to match any existing node "
        "AND weren't newly inserted",
        "sql": """
        SELECT ingest_id, source_name, source_id,
               st_astext(geom) AS coords,
               matching_method
        FROM staging_sensornodes
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND sensor_nodes_id IS NULL
        ORDER BY source_name, source_id
    """,
    },
    "nearby-existing-nodes": {
        "title": "For each new node, list all pre-existing nodes within 500m",
        "sql": """
        SELECT n.source_name AS new_source,
               n.source_id AS new_source_id,
               st_astext(n.geom) AS new_coords,
               n2.source_name AS existing_source,
               n2.source_id AS existing_source_id,
               n2.sensor_nodes_id AS existing_id,
               ROUND((st_distance(n.geom::geography, n2.geom::geography))::numeric, 1)
                 AS distance_m,
               p.label AS existing_provider
        FROM sensor_nodes n
        JOIN staging_sensornodes s
          ON s.sensor_nodes_id = n.sensor_nodes_id
         AND s.fetchlogs_id = %(fetchlogs_id)s
         AND s.is_new
        JOIN sensor_nodes n2
          ON st_distance(n.geom::geography, n2.geom::geography) < 500
         AND n2.sensor_nodes_id != n.sensor_nodes_id
        LEFT JOIN providers p ON n2.providers_id = p.providers_id
        ORDER BY new_source, new_source_id, distance_m
    """,
    },
    "cross-provider-collisions": {
        "title": "Nodes at same coordinates but different providers "
        "(potential deduplication targets)",
        "sql": """
        SELECT n.source_name, n.source_id, st_astext(n.geom) AS coords,
               p.label AS provider,
               n2.source_name AS other_source,
               p2.label AS other_provider
        FROM sensor_nodes n
        JOIN staging_sensornodes s
          ON s.sensor_nodes_id = n.sensor_nodes_id
         AND s.fetchlogs_id = %(fetchlogs_id)s
        JOIN sensor_nodes n2
          ON st_distance(n.geom, n2.geom) < 0.0001
         AND n2.sensor_nodes_id != n.sensor_nodes_id
         AND n2.providers_id != n.providers_id
        LEFT JOIN providers p ON n.providers_id = p.providers_id
        LEFT JOIN providers p2 ON n2.providers_id = p2.providers_id
        ORDER BY n.source_name, n.source_id
    """,
    },
    "orphan-sensors": {
        "title": "Sensors in staging with no sensor_systems_id assigned",
        "sql": """
        SELECT ingest_id, ingest_sensor_systems_id,
               measurand, units
        FROM staging_sensors
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND sensor_systems_id IS NULL
        LIMIT 50
    """,
    },
    "staged-summary": {
        "title":"Objects by source. Provided as counts by unique keys/ids",
        "sql":"""
        WITH summary_counts AS (
        SELECT source_name
        , COUNT(DISTINCT n.ingest_id) as node_keys
        , COUNT(DISTINCT y.ingest_id) as system_keys
        , COUNT(DISTINCT s.ingest_id) as sensor_keys
        , COUNT(DISTINCT n.sensor_nodes_id) as node_ids
        , COUNT(DISTINCT y2.sensor_systems_id) as system_ids
        , COUNT(DISTINCT s2.sensors_id) as sensor_ids
        FROM staging_sensornodes n
        LEFT JOIN staging_sensorsystems y ON (y.ingest_sensor_nodes_id = n.ingest_id)
        LEFT JOIN staging_sensors s ON (s.ingest_sensor_systems_id = y.ingest_id)
        LEFT JOIN staging_sensorsystems y2 ON (y2.sensor_nodes_id = n.sensor_nodes_id)
        LEFT JOIN staging_sensors s2 ON (s2.sensor_systems_id = y2.sensor_systems_id)
        GROUP BY 1
        )
        SELECT source_name
        , node_keys::text||'/'||node_ids::text as nodes
        , system_keys::text||'/'||system_ids::text as systems
        , sensor_keys::text||'/'||sensor_ids::text as sensors
        FROM summary_counts
        """,
    },
    "sensor-unit-summary": {
        "title": "Sensor units grouped by measurand (spot outliers)",
        "sql": """
        SELECT measurand, units, COUNT(*) AS n
        FROM staging_sensors
        WHERE fetchlogs_id = %(fetchlogs_id)s
        GROUP BY measurand, units
        ORDER BY measurand, n DESC
    """,
    },
    "instrument-key-summary": {
        "title": "Instrument keys in staging for this fetchlog",
        "sql": """
            SELECT s.manufacturer_key
            , s.model_key
            , COUNT(1) as n
            , COUNT(sensor_systems_id) as with_id
            , SUM(is_new::int) as is_new
            FROM staging_sensorsystems s
            WHERE s.fetchlogs_id = %(fetchlogs_id)s
            GROUP BY 1,2
            ORDER BY 1,2
        """,
    },
    "instrument-summary": {
        "title": "Instruments linked in staging for this fetchlog",
        "sql": """
            SELECT i.instruments_id
            , m.full_name as manufacturer
            , i.label as model
            , COUNT(sensor_systems_id) as with_id
            , SUM(is_new::int) as is_new
            FROM staging_sensorsystems s
            JOIN instruments i ON (i.ingest_id = s.instrument_ingest_id)
            JOIN entities m ON (i.manufacturer_entities_id = m.entities_id)
            WHERE s.fetchlogs_id = %(fetchlogs_id)s
            GROUP BY 1,2,3
            ORDER BY 2,3
        """,
    },
    "measurement-unit-summary": {
        "title": "Measurement units grouped by measurand (compare to sensors above)",
        "sql": """
        SELECT measurand, units, COUNT(*) AS n
        FROM staging_measurements
        WHERE fetchlogs_id = %(fetchlogs_id)s
        GROUP BY measurand, units
        ORDER BY measurand, n DESC
    """,
    },
    "unit-conversions-applied": {
        "title": "Sensor units that differ from measurement units "
        "(conversions applied by ETL)",
        "sql": """
        SELECT DISTINCT s.measurand,
               s.units AS staged_sensor_units,
               m2.units AS current_sensor_units,
               (SELECT COUNT(*) FROM staging_measurements sm
                WHERE sm.fetchlogs_id = %(fetchlogs_id)s
                  AND sm.sensors_id = s.sensors_id) AS meas_count
        FROM staging_sensors s
        JOIN measurands m2 ON s.measurands_id = m2.measurands_id
        WHERE s.fetchlogs_id = %(fetchlogs_id)s
          AND s.units IS DISTINCT FROM m2.units
        LIMIT 50
    """,
    },
    "flagged-measurements": {
        "title": "Measurement values flagged as out-of-range and nullified",
        "sql": """
        SELECT s.ingest_id, s.measurand, s.units,
               COUNT(*) FILTER (WHERE m.value IS NULL
                                AND m.value_original IS NOT NULL) AS flagged,
               COUNT(*) AS total,
               MIN(m.value_original) AS min_flagged,
               MAX(m.value_original) AS max_flagged,
               p.lower_limit, p.upper_limit
        FROM staging_measurements m
        JOIN staging_sensors s ON s.sensors_id = m.sensors_id
        JOIN measurands p ON s.measurands_id = p.measurands_id
        WHERE m.fetchlogs_id = %(fetchlogs_id)s
          AND p.upper_limit IS NOT NULL
        GROUP BY s.ingest_id, s.measurand, s.units,
                 p.lower_limit, p.upper_limit
        HAVING COUNT(*) FILTER (WHERE m.value IS NULL
                                AND m.value_original IS NOT NULL) > 0
        ORDER BY flagged DESC
        LIMIT 50
    """,
    },
    "time-gaps": {
        "title": "Sensors with unusual gaps between consecutive readings",
        "sql": """
        WITH gaps AS (
            SELECT sensors_id, datetime,
                   datetime - lag(datetime) OVER (
                       PARTITION BY sensors_id ORDER BY datetime
                   ) AS gap
            FROM staging_measurements
            WHERE fetchlogs_id = %(fetchlogs_id)s
        )
        SELECT g.sensors_id, s.source_id AS ingest_id,
               MAX(g.gap) AS max_gap,
               COUNT(*) FILTER (WHERE g.gap > interval '1 day') AS large_gaps
        FROM gaps g
        JOIN sensors s ON g.sensors_id = s.sensors_id
        WHERE g.gap IS NOT NULL
        GROUP BY g.sensors_id, s.source_id
        HAVING MAX(g.gap) > interval '1 day'
        ORDER BY max_gap DESC
        LIMIT 25
    """,
    },
    "suspicious-values": {
        "title": "Repeated identical measurement values (stuck sensor?)",
        "sql": """
        SELECT s.source_id AS ingest_id, s.measurand,
               m.value, COUNT(*) AS n,
               MIN(m.datetime) AS first_seen,
               MAX(m.datetime) AS last_seen
        FROM staging_measurements m
        JOIN sensors s ON m.sensors_id = s.sensors_id
        WHERE m.fetchlogs_id = %(fetchlogs_id)s
          AND m.value IS NOT NULL
        GROUP BY s.source_id, s.measurand, m.value
        HAVING COUNT(*) >= 5
        ORDER BY n DESC
        LIMIT 25
    """,
    },
    "measurement-timespan": {
        "title": "Measurement date range and cadence per sensor",
        "sql": """
        SELECT s.source_id AS ingest_id,
               s.measurand,
               COUNT(*) AS n,
               MIN(m.datetime) AS first_reading,
               MAX(m.datetime) AS last_reading,
               MAX(m.datetime) - MIN(m.datetime) AS span,
               ROUND(EXTRACT(EPOCH FROM (
                   MAX(m.datetime) - MIN(m.datetime)
               ) / GREATEST(COUNT(*) - 1, 1))::numeric, 1)
                 AS avg_seconds_between
        FROM staging_measurements m
        JOIN sensors s ON m.sensors_id = s.sensors_id
        WHERE m.fetchlogs_id = %(fetchlogs_id)s
        GROUP BY s.source_id, s.measurand
        ORDER BY n DESC
        LIMIT 25
    """,
    },
    "future-datetimes": {
        "title": "Measurements with timestamps in the future (clock/timezone bug?)",
        "sql": """
        SELECT ingest_id, source_id, measurand, datetime, value
        FROM staging_measurements
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND datetime > now() + interval '1 hour'
        ORDER BY datetime DESC
        LIMIT 25
    """,
    },
    "ancient-datetimes": {
        "title": "Measurements with suspiciously old timestamps (before 2000)",
        "sql": """
        SELECT ingest_id, source_id, measurand, datetime, value
        FROM staging_measurements
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND datetime < '2000-01-01'
        ORDER BY datetime ASC
        LIMIT 25
    """,
    },
    "staged-flags": {
        "title": "Flags staged for this fetchlog",
        "sql": """
        SELECT f.ingest_id,
               f.sensor_ingest_id,
               f.flag_types_id,
               ft.label AS flag_type,
               f.period,
               f.note
        FROM staging_flags f
        LEFT JOIN flag_types ft ON f.flag_types_id = ft.flag_types_id
        WHERE f.fetchlogs_id = %(fetchlogs_id)s
        ORDER BY f.sensor_ingest_id, lower(f.period)
        LIMIT 100
    """,
    },
    "unmatched-flags": {
        "title": "Flags that couldn't be linked to a sensor or node",
        "sql": """
        SELECT ingest_id, sensor_ingest_id, flag_types_id,
               datetime_from, datetime_to
        FROM staging_flags
        WHERE fetchlogs_id = %(fetchlogs_id)s
          AND (sensors_id IS NULL OR sensor_nodes_id IS NULL
               OR flag_types_id IS NULL)
        LIMIT 50
    """,
    },
    "already-ingested-overlap": {
        "title": "Measurements at (sensor, datetime) pairs that already existed",
        "sql": """
        SELECT COUNT(*) AS overlap_count,
               MIN(m.datetime) AS earliest_overlap,
               MAX(m.datetime) AS latest_overlap
        FROM staging_measurements m
        JOIN measurements existing
          ON existing.sensors_id = m.sensors_id
         AND existing.datetime = m.datetime
        WHERE m.fetchlogs_id = %(fetchlogs_id)s
    """,
    },
    "nodes-touched-recently": {
        "title": "Nodes updated by this fetchlog and their last-touched times",
        "sql": """
        SELECT n.source_name, n.source_id, n.site_name,
               n.added_on, n.modified_on,
               (SELECT MAX(sr.datetime_last)
                FROM sensor_systems sy
                JOIN sensors s ON s.sensor_systems_id = sy.sensor_systems_id
                JOIN sensors_rollup sr ON sr.sensors_id = s.sensors_id
                WHERE sy.sensor_nodes_id = n.sensor_nodes_id) AS latest_measurement
        FROM sensor_nodes n
        JOIN staging_sensornodes s
          ON s.sensor_nodes_id = n.sensor_nodes_id
         AND s.fetchlogs_id = %(fetchlogs_id)s
        ORDER BY n.modified_on DESC NULLS LAST
        LIMIT 50
    """,
    },
}

PACKS = {
    "summary": [
        "staged-summary",
        "added-summary",
        "ingested-summary",
        "sensor-unit-summary",
        "instrument-summary",
        "rejects-by-reason",
    ],
    "staged": [
        "staged-nodes",
        "staged-systems",
        "staged-sensors",
        "staged-measurements",
    ],
    "instruments": [
        "instrument-key-summary",
        "instrument-summary",
    ],
    "matching": [
        "unmatched-nodes",
        "new-nodes-with-nearby",
        "cross-provider-collisions",
        "matched-nodes",
    ],
    "rejects": [
        "rejects-by-reason",
        "rejects-missing-measurand",
        "rejects-unsupported-measurand",
        "rejects-unit-conversion",
    ],
    "units": [
        "sensor-unit-summary",
        "measurement-unit-summary",
        "unit-conversions-applied",
    ],
    "quality": [
        "flagged-measurements",
        "future-datetimes",
        "ancient-datetimes",
        "suspicious-values",
    ],
    "spatial": [
        "new-nodes-with-nearby",
        "cross-provider-collisions",
        "nearby-existing-nodes",
    ],
}


def list_queries() -> str:
    lines = ["Diagnostic packs (groups of related queries):", ""]
    for name, members in PACKS.items():
        lines.append(f"  {name:<15} → {', '.join(members)}")
    lines.append("")
    lines.append("Individual queries:")
    lines.append("")
    for name, spec in QUERIES.items():
        lines.append(f"  {name:<32} {spec['title']}")
    return "\n".join(lines)


def resolve_names(names: list[str]) -> list[str]:
    """Expand 'all', pack names, and validate query names."""
    expanded = []
    for n in names:
        if n == "all":
            expanded.extend(QUERIES.keys())
        elif n in PACKS:
            expanded.extend(PACKS[n])
        elif n in QUERIES:
            expanded.append(n)
        else:
            raise ValueError(
                f"Unknown query or pack: {n!r}. "
                f"Available packs: {list(PACKS)}. "
                f"See --diagnose list for individual queries."
            )
    # Dedupe while preserving order
    seen = set()
    return [x for x in expanded if not (x in seen or seen.add(x))]
