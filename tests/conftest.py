import pytest
import psycopg2
import boto3
import logging
from moto import mock_aws
from pathlib import Path
import os
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
import json

from psycopg2.extras import RealDictCursor
from ingest.settings import settings
from ingest.resources import Resources

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


def pytest_addoption(parser):
    parser.addoption(
        "--persist-db",
        action="store_true",
        default=False,
        help="Commit test DB changes instead of rolling back (for debugging).",
    )

def pytest_configure(config):
    if config.getoption("--persist-db"):
        print("\n⚠️  --persist-db active: DB changes will be COMMITTED\n")

@pytest.fixture(scope="function")
def persist_db(request):
    return request.config.getoption("--persist-db")


@pytest.fixture(scope="session")
def test_data_dir():
    """Returns path to tests directory for test data files."""
    return Path(__file__).parent


@pytest.fixture(scope="function")
def db_connection(persist_db):
    """
    Provides fresh psycopg2 connection per test with automatic rollback.
    Uses transaction isolation to prevent side effects between tests.
    """
    conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
    conn.set_session(autocommit=False)
    yield conn
    if persist_db:
        conn.commit()
    else:
        conn.rollback()
    conn.close()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """Provides cursor from db_connection with automatic cleanup."""
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture(scope="function")
def ingest_resources(db_connection, mock_s3):
    """
    Provides Resources with test-managed resources.

    Database connection and S3 client are managed by test fixtures.
    Context will NOT close these resources (ownership=False).
    """
    ctx = Resources(
        connection=db_connection,
        s3_client=mock_s3
    )
    return ctx


@pytest.fixture(scope="function")
def clean_fetchlogs(db_cursor):
    """Truncates fetchlogs table before test."""
    db_cursor.execute("TRUNCATE TABLE fetchlogs CASCADE")
    yield
    # Cleanup happens via db_connection rollback


@pytest.fixture(scope="function")
def mock_s3():
    """
    Provides mocked S3 client using moto.
    Creates test bucket and mocks S3 API calls.
    """
    with mock_aws():
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = settings.FETCH_BUCKET
        s3_client.create_bucket(Bucket=bucket_name)
        yield s3_client


@pytest.fixture(scope="function")
def mock_s3_with_object(mock_s3):
    """
    Mocked S3 with a test object uploaded.
    Returns (s3_client, bucket, key, file_size, last_modified).
    """
    bucket = settings.FETCH_BUCKET
    key = "lcs-etl-pipeline/test.json"
    content = b'{"test": "data"}'

    mock_s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content
    )

    # Get object metadata
    response = mock_s3.head_object(Bucket=bucket, Key=key)

    yield mock_s3, bucket, key, len(content), response['LastModified']


@pytest.fixture
def mock_cloudwatch(mocker):
    """Mocks CloudWatch metrics calls."""
    mock_cw = mocker.patch('ingest.handler.cw')
    mock_cw.put_metric_data = mocker.MagicMock()
    return mock_cw


@pytest.fixture
def sample_s3_event(test_data_dir):
    """Returns mock S3 event structure for single file."""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": settings.FETCH_BUCKET
                    },
                    "object": {
                        "key": "lcs-etl-pipeline/test.json"
                    }
                }
            }
        ]
    }


@pytest.fixture
def sample_sns_event(test_data_dir):
    """Returns mock SNS event wrapping S3 event."""
    s3_message = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": settings.FETCH_BUCKET},
                    "object": {"key": "lcs-etl-pipeline/test.json"}
                }
            }
        ]
    }

    return {
        "Records": [
            {
                "EventSource": "aws:sns",
                "Sns": {
                    "Message": json.dumps(s3_message)
                }
            }
        ]
    }


@pytest.fixture
def sample_batch_s3_event():
    """Returns S3 event with multiple records."""
    bucket = settings.FETCH_BUCKET
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": f"lcs-etl-pipeline/test{i}.json"}
                }
            }
            for i in range(5)
        ]
    }


@pytest.fixture
def sample_batch_sns_event():
    """Returns SNS event wrapping S3 event with multiple S3 records."""
    s3_message = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": settings.FETCH_BUCKET},
                    "object": {"key": f"lcs-etl-pipeline/test{i}.json"}
                }
            }
            for i in range(5)
        ]
    }

    return {
        "Records": [
            {
                "EventSource": "aws:sns",
                "Sns": {
                    "Message": json.dumps(s3_message)
                }
            }
        ]
    }

@pytest.fixture
def cloudwatch_event():
    """Returns mock CloudWatch/EventBridge event."""
    return {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "resources": ["arn:aws:events:us-east-1:123456789012:rule/test-rule"]
    }


@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    class MockContext:
        function_name = "openaq-ingest"
        memory_limit_in_mb = 512
        invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
        aws_request_id = "test-request-id-123"

    return MockContext()


@pytest.fixture
def sample_metadata_fetchlogs(db_cursor, db_connection, clean_fetchlogs):
    """Create sample metadata fetchlogs for testing."""
    test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    fetchlog_ids = []

    for i in range(5):
        key = f"lcs-etl-pipeline/stations/test-{i}.json"
        modified = test_time + timedelta(hours=i)

        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES (%s, %s, %s)
            RETURNING fetchlogs_id
        """, (key, modified, test_time))
        fetchlog_ids.append(db_cursor.fetchone()[0])

    return fetchlog_ids



@pytest.fixture
def mock_cronhandler_settings(mocker):
    """Mock settings for cronhandler tests with safe defaults."""
    mock_settings = mocker.patch('ingest.handler.settings')
    mock_settings.PAUSE_INGESTING = False
    mock_settings.INGEST_TIMEOUT = 300
    mock_settings.FETCH_ASCENDING = True
    mock_settings.METADATA_LIMIT = 10
    mock_settings.REALTIME_LIMIT = 10
    mock_settings.PIPELINE_LIMIT = 10
    return mock_settings

def _get_measurand_id(cursor, measurand: str, units: str = None):
    """Look up measurands_id by name (and optionally units)."""
    if units:
        cursor.execute(
            "SELECT measurands_id, units FROM measurands WHERE measurand = %s AND units = %s AND is_active LIMIT 1",
            (measurand, units),
        )
    else:
        cursor.execute(
            "SELECT measurands_id, units FROM measurands WHERE measurand = %s AND is_active LIMIT 1",
            (measurand,),
        )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Measurand '{measurand}' (units={units}) not found in measurands table")
    return row[0], row[1]

def _insert_flag(cursor, sensor_nodes_id: int, sensors_ids, flag_spec: dict) -> int:
    """
    Insert a flag row.

    flag_spec keys:
      - flag_types_id (required)
      - period: (start, end) tuple/list of timestamptz strings, or a tstzrange literal
      - note (optional)
      - bounds (optional, default '[]')  e.g. '[)', '[]'
    """
    flag_types_id = flag_spec["flag_types_id"]
    note = flag_spec.get("note")
    bounds = flag_spec.get("bounds", "[]")
    period = flag_spec["period"]

    if isinstance(period, (list, tuple)) and len(period) == 2:
        period_sql = "tstzrange(%s, %s, %s)"
        period_params = (period[0], period[1], bounds)
    else:
        # assume caller passed a range literal or a psycopg2 Range
        period_sql = "%s"
        period_params = (period,)

    cursor.execute(
        f"""
        INSERT INTO flags (sensor_nodes_id, sensors_ids, flag_types_id, period, note)
        VALUES (%s, %s, %s, {period_sql}, %s)
        RETURNING flags_id
        """,
        (sensor_nodes_id, sensors_ids, flag_types_id, *period_params, note),
    )
    return cursor.fetchone()[0]

def _create_node(cursor, node_spec: dict) -> dict:
    source_name = node_spec.get("source_name", "testing")
    source_id = node_spec.get("source_id") or node_spec.get("location") or f"{source_name}-node"
    site_name = node_spec.get("site_name", source_id)
    ismobile = node_spec.get("ismobile", False)

    # Accept coordinates in a few shapes:
    #   "coordinates": {"lat": 42.05, "lon": -123.04}
    #   "coordinates": [lon, lat]
    #   "lat": ..., "lon": ...
    lat = lon = None
    coords = node_spec.get("coordinates")
    if isinstance(coords, dict):
        lat = coords.get("lat") or coords.get("latitude")
        lon = coords.get("lon") or coords.get("lng") or coords.get("longitude")
    elif isinstance(coords, (list, tuple)) and len(coords) == 2:
        lon, lat = coords
    else:
        lat = node_spec.get("lat") or node_spec.get("latitude")
        lon = node_spec.get("lon") or node_spec.get("longitude")

    geom_sql = "NULL"
    geom_params = ()
    if lat is not None and lon is not None:
        geom_sql = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
        geom_params = (float(lon), float(lat))

    cursor.execute(
        f"""
        INSERT INTO sensor_nodes (site_name, source_name, source_id, ismobile, geom)
        VALUES (%s, %s, %s, %s, {geom_sql})
        ON CONFLICT (source_name, source_id) DO UPDATE
          SET site_name = EXCLUDED.site_name,
              geom = EXCLUDED.geom
        RETURNING sensor_nodes_id
        """,
        (site_name, source_name, source_id, ismobile, *geom_params),
    )
    sensor_nodes_id = cursor.fetchone()[0]

    # Normalize: if "sensors" given at node level, wrap them in a default system
    if "systems" in node_spec:
        systems = node_spec["systems"]
    elif "sensors" in node_spec:
        systems = [{"sensors": node_spec["sensors"]}]
    else:
        systems = []

    created_systems = []
    for i, sys_spec in enumerate(systems):
        sys_source_id = sys_spec.get("source_id", f"{source_id}-sys{i+1}" if len(systems) > 1 else f"{source_name}-{source_id}")

        cursor.execute(
            """
            INSERT INTO sensor_systems (sensor_nodes_id, source_id)
            VALUES (%s, %s)
            ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE
              SET source_id = EXCLUDED.source_id
            RETURNING sensor_systems_id
            """,
            (sensor_nodes_id, sys_source_id),
        )
        sensor_systems_id = cursor.fetchone()[0]


        created_sensors = []
        for sensor_spec in sys_spec.get("sensors", []):
            measurand = sensor_spec["measurand"]
            units = sensor_spec.get("units")
            measurands_id, resolved_units = _get_measurand_id(cursor, measurand, units)

            sensor_source_id = sensor_spec.get(
                "source_id", f"{source_id}-{measurand}"
            )
            period = sensor_spec.get("period")  # data_averaging_period_seconds
            logging_period = sensor_spec.get("logging_period", period)

            cursor.execute(
                """
                INSERT INTO sensors (
                    sensor_systems_id, measurands_id, source_id,
                    data_averaging_period_seconds, data_logging_period_seconds
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sensor_systems_id, measurands_id, source_id) DO UPDATE
                  SET data_averaging_period_seconds = EXCLUDED.data_averaging_period_seconds
                RETURNING sensors_id
                """,
                (sensor_systems_id, measurands_id, sensor_source_id, period, logging_period),
            )
            sensors_id = cursor.fetchone()[0]

            sensor_flag_ids = []
            for flag_spec in sensor_spec.get("flags", []):
                fid = _insert_flag(cursor, sensor_nodes_id, [sensors_id], flag_spec)
                sensor_flag_ids.append(fid)

            created_sensors.append({
                "sensors_id": sensors_id,
                "source_id": sensor_source_id,
                "measurand": measurand,
                "units": resolved_units,
                "measurands_id": measurands_id,
                "flags": sensor_flag_ids,
            })

        created_systems.append({
            "sensor_systems_id": sensor_systems_id,
            "source_id": sys_source_id,
            "sensors": created_sensors,
        })

    node_flag_ids = []
    for flag_spec in node_spec.get("flags", []):
        fid = _insert_flag(cursor, sensor_nodes_id, None, flag_spec)
        node_flag_ids.append(fid)

    return {
        "sensor_nodes_id": sensor_nodes_id,
        "source_name": source_name,
        "source_id": source_id,
        "site_name": site_name,
        "systems": created_systems,
        "flags": node_flag_ids,
    }


@pytest.fixture
def create_node(ingest_resources):
    """
    Fixture: returns a factory to create sensor_nodes with systems/sensors.

    Usage:
        def test_something(create_node):
            node = create_node({
                "site_name": "station1",
                "source_name": "testing",
                "source_id": "testing-station1",
                "sensors": [{"period": 900, "measurand": "no"}],
            })
            assert node["sensor_nodes_id"] is not None
            sensor_id = node["systems"][0]["sensors"][0]["sensors_id"]
    """
    def _factory(node_spec: dict) -> dict:
        with ingest_resources.cursor() as cursor:
            return _create_node(cursor, node_spec)
    return _factory


def get_test_path(relpath: str) -> str:
    """Absolute path to a file in the tests/ directory."""
    return os.path.join(os.path.dirname(__file__), relpath)


@pytest.fixture
def disable_temp_tables():
    """Force staging tables (not TEMP) so tests can inspect them."""
    with patch.object(settings, "USE_TEMP_TABLES", False):
        yield


@pytest.fixture
def sample_fetchlog(db_cursor, clean_fetchlogs):
    """Insert a fetchlogs row and return its id."""
    test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    db_cursor.execute("""
        INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
        VALUES ('test-data.json', %s, %s, NULL, false)
        RETURNING fetchlogs_id
    """, (test_time, test_time))
    return db_cursor.fetchone()[0]


@pytest.fixture
def make_test_file(tmp_path):
    """
    Write content to a temp file and return its path.

    Usage:
        path = make_test_file("realtime.ndjson", '{"foo": "bar"}')
    """
    def _make(name: str, content: str) -> str:
        p = tmp_path / name
        p.write_text(content)
        return str(p)
    return _make

# Registry of named queries. Keeps SQL out of the fixture body
# and makes it trivial to add new ones.
_QUERIES = {
    "rejects": """
        SELECT *
        FROM rejects
        WHERE fetchlogs_id = %(fetchlogs_id)s
    """,
    "staged_sensor_nodes": """
        SELECT ingest_id, source_id, source_name, site_name,
               sensor_nodes_id, is_new, is_moved
        FROM staging_sensornodes
        ORDER BY ingest_id
    """,
    "staged_systems": """
        SELECT ingest_id, sensor_systems_id, is_new
        FROM staging_sensorsystems
        ORDER BY ingest_id
    """,
    "staged_sensors": """
        SELECT *
        FROM staging_sensors
        ORDER BY ingest_id
    """,
    "staged_flags": """
        SELECT *
        FROM staging_flags
    """,
    "flags": """
        SELECT flags_id
        , flag_types_id
        , to_char(lower(period), 'YYYY-MM-DD HH24:MI:SS')|| ' to ' ||
          to_char(upper(period), 'YYYY-MM-DD HH24:MI:SS') AS formatted_range
        , sensor_nodes_id
        , sensors_ids
        , added_on
        , modified_on
        , note
        FROM flags
    """,
    "staged_measurements": """
        SELECT m.*
               , m2.units AS units_needed
        FROM staging_measurements m
        LEFT JOIN measurands m2 ON m.measurands_id = m2.measurands_id
        ORDER BY m.ingest_id
    """,
    "sensors": """
        SELECT s.sensors_id,
               s.measurands_id,
               s.sensor_systems_id,
               s.data_averaging_period_seconds,
               s.data_logging_period_seconds,
               m.measurand, m.units, s.source_id
               , s.metadata
               , s.added_on, s.modified_on
        FROM sensors s
        JOIN measurands m USING (measurands_id)
        ORDER BY s.added_on
    """,
    "nodes": """
        SELECT *
        FROM sensor_nodes s
        ORDER BY s.source_id
    """,
    "systems": """
        SELECT *
        FROM sensor_systems s
        ORDER BY s.source_id
    """,
}

@pytest.fixture
def get_object(ingest_resources):
    """
    Fetch rows from a named query.

    Usage:
        rows = get_object("staged_sensors")
        rejects = get_object("rejects", fetchlogs_id=42)
    """
    def _fetch(name: str, **params):
        if name not in _QUERIES:
            raise ValueError(
                f"Unknown query '{name}'. Available: {sorted(_QUERIES)}"
            )
        with ingest_resources.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(_QUERIES[name], params)
            return cur.fetchall()

    return _fetch
