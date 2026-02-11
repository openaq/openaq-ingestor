import pytest
import psycopg2
import boto3
from moto import mock_aws
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json

from ingest.settings import settings
from ingest.context import IngestContext


@pytest.fixture(scope="session")
def test_data_dir():
    """Returns path to tests directory for test data files."""
    return Path(__file__).parent


@pytest.fixture(scope="function")
def db_connection():
    """
    Provides fresh psycopg2 connection per test with automatic rollback.
    Uses transaction isolation to prevent side effects between tests.
    """
    conn = psycopg2.connect(settings.DATABASE_WRITE_URL)
    conn.set_session(autocommit=False)  # Explicit transaction control
    yield conn
    conn.rollback()  # Rollback all changes after test
    conn.close()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """Provides cursor from db_connection with automatic cleanup."""
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture(scope="function")
def ingest_context(db_connection, mock_s3):
    """
    Provides IngestContext with test-managed resources.

    Database connection and S3 client are managed by test fixtures.
    Context will NOT close these resources (ownership=False).
    """
    ctx = IngestContext(
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

    db_connection.commit()
    return fetchlog_ids


@pytest.fixture
def sample_pipeline_fetchlogs(db_cursor, db_connection, clean_fetchlogs):
    """Create sample pipeline measurement fetchlogs."""
    test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    fetchlog_ids = []

    for i in range(5):
        key = f"lcs-etl-pipeline/measures/test-{i}.csv"
        modified = test_time + timedelta(hours=i)

        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES (%s, %s, %s)
            RETURNING fetchlogs_id
        """, (key, modified, test_time))
        fetchlog_ids.append(db_cursor.fetchone()[0])

    db_connection.commit()
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
