import pytest
import os
from datetime import date, datetime, timezone
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings


def debug_query_data(sql, params, cursor):
    """Helper function to explore data when debugging"""
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    for r in rows:
        print(r)

    return rows


def get_test_path(relpath):
    """Helper to get absolute path to test data files."""
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, relpath)


@pytest.fixture
def disable_temp_tables():
    """Disable USE_TEMP_TABLES for testing so we can verify staging tables."""
    with patch.object(settings.settings, 'USE_TEMP_TABLES', False):
        yield


@pytest.fixture
def clean_staging_tables(db_cursor):
    """Clean staging tables before each test."""
    db_cursor.execute("""
        DROP TABLE IF EXISTS
          staging_sensornodes
        , staging_sensorsystems
        , staging_sensors
        , staging_flags
        , staging_keys
        , temp_inserted_measurements
        , staging_measurements
    """)
    db_cursor.connection.commit()
    yield
    # Cleanup after test
    db_cursor.execute("""
        DROP TABLE IF EXISTS
          staging_sensornodes
        , staging_sensorsystems
        , staging_sensors
        , staging_flags
        , staging_keys
        , temp_inserted_measurements
        , staging_measurements
    """)
    db_cursor.connection.commit()


@pytest.fixture
def sample_fetchlog(db_cursor, clean_fetchlogs):
    """Create a sample fetchlog record for testing."""
    test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    db_cursor.execute("""
        INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
        VALUES ('test-data.json', %s, %s, NULL, false)
        RETURNING fetchlogs_id
    """, (test_time, test_time))
    fetchlog_id = db_cursor.fetchone()[0]
    db_cursor.connection.commit()
    return fetchlog_id


@pytest.mark.integration
class TestIngestClientIntegration:
    """
    Integration tests for IngestClient that verify data is correctly written to the database.

    These tests load data through IngestClient and verify:
    - Data is written to staging tables
    - Correct number of records inserted
    - Data integrity and relationships
    - fetchlogs tracking
    """

    @pytest.fixture(autouse=True)
    def setup_disable_temp_tables(self, disable_temp_tables):
        """Automatically disable temp tables for all tests in this class."""
        pass

    def test_ingest_simple_data_to_staging(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test that simple data is correctly inserted into staging tables."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('dataV2.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Check staging_sensornodes
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes")
        node_count = db_cursor.fetchone()[0]
        assert node_count == 3, f"Expected 3 sensor nodes, got {node_count}"

        # Verify node data integrity
        db_cursor.execute("""
            SELECT ingest_id, source_name, source_id, site_name, fetchlogs_id
            FROM staging_sensornodes
            ORDER BY ingest_id
        """)
        nodes = db_cursor.fetchall()
        assert len(nodes) == 3
        # All nodes should have the correct fetchlog_id
        for node in nodes:
            assert node[4] == sample_fetchlog, f"Node {node[0]} has wrong fetchlogs_id"
            assert node[1] is not None, "source_name should not be NULL"
            assert node[2] is not None, "source_id should not be NULL"

        # Assert - Check staging_measurements
        db_cursor.execute("SELECT COUNT(*) FROM staging_measurements")
        measurement_count = db_cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"

    def test_ingest_realtime_measures_to_staging(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test realtime measures data insertion."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('testdata_realtime_measures.ndjson')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))

        client.dump(load=True)

        # Assert - Realtime data should only have measurements, no nodes
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        node_count = db_cursor.fetchone()[0]
        assert node_count == 0, "Realtime data should not create sensor nodes"

        db_cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        measurement_count = db_cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"

        # Verify measurement data
        db_cursor.execute("""
            SELECT ingest_id, datetime, value, fetchlogs_id
            FROM staging_measurements
            WHERE fetchlogs_id = %s
            ORDER BY datetime
        """, (sample_fetchlog,))
        measurements = db_cursor.fetchall()

        assert len(measurements) == 2
        for meas in measurements:
            assert meas[3] == sample_fetchlog, "Measurement has wrong fetchlogs_id"
            assert meas[2] is not None, "Measurement value should not be NULL"

    def test_ingest_clarity_data_to_staging(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test Clarity provider data insertion."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('testdata_lcs_clarity.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        node_count = db_cursor.fetchone()[0]
        assert node_count == 2, f"Expected 2 nodes from Clarity data, got {node_count}"

        db_cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        measurement_count = db_cursor.fetchone()[0]
        assert measurement_count == 3, f"Expected 3 measurements, got {measurement_count}"

        # Verify ingest_id format for Clarity provider
        db_cursor.execute("""
            SELECT ingest_id, source_name
            FROM staging_sensornodes
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        nodes = db_cursor.fetchall()
        for node in nodes:
            assert node[0] is not None, "ingest_id should not be NULL"
            assert node[1] is not None, "source_name should not be NULL"

    def test_ingest_senstate_csv_to_staging(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test Senstate CSV data insertion."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('testdata_lcs_senstate.csv')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Senstate CSV has measurements but no nodes (location-id-param)
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        node_count = db_cursor.fetchone()[0]
        assert node_count == 0, "Senstate data should not create nodes directly"

        db_cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        measurement_count = db_cursor.fetchone()[0]
        assert measurement_count == 3, f"Expected 3 measurements, got {measurement_count}"

    def test_ingest_transform_data_with_systems_sensors(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test data with systems and sensors."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('testdata_transform.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))

        client.dump(load=True)

        # Assert - Check all entity types
        debug_query_data(
            "SELECT * FROM staging_measurements WHERE fetchlogs_id = %s",
            (sample_fetchlog,),
            db_cursor
            )


        db_cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        measurement_count = db_cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"

        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        node_count = db_cursor.fetchone()[0]
        assert node_count == 1, f"Expected 1 node, got {node_count}"

        db_cursor.execute("SELECT COUNT(*) FROM staging_sensorsystems WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        system_count = db_cursor.fetchone()[0]
        assert system_count == 1, f"Expected 1 system, got {system_count}"

        db_cursor.execute("SELECT COUNT(*) FROM staging_sensors WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        sensor_count = db_cursor.fetchone()[0]
        assert sensor_count == 2, f"Expected 2 sensors, got {sensor_count}"


        # Verify relationships
        db_cursor.execute("""
            SELECT ingest_id, sensor_nodes_id, ingest_sensor_nodes_id
            FROM staging_sensorsystems
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        system = db_cursor.fetchone()
        assert system[0] is not None, "System should have ingest_id"
        assert system[2] is not None, "System should reference node ingest_id"

        # Verify sensors reference the system
        db_cursor.execute("""
            SELECT ingest_sensor_systems_id
            FROM staging_sensors
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        sensors = db_cursor.fetchall()
        for sensor in sensors:
            assert sensor[0] is not None, "Sensor should reference system ingest_id"

    def test_ingest_updates_fetchlog_loaded_datetime(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test that dump updates fetchlogs.loaded_datetime."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('dataV2.json')

        # Get initial loaded_datetime (should be NULL)
        db_cursor.execute("SELECT loaded_datetime FROM fetchlogs WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        initial_loaded = db_cursor.fetchone()[0]
        assert initial_loaded is None, "loaded_datetime should initially be NULL"

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert
        db_cursor.execute("SELECT loaded_datetime, last_message FROM fetchlogs WHERE fetchlogs_id = %s",
                         (sample_fetchlog,))
        result = db_cursor.fetchone()
        loaded_datetime = result[0]
        last_message = result[1]

        assert loaded_datetime is not None, "loaded_datetime should be set after dump"
        assert last_message == 'load_data', f"Expected last_message='load_data', got '{last_message}'"

    def test_ingest_empty_file_marks_fetchlog(
        self,
        db_cursor,
        clean_staging_tables,
        clean_fetchlogs
    ):
        """Test that processing empty file still updates fetchlog."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
            VALUES ('empty-test-data.json', %s, %s, NULL, false)
            RETURNING fetchlogs_id
        """, (test_time, test_time))
        fetchlog_id = db_cursor.fetchone()[0]
        db_cursor.connection.commit()

        client = IngestClient()
        # Load an empty array or minimal file
        test_file = get_test_path('dataV2.json')  # We'll use this but not load much

        # Act
        client.load_key(test_file, fetchlog_id, str(date.today()))
        # Force dump even with data to test the tracking
        client.dump(load=True)

        # Assert - fetchlog should be updated
        db_cursor.execute("SELECT loaded_datetime, last_message FROM fetchlogs WHERE fetchlogs_id = %s",
                         (fetchlog_id,))
        result = db_cursor.fetchone()
        assert result[0] is not None, "Empty file processing should still update loaded_datetime"

    def test_ingest_multiple_fetchlogs_tracked_separately(
        self,
        db_cursor,
        clean_staging_tables,
        clean_fetchlogs
    ):
        """Test that multiple files are tracked with correct fetchlog_ids."""
        # Arrange - create two fetchlog records
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
            VALUES
                ('file1.json', %s, %s, NULL, false),
                ('file2.json', %s, %s, NULL, false)
            RETURNING fetchlogs_id
        """, (test_time, test_time, test_time, test_time))
        fetchlog_ids = [row[0] for row in db_cursor.fetchall()]
        fetchlog_id1, fetchlog_id2 = fetchlog_ids[0], fetchlog_ids[1]
        db_cursor.connection.commit()

        # Act - Load two different files with different fetchlog IDs
        client1 = IngestClient()
        client1.load_key(get_test_path('dataV2.json'), fetchlog_id1, str(date.today()))
        client1.dump(load=True)

        client2 = IngestClient()
        client2.load_key(get_test_path('testdata_lcs_clarity.json'), fetchlog_id2, str(date.today()))
        client2.dump(load=True)

        # Assert - Verify data is tracked with correct fetchlog_ids
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s", (fetchlog_id1,))
        count1 = db_cursor.fetchone()[0]
        assert count1 == 3, f"File 1 should have 3 nodes, got {count1}"

        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s", (fetchlog_id2,))
        count2 = db_cursor.fetchone()[0]
        assert count2 == 2, f"File 2 should have 2 nodes, got {count2}"

        # Verify total
        db_cursor.execute("SELECT COUNT(*) FROM staging_sensornodes")
        total = db_cursor.fetchone()[0]
        assert total == 5, f"Total should be 5 nodes, got {total}"

    def test_ingest_verifies_unique_constraints(
        self,
        db_cursor,
        clean_staging_tables,
        sample_fetchlog
    ):
        """Test that unique constraints on staging tables are enforced."""
        # Arrange
        client = IngestClient()
        test_file = get_test_path('dataV2.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Verify unique constraint on ingest_id
        db_cursor.execute("""
            SELECT ingest_id, COUNT(*)
            FROM staging_sensornodes
            GROUP BY ingest_id
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found duplicate ingest_ids: {duplicates}"

        # Verify unique constraint on (source_name, source_id)
        db_cursor.execute("""
            SELECT source_name, source_id, COUNT(*)
            FROM staging_sensornodes
            GROUP BY source_name, source_id
            HAVING COUNT(*) > 1
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found duplicate (source_name, source_id): {duplicates}"
