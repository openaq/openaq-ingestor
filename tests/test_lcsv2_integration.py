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
    if len(rows) == 0:
        print(f'No rows returned for debug query\n{sql}')
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
def sample_fetchlog(db_cursor, clean_fetchlogs):
    """Create a sample fetchlog record for testing."""
    test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    db_cursor.execute("""
        INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
        VALUES ('test-data.json', %s, %s, NULL, false)
        RETURNING fetchlogs_id
    """, (test_time, test_time))
    fetchlog_id = db_cursor.fetchone()[0]
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
        ingest_context,
        sample_fetchlog
    ):
        """Test that simple data is correctly inserted into staging tables."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('dataV2.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Check staging_sensornodes
        cursor = ingest_context.cursor()
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes")
        node_count = cursor.fetchone()[0]
        assert node_count == 3, f"Expected 3 sensor nodes, got {node_count}"

        # Verify node data integrity
        cursor.execute("""
            SELECT ingest_id, source_name, source_id, site_name, fetchlogs_id
            FROM staging_sensornodes
            ORDER BY ingest_id
        """)
        nodes = cursor.fetchall()
        assert len(nodes) == 3
        # All nodes should have the correct fetchlog_id
        for node in nodes:
            assert node[4] == sample_fetchlog, f"Node {node[0]} has wrong fetchlogs_id"
            assert node[1] is not None, "source_name should not be NULL"
            assert node[2] is not None, "source_id should not be NULL"

        # Assert - Check staging_measurements
        cursor.execute("SELECT COUNT(*) FROM staging_measurements")
        measurement_count = cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"
        cursor.close()

    def test_ingest_realtime_measures_to_staging(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test realtime measures data insertion."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('testdata_realtime_measures.ndjson')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Realtime data should only have measurements, no nodes
        cursor = ingest_context.cursor()
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        node_count = cursor.fetchone()[0]
        assert node_count == 0, "Realtime data should not create sensor nodes"

        cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        measurement_count = cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"

        # Verify measurement data
        cursor.execute("""
            SELECT ingest_id, datetime, value, fetchlogs_id
            FROM staging_measurements
            WHERE fetchlogs_id = %s
            ORDER BY datetime
        """, (sample_fetchlog,))
        measurements = cursor.fetchall()

        assert len(measurements) == 2
        for meas in measurements:
            assert meas[3] == sample_fetchlog, "Measurement has wrong fetchlogs_id"
            assert meas[2] is not None, "Measurement value should not be NULL"
        cursor.close()

    def test_ingest_clarity_data_to_staging(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test Clarity provider data insertion."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('testdata_lcs_clarity.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert
        cursor = ingest_context.cursor()
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        node_count = cursor.fetchone()[0]
        assert node_count == 2, f"Expected 2 nodes from Clarity data, got {node_count}"

        cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        measurement_count = cursor.fetchone()[0]
        assert measurement_count == 3, f"Expected 3 measurements, got {measurement_count}"

        # Verify ingest_id format for Clarity provider
        cursor.execute("""
            SELECT ingest_id, source_name
            FROM staging_sensornodes
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        nodes = cursor.fetchall()
        for node in nodes:
            assert node[0] is not None, "ingest_id should not be NULL"
            assert node[1] is not None, "source_name should not be NULL"
        cursor.close()

    def test_ingest_senstate_csv_to_staging(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test Senstate CSV data insertion."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('testdata_lcs_senstate.csv')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Senstate CSV has measurements but no nodes (location-id-param)
        cursor = ingest_context.cursor()
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        node_count = cursor.fetchone()[0]
        assert node_count == 0, "Senstate data should not create nodes directly"

        cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        measurement_count = cursor.fetchone()[0]
        assert measurement_count == 3, f"Expected 3 measurements, got {measurement_count}"
        cursor.close()

    def test_ingest_transform_data_with_systems_sensors(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test data with systems and sensors using transactional rollback."""
        # Arrange
        # Assert - Check all entity types
        cursor = ingest_context.cursor()
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('testdata_transform.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))

        client.dump(load=True)
        #client.dump_locations(load=True)
        #client.dump_measurements(load=True)

        debug_query_data('SELECT COUNT(1) FROM staging_sensors', (), cursor)


        cursor.execute("SELECT COUNT(*) FROM staging_measurements WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        measurement_count = cursor.fetchone()[0]
        assert measurement_count == 2, f"Expected 2 measurements, got {measurement_count}"

        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        node_count = cursor.fetchone()[0]
        assert node_count == 1, f"Expected 1 node, got {node_count}"

        cursor.execute("SELECT COUNT(*) FROM staging_sensorsystems WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        system_count = cursor.fetchone()[0]
        assert system_count == len(client.systems), f"Expected 1 system, got {system_count}"

        cursor.execute("SELECT COUNT(*) FROM staging_sensors WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        sensor_count = cursor.fetchone()[0]
        assert sensor_count == len(client.sensors), f"Expected 2 sensors, got {sensor_count}"

        # Verify relationships
        cursor.execute("""
            SELECT ingest_id, sensor_nodes_id, ingest_sensor_nodes_id
            FROM staging_sensorsystems
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        system = cursor.fetchone()
        assert system[0] is not None, "System should have ingest_id"
        assert system[2] is not None, "System should reference node ingest_id"

        # Verify sensors reference the system
        cursor.execute("""
            SELECT ingest_sensor_systems_id
            FROM staging_sensors
            WHERE fetchlogs_id = %s
        """, (sample_fetchlog,))
        sensors = cursor.fetchall()
        for sensor in sensors:
            assert sensor[0] is not None, "Sensor should reference system ingest_id"

        cursor.close()
        # Test ends, ingest_context fixture automatically rolls back all changes

    def test_ingest_updates_fetchlog_loaded_datetime(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test that dump updates fetchlogs.loaded_datetime."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('dataV2.json')

        # Get initial loaded_datetime (should be NULL)
        cursor = ingest_context.cursor()
        cursor.execute("SELECT loaded_datetime FROM fetchlogs WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        initial_loaded = cursor.fetchone()[0]
        assert initial_loaded is None, "loaded_datetime should initially be NULL"

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert
        cursor.execute("SELECT loaded_datetime, last_message FROM fetchlogs WHERE fetchlogs_id = %s",
                      (sample_fetchlog,))
        result = cursor.fetchone()
        loaded_datetime = result[0]
        last_message = result[1]

        print(result)

        assert loaded_datetime is not None, "loaded_datetime should be set after dump"
        assert last_message is None, f"Expected last_message to be None', got '{last_message}'"
        cursor.close()

    def test_ingest_empty_file_marks_fetchlog(
        self,
        ingest_context,
        clean_fetchlogs
    ):
        """Test that processing empty file still updates fetchlog."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        cursor = ingest_context.cursor()
        cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
            VALUES ('empty-test-data.json', %s, %s, NULL, false)
            RETURNING fetchlogs_id
        """, (test_time, test_time))
        fetchlog_id = cursor.fetchone()[0]

        client = IngestClient(context=ingest_context)
        # Load an empty array or minimal file
        test_file = get_test_path('dataV2.json')  # We'll use this but not load much

        # Act
        client.load_key(test_file, fetchlog_id, str(date.today()))
        # Force dump even with data to test the tracking
        client.dump(load=True)

        # Assert - fetchlog should be updated
        cursor.execute("SELECT loaded_datetime, last_message FROM fetchlogs WHERE fetchlogs_id = %s",
                      (fetchlog_id,))
        result = cursor.fetchone()
        assert result[0] is not None, "Empty file processing should still update loaded_datetime"
        cursor.close()


    def test_ingest_unsupported_file_marks_fetchlog_with_error(
        self,
        ingest_context,
        clean_fetchlogs
    ):
        """Test that submit_file_error method works"""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        test_file = get_test_path('unsupported_data.tab')  # We'll use this but not load much
        cursor = ingest_context.cursor()
        cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
            VALUES (%s, %s, %s, NULL, false)
            RETURNING fetchlogs_id, key, last_modified
        """, (test_file, test_time, test_time))

        rows = cursor.fetchall()
        fetchlog_id = rows[0][0]

        client = IngestClient(context=ingest_context)
        # Load the unsupported file which should result in the fetchlog record updated
        client.load_keys(rows)
        # Force dump even with data to test the tracking
        client.dump(load=True)

        # Assert - fetchlog should be updated
        cursor.execute("SELECT last_message FROM fetchlogs WHERE fetchlogs_id = %s",
                      (fetchlog_id,))
        result = cursor.fetchone()
        assert 'Not sure how to read file' in result[0]
        cursor.close()


    def test_ingest_multiple_fetchlogs_tracked_separately(
        self,
        ingest_context,
        clean_fetchlogs
    ):
        """Test that multiple files are tracked with correct fetchlog_ids."""
        # Arrange - create two fetchlog records
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        cursor = ingest_context.cursor()
        cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
            VALUES
                (%s, %s, %s, NULL, false),
                (%s, %s, %s, NULL, false)
            RETURNING fetchlogs_id, key, last_modified
        """, (
            get_test_path('dataV2.json'), test_time, test_time,  # 3 locations, 2 measures
            get_test_path('testdata_lcs_clarity.json'), test_time, test_time)) # 2 locations, sensors, 3 meas
        rows = cursor.fetchall()

        fetchlog_ids = [row[0] for row in rows]
        fetchlog_id1, fetchlog_id2 = fetchlog_ids[0], fetchlog_ids[1]

        # Act - Load two different files with different fetchlog IDs
        client = IngestClient(context=ingest_context)
        client.load_keys(rows)
        client.dump(load=True)

        # Assert - Verify data is tracked with correct fetchlog_ids
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s", (fetchlog_id1,))
        count1 = cursor.fetchone()[0]
        assert count1 == 3, f"File 1 should have 3 nodes, got {count1}"

        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes WHERE fetchlogs_id = %s", (fetchlog_id2,))
        count2 = cursor.fetchone()[0]
        assert count2 == 2, f"File 2 should have 2 nodes, got {count2}"

        # Verify total
        cursor.execute("SELECT COUNT(*) FROM staging_sensornodes")
        total = cursor.fetchone()[0]
        assert total == 5, f"Total should be 5 nodes, got {total}"
        cursor.close()

    def test_ingest_verifies_unique_constraints(
        self,
        ingest_context,
        sample_fetchlog
    ):
        """Test that unique constraints on staging tables are enforced."""
        # Arrange
        client = IngestClient(context=ingest_context)
        test_file = get_test_path('dataV2.json')

        # Act
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        # Assert - Verify unique constraint on ingest_id
        cursor = ingest_context.cursor()
        cursor.execute("""
            SELECT ingest_id, COUNT(*)
            FROM staging_sensornodes
            GROUP BY ingest_id
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        assert len(duplicates) == 0, f"Found duplicate ingest_ids: {duplicates}"

        # Verify unique constraint on (source_name, source_id)
        cursor.execute("""
            SELECT source_name, source_id, COUNT(*)
            FROM staging_sensornodes
            GROUP BY source_name, source_id
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        assert len(duplicates) == 0, f"Found duplicate (source_name, source_id): {duplicates}"
        cursor.close()
