import pytest
import os
from datetime import date, datetime, timezone
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings




@pytest.fixture
def disable_temp_tables():
    """Disable USE_TEMP_TABLES for testing so we can verify staging tables."""
    with patch.object(settings.settings, 'USE_TEMP_TABLES', False):
        yield


@pytest.mark.integration
class TestIngestDataScenarios:
    """
    Integration tests for IngestClient that verify data is correctly written to the database.

    The purpose of these tests is to verify more complex data scenarios
    """

    def test_ingest_new_location(
        self,
        ingest_resources,
        disable_temp_tables,
    ):
        """Test that a simple new location works"""
        # Arrange
        client = IngestClient(resources=ingest_resources)
        locations = [
            {"location":"testing-site1", "label": "Site #1", "lon": "-123.04", "lat": "42.05"}
            ]

        # Act
        client.load_locations(locations)
        client.dump(load=True)

        # Assert - Check staging_sensornodes
        with ingest_resources.cursor() as cursor:
            # Verify node data integrity
            cursor.execute("""
                SELECT ingest_id, source_name, source_id, site_name, fetchlogs_id
                FROM staging_sensornodes
            """)
            nodes = cursor.fetchall()
            assert len(nodes) == 1
            # All nodes should have the correct fetchlog_id
            node = nodes[0]
            ## check the data
            assert node[4] is None
            assert node[1] == "testing"
            assert node[2] == "site1"
            assert node[3] == "Site #1"


    def test_ingest_updates_existing_node(
        self,
        ingest_resources,
        disable_temp_tables,
    ):
        """Test that a location ..."""
        # Arrange
        client = IngestClient(resources=ingest_resources)
        locations = [
            {"location":"testing-site1", "label": "Site #1", "lon": "-123.04", "lat": "42.05"}
            ]

        # Assert - Check staging_sensornodes
        with ingest_resources.cursor() as cursor:
            ## first thing we do is we add the location to the db
            site_label = 'MY_FAKE_SITE'

            cursor.execute("""
                INSERT INTO sensor_nodes (site_name, source_name, source_id)
                VALUES (%s, 'testing', 'site1')
                RETURNING site_name, sensor_nodes_id
                """, (site_label, ))
            existing = cursor.fetchone()

            # Act
            client.load_locations(locations)
            client.dump(load=True)

            # Verify node data integrity
            cursor.execute("""
                SELECT ingest_id, source_name, source_id, site_name, sensor_nodes_id
                FROM staging_sensornodes
            """)
            nodes = cursor.fetchall()
            assert len(nodes) == 1
            # All nodes should have the correct fetchlog_id
            node = nodes[0]
            ## check the data
            assert node[4] == existing[1]
            assert node[3] != existing[0] ## not really needed
            assert node[1] == "testing"
            assert node[2] == "site1"
            assert node[3] == "Site #1"


    def test_database_flags_exceedance_values(
        self,
        ingest_resources,
        disable_temp_tables,
    ):
        client = IngestClient(resources=ingest_resources);

        data = {
            "locations": [
                {"location":"testing-site1", "label": "Site #1", "lon": "-123.04", "lat": "42.05"}
            ],
            "measurements": [
                {"ingest_id":"testing-site1-pm25", "datetime":"2025-01-01 01:00:00", "value": -99 },
                {"ingest_id":"testing-site1-pm25", "datetime":"2025-01-01 02:00:00", "value": -99 },
                {"ingest_id":"testing-site1-pm25", "datetime":"2025-01-01 03:00:00", "value": 5 },
                {"ingest_id":"testing-site1-pm25", "datetime":"2025-01-01 04:00:00", "value": -99 },
                {"ingest_id":"testing-site1-pm10", "datetime":"2025-01-01 01:00:00", "value": -99 },
                {"ingest_id":"testing-site1-pm10", "datetime":"2025-01-01 02:00:00", "value": -99 },
                {"ingest_id":"testing-site1-pm10", "datetime":"2025-01-01 03:00:00", "value": 5 },
                {"ingest_id":"testing-site1-pm10", "datetime":"2025-01-01 04:00:00", "value": -99 },
            ]
        }

        client.load(data)
        assert len(client.nodes) == 1, "Client has the right number of locations"
        assert len(client.measurements) == 8, "Client has the right number of measurements"

        with ingest_resources.cursor() as cursor:

            ## first we add some existing data to test that flags get appended
            cursor.execute("""
                WITH inserted_node AS (
                  INSERT INTO sensor_nodes (site_name, source_name, source_id)
                  VALUES ('test_site', 'testing', 'site1')
                  ON CONFLICT (source_name, source_id) DO UPDATE
                  SET site_name = EXCLUDED.site_name
                  RETURNING source_id, sensor_nodes_id
                ), inserted_system AS (
                  INSERT INTO sensor_systems (sensor_nodes_id, source_id)
                  SELECT sensor_nodes_id, source_id
                  FROM inserted_node
                  ON CONFLICT (sensor_nodes_id, source_id) DO UPDATE
                  SET source_id = EXCLUDED.source_id
                  RETURNING sensor_systems_id
                ), inserted_sensor AS (
                  INSERT INTO sensors (sensor_systems_id, measurands_id, source_id, data_averaging_period_seconds)
                  SELECT sensor_systems_id
                  , 2 -- pm25
                  , 'testing-site1-pm25'
                  , 3600
                  FROM inserted_system
                  ON CONFLICT (source_id) DO UPDATE
                  SET measurands_id = EXCLUDED.measurands_id
                  RETURNING sensors_id
                ) INSERT INTO flags (sensor_nodes_id, sensors_ids, flag_types_id, period, note)
                  SELECT sensor_nodes_id
                  , ARRAY[sensors_id]
                  , 4
                  , tstzrange('2024-12-31 23:00:00', '2025-01-01 00:00:00', '[]')
                  , 'test flag to join'
                  FROM inserted_sensor, inserted_node
                  RETURNING flags_id;
                """)
            existing = cursor.fetchone()

            ## now we dump or our data for ingest
            client.dump()

            # Verify node data integrity
            cursor.execute("""
            SELECT measurands_id, measurand, sensors_id
            FROM staging_measurements
            WHERE value IS NOT NULL
            """)
            meas = cursor.fetchall()
            assert len(meas) == 2, "Staging contains all the measurements"

            cursor.execute("""
            SELECT datetime_from
            , sensors_id
            , flags_id
            FROM staging_flags
            WHERE flags_id IS NOT NULL
            """)
            oflags = cursor.fetchall()
            assert len(oflags) == 1, "Database has the right number of old flags staged"

            cursor.execute("""
            SELECT datetime_from
            , sensors_id
            FROM staging_flags
            WHERE flags_id IS NULL
            """)
            nflags = cursor.fetchall()
            assert len(nflags) == 3, "Database has the right number of new flags staged"

            cursor.execute("""
            SELECT datetime
            FROM measurements
            WHERE value IS NOT NULL
            """)
            rows = cursor.fetchall()
            assert len(rows) == 2, "Database has the right number of good measurements"

            cursor.execute("""
            SELECT datetime
            FROM measurements
            WHERE value IS NULL
            """)
            rows = cursor.fetchall()
            assert len(rows) == 6, "Database has the right number of null measurements"
