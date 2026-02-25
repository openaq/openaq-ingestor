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
