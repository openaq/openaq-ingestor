import pytest
import os
from datetime import date, datetime, timezone
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings


#from tests._debug import dump


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
        get_object,
        create_node,
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

        existing_node = create_node({
            "site_name": "test_site",
            "source_name": "testing",
            "source_id": "site1",
            "sensors": [
                {
                    "source_id": "testing-site1-pm10",
                    "measurand": "pm10",
                    "period": 3600,
                    "flags": [{
                        "flag_types_id": 4,
                        "period": ("2024-12-31 23:00:00", "2025-01-01 00:00:00"),
                        "note": "test flag to join",
                    }],
                },
                {
                    "source_id": "testing-site1-pm25",
                    "measurand": "pm25",
                    "period": 3600,
                    "flags": [{
                        "flag_types_id": 4,
                        "period": ("2024-12-31 23:00:00", "2025-01-01 00:00:00"),
                        "note": "test flag to join",
                    }],
                },
            ],
        })

        ## now we dump or our data for ingest
        client.dump()

        meas = get_object('staged_measurements')
        assert len(meas) == 8, f"Staging does not contain all the measurements, has {len(meas)}"

        flags = get_object("staged_flags")

        oflags = [x for x in flags if x['flags_id'] is not None]
        assert len(oflags) == 2, f"Database does not have the right number of old flags staged, has {len(oflags)}"

        nflags = [x for x in flags if x['flags_id'] is None]
        assert len(nflags) == 2, f"Database does not have the right number of new flags staged, has {len(nflags)}"

        null_values = [x for x in meas if x['value'] is None]
        assert len(null_values) == 6, "Database has the right number of null measurements"
