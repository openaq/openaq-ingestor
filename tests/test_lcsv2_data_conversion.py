import pytest
import os
from datetime import date, datetime, timezone
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings
from psycopg2.extras import RealDictCursor

from tests.conftest import get_test_path



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

    def test_ingest_realtime_shape_is_converted(
        self,
        ingest_resources,
        sample_fetchlog,
        make_test_file,
        get_object,
    ):
        """Test that realtime data shape is added with the right measurand and converted."""
        # Arrange
        client = IngestClient(resources=ingest_resources)
        content = """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "no","value": 0.002,"unit": "ppm","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }"""
        test_file = make_test_file("realtime_bad_param.ndjson", content)
        client.load_key(test_file, sample_fetchlog, str(date.today()))

        assert len(client.nodes) == 1
        assert len(client.measurements) == 1
        assert len(client.systems) == 1, "System was not added"
        assert len(client.sensors) == 1, "Sensor was not added"
        assert client.measurements[0][5] == 0.002 ## not converted yet

        client.dump(load=True)
        #client.dump_locations(load=True)
        #client.dump_measurements(load=True)


        # Assert - Check staging_sensornodes
        cursor = ingest_resources.cursor(cursor_factory=RealDictCursor)


        # Verify node data integrity
        rejects = get_object("rejects", fetchlogs_id=sample_fetchlog)
        staged_nodes = get_object("staged_sensor_nodes")
        staged_systems = get_object("staged_systems")
        staged_sensors = get_object("staged_sensors")
        staged_measurements = get_object("staged_measurements")


        print(staged_measurements)
        assert len(staged_nodes) == 1
        assert len(staged_systems) == 1
        #assert len(sensors) == 1


        node = staged_nodes[0]
        assert node.get('is_new') == True
        assert node.get('is_moved') == False
        assert node.get('source_id') == 'station1'
        assert node.get('site_name') == 'Station #1', 'site_name is not being carried over'

        system = staged_systems[0]
        assert system.get('is_new') == True

        meas = staged_measurements[0]

        ## updated value
        assert meas.get('value') == 2
        cursor.close()


    def test_ingest_realtime_is_converted_when_sensor_exists(
        self,
        create_node,
        ingest_resources,
        sample_fetchlog,
        make_test_file,
        get_object,
    ):

        node = create_node({
            "site_name": "station1",
            "source_name": "testing",
            "source_id": "testing-station1",
            "coordinates": {"lat": 42.8011974, "lon": -122.99144547},
            "sensors": [{"period": 900, "measurand": "no"}],
        })

        assert len(node["systems"]) == 1
        assert len(node["systems"][0]["sensors"]) == 1

        with ingest_resources.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM sensor_systems WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            assert cur.fetchone()[0] == 1


        client = IngestClient(resources=ingest_resources)
        content = """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "no","value": 0.002,"unit": "ppm","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }"""
        test_file = make_test_file("realtime_bad_param.ndjson", content)
        client.load_key(test_file, sample_fetchlog, str(date.today()))

        assert len(client.nodes) == 1
        assert len(client.measurements) == 1
        assert len(client.systems) == 1, "System was not added"
        assert len(client.sensors) == 1, "Sensor was not added"
        assert client.measurements[0][5] == 0.002 ## not converted yet

        client.dump(load=True)

        # Verify node data integrity
        rejects = get_object("rejects", fetchlogs_id=sample_fetchlog)
        staged_nodes = get_object("staged_sensor_nodes")
        staged_systems = get_object("staged_systems")
        staged_sensors = get_object("staged_sensors")
        staged_measurements = get_object("staged_measurements")


        assert len(staged_nodes) == 1
        assert len(staged_systems) == 1
        #assert len(sensors) == 1


        node = staged_nodes[0]
        assert node.get('is_new') == False, 'Node appears to be new'
        assert node.get('is_moved') == False, 'Node has been marked as moved'
        assert node.get('source_id') == 'station1'
        assert node.get('site_name') == 'Station #1', 'site_name is not being carried over'

        system = staged_systems[0]
        assert system.get('is_new') == False, 'System appears to be new'

        meas = staged_measurements[0]

        ## updated value
        assert meas.get('value') == 2


    def test_ingest_uses_correct_conversion_when_measurand_specific_one_exists(
        self,
        create_node,
        ingest_resources,
        sample_fetchlog,
        make_test_file,
        get_object,
    ):
        client = IngestClient(resources=ingest_resources)

        ## This is wrong on purpose
        ## this one should be used instead of the one that exists
        with ingest_resources.cursor() as cursor:
            cursor.execute("""
            INSERT INTO unit_conversions (from_units_id, to_units_id, factor, intercept, measurand)
            VALUES (
              (SELECT units_id FROM units WHERE units = 'ppm'),
              (SELECT units_id FROM units WHERE units = 'ppb'),
               10, 0, 'no'
            ) RETURNING from_units_id, to_units_id
            """)
            existing = cursor.fetchone()

        content = """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "no","value": 0.002,"unit": "ppm","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }"""

        test_file = make_test_file("realtime_bad_unit.ndjson", content)
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)
        staged_measurements = get_object("staged_measurements")
        assert len(staged_measurements) == 1
        assert staged_measurements[0].get('value') == pytest.approx(0.02)


    def test_ingest_realtime_is_rejected_when_units_are_not_convertable(
        self,
        create_node,
        ingest_resources,
        sample_fetchlog,
        make_test_file,
        get_object,
    ):
        client = IngestClient(resources=ingest_resources)

        content = """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "no","value": 0.002,"unit": "???","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }"""

        test_file = make_test_file("realtime_bad_unit.ndjson", content)
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        rejects = get_object("rejects", fetchlogs_id=sample_fetchlog)
        staged_measurements = get_object("staged_measurements")

        assert len(rejects) == 1
        assert rejects[0].get('tbl') == 'meas-no-unit-conversion'
        assert len(staged_measurements) == 0


    def test_ingest_realtime_is_rejected_when_measurand_is_not_supported(
        self,
        create_node,
        ingest_resources,
        sample_fetchlog,
        make_test_file,
        get_object,
    ):
        client = IngestClient(resources=ingest_resources)

        content = """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "tox","value": 0.002,"unit": "ppb","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }"""

        test_file = make_test_file("realtime_bad_unit.ndjson", content)
        client.load_key(test_file, sample_fetchlog, str(date.today()))
        client.dump(load=True)

        rejects = get_object("rejects", fetchlogs_id=sample_fetchlog)
        staged_measurements = get_object("staged_measurements")

        assert len(rejects) == 3 ## sensor and measurand rejected and then the measurement
        assert rejects[2].get('tbl') == 'meas-unsupported-measurand'
        assert len(staged_measurements) == 0
