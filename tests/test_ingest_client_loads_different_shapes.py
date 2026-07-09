import pytest
import os
from datetime import date, datetime, timezone
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings
from psycopg2.extras import RealDictCursor


def create_test_file(tmp_path, name: str, content: str) -> str:
    """Write content to a temp file and return its path."""
    p = tmp_path / name
    p.write_text(content)
    return str(p)


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


## I have added the interval keys to the lcs file shape for the sake of simplicity
## we could also add them to the lcs adapters if those wont be converted to transform right away
files = {
    "realtime.ndjson": """{"date": {  "utc": "2024-04-08T21:25:00.000Z",  "local": "2024-04-09T00:25:00+03:00"},"parameter": "no","value": 0.2,"unit": "ppb","averagingPeriod": {  "unit": "hours",  "value": 0.25},"location": "station1","city": "portland, OR","country": "US","coordinates": {  "latitude": 42.8011974,  "longitude": -122.99144547},"attribution": [  { "name": "Station #1", "url": "https://fake-stations.gov"  }],"sourceName": "testing","sourceType": "government","mobile": false }""",
    "lcs.json":"""
    {
  "meta": {
    "schema": "v0.1",
    "source": "testing",
    "matching_method": "source-spatial"
  },
  "measures": [
    {
      "sensor_id": "testing-station1-no",
      "measure": 0.2,
      "timestamp": "2024-04-08T21:25:00.000Z"
    }
  ],
  "locations": [
    {
      "location": "testing-station1",
      "label": "Station #1",
      "ismobile": false,
      "lon": -122.99144547,
      "lat": 42.8011974,
      "averaging_interval_secs": 900,
      "logging_interval_secs": 900
    }
  ]
}
""",
    "transform.json":"""
    {
  "meta": {
    "ingestMatchingMethod": "source-spatial",
    "schema": "v0.1",
    "sourceName": "testing",
    "startedOn": "2024-04-08T21:25:00.000Z",
    "finishedOn": "2024-04-08T21:25:00.000Z",
    "exportedOn": "2024-04-08T21:25:00.000Z",
    "fetchSummary": {
      "sourceName": "testing",
      "locations": 1,
      "bounds": [42.8011974, -122.99144547, 42.8011974, -122.99144547],
      "systems": 1,
      "sensors": 1,
      "flags": 0,
      "measurements": 1,
      "datetimeFrom": "2024-04-08T21:25:00.000Z",
      "datetimeTo": "2024-04-08T21:25:00.000Z",
      "errors": {}
    }
  },
  "locations": [
    {
      "key": "testing-station1",
      "site_id": "station1",
      "site_name": "Station #1",
      "coordinates": {
        "lat": 42.8011974,
        "lon": -122.99144547,
        "proj": "WSG84"
      },
      "ismobile": false,
      "flags": [],
      "systems": [
        {
          "key": "testing-station1",
          "manufacturer_name": "default",
          "model_name": "default",
          "sensors": [
            {
              "key": "testing-station1-no",
              "parameter": "no",
              "units": "ppb",
              "averaging_interval_secs": 900,
              "logging_interval_secs": 900,
              "status": "active",
              "flags": []
            }
          ]
        }
      ]
    }
  ],
  "measures": [
    {
      "key": "testing-station1-no",
      "timestamp": "2024-04-08T21:25:00.000Z",
      "value": 0.2
    }
  ]
}
""",
    }


@pytest.mark.parametrize("key", files.keys())
def test_all_shapes_are_converted(
    ingest_resources,
    sample_fetchlog,
    tmp_path,
    key,
):
    """Test that all data shapes are loaded to the client in the same way."""
    client = IngestClient(resources=ingest_resources)
    content = files.get(key)
    test_file = create_test_file(tmp_path, key, content)
    client.load_key(test_file, sample_fetchlog, str(date.today()))

    assert len(client.nodes) == 1
    assert len(client.measurements) == 1
    assert len(client.systems) == 1, "System was not added"
    assert len(client.sensors) == 1, "Sensor was not added"

    node = next(iter(client.nodes.values()))
    expected_node_fields = {
        "fetchlogs_id": sample_fetchlog,
        "matching_method": "source-spatial",
        "source_name": "testing",
        "source_id": "station1",
        "site_name": "Station #1",
        "geom": "SRID=4326;POINT(-122.99144547 42.8011974)",
        "ismobile": False,
        "ingest_id": "testing-station1",
        "metadata": "{}",
    }
    for field, expected in expected_node_fields.items():
        assert node.get(field) == expected, (
            f"node.{field}: {node.get(field)!r} != {expected!r}"
        )

    system = next(iter(client.systems.values()))
    expected_system_fields = {
        "fetchlogs_id": sample_fetchlog,
        "ingest_id": "testing-station1",
        "ingest_sensor_nodes_id": "testing-station1",
        "manufacturer": "testing",
        "model": "default",
        "metadata": "{}",
    }
    for field, expected in expected_system_fields.items():
        assert system.get(field) == expected, (
            f"system.{field}: {system.get(field)!r} != {expected!r}"
        )

    sensor = next(iter(client.sensors.values()))
    expected_sensor_fields = {
        "fetchlogs_id": sample_fetchlog,
        "ingest_sensor_systems_id": "testing-station1",
        "ingest_id": "testing-station1-no",
        "measurand": "no",
        "status": "active",
        "averaging_interval_seconds": 900,
        "logging_interval_seconds": 900,
        "metadata": "{}",
    }
    for field, expected in expected_sensor_fields.items():
        assert sensor.get(field) == expected, (
            f"sensor.{field}: {sensor.get(field)!r} != {expected!r}"
        )

    assert client.measurements[0] == [
        "testing-station1-no",
        "testing",
        "station1",
        "no",
        0.2,
        "2024-04-08T21:25:00.000Z",
        None,
        None,
        sample_fetchlog,
    ]
