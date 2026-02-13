"""
Data validation tests for lcsV2.py helper functions.

These tests verify that data transformation and validation functions correctly
handle various input formats, edge cases, and invalid data.

KNOWN BUGS (marked with pytest.mark.xfail):
1. to_timestamp() - Missing return statement after numeric conversion
   - When timestamp is numeric (10 or 13 digits), function converts to datetime
     but doesn't return it, resulting in None
   - Affects: test_to_timestamp_with_13_digit_milliseconds, test_to_timestamp_with_10_digit_seconds

2. to_geometry() - Uninitialized variables when key != 'coordinates'
   - Variables lat/lon are not initialized before conditional checks
   - Causes "NoneType is not iterable" error when checking 'lat' in data
   - Affects: All to_geometry tests except test_to_geometry_with_coordinates_object
"""

import pytest
from datetime import datetime, timezone
from ingest.lcsV2 import (
    to_geometry,
    to_timestamp,
    to_sensorid,
    to_nodeid,
    IngestClient,
)


class TestToGeometry:
    """Tests for to_geometry() coordinate conversion function."""

    #@pytest.mark.xfail(reason="BUG: lat/lon not initialized when key != 'coordinates'")
    def test_to_geometry_with_lat_lon_keys(self):
        """Test basic lat/lon conversion."""
        data = {'coordinates': {'lat': 40.7128, 'lon': -74.0060} }
        result = to_geometry('coordinates', data)
        assert result == "SRID=4326;POINT(-74.006 40.7128)"

    def test_to_geometry_with_latitude_longitude_keys(self):
        """Test conversion with full latitude/longitude key names."""
        data = {'coordinates': {'latitude': 51.5074, 'longitude': -0.1278} }
        result = to_geometry('coordinates', data)
        assert result == "SRID=4326;POINT(-0.1278 51.5074)"

    def test_to_geometry_with_coordinates_object(self):
        """Test conversion when data is nested under 'coordinates' key."""
        data = {
            'coordinates': {
                'lat': 48.8566,
                'lon': 2.3522
            }
        }
        result = to_geometry('coordinates', data)
        assert result == "SRID=4326;POINT(2.3522 48.8566)"

    def test_to_geometry_with_custom_srid(self):
        """Test geometry with custom SRID."""
        data = {'lat': 40.7128, 'lon': -74.0060, 'srid': '3857'}
        result = to_geometry('lat', data)
        assert result == "SRID=3857;POINT(-74.006 40.7128)"

    def test_to_geometry_missing_latitude_raises_exception(self):
        """Test that missing latitude raises exception."""
        data = {'coordinates': {'lon': -74.0060} }
        with pytest.raises(Exception, match="Missing value for coordinates"):
            to_geometry('coordinates', data)

    def test_to_geometry_missing_longitude_raises_exception(self):
        """Test that missing longitude raises exception."""
        data = {'coordinates': {'lat': 40.7128} }
        with pytest.raises(Exception, match="Missing value for coordinates"):
            to_geometry('coordinates', data)

    def test_to_geometry_with_none_latitude(self):
        """Test that None latitude raises exception."""
        data = {'coordinates': {'lat': None, 'lon': -74.0060} }
        with pytest.raises(Exception, match="Missing value for coordinates"):
            to_geometry('coordinates', data)

    def test_to_geometry_with_none_longitude(self):
        """Test that None longitude raises exception."""
        data = {'coordinates': {'lat': 40.7128, 'lon': None} }
        with pytest.raises(Exception, match="Missing value for coordinates"):
            to_geometry('coordinates', data)

    def test_to_geometry_negative_coordinates(self):
        """Test conversion with negative coordinates."""
        data = {'coordinates': {'lat': -33.8688, 'lon': 151.2093} }
        result = to_geometry('coordinates', data)
        assert result == "SRID=4326;POINT(151.2093 -33.8688)"

    def test_to_geometry_zero_coordinates(self):
        """Test conversion with zero coordinates."""
        data = {'coordinates': {'lat': 0, 'lon': 0} }
        result = to_geometry('coordinates', data)
        assert result == "SRID=4326;POINT(0 0)"


class TestToTimestamp:
    """Tests for to_timestamp() timestamp parsing function."""

    def test_to_timestamp_with_none_value(self):
        """Test that None timestamp returns None."""
        data = {'timestamp': None}
        result = to_timestamp('timestamp', data)
        assert result is None

    def test_to_timestamp_with_empty_string(self):
        """Test that empty string returns None."""
        data = {'timestamp': ''}
        result = to_timestamp('timestamp', data)
        assert result is None

    def test_to_timestamp_with_dict_utc_key(self):
        """Test legacy realtime format with dict containing 'utc' key."""
        utc_time = '2024-01-15T12:00:00Z'
        data = {'timestamp': {'utc': utc_time}}
        result = to_timestamp('timestamp', data)
        assert result == utc_time

    def test_to_timestamp_with_13_digit_milliseconds(self):
        """Test numeric timestamp with milliseconds (13 digits)."""
        # 1705320000000 = 2024-01-15 12:00:00 UTC in milliseconds
        data = {'timestamp': '1705320000000'}
        result = to_timestamp('timestamp', data)

        expected = datetime.fromtimestamp(1705320000000 / 1000.0, timezone.utc).isoformat()
        assert result == expected

    def test_to_timestamp_with_10_digit_seconds(self):
        """Test numeric timestamp with seconds (10 digits)."""
        # 1705320000 = 2024-01-15 12:00:00 UTC in seconds
        data = {'timestamp': '1705320000'}
        result = to_timestamp('timestamp', data)

        expected = datetime.fromtimestamp(1705320000, timezone.utc).isoformat()
        assert result == expected

    def test_to_timestamp_with_string_passthrough(self):
        """Test that non-numeric string is returned as-is."""
        timestamp_str = '2024-01-15T12:00:00Z'
        data = {'timestamp': timestamp_str}
        result = to_timestamp('timestamp', data)
        assert result == timestamp_str

    def test_to_timestamp_with_iso_format(self):
        """Test ISO format string timestamp."""
        timestamp_str = '2024-01-15T12:00:00+00:00'
        data = {'datetime': timestamp_str}
        result = to_timestamp('datetime', data)
        assert result == timestamp_str

    def test_to_timestamp_missing_key_returns_none(self):
        """Test that missing key returns None."""
        data = {'other_field': 'value'}
        result = to_timestamp('timestamp', data)
        assert result is None


class TestToSensorid:
    """Tests for to_sensorid() sensor ID generation function."""

    def test_to_sensorid_basic(self):
        """Test basic sensor ID generation."""
        data = {
            'parameter': 'pm25',
            'location': 'station-001',
            'sourceName': 'clarity'
        }
        result = to_sensorid('parameter', data)
        assert result == "clarity-station-001-pm25"

    def test_to_sensorid_with_different_parameter(self):
        """Test sensor ID with different parameter."""
        data = {
            'parameter': 'temperature',
            'location': 'site-A',
            'sourceName': 'provider-x'
        }
        result = to_sensorid('parameter', data)
        assert result == "provider-x-site-A-temperature"

    def test_to_sensorid_with_hyphenated_location(self):
        """Test sensor ID with location containing hyphens."""
        data = {
            'parameter': 'no2',
            'location': 'station-001-outdoor',
            'sourceName': 'airnow'
        }
        result = to_sensorid('parameter', data)
        assert result == "airnow-station-001-outdoor-no2"


class TestToNodeid:
    """Tests for to_nodeid() node ID generation function."""

    def test_to_nodeid_basic(self):
        """Test basic node ID generation."""
        data = {
            'location': 'station-001',
            'sourceName': 'clarity'
        }
        result = to_nodeid('location', data)
        assert result == "clarity-station-001"

    def test_to_nodeid_with_different_source(self):
        """Test node ID with different source name."""
        data = {
            'location': 'site-A',
            'sourceName': 'provider-x'
        }
        result = to_nodeid('location', data)
        assert result == "provider-x-site-A"

    def test_to_nodeid_with_uuid_location(self):
        """Test node ID with UUID-style location."""
        data = {
            'location': 'abc123-def456-ghi789',
            'sourceName': 'custom-provider'
        }
        result = to_nodeid('location', data)
        assert result == "custom-provider-abc123-def456-ghi789"


class TestIngestClientProcess:
    """Tests for IngestClient.process() mapping function."""

    def test_process_simple_value_mapping(self):
        """Test processing simple value without function."""
        client = IngestClient()
        data = {'site_name': 'Test Station'}

        col, value = client.process('site_name', data, client.node_map)

        assert col == 'site_name'
        assert value == 'Test Station'

    def test_process_column_rename(self):
        """Test processing with column rename."""
        client = IngestClient()
        data = {'label': 'My Station'}

        col, value = client.process('label', data, client.node_map)

        assert col == 'site_name'  # label maps to site_name
        assert value == 'My Station'

    def test_process_with_function_transformation(self):
        """Test processing with transformation function."""
        client = IngestClient()
        data = {
            'coordinates': {
                'lat': 40.7128,
                'lon': -74.0060
            }
        }

        col, value = client.process('coordinates', data, client.node_map)

        assert col == 'geom'
        # Note: Floating point representation may vary (-74.006 vs -74.0060)
        assert value == "SRID=4326;POINT(-74.006 40.7128)"

    def test_process_unmapped_key_returns_none(self):
        """Test that unmapped keys return None."""
        client = IngestClient()
        data = {'unknown_field': 'value'}

        col, value = client.process('unknown_field', data, client.node_map)

        assert col is None
        assert value is None

    def test_process_ingest_id_mapping(self):
        """Test ingest_id mapping for measurements."""
        client = IngestClient()
        data = {'key': 'provider-location-pm25'}

        col, value = client.process('key', data, client.measurement_map)

        assert col == 'ingest_id'
        assert value == 'provider-location-pm25'


class TestAddMeasurement:
    """Tests for IngestClient.add_measurement() validation."""

    def test_add_measurement_from_dict_valid(self):
        """Test adding measurement from valid dict."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'ingest_id': 'clarity-station001-pm25',
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z'
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[0] == 'clarity-station001-pm25'  # ingest_id
        assert measurement[1] == 'clarity'  # source_name
        assert measurement[2] == 'station001'  # source_id
        assert measurement[3] == 'pm25'  # measurand
        assert measurement[4] == 15.5  # value

    def test_add_measurement_from_csv_list_valid(self):
        """Test adding measurement from CSV list format."""
        client = IngestClient()
        client.fetchlogs_id = 123

        # CSV format: [ingest_id, value, datetime]
        meas = ['provider-location-no2', '25.3', '2024-01-15T12:00:00Z']

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[0] == 'provider-location-no2'
        assert measurement[1] == 'provider'
        assert measurement[2] == 'location'
        assert measurement[3] == 'no2'
        assert measurement[4] == '25.3'

    def test_add_measurement_from_csv_with_coordinates(self):
        """Test adding measurement from CSV with lat/lon."""
        client = IngestClient()
        client.fetchlogs_id = 123

        # CSV format with coordinates: [ingest_id, value, datetime, lat, lon]
        meas = ['source-site-pm10', '42.0', '2024-01-15T12:00:00Z', '40.7128', '-74.0060']

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[6] == '-74.0060'  # lon
        assert measurement[7] == '40.7128'  # lat

    def test_add_measurement_csv_too_short_skipped(self):
        """Test that CSV list with <3 elements is skipped."""
        client = IngestClient()
        client.fetchlogs_id = 123

        # Only 2 elements - should be skipped
        meas = ['provider-location-pm25', '15.5']

        client.add_measurement(meas)

        assert len(client.measurements) == 0

    def test_add_measurement_missing_ingest_id_raises_exception(self):
        """Test that missing ingest_id raises exception."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z'
        }

        with pytest.raises(Exception, match="Could not find ingest id"):
            client.add_measurement(meas)

    def test_add_measurement_ingest_id_too_short_skipped(self):
        """Test that ingest_id with <3 parts is skipped."""
        client = IngestClient()
        client.fetchlogs_id = 123

        # Only 2 parts: provider-location (missing parameter)
        meas = {
            'ingest_id': 'provider-location',
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z'
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 0

    def test_add_measurement_with_none_datetime_skipped(self):
        """Test that measurement with None datetime is skipped."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'ingest_id': 'provider-location-pm25',
            'value': 15.5,
            'datetime': None
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 0

    def test_add_measurement_with_parameter_key_creates_sensor_id(self):
        """Test that parameter key triggers sensor ID creation."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'parameter': 'pm25',
            'location': 'station001',
            'sourceName': 'clarity',
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z'
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[0] == 'clarity-station001-pm25'

    def test_add_measurement_with_uuid_location(self):
        """Test measurement with UUID-style multi-part location."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'ingest_id': 'provider-abc-def-ghi-pm25',
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z'
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[0] == 'provider-abc-def-ghi-pm25'
        assert measurement[1] == 'provider'
        assert measurement[2] == 'abc-def-ghi'  # multi-part source_id
        assert measurement[3] == 'pm25'

    def test_add_measurement_optional_coordinates(self):
        """Test measurement with optional lat/lon coordinates."""
        client = IngestClient()
        client.fetchlogs_id = 123

        meas = {
            'ingest_id': 'provider-mobile-pm25',
            'value': 15.5,
            'datetime': '2024-01-15T12:00:00Z',
            'lat': 40.7128,
            'lon': -74.0060
        }

        client.add_measurement(meas)

        assert len(client.measurements) == 1
        measurement = client.measurements[0]
        assert measurement[6] == -74.0060  # lon
        assert measurement[7] == 40.7128  # lat


class TestAddNode:
    """Tests for IngestClient.add_node() validation."""

    def test_add_node_basic_valid(self):
        """Test adding a valid node."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'clarity-station001',
            'site_name': 'Test Station',
            'lat': 40.7128,
            'lon': -74.0060
        }

        client.add_node(node_data)

        assert len(client.nodes) == 1
        node = client.nodes[0]
        assert node['ingest_id'] == 'clarity-station001'
        assert node['source_name'] == 'clarity'
        assert node['source_id'] == 'station001'
        assert node['site_name'] == 'Test Station'
        assert 'POINT' in node['geom']

    def test_add_node_missing_ingest_id_raises_exception(self):
        """Test that missing ingest_id raises exception."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'site_name': 'Test Station',
            'lat': 40.7128,
            'lon': -74.0060
        }

        with pytest.raises(Exception, match="Missing ingest id"):
            client.add_node(node_data)

    def test_add_node_extracts_source_name_from_ingest_id(self):
        """Test that source_name is extracted from ingest_id."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location123',
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_name'] == 'provider'

    def test_add_node_explicit_source_name_overrides_extraction(self):
        """Test that explicit source_name is used if provided."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location123',
            'source_name': 'custom-source',
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_name'] == 'custom-source'

    def test_add_node_uses_metadata_source_if_no_ingest_id_parts(self):
        """Test that source from metadata is used if ingest_id is truly single part.

        Note: 'single-part' has a hyphen so splits into ['single', 'part'].
        For true single-part behavior, use an ID without hyphens.
        """
        client = IngestClient()
        client.fetchlogs_id = 123
        client.source = 'metadata-source'

        node_data = {
            'ingest_id': 'singlepart',  # No hyphen - truly single part
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_name'] == 'metadata-source'

    def test_add_node_missing_source_name_raises_exception(self):
        """Test that missing source_name (can't be determined) raises exception."""
        client = IngestClient()
        client.fetchlogs_id = 123
        client.source = None

        node_data = {
            'ingest_id': 'singlepart',  # No hyphen - truly single part
            'site_name': 'Station A'
        }

        with pytest.raises(Exception, match="Could not find source name"):
            client.add_node(node_data)

    def test_add_node_extracts_source_id_from_ingest_id(self):
        """Test that source_id is extracted from ingest_id."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location123',
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_id'] == 'location123'

    def test_add_node_uuid_source_id_extraction(self):
        """Test source_id extraction with UUID-style multi-part IDs."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-abc-def-ghi',
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_id'] == 'abc-def-ghi'

    def test_add_node_single_part_ingest_id_uses_as_source_id(self):
        """Test that single-part ingest_id (no hyphens) is used as source_id."""
        client = IngestClient()
        client.fetchlogs_id = 123
        client.source = 'explicit-source'

        node_data = {
            'ingest_id': 'locationonly',  # No hyphen - truly single part
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['source_id'] == 'locationonly'

    def test_add_node_uses_default_matching_method(self):
        """Test that default matching_method is used."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location',
            'site_name': 'Station A'
        }

        client.add_node(node_data)

        node = client.nodes[0]
        assert node['matching_method'] == 'ingest-id'

    def test_add_node_prevents_duplicates(self):
        """Test that duplicate nodes are not added."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location',
            'site_name': 'Station A'
        }

        client.add_node(node_data)
        client.add_node(node_data)  # Try to add again

        assert len(client.nodes) == 1

    def test_add_node_stores_unmapped_fields_in_metadata(self):
        """Test that unmapped fields are stored in metadata JSON."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location',
            'site_name': 'Station A',
            'custom_field': 'custom_value',
            'another_field': 123
        }

        client.add_node(node_data)

        node = client.nodes[0]
        import json
        metadata = json.loads(node['metadata'])
        assert metadata['custom_field'] == 'custom_value'
        assert metadata['another_field'] == 123

    def test_add_node_processes_systems(self):
        """Test that node with systems triggers add_systems."""
        client = IngestClient()
        client.fetchlogs_id = 123

        node_data = {
            'ingest_id': 'provider-location',
            'site_name': 'Station A',
            'systems': [
                {
                    'system_id': 'provider-location-instrument1',
                    'sensors': []
                }
            ]
        }

        client.add_node(node_data)

        assert len(client.nodes) == 1
        assert len(client.systems) == 1
        assert client.systems[0]['ingest_id'] == 'provider-location-instrument1'
