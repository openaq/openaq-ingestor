import pytest
from psycopg2.extras import RealDictCursor


@pytest.mark.integration
class TestCreateNodeFixture:
    """
    Tests for the `create_node` test fixture itself.

    Verifies that the fixture correctly translates its flexible spec
    into sensor_nodes / sensor_systems / sensors rows.
    """

    def test_creates_minimal_node(self, create_node, ingest_resources):
        """A minimal spec produces a node with no systems/sensors."""
        node = create_node({
            "site_name": "minimal",
            "source_name": "testing",
            "source_id": "testing-minimal",
        })

        print(node)
        assert node["sensor_nodes_id"] is not None
        assert node["systems"] == []

        with ingest_resources.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT site_name, source_name, source_id, geom "
                "FROM sensor_nodes WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            row = cur.fetchone()
            assert row["site_name"] == "minimal"
            assert row["source_name"] == "testing"
            assert row["source_id"] == "testing-minimal"
            assert row["geom"] is None

    def test_flat_sensors_creates_default_system(self, create_node, ingest_resources):
        """Sensors at the node level get wrapped in a default system."""
        node = create_node({
            "site_name": "station1",
            "source_name": "testing",
            "source_id": "testing-station1",
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

    def test_flat_and_nested_forms_are_equivalent(self, create_node, ingest_resources):
        """Two forms of the same spec produce the same db shape."""
        flat = create_node({
            "site_name": "flat_form",
            "source_name": "testing",
            "source_id": "testing-flat",
            "sensors": [{"period": 900, "measurand": "no"}],
        })

        nested = create_node({
            "site_name": "nested_form",
            "source_name": "testing",
            "source_id": "testing-nested",
            "systems": [{"sensors": [{"period": 900, "measurand": "no"}]}],
        })

        # Both have exactly 1 system with 1 sensor
        assert len(flat["systems"]) == len(nested["systems"]) == 1
        assert len(flat["systems"][0]["sensors"]) == 1
        assert len(nested["systems"][0]["sensors"]) == 1

        # Both sensors reference the same measurand
        assert (
            flat["systems"][0]["sensors"][0]["measurands_id"]
            == nested["systems"][0]["sensors"][0]["measurands_id"]
        )

    def test_sensor_defaults(self, create_node, ingest_resources):
        """Sensor source_id defaults to '{node_source_id}-{measurand}'."""
        node = create_node({
            "source_name": "testing",
            "source_id": "testing-defaults",
            "sensors": [{"period": 900, "measurand": "no"}],
        })

        sensor = node["systems"][0]["sensors"][0]
        assert sensor["source_id"] == "testing-defaults-no"

        with ingest_resources.cursor() as cur:
            cur.execute(
                """
                SELECT source_id, data_averaging_period_seconds,
                       data_logging_period_seconds
                FROM sensors WHERE sensors_id = %s
                """,
                (sensor["sensors_id"],),
            )
            row = cur.fetchone()
            assert row[0] == "testing-defaults-no"
            assert row[1] == 900
            assert row[2] == 900  # defaults to period when not given

    def test_coordinates_dict_form(self, create_node, ingest_resources):
        """Coordinates as {lat, lon} dict are stored as geom."""
        node = create_node({
            "site_name": "with_coords",
            "source_name": "testing",
            "source_id": "testing-coords1",
            "coordinates": {"lat": 42.05, "lon": -123.04},
        })

        with ingest_resources.cursor() as cur:
            cur.execute(
                "SELECT ST_X(geom), ST_Y(geom), ST_SRID(geom) "
                "FROM sensor_nodes WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            x, y, srid = cur.fetchone()
            assert x == pytest.approx(-123.04)
            assert y == pytest.approx(42.05)
            assert srid == 4326

    def test_coordinates_list_form(self, create_node, ingest_resources):
        """Coordinates as [lon, lat] (GeoJSON order) are stored correctly."""
        node = create_node({
            "source_name": "testing",
            "source_id": "testing-coords2",
            "coordinates": [-123.04, 42.05],
        })

        with ingest_resources.cursor() as cur:
            cur.execute(
                "SELECT ST_X(geom), ST_Y(geom) FROM sensor_nodes WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            x, y = cur.fetchone()
            assert x == pytest.approx(-123.04)
            assert y == pytest.approx(42.05)

    def test_coordinates_flat_form(self, create_node, ingest_resources):
        """Coordinates as top-level lat/lon keys work too."""
        node = create_node({
            "source_name": "testing",
            "source_id": "testing-coords3",
            "lat": 42.05,
            "lon": -123.04,
        })

        with ingest_resources.cursor() as cur:
            cur.execute(
                "SELECT ST_X(geom), ST_Y(geom) FROM sensor_nodes WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            x, y = cur.fetchone()
            assert x == pytest.approx(-123.04)
            assert y == pytest.approx(42.05)

    def test_multiple_systems(self, create_node, ingest_resources):
        """Multiple systems are created with distinct source_ids."""
        node = create_node({
            "source_name": "testing",
            "source_id": "testing-multisys",
            "systems": [
                {"sensors": [{"period": 900, "measurand": "no"}]},
                {"sensors": [{"period": 900, "measurand": "no2"}]},
            ],
        })

        assert len(node["systems"]) == 2
        sys_ids = [s["sensor_systems_id"] for s in node["systems"]]
        assert len(set(sys_ids)) == 2  # unique

        with ingest_resources.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM sensor_systems WHERE sensor_nodes_id = %s",
                (node["sensor_nodes_id"],),
            )
            assert cur.fetchone()[0] == 2

    def test_multiple_sensors_in_one_system(self, create_node, ingest_resources):
        """A single system can hold multiple sensors."""
        node = create_node({
            "source_name": "testing",
            "source_id": "testing-multisensor",
            "sensors": [
                {"period": 900, "measurand": "no"},
                {"period": 3600, "measurand": "pm25"},
            ],
        })

        sensors = node["systems"][0]["sensors"]
        assert len(sensors) == 2
        measurands = {s["measurand"] for s in sensors}
        assert measurands == {"no", "pm25"}

    def test_is_idempotent(self, create_node, ingest_resources):
        """Calling twice with the same spec doesn't create duplicates."""
        spec = {
            "source_name": "testing",
            "source_id": "testing-idempotent",
            "sensors": [{"period": 900, "measurand": "no"}],
        }

        first = create_node(spec)
        second = create_node(spec)

        assert first["sensor_nodes_id"] == second["sensor_nodes_id"]
        assert (
            first["systems"][0]["sensor_systems_id"]
            == second["systems"][0]["sensor_systems_id"]
        )
        assert (
            first["systems"][0]["sensors"][0]["sensors_id"]
            == second["systems"][0]["sensors"][0]["sensors_id"]
        )

    def test_unknown_measurand_raises(self, create_node):
        """A bogus measurand raises a clear error."""
        with pytest.raises(ValueError, match="not found"):
            create_node({
                "source_name": "testing",
                "source_id": "testing-bad-measurand",
                "sensors": [{"period": 900, "measurand": "not_a_real_pollutant"}],
            })
