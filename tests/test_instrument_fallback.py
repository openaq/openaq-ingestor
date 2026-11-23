"""
Sensor systems should always be linked to an instrument.

Resolution order during etl_process_nodes:
  named instrument -> provider default (<source_name>::default) -> generic (1)
A fallback instrument only fills a missing link and never replaces one.
"""
import pytest
from unittest.mock import patch
from ingest.lcsV2 import IngestClient
from ingest import settings
from pathlib import Path

SOURCE = "instfallback"
GENERIC_INSTRUMENT_ID = 1
DROP_STAGING_SQL = (
    Path(__file__).parent.parent / "ingest" / "drop_temporary_tables.sql"
)


@pytest.fixture
def disable_temp_tables():
    with patch.object(settings.settings, "USE_TEMP_TABLES", False):
        yield


def location(site, system=None):
    system = system or {}
    return {
        "location": f"{SOURCE}-{site}",
        "label": site,
        "lon": -122.99,
        "lat": 42.80,
        "systems": [{"system_id": f"{SOURCE}-{site}", "sensors": [], **system}],
    }


def ingest(resources, locations):
    # staging tables persist within the test transaction so clear them
    # to allow a second ingest to be run
    with resources.cursor() as cursor:
        cursor.execute(open(DROP_STAGING_SQL).read())
    client = IngestClient(resources=resources)
    client.load_locations(locations)
    client.dump(load=True)


def system_instrument(cursor, site):
    cursor.execute(
        """
        SELECT sy.instruments_id
        FROM sensor_systems sy
        JOIN sensor_nodes n USING (sensor_nodes_id)
        WHERE n.source_name = %s AND n.source_id = %s
        """,
        (SOURCE, site),
    )
    rows = cursor.fetchall()
    assert len(rows) == 1
    return rows[0][0]


def add_instrument(cursor, ingest_id, label):
    cursor.execute(
        """
        INSERT INTO instruments (manufacturer_entities_id, label, ingest_id)
        VALUES (8, %s, %s)
        RETURNING instruments_id
        """,
        (label, ingest_id),
    )
    return cursor.fetchone()[0]


class TestAddSystemsInstrumentKeys:

    def add(self, system, delim="-"):
        client = IngestClient()
        client.delim = delim
        client.add_systems([system], delim.join(["src", "site1"]), 1)
        return next(iter(client.systems.values()))

    def test_no_instrument_leaves_keys_null(self):
        system = self.add({"system_id": "src-site1"})
        assert system["manufacturer_key"] is None
        assert system["model_key"] is None
        assert system["instrument_ingest_id"] is None

    def test_transform_default_names_treated_as_no_instrument(self):
        system = self.add(
            {
                "key": "src/site1",
                "manufacturer_name": "default",
                "model_name": "default",
            },
            delim="/",
        )
        assert system["manufacturer_key"] is None
        assert system["model_key"] is None
        assert system["instrument_ingest_id"] is None

    def test_instrument_key_sets_ingest_id(self):
        system = self.add({"system_id": "src-site1", "instrument": "acme::x100"})
        assert system["manufacturer_key"] == "acme"
        assert system["model_key"] == "x100"
        assert system["instrument_ingest_id"] == "acme::x100"

    def test_manufacturer_and_model_names_set_ingest_id(self):
        system = self.add(
            {
                "key": "src/site1",
                "manufacturer_name": "acme",
                "model_name": "x100",
            },
            delim="/",
        )
        assert system["instrument_ingest_id"] == "acme::x100"

    def test_model_only_uses_source_as_manufacturer(self):
        system = self.add({"system_id": "src-site1", "model_name": "x100"})
        assert system["manufacturer_key"] == "src"
        assert system["instrument_ingest_id"] == "src::x100"

    def test_three_part_id_unchanged(self):
        system = self.add({"system_id": "src-site1-acme::x100"})
        assert system["manufacturer_key"] == "acme"
        assert system["model_key"] == "x100"
        assert system["instrument_ingest_id"] == "acme::x100"


@pytest.mark.integration
class TestInstrumentFallback:

    def test_new_system_falls_back_to_generic(
        self, ingest_resources, disable_temp_tables,
    ):
        ingest(ingest_resources, [location("site1")])
        with ingest_resources.cursor() as cursor:
            assert system_instrument(cursor, "site1") == GENERIC_INSTRUMENT_ID

    def test_new_system_falls_back_to_provider_default(
        self, ingest_resources, disable_temp_tables,
    ):
        with ingest_resources.cursor() as cursor:
            default_id = add_instrument(
                cursor, f"{SOURCE}::default", f"{SOURCE} default instrument",
            )
            ingest(ingest_resources, [location("site1")])
            assert system_instrument(cursor, "site1") == default_id

    def test_named_instrument_is_linked(
        self, ingest_resources, disable_temp_tables,
    ):
        with ingest_resources.cursor() as cursor:
            named_id = add_instrument(cursor, "acme::x100", "acme x100 test")
            ingest(
                ingest_resources,
                [location("site1", {"instrument": "acme::x100"})],
            )
            assert system_instrument(cursor, "site1") == named_id

    def test_fallback_does_not_replace_existing_instrument(
        self, ingest_resources, disable_temp_tables,
    ):
        with ingest_resources.cursor() as cursor:
            named_id = add_instrument(cursor, "acme::x100", "acme x100 test")
            ingest(
                ingest_resources,
                [location("site1", {"instrument": "acme::x100"})],
            )
            ingest(ingest_resources, [location("site1")])
            assert system_instrument(cursor, "site1") == named_id

    def test_fallback_fills_missing_instrument(
        self, ingest_resources, disable_temp_tables,
    ):
        ingest(ingest_resources, [location("site1")])
        with ingest_resources.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sensor_systems sy
                SET instruments_id = NULL
                FROM sensor_nodes n
                WHERE n.sensor_nodes_id = sy.sensor_nodes_id
                  AND n.source_name = %s
                """,
                (SOURCE,),
            )
            ingest(ingest_resources, [location("site1")])
            assert system_instrument(cursor, "site1") == GENERIC_INSTRUMENT_ID

    def test_no_instrument_records_created_for_unnamed_systems(
        self, ingest_resources, disable_temp_tables,
    ):
        with ingest_resources.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM instruments")
            instruments_before = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM entities")
            entities_before = cursor.fetchone()[0]
            ingest(ingest_resources, [location("site1")])
            cursor.execute("SELECT COUNT(*) FROM instruments")
            assert cursor.fetchone()[0] == instruments_before
            cursor.execute("SELECT COUNT(*) FROM entities")
            assert cursor.fetchone()[0] == entities_before
