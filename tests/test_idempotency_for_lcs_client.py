"""
Idempotency tests for LCS ingest client.

Each scenario is loaded twice through separate IngestClient instances
sharing the same DB connection. Between passes, measurement timestamps
are advanced by one hour to simulate a legitimately new batch. Nothing
about nodes, systems, or sensors -- including modified_on -- should
change on pass 2, since replayed data is otherwise identical.
"""

import copy
from datetime import datetime, timedelta

import pytest

from ingest.lcsV2 import IngestClient

from tests._debug import dump


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------

## locations are added with default provider
## and default provider has a tolerance of 0.002
def _location(source, station, systems, lat=45.0, lon=-122.0, mobile=False):
    return {
        "key": f"{source}-{station}",
        "site_id": station,
        "site_name": f"Station {station}",
        "coordinates": {"lat": lat, "lon": lon},
        "ismobile": mobile,
        "systems": systems,
    }


def _sensor(source, station, param, units="µg/m³"):
    return {
        "key": f"{source}-{station}-{param}",
        "parameter": param,
        "units": units,
        "averaging_interval_secs": 3600,
        "logging_interval_secs": 3600,
        "status": "active",
    }


def _measure(source, station, param, ts, value):
    return {
        "key": f"{source}-{station}-{param}",
        "timestamp": ts,
        "value": value,
    }


TS = "2024-04-08T21:00:00.000Z"


SCENARIOS = [
    {
        "id": "single-system-single-sensor-ingest-id",
        "source": "replay1",
        "expected": {"nodes": 1, "systems": 1, "sensors": 1},
        "data": {
            "meta": {"sourceName": "replay1", "matching_method": "ingest-id"},
            "locations": [_location("replay1", "s1", [{
                "key": "replay1-s1",
                "sensors": [_sensor("replay1", "s1", "pm25")],
            }])],
            "measures": [_measure("replay1", "s1", "pm25", TS, 12.5)],
        },
    },
    {
        "id": "generic-system-multi-sensor",
        "source": "replay2",
        "expected": {"nodes": 1, "systems": 1, "sensors": 2},
        "data": {
            "meta": {"sourceName": "replay2", "matching_method": "ingest-id"},
            "locations": [_location("replay2", "s1", [{
                "key": "replay2-s1",
                "sensors": [
                    _sensor("replay2", "s1", "pm25"),
                    _sensor("replay2", "s1", "no", units="ppb"),
                ],
            }])],
            "measures": [
                _measure("replay2", "s1", "pm25", TS, 12.5),
                _measure("replay2", "s1", "no", TS, 0.3),
            ],
        },
    },
    {
        "id": "custom-systems-per-sensor",
        "source": "replay3",
        "expected": {"nodes": 1, "systems": 2, "sensors": 2},
        "data": {
            "meta": {"sourceName": "replay3", "matching_method": "ingest-id"},
            "locations": [_location("replay3", "s1", [
                {
                    "key": "replay3-s1-teledyne::t500u",
                    "sensors": [_sensor("replay3", "s1", "pm25")],
                },
                {
                    "key": "replay3-s1-thermo::42i",
                    "sensors": [_sensor("replay3", "s1", "no", units="ppb")],
                },
            ])],
            "measures": [
                _measure("replay3", "s1", "pm25", TS, 12.5),
                _measure("replay3", "s1", "no", TS, 0.3),
            ],
        },
    },
    {
        "id": "mixed-generic-and-custom-systems",
        "source": "replay4",
        "expected": {"nodes": 1, "systems": 2, "sensors": 3},
        "data": {
            "meta": {"sourceName": "replay4", "matching_method": "ingest-id"},
            "locations": [_location("replay4", "s1", [
                {
                    "key": "replay4-s1",
                    "sensors": [
                        _sensor("replay4", "s1", "pm25"),
                        _sensor("replay4", "s1", "pm10"),
                    ],
                },
                {
                    "key": "replay4-s1-thermo::42i",
                    "sensors": [_sensor("replay4", "s1", "no", units="ppb")],
                },
            ])],
            "measures": [
                _measure("replay4", "s1", "pm25", TS, 12.5),
                _measure("replay4", "s1", "pm10", TS, 20.1),
                _measure("replay4", "s1", "no", TS, 0.3),
            ],
        },
    },
    {
        "id": "mobile-node",
        "source": "replay5",
        "expected": {"nodes": 1, "systems": 1, "sensors": 1},
        "data": {
            "meta": {"sourceName": "replay5", "matching_method": "ingest-id"},
            "locations": [_location("replay5", "s1", [{
                "key": "replay5-s1",
                "sensors": [_sensor("replay5", "s1", "pm25")],
            }], mobile=True)],
            "measures": [_measure("replay5", "s1", "pm25", TS, 12.5)],
        },
    },
    {
        "id": "source-spatial-within-threshold",
        "source": "replay6",
        "expected": {"nodes": 1, "systems": 1, "sensors": 1},
        "coord_shift": (0.00005, 0.00005),  # ~0.5m
        "coord_shift_creates_node": False,
        "data": {
            "meta": {
                "sourceName": "replay6",
                "matching_method": "source-spatial",
            },
            "locations": [_location("replay6", "s1", [{
                "key": "replay6-s1",
                "sensors": [_sensor("replay6", "s1", "pm25")],
            }])],
            "measures": [_measure("replay6", "s1", "pm25", TS, 12.5)],
        },
    },
    {
        "id": "source-spatial-outside-threshold",
        "source": "replay7",
        "expected": {"nodes": 1, "systems": 1, "sensors": 1},
        "coord_shift": (0.01, 0.01),
        "coord_shift_creates_node": True,
        "data": {
            "meta": {
                "sourceName": "replay7",
                "matching_method": "source-spatial",
            },
            "locations": [_location("replay7", "s1", [{
                "key": "replay7-s1",
                "sensors": [_sensor("replay7", "s1", "pm25")],
            }])],
            "measures": [_measure("replay7", "s1", "pm25", TS, 12.5)],
        },
    },
]


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _advance_measurements(data, hours):
    """Return deep copy of data with all measurement timestamps shifted."""
    out = copy.deepcopy(data)
    for m in out.get("measures", []):
        ts = datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00"))
        m["timestamp"] = (ts + timedelta(hours=hours)).isoformat()
    return out


def _capture_state(get_object, source_name):
    """Snapshot persistent entity state for one source."""
    return {
        "nodes": get_object("nodes_by_source", source_name=source_name),
        "systems": get_object("systems_by_source", source_name=source_name),
        "sensors": get_object("sensors_by_source", source_name=source_name),
        "history": get_object("node_history", source_name=source_name),
        "measurement_counts": {
            r["source_id"]: r["n"]
            for r in get_object("measurement_counts",
                                source_name=source_name)
        },
    }

def _rename_locations(data, suffix=" (updated)"):
    out = copy.deepcopy(data)
    for loc in out.get("locations", []):
        loc["site_name"] = loc["site_name"] + suffix
    return out


def _shift_coordinates(data, lat_delta, lon_delta):
    out = copy.deepcopy(data)
    for loc in out.get("locations", []):
        coords = loc.get("coordinates", {})
        if "lat" in coords:
            coords["lat"] += lat_delta
        if "lon" in coords:
            coords["lon"] += lon_delta
    return out

# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.parametrize(
    "scenario", SCENARIOS, ids=[s["id"] for s in SCENARIOS]
)
def test_replay_is_idempotent(
    ingest_resources,
    disable_temp_tables,
    make_fetchlog,
    get_object,
    scenario,
):
    source = scenario["source"]
    expected = scenario["expected"]

    # ------- Pass 1: greenfield -------
    fl1 = make_fetchlog(f"replay-{source}-pass1")
    client1 = IngestClient(resources=ingest_resources, fetchlogs_id=fl1)
    client1.load(scenario["data"])
    client1.dump(load=True)

    snapshot_1 = _capture_state(get_object, source)

    assert len(snapshot_1["nodes"]) == expected["nodes"], (
        f"pass 1: wrong node count for {source}"
    )
    assert len(snapshot_1["systems"]) == expected["systems"], (
        f"pass 1: wrong system count for {source}"
    )
    assert len(snapshot_1["sensors"]) == expected["sensors"], (
        f"pass 1: wrong sensor count for {source}"
    )
    total_measures = len(scenario["data"].get("measures", []))
    assert sum(snapshot_1["measurement_counts"].values()) == total_measures, (
        f"pass 1: wrong measurement count for {source}"
    )
    assert len(snapshot_1["history"]) == 0, (
        f"pass 1: wrong history count for {source}"
    )

    # ------- Pass 2: replay with advanced measurements -------
    fl2 = make_fetchlog(f"replay-{source}-pass2")
    replay_data = _advance_measurements(scenario["data"], hours=1)

    client2 = IngestClient(resources=ingest_resources, fetchlogs_id=fl2)
    client2.load(replay_data)
    client2.dump(load=True)

    snapshot_2 = _capture_state(get_object, source)

    # Idempotency: entity rows are frozen (including modified_on)
    assert snapshot_2["nodes"] == snapshot_1["nodes"], (
        f"{source}: sensor_nodes changed on replay"
    )
    assert snapshot_2["systems"] == snapshot_1["systems"], (
        f"{source}: sensor_systems changed on replay"
    )
    assert snapshot_2["sensors"] == snapshot_1["sensors"], (
        f"{source}: sensors changed on replay"
    )
    assert len(snapshot_2["history"]) == 0, (
        f"pass 2: wrong history count for {source}"
    )


    # Measurements: pass 2 added new rows, pass 1 rows untouched
    for source_id, count in snapshot_2["measurement_counts"].items():
        prior = snapshot_1["measurement_counts"][source_id]
        assert count == prior * 2, (
            f"{source}: sensor {source_id} measurement count wrong on replay "
            f"({prior} -> {count}, expected {prior * 2})"
        )

    # ------- Pass 3: modify site_name only -------
    fl3 = make_fetchlog(f"replay-{source}-pass3")
    renamed_data = _rename_locations(
        _advance_measurements(scenario["data"], hours=2)
    )

    client3 = IngestClient(resources=ingest_resources, fetchlogs_id=fl3)
    client3.load(renamed_data)
    client3.dump(load=True)

    snapshot_3 = _capture_state(get_object, source)

    # Nodes: same IDs, site_name updated, modified_on advances
    assert len(snapshot_3["nodes"]) == expected["nodes"]
    for n2, n3 in zip(snapshot_2["nodes"], snapshot_3["nodes"]):
        assert n3["sensor_nodes_id"] == n2["sensor_nodes_id"]
        assert n3["site_name"] == n2["site_name"] + " (updated)"
        assert n3["geom_wkt"] == n2["geom_wkt"]
        prior = n2["modified_on"] or n2["added_on"]
        assert n3["modified_on"] is not None
        assert n3["modified_on"] > prior

    assert snapshot_3["systems"] == snapshot_2["systems"]
    assert snapshot_3["sensors"] == snapshot_2["sensors"]
    assert len(snapshot_3["history"]) == 1, (
        f"pass 3: wrong history count for {source}"
    )

    # ------- Pass 4: shift coordinates -------
    fl4 = make_fetchlog(f"replay-{source}-pass4")
    lat_delta, lon_delta = scenario.get("coord_shift", (0.01, 0.01))
    shifted_data = _shift_coordinates(
        _advance_measurements(renamed_data, hours=3),
        lat_delta=lat_delta,
        lon_delta=lon_delta,
    )

    client4 = IngestClient(resources=ingest_resources, fetchlogs_id=fl4)
    client4.load(shifted_data)
    client4.dump(load=True)

    snapshot_4 = _capture_state(get_object, source)

    if scenario.get("coord_shift_creates_node", True):
        # New node created; old one untouched
        assert len(snapshot_4["nodes"]) == expected["nodes"] + 1
        prior_ids = {n["sensor_nodes_id"] for n in snapshot_3["nodes"]}
        new_ids = {n["sensor_nodes_id"] for n in snapshot_4["nodes"]}
        assert prior_ids.issubset(new_ids)
        assert len(new_ids - prior_ids) == 1
    else:
        # Existing node updated in place
        assert len(snapshot_4["nodes"]) == expected["nodes"]
        assert len(snapshot_4["history"]) == 2, (
            f"pass 4: wrong history count for {source}"
        )
        for n3, n4 in zip(snapshot_3["nodes"], snapshot_4["nodes"]):
            assert n4["sensor_nodes_id"] == n3["sensor_nodes_id"]
            assert n4["geom_wkt"] != n3["geom_wkt"]
            assert n4["site_name"] == n3["site_name"]  # unchanged
            prior = n3["modified_on"] or n3["added_on"]
            assert n4["modified_on"] > prior
