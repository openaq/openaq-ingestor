import logging
import pytest
import json
import os
from datetime import date

from ingest.lcsV2 import (
    IngestClient,
)

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level='DEBUG',
    force=True,
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

def get_path(relpath):
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, relpath);


def test_ingest_client():
    """
    Very simple data example
    """
    client = IngestClient();
    client.load_key(get_path('dataV2.json'), 1, str(date.today()))
    assert len(client.nodes) == 3
    assert len(client.systems) == 0
    assert len(client.sensors) == 0
    assert len(client.measurements) == 2


def test_ingest_client_realtime_measures():
    """
    Actually uses the fetch/load_db method but not much to test in that method
    because it dumps it directly into postgres to do the work
    """
    client = IngestClient();
    client.load_key(get_path('testdata_realtime_measures.ndjson'), 1, str(date.today()))
    assert len(client.nodes) == 0
    assert len(client.systems) == 0
    assert len(client.sensors) == 0
    assert len(client.measurements) == 2


def test_ingest_client_clarity():
    client = IngestClient();
    client.load_key(get_path('testdata_lcs_clarity.json'), 1, str(date.today()))
    assert len(client.nodes) == 2
    assert len(client.systems) == 0
    assert len(client.sensors) == 0
    assert len(client.measurements) == 3


def test_ingest_client_senstate():
    """
    senstate passes very simple csv files where the only location information is in the form
    of a location-id-param value and so location is created
    """
    client = IngestClient();
    client.load_key(get_path('testdata_lcs_senstate.csv'), 1, str(date.today()))
    assert len(client.measurements) == 3
    assert len(client.nodes) == 0
    assert len(client.systems) == 0
    assert len(client.sensors) == 0


def test_ingest_client_transform():
    client = IngestClient();
    client.load_key(get_path('testdata_transform.json'), 1, str(date.today()))
    assert len(client.nodes) == 1, "Client has the right number of nodes"
    assert len(client.systems) == 1, "Client has the right number of systems"
    assert len(client.sensors) == 2, "Client has the right number of sensors"
    assert len(client.measurements) == 2, "Client has the right number of measurements"
    assert len(client.flags) == 3, "Client has the right number of flags"


def test_ingest_client_handles_variable_flag_formats():
    client = IngestClient();
    data = { "locations": [
        {
            "key": "provider-l1",
            "site_id": "l1",
            "coordinates": {
                "lat": 45.56665,
                "lon": -123.12121,
                "proj": "WSG84"
            },
            "flags": [
                {
                    "starts": "2024-01-01T00:00:00-08:00",
                    "ends": "2024-01-02T00:00:00-08:00",
                    "flag": "info::node-info",
                    "note": "Sensor node level information"
                }
            ]
        }
    ]}
    client.load(data)
    flag = client.flags[0]
    assert len(client.flags) == 1, "Client has the right number of flags"
    assert flag['sensor_ingest_id'] == 'provider-l1'
    assert flag['ingest_id'] == 'info::node-info'
