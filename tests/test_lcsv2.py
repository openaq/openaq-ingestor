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
    client = IngestClient();
    client.load_key(get_path('testdata.json'), 1, str(date.today()))
    assert len(client.nodes) == 3
    assert len(client.systems) == 0
    assert len(client.sensors) == 0
    assert len(client.measurements) == 2


def test_ingest_client_realtime_measures():
    """
    Actually uses the fetch/load_db method but not much to test in that method
    because it dumpts it directly into postgres to do the work
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

@pytest.mark.skip(reason="placeholder because currently the transform format is not fully supported")
def test_ingest_client_transform():
    client = IngestClient();
    client.load_key(get_path('testdata_transform.json'), 1, str(date.today()))
    assert len(client.nodes) == 3
    assert len(client.systems) == 0
    assert len(client.sensors) == 0
    assert len(client.measurements) == 2
