import logging
import pytest
import json
import os
from datetime import date

from ingest.models import (
    Meta,
    FetchSummary,
    IngestMatchingMethod,
    Coordinates,
    Measure,
    Sensor,
    System,
    Location,
    RealtimeMeasure,
    RealtimeSchema,
    LcsSchema,
    TransformSchema,
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

def test_location_defaults_siteid_and_key():
    l = Location(location='testing-s1', coordinates=dict(lat=1,lon=1))
    assert l.site_id == 'testing-s1'
    assert l.key == 'testing-s1'
    assert l.site_name == 'testing-s1'
    assert l.coordinates.proj == 'WGS84'

def test_location_selects_approprate_key():
    l = Location(location='testing-s1', site_id='s1', coordinates=dict(lat=1,lon=1))
    assert l.key == 'testing-s1'
    assert l.site_id == 's1'
    assert l.site_name == 's1'
    assert l.coordinates.proj == 'WGS84'

def test_meta_optional_fields_are_not_required():
    m = Meta(source_name='testing', schema='v1')
    assert m.source_name == 'testing'

def test_transform_data_model():
    with open(get_path('testdata_transform.json'), 'r') as f:
        raw_data = json.load(f)
    data = TransformSchema(**raw_data)


def test_realtime_data_model():
    with open(get_path('testdata_realtime_measures.ndjson'), 'r') as f:
        measures = []
        for obj in f.read().split('\n'):
            if obj != "":
                measures.append(RealtimeMeasure(**json.loads(obj)))
    data = RealtimeSchema(measures=measures)
    print(data)



def test_lcs_data_model():
    with open(get_path('testdata_lcs_clarity.json'), 'r') as f:
        raw_data = json.load(f)
    data = LcsSchema(**raw_data)
