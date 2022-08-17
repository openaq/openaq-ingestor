import os

if 'DOTENV' not in os.environ.keys():
    os.environ['DOTENV'] = '.env.testing'

if 'AWS_PROFILE' not in os.environ.keys():
    os.environ['AWS_PROFILE'] = 'python-user'


from openaq_fastapi.ingest.utils import (
    submit_metric
)


rsp = submit_metric(
    namespace='Ingest',
    metricname='get-station',
    units='Seconds',
    value=2.45,
    attributes={
        'fetchLogsId': 234235
    }
)
