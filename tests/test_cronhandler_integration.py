"""
Tests for cronhandler's orchestration logic in handler.py

cronhandler runs four stages (metadata, realtime, adapter, pipeline) and every
stage goes through load_measurements_db, differing only by the fetchlog key
pattern. load_measurements_db is replaced with a stub here so no database or
S3 access is needed. Fetchlog selection is covered in test_load_fetchlogs.py.
"""

import pytest
from itertools import count
from unittest.mock import patch
from ingest.handler import cronhandler


STAGES = ['metadata', 'realtime', 'adapter', 'pipeline']


def stage_of(pattern):
    """Map the pattern passed to load_measurements_db to its cronhandler stage."""
    if pattern is None:
        # pipeline relies on the default pattern
        return 'pipeline'
    if 'stations/' in pattern:
        return 'metadata'
    if 'realtime-gzipped/' in pattern:
        return 'realtime'
    if pattern.startswith('^data/'):
        return 'adapter'
    raise AssertionError(f"Unexpected pattern: {pattern}")


class LoaderStub:
    """
    Stand in for load_measurements_db.

    Set results[stage] to an int (returned on every call), a list (returned
    in order, then 0) or an Exception (raised). Stages default to 0.
    """

    def __init__(self):
        self.results = {}
        self.calls = []

    def __call__(self, limit, ascending, pattern=None):
        stage = stage_of(pattern)
        self.calls.append((stage, limit, ascending))
        result = self.results.get(stage, 0)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, list):
            return result.pop(0) if result else 0
        return result

    def calls_for(self, stage):
        return [c for c in self.calls if c[0] == stage]

    @property
    def stages_called(self):
        return [c[0] for c in self.calls]


@pytest.fixture
def loader(mock_cronhandler_settings):
    stub = LoaderStub()
    with patch('ingest.handler.load_measurements_db', new=stub):
        yield stub


class TestCronhandlerOrchestration:
    """Tests for which stages run, in what order and with what arguments."""

    def test_cronhandler_calls_all_stages_in_sequence(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
    ):
        cronhandler(cloudwatch_event, lambda_context)

        assert loader.calls == [(stage, 10, True) for stage in STAGES]

    def test_cronhandler_processes_multiple_batches(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
        caplog,
    ):
        """Each stage loops until its loader returns 0."""
        loader.results['metadata'] = [10, 10, 5, 0]

        cronhandler(cloudwatch_event, lambda_context)

        assert len(loader.calls_for('metadata')) == 4
        assert "loaded 25 metadata records" in caplog.text
        # the other stages still run once each afterwards
        assert loader.stages_called[4:] == ['realtime', 'adapter', 'pipeline']

    @pytest.mark.parametrize('enabled', STAGES)
    def test_cronhandler_zero_limits_skip_stages(
        self,
        enabled,
        cloudwatch_event,
        lambda_context,
        loader,
    ):
        event = {
            **cloudwatch_event,
            **{f'{stage}_limit': 0 for stage in STAGES},
            f'{enabled}_limit': 10,
        }

        cronhandler(event, lambda_context)

        assert loader.calls == [(enabled, 10, True)]

    def test_cronhandler_event_parameters_override_settings(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
    ):
        event = {
            **cloudwatch_event,
            'ascending': False,
            'metadata_limit': 100,
            'realtime_limit': 200,
            'adapter_limit': 300,
            'pipeline_limit': 400,
        }

        cronhandler(event, lambda_context)

        assert loader.calls == [
            ('metadata', 100, False),
            ('realtime', 200, False),
            ('adapter', 300, False),
            ('pipeline', 400, False),
        ]

    def test_cronhandler_fetchlogKey_parameter(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
    ):
        """fetchlogKey force loads the matching keys and skips the stages."""
        with patch('ingest.handler.load_fetchlog') as mock_load_fetchlog:
            mock_load_fetchlog.return_value = 5

            event = {
                **cloudwatch_event,
                'fetchlogKey': 'lcs-etl-pipeline/test-*.json',
                'limit': 25,
            }
            result = cronhandler(event, lambda_context)

        mock_load_fetchlog.assert_called_once_with(
            limit=25,
            key='lcs-etl-pipeline/test-*.json',
        )
        assert result == 5
        assert loader.calls == []

    def test_cronhandler_fetchlogKey_default_limit(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
    ):
        with patch('ingest.handler.load_fetchlog') as mock_load_fetchlog:
            event = {**cloudwatch_event, 'fetchlogKey': 'lcs-etl-pipeline/test.json'}
            cronhandler(event, lambda_context)

        mock_load_fetchlog.assert_called_once_with(
            limit=10,
            key='lcs-etl-pipeline/test.json',
        )

    def test_cronhandler_paused_skips_everything(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
        mock_cronhandler_settings,
    ):
        mock_cronhandler_settings.PAUSE_INGESTING = True

        assert cronhandler(cloudwatch_event, lambda_context) is None
        assert loader.calls == []


class TestCronhandlerErrorHandling:
    """A failure in one stage must not stop the remaining stages."""

    @pytest.mark.parametrize('failing', STAGES)
    def test_cronhandler_stage_exception_continues(
        self,
        failing,
        cloudwatch_event,
        lambda_context,
        loader,
        caplog,
    ):
        loader.results[failing] = Exception("Database connection failed")

        # should not raise
        cronhandler(cloudwatch_event, lambda_context)

        assert loader.stages_called == STAGES
        assert f"load {failing} failed: Database connection failed" in caplog.text
        assert "done processing:" in caplog.text


class TestCronhandlerTimeout:
    """Tests for timeout handling."""

    def test_cronhandler_respects_timeout(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
        mock_cronhandler_settings,
    ):
        """Stops looping once the timeout passes, even with records remaining."""
        mock_cronhandler_settings.INGEST_TIMEOUT = 2
        # loader never runs out of records
        loader.results['metadata'] = 10

        # every call to time() advances the clock 1.5 seconds:
        # start=0.0, first loop check=1.5 (runs), log=3.0, second check=4.5 (stops)
        clock = count(0, 1.5)
        with patch('ingest.handler.time', side_effect=lambda: next(clock)):
            cronhandler(cloudwatch_event, lambda_context)

        assert len(loader.calls_for('metadata')) == 1
        # timeout is shared so later stages do not start
        assert loader.stages_called == ['metadata']


class TestCronhandlerLogging:
    """Tests that verify logging output."""

    @pytest.mark.parametrize('stage,label', [
        ('metadata', 'metadata'),
        ('realtime', 'fetch'),
        ('adapter', 'adapter'),
        ('pipeline', 'pipeline'),
    ])
    def test_cronhandler_logging_shows_record_counts(
        self,
        stage,
        label,
        cloudwatch_event,
        lambda_context,
        loader,
        caplog,
    ):
        loader.results[stage] = [10, 5, 0]

        cronhandler(cloudwatch_event, lambda_context)

        assert f"loaded 15 {label} records, timer:" in caplog.text

    def test_cronhandler_logging_shows_done_message(
        self,
        cloudwatch_event,
        lambda_context,
        loader,
        caplog,
    ):
        cronhandler(cloudwatch_event, lambda_context)

        assert "done processing:" in caplog.text
        assert "seconds" in caplog.text
