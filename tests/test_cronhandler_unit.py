import pytest
from unittest.mock import patch, MagicMock, call
from time import time
from ingest.handler import cronhandler


class TestCronhandlerPaused:
    """Tests for PAUSE_INGESTING behavior."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_paused(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler returns early when PAUSE_INGESTING is True."""
        # Arrange
        mock_settings.PAUSE_INGESTING = True

        # Act
        result = cronhandler(cloudwatch_event, lambda_context)

        # Assert
        assert result is None
        mock_load_metadata.assert_not_called()
        mock_load_db.assert_not_called()
        mock_load_measurements.assert_not_called()


class TestCronhandlerFetchlogPattern:
    """Tests for fetchlogKey pattern processing."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_measurements_pattern')
    def test_cronhandler_fetchlog_key_pattern(
        self,
        mock_load_pattern,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler processes specific fetchlogKey pattern."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_load_pattern.return_value = {"processed": 5}
        event = {**cloudwatch_event, "fetchlogKey": "lcs-etl-pipeline/test.json"}

        # Act
        result = cronhandler(event, lambda_context)

        # Assert
        mock_load_pattern.assert_called_once_with(
            limit=10,
            pattern="lcs-etl-pipeline/test.json"
        )
        assert result == {"processed": 5}

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_measurements_pattern')
    def test_cronhandler_fetchlog_key_custom_limit(
        self,
        mock_load_pattern,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler respects custom limit for fetchlogKey."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        event = {
            **cloudwatch_event,
            "fetchlogKey": "lcs-etl-pipeline/test.json",
            "limit": 25
        }

        # Act
        cronhandler(event, lambda_context)

        # Assert
        mock_load_pattern.assert_called_once_with(
            limit=25,
            pattern="lcs-etl-pipeline/test.json"
        )


class TestCronhandlerLoaderOrchestration:
    """Tests for loader function orchestration."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_calls_all_loaders(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler calls all three loaders in order."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        # Each loader returns 0 on first call (no records to process)
        mock_load_metadata.return_value = 0
        mock_load_db.return_value = 0
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - verify all three called with correct parameters
        mock_load_metadata.assert_called_once_with(30, True)
        mock_load_db.assert_called_once_with(20, True)
        mock_load_measurements.assert_called_once_with(10, True)

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_loader_returns_zero_stops_loop(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test loader loop stops when loader returns 0 (no more records)."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        # Metadata processes 3 batches then returns 0
        mock_load_metadata.side_effect = [30, 25, 15, 0]
        mock_load_db.return_value = 0
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - metadata called 4 times until it returned 0
        assert mock_load_metadata.call_count == 4

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_zero_limits_skip_loaders(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test loaders not called when their limit is 0."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 0  # Disabled
        mock_settings.REALTIME_LIMIT = 0  # Disabled
        mock_settings.METADATA_LIMIT = 30  # Enabled

        mock_load_metadata.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - only metadata called
        mock_load_metadata.assert_called_once()
        mock_load_db.assert_not_called()
        mock_load_measurements.assert_not_called()


class TestCronhandlerEventOverrides:
    """Tests for event parameter overrides."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_event_overrides_settings(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test event parameters override settings."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True  # Will be overridden
        mock_settings.PIPELINE_LIMIT = 10    # Will be overridden
        mock_settings.REALTIME_LIMIT = 20    # Will be overridden
        mock_settings.METADATA_LIMIT = 30    # Will be overridden

        # Override with event parameters
        event = {
            **cloudwatch_event,
            "ascending": False,
            "pipeline_limit": 100,
            "realtime_limit": 200,
            "metadata_limit": 300
        }

        mock_load_metadata.return_value = 0
        mock_load_db.return_value = 0
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(event, lambda_context)

        # Assert - verify overridden parameters used
        mock_load_metadata.assert_called_once_with(300, False)
        mock_load_db.assert_called_once_with(200, False)
        mock_load_measurements.assert_called_once_with(100, False)


class TestCronhandlerErrorHandling:
    """Tests for error handling and resilience."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_metadata_exception_continues(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler continues to realtime/pipeline when metadata fails."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        # Metadata throws exception
        mock_load_metadata.side_effect = Exception("Database connection failed")
        mock_load_db.return_value = 0
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - other loaders still called despite metadata failure
        mock_load_metadata.assert_called_once()
        mock_load_db.assert_called_once()
        mock_load_measurements.assert_called_once()

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_realtime_exception_continues(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler continues to pipeline when realtime fails."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        mock_load_metadata.return_value = 0
        mock_load_db.side_effect = Exception("S3 access denied")
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - pipeline still called despite realtime failure
        mock_load_metadata.assert_called_once()
        mock_load_db.assert_called_once()
        mock_load_measurements.assert_called_once()

    @patch('ingest.handler.settings')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_pipeline_exception_completes(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler completes when pipeline fails."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 300
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        mock_load_metadata.return_value = 0
        mock_load_db.return_value = 0
        mock_load_measurements.side_effect = Exception("Pipeline processing error")

        # Act - should not raise exception
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - all loaders were attempted
        mock_load_metadata.assert_called_once()
        mock_load_db.assert_called_once()
        mock_load_measurements.assert_called_once()


class TestCronhandlerTimeout:
    """Tests for timeout handling."""

    @patch('ingest.handler.settings')
    @patch('ingest.handler.time')
    @patch('ingest.handler.load_metadata_db')
    @patch('ingest.handler.load_db')
    @patch('ingest.handler.load_measurements_db')
    def test_cronhandler_respects_timeout(
        self,
        mock_load_measurements,
        mock_load_db,
        mock_load_metadata,
        mock_time,
        mock_settings,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler stops processing when timeout is reached."""
        # Arrange
        mock_settings.PAUSE_INGESTING = False
        mock_settings.INGEST_TIMEOUT = 10  # 10 second timeout
        mock_settings.FETCH_ASCENDING = True
        mock_settings.PIPELINE_LIMIT = 10
        mock_settings.REALTIME_LIMIT = 20
        mock_settings.METADATA_LIMIT = 30

        # Simulate time progression
        # Control flow: start_time = time(), while check time(), logger time(), repeat
        start_time = 100.0
        mock_time.side_effect = [
            start_time,      # Initial start_time = time()
            start_time + 3,  # First while condition check (3 < 10, continues)
            start_time + 3,  # First logger.info after load
            start_time + 6,  # Second while condition check (6 < 10, continues)
            start_time + 6,  # Second logger.info after load
            start_time + 11, # Third while condition check (11 > 10, stops)
            start_time + 11, # Realtime start check
            start_time + 11, # Pipeline start check
            start_time + 11, # Final log
        ]

        # Metadata keeps returning records (would loop forever without timeout)
        mock_load_metadata.return_value = 30
        mock_load_db.return_value = 0
        mock_load_measurements.return_value = 0

        # Act
        cronhandler(cloudwatch_event, lambda_context)

        # Assert - metadata called twice before timeout, then stopped
        assert mock_load_metadata.call_count == 2
