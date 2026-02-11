"""
Integration tests for cronhandler in handler.py

These tests verify cronhandler's orchestration logic with real database state.
Loader functions are mocked to avoid S3 dependencies, but database interactions
are real to verify state management.
"""

import pytest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
from ingest.handler import cronhandler


@pytest.mark.integration
class TestCronhandlerOrchestration:
    """Tests for cronhandler's loader orchestration with real database."""

    def test_cronhandler_calls_all_loaders_in_sequence(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test that all three loaders are called in sequence."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.return_value = 0
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - all three loaders were called
        mock_metadata.assert_called_once_with(10, True)
        mock_realtime.assert_called_once_with(10, True)
        mock_pipeline.assert_called_once_with(10, True)

    def test_cronhandler_processes_multiple_batches(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings,
        caplog
    ):
        """Test that cronhandler loops through multiple batches until exhausted."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            # Simulate processing 10, 10, 5, then 0 (exhausted)
            mock_metadata.side_effect = [10, 10, 5, 0]
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act
            event = {**cloudwatch_event, 'metadata_limit': 10}
            cronhandler(event, lambda_context)

        # Assert - verify 4 calls (last one returns 0)
        assert mock_metadata.call_count == 4, "Should loop until loader returns 0"

        # Verify logging shows total processed
        assert "loaded 25 metadata records" in caplog.text

    def test_cronhandler_zero_limits_skip_loaders(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test that zero limits skip loader stages."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_pipeline.return_value = 0

            # Act - only pipeline enabled
            event = {
                **cloudwatch_event,
                'metadata_limit': 0,
                'realtime_limit': 0,
                'pipeline_limit': 10
            }
            cronhandler(event, lambda_context)

        # Assert - only pipeline called
        mock_metadata.assert_not_called()
        mock_realtime.assert_not_called()
        mock_pipeline.assert_called_once()

    def test_cronhandler_event_parameters_override_settings(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test event parameters override settings."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.return_value = 0
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act - override with event parameters
            event = {
                **cloudwatch_event,
                'ascending': False,
                'metadata_limit': 100,
                'realtime_limit': 200,
                'pipeline_limit': 300
            }
            cronhandler(event, lambda_context)

        # Assert - verify overridden parameters used
        mock_metadata.assert_called_once_with(100, False)
        mock_realtime.assert_called_once_with(200, False)
        mock_pipeline.assert_called_once_with(300, False)

    def test_cronhandler_fetchlogKey_parameter(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test fetchlogKey parameter processes specific pattern."""
        with patch('ingest.handler.load_measurements_pattern') as mock_pattern:
            mock_pattern.return_value = {"processed": 5}

            event = {
                **cloudwatch_event,
                'fetchlogKey': 'lcs-etl-pipeline/test-*.json',
                'limit': 25
            }

            # Act
            result = cronhandler(event, lambda_context)

        # Assert
        mock_pattern.assert_called_once_with(
            limit=25,
            pattern='lcs-etl-pipeline/test-*.json'
        )
        assert result == {"processed": 5}


@pytest.mark.integration
class TestCronhandlerErrorHandling:
    """Tests for error handling and resilience."""

    def test_cronhandler_metadata_exception_continues_to_realtime(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test cronhandler continues to realtime/pipeline when metadata fails."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            # Metadata throws exception
            mock_metadata.side_effect = Exception("Database connection failed")
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - other loaders still called despite metadata failure
        mock_metadata.assert_called_once()
        mock_realtime.assert_called_once()
        mock_pipeline.assert_called_once()

    def test_cronhandler_realtime_exception_continues_to_pipeline(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test cronhandler continues to pipeline when realtime fails."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.return_value = 0
            mock_realtime.side_effect = Exception("S3 access denied")
            mock_pipeline.return_value = 0

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - pipeline still called despite realtime failure
        mock_metadata.assert_called_once()
        mock_realtime.assert_called_once()
        mock_pipeline.assert_called_once()

    def test_cronhandler_pipeline_exception_completes(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings
    ):
        """Test cronhandler completes when pipeline fails."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.return_value = 0
            mock_realtime.return_value = 0
            mock_pipeline.side_effect = Exception("Pipeline processing error")

            # Act - should not raise exception
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - all loaders were attempted
        mock_metadata.assert_called_once()
        mock_realtime.assert_called_once()
        mock_pipeline.assert_called_once()


@pytest.mark.integration
class TestCronhandlerTimeout:
    """Tests for timeout handling."""

    def test_cronhandler_respects_timeout(
        self,
        cloudwatch_event,
        lambda_context
    ):
        """Test cronhandler stops processing when timeout is reached."""
        with patch('ingest.handler.settings') as mock_settings, \
             patch('ingest.handler.time') as mock_time, \
             patch('ingest.handler.load_metadata_db') as mock_metadata:

            mock_settings.PAUSE_INGESTING = False
            mock_settings.INGEST_TIMEOUT = 2  # 2 second timeout
            mock_settings.FETCH_ASCENDING = True
            mock_settings.METADATA_LIMIT = 10
            mock_settings.REALTIME_LIMIT = 0
            mock_settings.PIPELINE_LIMIT = 0

            # Simulate time progression
            start_time = 100.0
            mock_time.side_effect = [
                start_time,      # Initial start_time
                start_time + 1,  # First while check (continues)
                start_time + 1,  # First logger.info
                start_time + 3,  # Second while check (timeout exceeded, stops)
                start_time + 3,  # Final log
            ]

            # Metadata keeps returning records
            mock_metadata.return_value = 10

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - processing stopped before exhausting records
        assert mock_metadata.call_count < 10, "Timeout should stop processing early"


@pytest.mark.integration
class TestCronhandlerLogging:
    """Tests that verify logging output."""

    def test_cronhandler_logging_shows_record_counts(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings,
        caplog
    ):
        """Test that logging shows record counts and timing."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.side_effect = [10, 5, 0]  # Process 15 total
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - verify logging output
        assert "loaded 15 metadata records" in caplog.text
        assert "timer:" in caplog.text

    def test_cronhandler_logging_shows_done_message(
        self,
        cloudwatch_event,
        lambda_context,
        mock_cronhandler_settings,
        caplog
    ):
        """Test that done message shows total processing time."""
        with patch('ingest.handler.load_metadata_db') as mock_metadata, \
             patch('ingest.handler.load_db') as mock_realtime, \
             patch('ingest.handler.load_measurements_db') as mock_pipeline:

            mock_metadata.return_value = 0
            mock_realtime.return_value = 0
            mock_pipeline.return_value = 0

            # Act
            cronhandler(cloudwatch_event, lambda_context)

        # Assert - verify done message
        assert "done processing:" in caplog.text
        assert "seconds" in caplog.text


@pytest.mark.integration
class TestCronhandlerDatabaseStateWithLoadFetchlogs:
    """
    Tests that verify database state management using load_fetchlogs directly.

    These tests call load_fetchlogs() (the utility function that loaders use)
    to verify that fetchlog records are correctly marked as loaded/completed.
    """

    def test_load_fetchlogs_sets_loaded_datetime(
        self,
        db_cursor,
        db_connection,
        sample_metadata_fetchlogs,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs sets loaded_datetime and increments jobs."""
        from ingest.utils import load_fetchlogs

        # Arrange - verify initial state
        db_cursor.execute("""
            SELECT loaded_datetime, jobs FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (sample_metadata_fetchlogs[0],))
        result = db_cursor.fetchone()
        assert result[0] is None, "loaded_datetime should initially be NULL"
        assert result[1] == 0, "jobs should initially be 0"

        # Act - load metadata fetchlogs
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/',
            limit=1,
            ascending=True
        )

        # Assert - verify at least one record was loaded
        assert len(rows) >= 1, "Should load at least one fetchlog"

        # Verify loaded_datetime and jobs updated
        db_cursor.execute("""
            SELECT loaded_datetime, jobs FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (sample_metadata_fetchlogs[0],))
        result = db_cursor.fetchone()
        assert result[0] is not None, "loaded_datetime should be set"
        assert result[1] > 0, "jobs counter should be incremented"

    def test_load_fetchlogs_skips_recently_loaded(
        self,
        db_cursor,
        db_connection,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs skips recently loaded files."""
        from ingest.utils import load_fetchlogs

        # Arrange - create fetchlog with recent loaded_datetime
        test_time = datetime.now(timezone.utc)
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, loaded_datetime, jobs)
            VALUES (%s, %s, %s, %s, 1)
            RETURNING fetchlogs_id
        """, (
            'lcs-etl-pipeline/stations/recent.json',
            test_time,
            test_time,
            test_time - timedelta(minutes=5)  # Loaded 5 minutes ago
        ))
        fetchlog_id = db_cursor.fetchone()[0]
        db_connection.commit()

        # Act - try to load again
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/recent',
            limit=10,
            ascending=True
        )

        # Assert - file should not be in results (skipped due to recent load)
        loaded_ids = [row[0] for row in rows]
        assert fetchlog_id not in loaded_ids, "Recently loaded file should be skipped"

    def test_load_fetchlogs_skips_error_files(
        self,
        db_cursor,
        db_connection,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs skips files with errors."""
        from ingest.utils import load_fetchlogs

        # Arrange - create fetchlog with error flag
        test_time = datetime.now(timezone.utc)
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, has_error)
            VALUES (%s, %s, %s, true)
            RETURNING fetchlogs_id
        """, (
            'lcs-etl-pipeline/stations/error.json',
            test_time,
            test_time
        ))
        fetchlog_id = db_cursor.fetchone()[0]
        db_connection.commit()

        # Act - try to load
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/error',
            limit=10,
            ascending=True
        )

        # Assert - error file should not be in results
        loaded_ids = [row[0] for row in rows]
        assert fetchlog_id not in loaded_ids, "Error files should be skipped"

    def test_load_fetchlogs_skips_completed_files(
        self,
        db_cursor,
        db_connection,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs skips already completed files."""
        from ingest.utils import load_fetchlogs

        # Arrange - create completed fetchlog
        test_time = datetime.now(timezone.utc)
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, jobs)
            VALUES (%s, %s, %s, %s, 1)
            RETURNING fetchlogs_id
        """, (
            'lcs-etl-pipeline/stations/completed.json',
            test_time,
            test_time,
            test_time
        ))
        fetchlog_id = db_cursor.fetchone()[0]
        db_connection.commit()

        # Act - try to load
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/completed',
            limit=10,
            ascending=True
        )

        # Assert - completed file should not be in results
        loaded_ids = [row[0] for row in rows]
        assert fetchlog_id not in loaded_ids, "Completed files should be skipped"

    def test_load_fetchlogs_ascending_order(
        self,
        db_cursor,
        db_connection,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs respects ascending order."""
        from ingest.utils import load_fetchlogs

        # Arrange - create fetchlogs with different timestamps
        old_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        new_time = datetime(2024, 1, 10, tzinfo=timezone.utc)

        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES
                ('lcs-etl-pipeline/stations/old.json', %s, %s),
                ('lcs-etl-pipeline/stations/new.json', %s, %s)
        """, (old_time, old_time, new_time, new_time))
        db_connection.commit()

        # Act - load in ascending order
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/',
            limit=10,
            ascending=True
        )

        # Assert - oldest file should be first
        assert len(rows) >= 2, "Should load both files"
        keys = [row[1] for row in rows]
        assert keys[0] == 'lcs-etl-pipeline/stations/old.json', \
            "Oldest file should be first in ascending order"

    def test_load_fetchlogs_descending_order(
        self,
        db_cursor,
        db_connection,
        clean_fetchlogs
    ):
        """Test that load_fetchlogs respects descending order."""
        from ingest.utils import load_fetchlogs

        # Arrange - create fetchlogs with different timestamps
        old_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        new_time = datetime(2024, 1, 10, tzinfo=timezone.utc)

        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES
                ('lcs-etl-pipeline/stations/old.json', %s, %s),
                ('lcs-etl-pipeline/stations/new.json', %s, %s)
        """, (old_time, old_time, new_time, new_time))
        db_connection.commit()

        # Act - load in descending order
        rows = load_fetchlogs(
            pattern='^lcs-etl-pipeline/stations/',
            limit=10,
            ascending=False
        )

        # Assert - newest file should be first
        assert len(rows) >= 2, "Should load both files"
        keys = [row[1] for row in rows]
        assert keys[0] == 'lcs-etl-pipeline/stations/new.json', \
            "Newest file should be first in descending order"
