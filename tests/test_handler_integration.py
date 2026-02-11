import pytest
from datetime import datetime, timezone
from ingest.handler import handler, getKeysFromSnsRecord
import json
from unittest.mock import patch


@pytest.mark.integration
class TestHandlerSNSEvents:
    """
    Integration tests for handler() function with SNS-wrapped S3 events.

    NOTE: In production, the Lambda is subscribed to an SNS topic that receives
    S3 events (see cdk/lambda_ingest_stack.py:104-116). These tests verify the
    production SNS-wrapped event path.
    """

    def test_handler_sns_event_single_file(
        self,
        ingest_context,
        mock_s3_with_object,
        sample_sns_event,
        lambda_context
    ):
        """Test handler processes SNS-wrapped S3 event and inserts fetchlog record."""
        # Arrange
        s3_client, bucket, key, file_size, last_modified = mock_s3_with_object

        # Act - pass ingest_context to handler
        handler(sample_sns_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify record inserted
        cursor = ingest_context.cursor()
        cursor.execute(
            "SELECT key, file_size, init_datetime, last_modified, completed_datetime FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = cursor.fetchone()

        assert result is not None, f"No fetchlog record found for key: {key}"
        assert result[0] == key
        assert result[1] == file_size
        assert result[2] is not None, f"No init_datetime created for file"
        assert result[3] is not None  # last_modified populated
        assert result[4] is None  # completed_datetime should be NULL
        cursor.close()

    def test_handler_sns_event_batch(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        sample_batch_sns_event,
        lambda_context
    ):
        """Test handler processes SNS event with multiple S3 records in batch."""
        # Arrange - create 5 objects in S3
        import json
        sns_message = json.loads(sample_batch_sns_event["Records"][0]["Sns"]["Message"])
        bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
        for i in range(5):
            key = f"lcs-etl-pipeline/test{i}.json"
            ingest_context.s3.put_object(Bucket=bucket, Key=key, Body=b'{}')

        # Act - pass ingest_context to handler
        handler(sample_batch_sns_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify 5 records inserted
        db_cursor.execute("SELECT COUNT(*) FROM fetchlogs")
        count = db_cursor.fetchone()[0]
        assert count == 5, f"Expected 5 fetchlog records, found {count}"

        # Verify all keys are unique
        db_cursor.execute("SELECT key FROM fetchlogs")
        keys = [row[0] for row in db_cursor.fetchall()]
        assert len(keys) == len(set(keys)), "Duplicate keys found"

    def test_handler_sns_event_multiple_sns_records(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        lambda_context
    ):
        """Test handler processes multiple SNS records (multiple S3 events)."""
        # Arrange - create event with 3 SNS records, each containing an S3 event
        bucket = "openaq-fetches"
        event = {
            "Records": []
        }
        for i in range(3):
            key = f"lcs-etl-pipeline/test{i}.json"
            ingest_context.s3.put_object(Bucket=bucket, Key=key, Body=b'{}')

            s3_message = {
                "Records": [{
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key}
                    }
                }]
            }
            event["Records"].append({
                "EventSource": "aws:sns",
                "Sns": {"Message": json.dumps(s3_message)}
            })

        # Act - pass ingest_context to handler
        handler(event, lambda_context, ingest_context=ingest_context)

        # Assert - verify 3 records inserted
        db_cursor.execute("SELECT COUNT(*) FROM fetchlogs")
        count = db_cursor.fetchone()[0]
        assert count == 3, f"Expected 3 fetchlog records, found {count}"

    def test_handler_duplicate_key_update(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        mock_s3_with_object,
        sample_sns_event,
        lambda_context
    ):
        """Test handler updates existing fetchlog when key already exists."""
        # Arrange - insert existing record
        key = "lcs-etl-pipeline/test.json"
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, completed_datetime)
            VALUES (%s, %s, NOW())
        """, (key, datetime(2024, 1, 1, tzinfo=timezone.utc)))

        # Act - process same key again, pass ingest_context to handler
        handler(sample_sns_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify record updated, not duplicated
        db_cursor.execute("SELECT COUNT(*) FROM fetchlogs WHERE key = %s", (key,))
        count = db_cursor.fetchone()[0]
        assert count == 1, "Expected single record, found duplicate"

        # Verify completed_datetime reset to NULL (for reprocessing)
        db_cursor.execute(
            "SELECT completed_datetime, last_modified FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = db_cursor.fetchone()
        assert result[0] is None, "completed_datetime should be NULL after update"
        assert result[1] > datetime(2024, 1, 1, tzinfo=timezone.utc), "last_modified should be updated"

    def test_handler_missing_s3_object(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        sample_sns_event,
        lambda_context
    ):
        """Test handler handles missing S3 object gracefully."""
        # Act - process event for non-existent object, pass ingest_context to handler
        handler(sample_sns_event, lambda_context, ingest_context=ingest_context)

        # Assert - record still inserted but file_size is NULL
        key = "lcs-etl-pipeline/test.json"
        db_cursor.execute(
            "SELECT key, file_size FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = db_cursor.fetchone()
        assert result is not None, "Record should be inserted even if S3 object missing"
        assert result[1] is None, "file_size should be NULL for missing object"


@pytest.mark.integration
class TestHandlerDirectS3Events:
    """
    Integration tests for handler() function with direct S3 events.

    NOTE: These tests verify the handler can process direct S3 events
    (not wrapped by SNS), useful for local testing and manual invocations.
    """

    def test_handler_direct_s3_event_single_file(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        mock_s3_with_object,
        sample_s3_event,
        lambda_context
    ):
        """Test handler processes direct S3 event and inserts fetchlog record."""
        # Arrange
        s3_client, bucket, key, file_size, last_modified = mock_s3_with_object

        # Act - pass ingest_context to handler
        handler(sample_s3_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify record inserted
        db_cursor.execute(
            "SELECT key, file_size, last_modified, completed_datetime FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = db_cursor.fetchone()

        assert result is not None, f"No fetchlog record found for key: {key}"
        assert result[0] == key
        assert result[1] == file_size
        assert result[2] is not None  # last_modified populated
        assert result[3] is None  # completed_datetime should be NULL

    def test_handler_direct_s3_event_batch(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        sample_batch_s3_event,
        lambda_context
    ):
        """Test handler processes direct S3 event with multiple records in batch."""
        # Arrange - create 5 objects in S3
        bucket = sample_batch_s3_event["Records"][0]["s3"]["bucket"]["name"]
        for i in range(5):
            key = f"lcs-etl-pipeline/test{i}.json"
            ingest_context.s3.put_object(Bucket=bucket, Key=key, Body=b'{}')

        # Act - pass ingest_context to handler
        handler(sample_batch_s3_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify 5 records inserted
        db_cursor.execute("SELECT COUNT(*) FROM fetchlogs")
        count = db_cursor.fetchone()[0]
        assert count == 5, f"Expected 5 fetchlog records, found {count}"

        # Verify all keys are unique
        db_cursor.execute("SELECT key FROM fetchlogs")
        keys = [row[0] for row in db_cursor.fetchall()]
        assert len(keys) == len(set(keys)), "Duplicate keys found"

    def test_handler_direct_s3_duplicate_key(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        mock_s3_with_object,
        sample_s3_event,
        lambda_context
    ):
        """Test handler updates existing fetchlog when key already exists (direct S3)."""
        # Arrange - insert existing record
        key = "lcs-etl-pipeline/test.json"
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, completed_datetime)
            VALUES (%s, %s, NOW())
        """, (key, datetime(2024, 1, 1, tzinfo=timezone.utc)))

        # Act - process same key again via direct S3 event, pass ingest_context to handler
        handler(sample_s3_event, lambda_context, ingest_context=ingest_context)

        # Assert - verify record updated, not duplicated
        db_cursor.execute("SELECT COUNT(*) FROM fetchlogs WHERE key = %s", (key,))
        count = db_cursor.fetchone()[0]
        assert count == 1, "Expected single record, found duplicate"

        # Verify completed_datetime reset to NULL (for reprocessing)
        db_cursor.execute(
            "SELECT completed_datetime, last_modified FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = db_cursor.fetchone()
        assert result[0] is None, "completed_datetime should be NULL after update"
        assert result[1] > datetime(2024, 1, 1, tzinfo=timezone.utc), "last_modified should be updated"

    def test_handler_direct_s3_missing_object(
        self,
        ingest_context,
        db_cursor,
        clean_fetchlogs,
        sample_s3_event,
        lambda_context
    ):
        """Test handler handles missing S3 object gracefully (direct S3)."""
        # Act - process direct S3 event for non-existent object, pass ingest_context to handler
        handler(sample_s3_event, lambda_context, ingest_context=ingest_context)

        # Assert - record still inserted but file_size is NULL
        key = "lcs-etl-pipeline/test.json"
        db_cursor.execute(
            "SELECT key, file_size FROM fetchlogs WHERE key = %s",
            (key,)
        )
        result = db_cursor.fetchone()
        assert result is not None, "Record should be inserted even if S3 object missing"
        assert result[1] is None, "file_size should be NULL for missing object"


@pytest.mark.integration
class TestCronhandlerIntegration:
    """
    Integration tests for cronhandler() function.

    NOTE: These tests verify the EventBridge/CloudWatch Events triggered
    cron processing path that orchestrates metadata, realtime, and pipeline loaders.
    """

    def test_handler_eventbridge_trigger(
        self,
        db_cursor,
        clean_fetchlogs,
        lambda_context
    ):
        """Test handler routes EventBridge event to cronhandler."""
        # Arrange
        eventbridge_event = {
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            "resources": ["arn:aws:events:us-east-1:123456789012:rule/test-rule"]
        }

        # Mock the loaders to avoid actually processing data
        with patch('ingest.handler.load_metadata_db', return_value=0) as mock_metadata, \
             patch('ingest.handler.load_db', return_value=0) as mock_realtime, \
             patch('ingest.handler.load_measurements_db', return_value=0) as mock_pipeline:

            # Act
            handler(eventbridge_event, lambda_context)

            # Assert - verify cronhandler was invoked (loaders were called)
            mock_metadata.assert_called_once()
            mock_realtime.assert_called_once()
            mock_pipeline.assert_called_once()

    def test_cronhandler_with_fetchlog_pattern(
        self,
        lambda_context
    ):
        """Test cronhandler processes specific fetchlogKey pattern."""
        # Arrange
        event = {
            "source": "aws.events",
            "fetchlogKey": "lcs-etl-pipeline/test%.json",
            "limit": 5
        }

        # Mock the pattern loader
        with patch('ingest.handler.load_measurements_pattern') as mock_pattern:
            mock_pattern.return_value = {"processed": 3}

            # Act
            result = handler(event, lambda_context)

            # Assert
            # Note: handler() calls cronhandler() which returns the result
            # handler() itself doesn't return anything, so we just verify the call
            mock_pattern.assert_called_once_with(limit=5, pattern="lcs-etl-pipeline/test%.json")

    def test_cronhandler_respects_pause_setting(
        self,
        lambda_context
    ):
        """Test cronhandler respects PAUSE_INGESTING setting."""
        # Arrange
        event = {
            "source": "aws.events",
            "detail-type": "Scheduled Event"
        }

        # Mock settings to pause ingesting
        with patch('ingest.handler.settings') as mock_settings, \
             patch('ingest.handler.load_metadata_db') as mock_metadata:
            mock_settings.PAUSE_INGESTING = True

            # Act
            handler(event, lambda_context)

            # Assert - loader should not be called when paused
            mock_metadata.assert_not_called()


@pytest.mark.integration
class TestGetKeysFromSnsRecord:
    """Tests for SNS message parsing helper function."""

    def test_getKeysFromSnsRecord_valid_message(self, sample_sns_event):
        """Test extracting S3 records from SNS message."""
        # Act
        records = getKeysFromSnsRecord(sample_sns_event["Records"][0])

        # Assert
        assert len(records) == 1
        assert records[0]["key"] == "lcs-etl-pipeline/test.json"

    def test_getKeysFromSnsRecord_multiple_s3_records(self):
        """Test SNS message containing multiple S3 records."""
        # Arrange
        s3_message = {
            "Records": [
                {"s3": {"bucket": {"name": "test-bucket"}, "object": {"key": f"test{i}.json"}}}
                for i in range(3)
            ]
        }
        sns_record = {
            "EventSource": "aws:sns",
            "Sns": {"Message": json.dumps(s3_message)}
        }

        # Act
        records = getKeysFromSnsRecord(sns_record)

        # Assert
        assert len(records) == 3
        assert records[0]["key"] == "test0.json"
        assert records[1]["key"] == "test1.json"
        assert records[2]["key"] == "test2.json"
