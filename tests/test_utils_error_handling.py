"""
Unit tests for error handling functions in utils.py

These tests verify that error tracking, success tracking, and duplicate
detection work correctly for fetchlog management.
"""

import pytest
from datetime import datetime, timezone
from ingest.utils import load_fail, load_success


class TestLoadFail:
    """Tests for load_fail() function that marks fetchlogs as failed."""

    def test_load_fail_marks_fetchlog_with_error(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_fail sets has_error=true."""
        # Arrange - create fetchlog
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-error.json', NOW(), NOW())
            RETURNING fetchlogs_id
        """)
        fetchlog_id = db_cursor.fetchone()[0]

        # Verify initial state
        db_cursor.execute("""
            SELECT has_error FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (fetchlog_id,))
        assert db_cursor.fetchone()[0] is False, "Initial has_error should be false"

        # Act
        load_fail(db_cursor, fetchlog_id, Exception("Test error message"))

        # Assert
        db_cursor.execute("""
            SELECT has_error, last_message
            FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (fetchlog_id,))
        has_error, last_message = db_cursor.fetchone()

        assert has_error is True, "has_error should be set to true"
        assert last_message is not None, "last_message should be set"

    def test_load_fail_sets_error_message(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_fail stores the exception message."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-message.json', NOW(), NOW())
            RETURNING fetchlogs_id
        """)
        fetchlog_id = db_cursor.fetchone()[0]

        error_message = "Database connection timeout"

        # Act
        load_fail(db_cursor, fetchlog_id, Exception(error_message))

        # Assert
        db_cursor.execute("""
            SELECT last_message FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (fetchlog_id,))
        last_message = db_cursor.fetchone()[0]

        assert error_message in last_message, \
            "Error message should be stored in last_message"

    def test_load_fail_updates_completed_datetime(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_fail sets completed_datetime."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-completed.json', NOW(), NOW())
            RETURNING fetchlogs_id
        """)
        fetchlog_id = db_cursor.fetchone()[0]

        # Verify initial state
        db_cursor.execute("""
            SELECT completed_datetime FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (fetchlog_id,))
        assert db_cursor.fetchone()[0] is None, \
            "Initial completed_datetime should be NULL"

        # Act
        load_fail(db_cursor, fetchlog_id, Exception("Test error"))

        # Assert
        db_cursor.execute("""
            SELECT completed_datetime FROM fetchlogs
            WHERE fetchlogs_id = %s
        """, (fetchlog_id,))
        completed_datetime = db_cursor.fetchone()[0]

        assert completed_datetime is not None, \
            "completed_datetime should be set even on failure"
        assert completed_datetime.tzinfo is not None, \
            "completed_datetime should have timezone info"

    def test_load_fail_handles_various_exception_types(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_fail handles different exception types."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-exception-types.json', NOW(), NOW())
            RETURNING fetchlogs_id
        """)
        fetchlog_id = db_cursor.fetchone()[0]

        # Act - test with different exception types
        exceptions = [
            ValueError("Invalid value"),
            KeyError("Missing key"),
            RuntimeError("Runtime error"),
        ]

        for exc in exceptions:
            load_fail(db_cursor, fetchlog_id, exc)

            # Assert - should store the message
            db_cursor.execute("""
                SELECT last_message FROM fetchlogs
                WHERE fetchlogs_id = %s
            """, (fetchlog_id,))
            last_message = db_cursor.fetchone()[0]
            assert str(exc) in last_message

    def test_load_fail_with_nonexistent_fetchlog_id(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_fail doesn't crash with invalid ID."""
        # Act - call with non-existent ID (should not raise exception)
        load_fail(db_cursor, 999999, Exception("Test error"))

        # Assert - verify no rows were affected
        db_cursor.execute("""
            SELECT COUNT(*) FROM fetchlogs
            WHERE fetchlogs_id = 999999
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, "No rows should be affected"


class TestLoadSuccess:
    """Tests for load_success() function that marks fetchlogs as successful."""

    def test_load_success_clears_error_flag(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success sets has_error=false."""
        # Arrange - create fetchlog with error
        db_cursor.execute("""
            INSERT INTO fetchlogs (
                key, last_modified, init_datetime,
                has_error, last_message
            )
            VALUES ('test-success.json', NOW(), NOW(), true, 'Previous error')
            RETURNING key
        """)
        key = db_cursor.fetchone()[0]

        # Verify initial state
        db_cursor.execute("""
            SELECT has_error FROM fetchlogs WHERE key = %s
        """, (key,))
        assert db_cursor.fetchone()[0] is True, "Initial has_error should be true"

        # Act
        load_success(db_cursor, [key], message='Success')

        # Assert
        db_cursor.execute("""
            SELECT has_error FROM fetchlogs WHERE key = %s
        """, (key,))
        has_error = db_cursor.fetchone()[0]

        assert has_error is False, "has_error should be set to false"

    def test_load_success_sets_completed_datetime(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success sets completed_datetime."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-completed-success.json', NOW(), NOW())
            RETURNING key
        """)
        key = db_cursor.fetchone()[0]

        # Verify initial state
        db_cursor.execute("""
            SELECT completed_datetime FROM fetchlogs WHERE key = %s
        """, (key,))
        assert db_cursor.fetchone()[0] is None, \
            "Initial completed_datetime should be NULL"

        # Act
        load_success(db_cursor, [key])

        # Assert
        db_cursor.execute("""
            SELECT completed_datetime FROM fetchlogs WHERE key = %s
        """, (key,))
        completed_datetime = db_cursor.fetchone()[0]

        assert completed_datetime is not None, \
            "completed_datetime should be set"
        assert completed_datetime.tzinfo is not None, \
            "completed_datetime should have timezone info"

    def test_load_success_sets_custom_message(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success stores custom success message."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-message-success.json', NOW(), NOW())
            RETURNING key
        """)
        key = db_cursor.fetchone()[0]

        custom_message = "Processed 100 measurements successfully"

        # Act
        load_success(db_cursor, [key], message=custom_message)

        # Assert
        db_cursor.execute("""
            SELECT last_message FROM fetchlogs WHERE key = %s
        """, (key,))
        last_message = db_cursor.fetchone()[0]

        assert last_message == custom_message, \
            "Custom message should be stored"

    def test_load_success_handles_multiple_keys(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success can update multiple fetchlogs at once."""
        # Arrange - create multiple fetchlogs
        keys = []
        for i in range(5):
            db_cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime)
                VALUES (%s, NOW(), NOW())
                RETURNING key
            """, (f'test-batch-{i}.json',))
            keys.append(db_cursor.fetchone()[0])

        # Act
        load_success(db_cursor, keys, message='Batch processed')

        # Assert - all should be marked successful
        db_cursor.execute("""
            SELECT COUNT(*)
            FROM fetchlogs
            WHERE key = ANY(%s)
            AND completed_datetime IS NOT NULL
            AND has_error = false
            AND last_message = 'Batch processed'
        """, (keys,))
        count = db_cursor.fetchone()[0]

        assert count == 5, "All 5 fetchlogs should be marked successful"

    def test_load_success_default_message(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success uses 'success' as default message."""
        # Arrange
        db_cursor.execute("""
            INSERT INTO fetchlogs (key, last_modified, init_datetime)
            VALUES ('test-default-message.json', NOW(), NOW())
            RETURNING key
        """)
        key = db_cursor.fetchone()[0]

        # Act - call without message parameter
        load_success(db_cursor, [key])

        # Assert
        db_cursor.execute("""
            SELECT last_message FROM fetchlogs WHERE key = %s
        """, (key,))
        last_message = db_cursor.fetchone()[0]

        assert last_message == 'success', \
            "Default message should be 'success'"

    def test_load_success_with_empty_keys_list(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success handles empty keys list gracefully."""
        # Act - call with empty list (should not crash)
        load_success(db_cursor, [])

        # Assert - verify no rows were affected
        db_cursor.execute("""
            SELECT COUNT(*) FROM fetchlogs
            WHERE last_message = 'success'
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, "No rows should be affected with empty keys list"

    def test_load_success_with_nonexistent_keys(
        self,
        db_cursor,
        clean_fetchlogs
    ):
        """Test that load_success doesn't crash with non-existent keys."""
        # Act - call with non-existent keys (should not crash)
        load_success(
            db_cursor,
            ['nonexistent1.json', 'nonexistent2.json'],
            message='test'
        )

        # Assert - verify no rows were affected
        db_cursor.execute("""
            SELECT COUNT(*) FROM fetchlogs
            WHERE key IN ('nonexistent1.json', 'nonexistent2.json')
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, "No rows should be created for non-existent keys"
