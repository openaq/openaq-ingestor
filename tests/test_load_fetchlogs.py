import pytest
from datetime import datetime, timezone, timedelta
from ingest.utils import load_fetchlogs
import psycopg2


@pytest.mark.integration
class TestLoadFetchlogs:
    """
    Integration tests for load_fetchlogs() function.

    This function selects fetchlog records for processing based on:
    - Pattern matching (regex)
    - Initialization status (init_datetime is not null)
    - Completion status (completed_datetime is null)
    - Error status (has_error is false)
    - Load timing (not loaded in last 30 minutes)
    - Order (ascending/descending by last_modified)
    - Limit (max records to return)
    """

    def test_load_fetchlogs_basic_pattern_match(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs returns records matching pattern."""
        # Arrange - insert test records
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/measures/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/measures/test2.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/station/test3.json', %s, %s, NULL, false),
                    ('realtime-gzipped/test4.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time, test_time, test_time, test_time))

        # Act - search for lcs-etl-pipeline/measures pattern
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline/measures', limit=10, connection=db_connection)

        # Assert
        assert len(rows) == 2, f"Expected 2 records, got {len(rows)}"
        keys = [row[1] for row in rows]
        assert 'lcs-etl-pipeline/measures/test1.json' in keys
        assert 'lcs-etl-pipeline/measures/test2.json' in keys

    def test_load_fetchlogs_respects_limit(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs respects limit parameter."""
        # Arrange - insert 10 records
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            for i in range(10):
                cursor.execute("""
                    INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                    VALUES (%s, %s, %s, NULL, false)
                """, (f'lcs-etl-pipeline/test{i}.json', test_time, test_time))

        # Act - request only 3 records
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=3, connection=db_connection)

        # Assert
        assert len(rows) == 3, f"Expected 3 records with limit=3, got {len(rows)}"

    def test_load_fetchlogs_ascending_order(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs returns records in ascending order by last_modified."""
        # Arrange - insert records with different timestamps
        times = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
        ]
        with db_connection.cursor() as cursor:
            for i, time in enumerate(times):
                cursor.execute("""
                    INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                    VALUES (%s, %s, %s, NULL, false)
                """, (f'lcs-etl-pipeline/test{i}.json', time, time))

        # Act - request in ascending order
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection, ascending=True)

        # Assert - should be ordered oldest to newest
        assert len(rows) == 3
        returned_times = [row[2] for row in rows]
        assert returned_times[0] == times[0]
        assert returned_times[1] == times[1]
        assert returned_times[2] == times[2]

    def test_load_fetchlogs_descending_order(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs returns records in descending order by last_modified."""
        # Arrange - insert records with different timestamps
        times = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
        ]
        with db_connection.cursor() as cursor:
            for i, time in enumerate(times):
                cursor.execute("""
                    INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                    VALUES (%s, %s, %s, NULL, false)
                """, (f'lcs-etl-pipeline/test{i}.json', time, time))

        # Act - request in descending order (default)
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection, ascending=False)

        # Assert - should be ordered newest to oldest
        assert len(rows) == 3
        returned_times = [row[2] for row in rows]
        assert returned_times[0] == times[2]
        assert returned_times[1] == times[1]
        assert returned_times[2] == times[0]

    def test_load_fetchlogs_skips_completed(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs skips records with completed_datetime set."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/test2.json', %s, %s, NOW(), false),
                    ('lcs-etl-pipeline/test3.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time, test_time))

        # Act
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Assert - should only return test1 and test3 (not test2 with completed_datetime)
        assert len(rows) == 2
        keys = [row[1] for row in rows]
        assert 'lcs-etl-pipeline/test1.json' in keys
        assert 'lcs-etl-pipeline/test3.json' in keys
        assert 'lcs-etl-pipeline/test2.json' not in keys

    def test_load_fetchlogs_skips_errors(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs skips records with has_error=true."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/test2.json', %s, %s, NULL, true),
                    ('lcs-etl-pipeline/test3.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time, test_time))

        # Act
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Assert - should only return test1 and test3 (not test2 with has_error=true)
        assert len(rows) == 2
        keys = [row[1] for row in rows]
        assert 'lcs-etl-pipeline/test1.json' in keys
        assert 'lcs-etl-pipeline/test3.json' in keys
        assert 'lcs-etl-pipeline/test2.json' not in keys

    def test_load_fetchlogs_skips_null_init_datetime(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs skips records with NULL init_datetime."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/test2.json', %s, NULL, NULL, false),
                    ('lcs-etl-pipeline/test3.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time))

        # Act
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Assert - should only return test1 and test3 (not test2 with NULL init_datetime)
        assert len(rows) == 2
        keys = [row[1] for row in rows]
        assert 'lcs-etl-pipeline/test1.json' in keys
        assert 'lcs-etl-pipeline/test3.json' in keys
        assert 'lcs-etl-pipeline/test2.json' not in keys

    def test_load_fetchlogs_skips_recently_loaded(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs skips records loaded within last 30 minutes."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        # Record loaded 10 minutes ago (should be skipped)
        recent_load = datetime.now(timezone.utc) - timedelta(minutes=10)
        # Record loaded 45 minutes ago (should be included)
        old_load = datetime.now(timezone.utc) - timedelta(minutes=45)

        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error, loaded_datetime)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false, NULL),
                    ('lcs-etl-pipeline/test2.json', %s, %s, NULL, false, %s),
                    ('lcs-etl-pipeline/test3.json', %s, %s, NULL, false, %s)
            """, (test_time, test_time, test_time, test_time, recent_load, test_time, test_time, old_load))

        # Act
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Assert - should return test1 (never loaded) and test3 (loaded >30min ago)
        # but not test2 (loaded <30min ago)
        keys = [row[1] for row in rows]
        assert 'lcs-etl-pipeline/test1.json' in keys
        assert 'lcs-etl-pipeline/test3.json' in keys
        assert 'lcs-etl-pipeline/test2.json' not in keys

    def test_load_fetchlogs_updates_loaded_datetime(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs updates loaded_datetime when selecting records."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error, loaded_datetime)
                VALUES ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false, NULL)
            """, (test_time, test_time))

            # Get initial state
            cursor.execute("SELECT loaded_datetime FROM fetchlogs WHERE key = %s",
                             ('lcs-etl-pipeline/test1.json',))
            initial_loaded = cursor.fetchone()[0]
            assert initial_loaded is None

            # Act
            rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

            # Assert - loaded_datetime should now be set
            cursor.execute("SELECT loaded_datetime FROM fetchlogs WHERE key = %s",
                             ('lcs-etl-pipeline/test1.json',))
            updated_loaded = cursor.fetchone()[0]
            assert updated_loaded is not None
            assert updated_loaded > test_time

    def test_load_fetchlogs_increments_jobs_counter(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs increments jobs counter."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error, jobs)
                VALUES ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false, 0)
            """, (test_time, test_time))

            # Act - load the record twice
            load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

            # Wait a bit and load again (simulate retry after 30min)
            cursor.execute("""
                UPDATE fetchlogs
                SET loaded_datetime = NOW() - INTERVAL '45 minutes'
                WHERE key = 'lcs-etl-pipeline/test1.json'
            """)

            load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

            # Assert - jobs should be incremented to 2
            cursor.execute("SELECT jobs FROM fetchlogs WHERE key = %s",
                             ('lcs-etl-pipeline/test1.json',))
            jobs_count = cursor.fetchone()[0]
            assert jobs_count == 2

    def test_load_fetchlogs_sets_batch_uuid(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs sets batch_uuid for tracking."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/test2.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time))

            # Act
            load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

            # Assert - both records should have same non-null batch_uuid
            cursor.execute("""
                SELECT DISTINCT batch_uuid
                FROM fetchlogs
                WHERE key LIKE 'lcs-etl-pipeline%'
            """)
            uuids = cursor.fetchall()
            assert len(uuids) == 1
            assert uuids[0][0] is not None

    def test_load_fetchlogs_empty_result(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs returns empty list when no matching records."""
        # Arrange - insert records with different pattern
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES ('realtime-gzipped/test1.json', %s, %s, NULL, false)
            """, (test_time, test_time))

            # Act - search for non-existent pattern
            rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

            # Assert
            assert len(rows) == 0

    def test_load_fetchlogs_null_last_modified(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs handles records with NULL last_modified (nulls last)."""
        # Arrange - insert records with and without last_modified
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/test2.json', NULL, %s, NULL, false),
                    ('lcs-etl-pipeline/test3.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time))

        # Act - get in descending order
        rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection, ascending=False)

        # Assert - all 3 should be returned, nulls should be last
        assert len(rows) == 3
        # Last row should have NULL last_modified
        assert rows[-1][2] is None
        assert rows[-1][1] == 'lcs-etl-pipeline/test2.json'

    def test_load_fetchlogs_concurrent_safety(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs uses FOR UPDATE SKIP LOCKED for concurrent safety."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES ('lcs-etl-pipeline/test1.json', %s, %s, NULL, false)
            """, (test_time, test_time))

        # Act - First load should get the record
        rows1 = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Second load immediately after should NOT get it (loaded_datetime < 30min)
        rows2 = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)

        # Assert
        assert len(rows1) == 1
        assert len(rows2) == 0  # Should be empty due to 30min rule

    def test_load_fetchlogs_pattern_variations(
        self,
        db_connection,
        clean_fetchlogs
    ):
        """Test load_fetchlogs with various regex patterns."""
        # Arrange
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (key, last_modified, init_datetime, completed_datetime, has_error)
                VALUES
                    ('lcs-etl-pipeline/measures/2024/test1.json', %s, %s, NULL, false),
                    ('lcs-etl-pipeline/station/2024/test2.json', %s, %s, NULL, false),
                    ('realtime-gzipped/2024-01-01.json', %s, %s, NULL, false)
            """, (test_time, test_time, test_time, test_time, test_time, test_time))

            # Act & Assert - Test different patterns

            # Pattern 1: All lcs-etl-pipeline
            rows = load_fetchlogs(pattern='^lcs-etl-pipeline', limit=10, connection=db_connection)
            assert len(rows) == 2

            # Reset loaded_datetime to allow re-selection
            cursor.execute("""
                UPDATE fetchlogs
                SET loaded_datetime = NOW() - INTERVAL '45 minutes'
                WHERE key LIKE 'lcs-etl-pipeline%'
            """)

            # Pattern 2: Only measures
            rows = load_fetchlogs(pattern='^lcs-etl-pipeline/measures', limit=10, connection=db_connection)
            assert len(rows) == 1
            assert 'measures' in rows[0][1]

            # Reset loaded_datetime to allow re-selection
            cursor.execute("""
                UPDATE fetchlogs
                SET loaded_datetime = NOW() - INTERVAL '45 minutes'
                WHERE key LIKE 'realtime%'
            """)

            # Pattern 3: Only realtime
            rows = load_fetchlogs(pattern='^realtime-gzipped', limit=10, connection=db_connection)
            assert len(rows) == 1
            assert 'realtime' in rows[0][1]
