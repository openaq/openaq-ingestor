"""
Tests for database query functions in utils.py

These tests verify that query functions correctly retrieve and filter
data from the database.
"""

import pytest
from datetime import datetime, timezone, timedelta
from ingest.utils import get_logs_from_pattern


class TestGetLogsFromPattern:
    """Tests for get_logs_from_pattern() function."""

    @pytest.fixture
    def sample_fetchlogs(self, db_connection, clean_fetchlogs):
        """Create sample fetchlog records with various patterns."""
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Create fetchlogs with different key patterns
        test_keys = [
            'lcs-etl-pipeline/measures/station-001.csv',
            'lcs-etl-pipeline/measures/station-002.csv',
            'lcs-etl-pipeline/stations/metadata-001.json',
            'lcs-etl-pipeline/stations/metadata-002.json',
            'realtime-gzipped/provider-a/data.json.gz',
            'realtime-gzipped/provider-b/data.json.gz',
            'other-bucket/random/file.txt',
        ]

        fetchlog_ids = []
        with db_connection.cursor() as cursor:
            for i, key in enumerate(test_keys):
                modified = test_time + timedelta(hours=i)
                cursor.execute("""
                    INSERT INTO fetchlogs (key, last_modified, init_datetime)
                    VALUES (%s, %s, %s)
                    RETURNING fetchlogs_id
                    """, (key, modified, test_time))
                fetchlog_ids.append(cursor.fetchone()[0])

        return fetchlog_ids, test_keys

    def test_get_logs_from_pattern_matches_csv_files(self, db_connection, sample_fetchlogs):
        """Test pattern matching for CSV files."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match all CSV files
        result = get_logs_from_pattern(r'\.csv$', connection=db_connection)

        # Should match the 2 CSV files
        assert len(result) == 2
        keys = [row[1] for row in result]
        assert 'lcs-etl-pipeline/measures/station-001.csv' in keys
        assert 'lcs-etl-pipeline/measures/station-002.csv' in keys

    def test_get_logs_from_pattern_matches_json_files(self, db_connection, sample_fetchlogs):
        """Test pattern matching for JSON files."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match all JSON files (including .gz)
        result = get_logs_from_pattern(r'\.json', connection=db_connection)

        # Should match 4 JSON files
        assert len(result) == 4
        keys = [row[1] for row in result]
        assert any('metadata-001.json' in k for k in keys)
        assert any('metadata-002.json' in k for k in keys)
        assert any('provider-a/data.json.gz' in k for k in keys)
        assert any('provider-b/data.json.gz' in k for k in keys)

    def test_get_logs_from_pattern_matches_prefix(self, db_connection, sample_fetchlogs):
        """Test pattern matching with prefix."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match files starting with lcs-etl-pipeline/measures/
        result = get_logs_from_pattern(r'^lcs-etl-pipeline/measures/', connection=db_connection)

        # Should match 2 files
        assert len(result) == 2
        keys = [row[1] for row in result]
        assert all('lcs-etl-pipeline/measures/' in k for k in keys)

    def test_get_logs_from_pattern_matches_stations(self, db_connection, sample_fetchlogs):
        """Test pattern matching for station metadata files."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match station metadata files
        result = get_logs_from_pattern(r'^lcs-etl-pipeline/stations/.*\.json$', connection=db_connection)

        # Should match 2 station files
        assert len(result) == 2
        keys = [row[1] for row in result]
        assert 'lcs-etl-pipeline/stations/metadata-001.json' in keys
        assert 'lcs-etl-pipeline/stations/metadata-002.json' in keys

    def test_get_logs_from_pattern_case_insensitive(self, db_connection, sample_fetchlogs):
        """Test that pattern matching is case-insensitive."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Use uppercase pattern - should still match
        result = get_logs_from_pattern(r'REALTIME-GZIPPED', connection=db_connection)

        # Should match 2 realtime files (case-insensitive)
        assert len(result) == 2
        keys = [row[1] for row in result]
        assert all('realtime-gzipped' in k.lower() for k in keys)

    def test_get_logs_from_pattern_respects_limit(self, db_connection, sample_fetchlogs):
        """Test that limit parameter is respected."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match all files but limit to 3
        result = get_logs_from_pattern(r'.*', limit=3, connection=db_connection)

        assert len(result) == 3

    def test_get_logs_from_pattern_returns_all_columns(self, db_connection, sample_fetchlogs):
        """Test that all expected columns are returned."""
        fetchlog_ids, test_keys = sample_fetchlogs

        result = get_logs_from_pattern(r'\.csv$', limit=1, connection=db_connection)

        print(result)
        assert len(result) == 1
        row = result[0]

        # Should have 7 columns
        assert len(row) == 7

        # Verify column order
        fetchlogs_id, key, init_datetime, loaded_datetime, completed_datetime, last_message, last_modified = row

        assert isinstance(fetchlogs_id, int)
        assert isinstance(key, str)
        assert isinstance(init_datetime, datetime)
        assert loaded_datetime is None  # Not loaded yet
        assert completed_datetime is None  # Not completed yet
        assert last_message is None  # No message yet
        assert isinstance(last_modified, datetime)

    def test_get_logs_from_pattern_empty_result(self, db_connection, sample_fetchlogs):
        """Test pattern that matches no files."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Pattern that won't match anything
        result = get_logs_from_pattern(r'^nonexistent-prefix/', connection=db_connection)

        assert len(result) == 0
        assert result == []

    def test_get_logs_from_pattern_with_gzipped_files(self, db_connection, sample_fetchlogs):
        """Test matching gzipped files specifically."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match files ending with .gz
        result = get_logs_from_pattern(r'\.gz$', connection=db_connection)

        # Should match 2 gzipped files
        assert len(result) == 2
        keys = [row[1] for row in result]
        assert all(k.endswith('.gz') for k in keys)

    def test_get_logs_from_pattern_complex_pattern(self, db_connection, sample_fetchlogs):
        """Test complex regex pattern."""
        fetchlog_ids, test_keys = sample_fetchlogs

        # Match files with 'station' OR 'metadata' in the name
        result = get_logs_from_pattern(r'(station|metadata)', connection=db_connection)

        # Should match 4 files (2 stations CSV + 2 metadata JSON)
        assert len(result) == 4
        keys = [row[1] for row in result]
        assert all('station' in k or 'metadata' in k for k in keys)

    def test_get_logs_from_pattern_with_completed_files(
        self,
        db_connection,
    ):
        """Test that completed files are still returned."""
        test_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Create completed fetchlog
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fetchlogs (
                    key,
                    last_modified,
                    init_datetime,
                    completed_datetime,
                    last_message
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                'completed-file.csv',
                test_time,
                test_time,
                test_time + timedelta(hours=1),
                'success'
            ))

        result = get_logs_from_pattern(r'completed-file', connection=db_connection)

        assert len(result) == 1
        row = result[0]
        assert row[1] == 'completed-file.csv'
        assert row[4] is not None  # completed_datetime
        assert row[5] == 'success'  # last_message
