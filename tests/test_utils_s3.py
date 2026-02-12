"""
Tests for S3 operations in utils.py

These tests verify S3 file operations including reading, writing,
and path parsing with both S3 and local files.
"""

import pytest
import os
import gzip
import io
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
from ingest.utils import (
    get_object,
    put_object,
    deconstruct_path,
    get_data,
    get_file,
)
from ingest.settings import settings


@pytest.fixture
def s3_setup():
    """Set up mocked S3 bucket with test data."""
    with mock_aws():
        # Create S3 client and bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        bucket_name = settings.FETCH_BUCKET
        s3_client.create_bucket(Bucket=bucket_name)

        # Upload test files
        test_content = '{"test": "data", "value": 123}'
        s3_client.put_object(
            Bucket=bucket_name,
            Key='test-data.json',
            Body=test_content.encode()
        )

        # Upload gzipped file
        gzipped_content = gzip.compress(test_content.encode())
        s3_client.put_object(
            Bucket=bucket_name,
            Key='test-data.json.gz',
            Body=gzipped_content
        )

        yield s3_client, bucket_name


# @pytest.fixture
# def mock_resources(s3_setup):
#     """Create a mock Resources object with mocked S3 client."""
#     s3_client, bucket_name = s3_setup

#     # Create a mock Resources object
#     mock_rs = MagicMock()
#     mock_rs.s3 = s3_client

#     return mock_rs


@pytest.fixture
def temp_local_file(tmp_path):
    """Create temporary local file for testing."""
    # Regular file
    regular_file = tmp_path / "test-local.txt"
    regular_file.write_text("local file content\nline 2")

    # Gzipped file
    gzipped_file = tmp_path / "test-local.txt.gz"
    with gzip.open(gzipped_file, 'wt') as f:
        f.write("gzipped local content\nline 2")

    return tmp_path


class TestGetObject:
    """Tests for get_object() S3 retrieval function."""

    def test_get_object_retrieves_plain_json(self, s3_setup, ingest_resources):
        """Test retrieving plain (non-gzipped) JSON from S3."""
        s3_client, bucket_name = s3_setup

        result = get_object('test-data.json', bucket=bucket_name, resources=ingest_resources)

        # For non-gzipped files, result is StreamingBody object
        # Read it to get the content
        if hasattr(result, 'read'):
            content = result.read().decode('utf-8')
        else:
            content = result

        assert 'test' in content
        assert 'data' in content

    def test_get_object_retrieves_gzipped_json(self, s3_setup, ingest_resources):
        """Test retrieving and decompressing gzipped JSON from S3."""
        s3_client, bucket_name = s3_setup

        result = get_object('test-data.json.gz', bucket=bucket_name, resources=ingest_resources)

        # For gzipped files, function decompresses and returns string
        assert isinstance(result, str)
        assert '{"test": "data"' in result
        assert 'value' in result

    def test_get_object_handles_url_encoded_key(self, s3_setup, ingest_resources):
        """Test that URL-encoded keys are decoded properly."""
        s3_client, bucket_name = s3_setup

        # Create file with space in name
        s3_client.put_object(
            Bucket=bucket_name,
            Key='test file.json',
            Body=b'{"space": "in name"}'
        )

        # Pass URL-encoded key
        result = get_object('test+file.json', bucket=bucket_name, resources=ingest_resources)

        if hasattr(result, 'read'):
            content = result.read().decode('utf-8')
        else:
            content = result

        assert 'space' in content

    def test_get_object_with_missing_file_raises_exception(self, s3_setup, ingest_resources):
        """Test that missing S3 object raises exception."""
        s3_client, bucket_name = s3_setup

        with pytest.raises(Exception):
            get_object('nonexistent-file.json', bucket=bucket_name, resources=ingest_resources)

    def test_get_object_without_resources_creates_default(self, s3_setup):
        """Test that get_object works without explicit resources parameter.

        When resources=None, function should create Resources() internally.
        """
        s3_client, bucket_name = s3_setup

        # Call without resources parameter - should use default Resources()
        result = get_object('test-data.json.gz', bucket=bucket_name)

        # Should still work and return decompressed content
        assert isinstance(result, str)
        assert 'test' in result


class TestPutObject:
    """Tests for put_object() S3 write function."""

    def test_put_object_writes_gzipped_to_s3(self, s3_setup, ingest_resources):
        """Test writing gzipped data to S3."""
        s3_client, bucket_name = s3_setup

        test_data = '{"uploaded": "data", "count": 456}'

        put_object(test_data, 'uploaded.json.gz', bucket=bucket_name, resources=ingest_resources)

        # Verify file was uploaded and is gzipped
        response = s3_client.get_object(Bucket=bucket_name, Key='uploaded.json.gz')
        compressed_content = response['Body'].read()
        decompressed = gzip.decompress(compressed_content).decode('utf-8')

        assert 'uploaded' in decompressed
        assert 'count' in decompressed
        assert '456' in decompressed

    def test_put_object_with_dryrun_writes_locally(self, s3_setup, tmp_path):
        """Test that DRYRUN mode writes to local filesystem."""
        s3_client, bucket_name = s3_setup

        test_data = '{"dryrun": "test"}'
        local_bucket = str(tmp_path)

        with patch.object(settings, 'DRYRUN', True):
            put_object(test_data, 'dryrun-test.json.gz', bucket=local_bucket)

        # Verify file was written locally
        local_file = tmp_path / 'dryrun-test.json.gz'
        assert local_file.exists()

        # Verify content is gzipped
        with gzip.open(local_file, 'rt') as f:
            content = f.read()
        assert 'dryrun' in content

    def test_put_object_creates_nested_directories_in_dryrun(self, s3_setup, tmp_path):
        """Test that nested directories are created in DRYRUN mode."""
        s3_client, bucket_name = s3_setup

        test_data = '{"nested": "path"}'
        local_bucket = str(tmp_path)

        with patch.object(settings, 'DRYRUN', True):
            put_object(test_data, 'path/to/nested/file.json.gz', bucket=local_bucket)

        # Verify nested path was created
        nested_file = tmp_path / 'path' / 'to' / 'nested' / 'file.json.gz'
        assert nested_file.exists()

    def test_put_object_without_resources_creates_default(self, s3_setup):
        """Test that put_object works without explicit resources parameter.

        When resources=None, function should create Resources() internally.
        """
        s3_client, bucket_name = s3_setup

        test_data = '{"default": "resources"}'

        # Call without resources parameter - should use default Resources()
        put_object(test_data, 'default-resources.json.gz', bucket=bucket_name)

        # Verify file was uploaded
        response = s3_client.get_object(Bucket=bucket_name, Key='default-resources.json.gz')
        compressed_content = response['Body'].read()
        decompressed = gzip.decompress(compressed_content).decode('utf-8')

        assert 'default' in decompressed
        assert 'resources' in decompressed


class TestDeconstructPath:
    """Tests for deconstruct_path() URL parsing function."""

    def test_deconstruct_path_parses_s3_url(self):
        """Test parsing full S3 URL."""
        result = deconstruct_path('s3://my-bucket/path/to/file.json')

        assert result['bucket'] == 'my-bucket'
        assert result['key'] == 'path/to/file.json'
        assert 'local' not in result

    def test_deconstruct_path_parses_local_file(self, temp_local_file):
        """Test parsing local file path."""
        local_file = temp_local_file / "test-local.txt"
        result = deconstruct_path(str(local_file))

        assert result['local'] is True
        assert result['key'] == str(local_file)
        assert 'bucket' not in result

    def test_deconstruct_path_uses_default_bucket_for_key_only(self):
        """Test that key-only path uses settings.FETCH_BUCKET."""
        result = deconstruct_path('data/measurements.json')

        assert result['bucket'] == settings.FETCH_BUCKET
        assert result['key'] == 'data/measurements.json'

    def test_deconstruct_path_handles_s3_with_multiple_slashes(self):
        """Test S3 URL with nested path."""
        result = deconstruct_path('s3://bucket-name/a/b/c/d/file.csv.gz')

        assert result['bucket'] == 'bucket-name'
        assert result['key'] == 'a/b/c/d/file.csv.gz'


class TestGetData:
    """Tests for get_data() file retrieval function."""

    def test_get_data_reads_from_s3(self, s3_setup, ingest_resources):
        """Test reading data from S3."""
        s3_client, bucket_name = s3_setup

        result = get_data(f's3://{bucket_name}/test-data.json', resources=ingest_resources)

        # Result is StreamingBody
        content = result.read().decode('utf-8')
        assert 'test' in content
        assert 'data' in content

    def test_get_data_reads_gzipped_from_s3(self, s3_setup, ingest_resources):
        """Test reading gzipped data from S3."""
        s3_client, bucket_name = s3_setup

        result = get_data(f's3://{bucket_name}/test-data.json.gz', resources=ingest_resources)

        # Result is GzipFile
        content = result.read().decode('utf-8')
        assert 'test' in content

    def test_get_data_reads_from_local_file(self, temp_local_file):
        """Test reading data from local file."""
        local_file = temp_local_file / "test-local.txt"

        result = get_data(str(local_file))

        content = result.read()
        assert 'local file content' in content

    def test_get_data_reads_gzipped_local_file(self, temp_local_file):
        """Test reading gzipped local file."""
        local_file = temp_local_file / "test-local.txt.gz"

        result = get_data(str(local_file))

        content = result.read()
        assert b'gzipped local content' in content

    def test_get_data_handles_local_prefix(self, temp_local_file):
        """Test handling local:// prefix.

        Note: local:// prefix is stripped and then path is checked with os.path.isfile().
        The prefix is meant for relative paths, not absolute paths with //.
        """
        local_file = temp_local_file / "test-local.txt"

        # Use path without leading slash to match the regex pattern
        # The regex in get_data is: r"local://[a-zA-Z]+"
        # which expects: local://relativepath not local:///absolute/path
        # So we test with the actual expected behavior: strip the prefix and use relative path

        # Create a file in current directory
        import os
        cwd_file = "test_local_prefix.txt"
        with open(cwd_file, 'w') as f:
            f.write("local prefix test content")

        try:
            result = get_data(f'local://{cwd_file}')
            content = result.read()
            assert 'local prefix test content' in content
        finally:
            # Cleanup
            if os.path.exists(cwd_file):
                os.remove(cwd_file)

    def test_get_data_uses_default_bucket_for_key_only(self, s3_setup, ingest_resources):
        """Test that key-only uses settings.FETCH_BUCKET."""
        s3_client, bucket_name = s3_setup

        result = get_data('test-data.json', resources=ingest_resources)

        content = result.read().decode('utf-8')
        assert 'test' in content

    def test_get_data_without_resources_creates_default(self, s3_setup):
        """Test that get_data works without explicit resources parameter.

        When resources=None, function should create Resources() internally.
        """
        s3_client, bucket_name = s3_setup

        # Call without resources parameter - should use default Resources()
        result = get_data(f's3://{bucket_name}/test-data.json.gz')

        # Should still work and decompress
        content = result.read().decode('utf-8')
        assert 'test' in content
        assert 'data' in content


class TestGetFile:
    """Tests for get_file() local file reading function."""

    def test_get_file_reads_plain_text(self, temp_local_file):
        """Test reading plain text file."""
        local_file = temp_local_file / "test-local.txt"

        result = get_file(str(local_file))

        content = result.read()
        assert 'local file content' in content
        assert 'line 2' in content

    def test_get_file_reads_gzipped_file(self, temp_local_file):
        """Test reading gzipped file."""
        local_file = temp_local_file / "test-local.txt.gz"

        result = get_file(str(local_file))

        # GzipFile in binary mode
        content = result.read()
        assert b'gzipped local content' in content

    def test_get_file_with_nonexistent_file_raises_error(self):
        """Test that nonexistent file raises exception."""
        with pytest.raises(Exception):
            get_file('/nonexistent/path/to/file.txt')

    def test_get_file_detects_gzip_by_extension(self, temp_local_file):
        """Test that .gz extension triggers gzip decompression."""
        gzipped_file = temp_local_file / "test-local.txt.gz"

        result = get_file(str(gzipped_file))

        # Should return GzipFile, not regular file
        assert hasattr(result, 'read')
        # Binary mode for gzip
        content = result.read()
        assert isinstance(content, bytes)
