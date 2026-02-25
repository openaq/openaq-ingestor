"""Resource management for OpenAQ ingest operations."""

import boto3
import psycopg2
from .settings import settings


class Resources:
    """
    Manages lifecycle of database connections and AWS clients for ingest operations.

    Supports both production (creates resources) and testing (accepts mocked resources).
    Resources are lazy-loaded - only created when first accessed.

    Usage:
        # Production - creates resources automatically
        with Resources() as ctx:
            ctx.s3.list_objects_v2(Bucket="my-bucket")
            cursor = ctx.cursor()
            cursor.execute("SELECT * FROM fetchlogs")

        # Testing - inject mocked resources
        ctx = Resources(connection=mock_db, s3_client=mock_s3)
        # ctx will not close injected resources
    """

    def __init__(self,
                 connection=None,
                 s3_client=None,
                 s3_resource=None,
                 sns_client=None,
                 cw_client=None):
        """
        Initialize context with optional pre-created resources.

        Args:
            connection: psycopg2 connection (for testing)
            s3_client: boto3 S3 client (for testing)
            s3_resource: boto3 S3 resource (for testing)
            sns_client: boto3 SNS client (for testing)
            cw_client: boto3 CloudWatch client (for testing)
        """
        # Database connection
        self._connection = connection
        self._owns_connection = connection is None

        # AWS clients
        self._s3_client = s3_client
        self._s3_resource = s3_resource
        self._sns_client = sns_client
        self._cw_client = cw_client

        # Track ownership (don't close externally-provided resources)
        self._owns_s3_client = s3_client is None
        self._owns_s3_resource = s3_resource is None
        self._owns_sns = sns_client is None
        self._owns_cw = cw_client is None

    # Database connection management

    def get_connection(self, autocommit=True):
        """
        Get database connection, creating if needed.

        Args:
            autocommit: Whether to enable autocommit mode (default: True)

        Returns:
            psycopg2 connection object
        """
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(settings.DATABASE_WRITE_URL)
            self._connection.set_session(autocommit=autocommit)
            self._owns_connection = True

        # Clear any accumulated notices (helps with debugging)
        if len(self._connection.notices) > 0:
            del self._connection.notices[:]

        return self._connection

    @property
    def connection(self):
        """Alias for get_connection() with autocommit=True."""
        return self.get_connection(autocommit=True)

    def cursor(self):
        """Get cursor from connection."""
        return self.get_connection().cursor()

    def commit(self):
        """Commit transaction if we own the connection."""
        if self._connection and self._owns_connection:
            self._connection.commit()

    def close(self):
        """Close connection if we own it."""
        if self._connection and self._owns_connection:
            if not self._connection.closed:
                self._connection.close()
            self._connection = None

    # AWS client properties (lazy-loaded)

    @property
    def s3(self):
        """Get S3 client, creating if needed."""
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
            self._owns_s3_client = True
        return self._s3_client

    @property
    def s3_resource(self):
        """Get S3 resource, creating if needed."""
        if self._s3_resource is None:
            self._s3_resource = boto3.resource("s3")
            self._owns_s3_resource = True
        return self._s3_resource

    @property
    def sns(self):
        """Get SNS client, creating if needed."""
        if self._sns_client is None:
            self._sns_client = boto3.client("sns")
            self._owns_sns = True
        return self._sns_client

    @property
    def cw(self):
        """Get CloudWatch client, creating if needed."""
        if self._cw_client is None:
            self._cw_client = boto3.client("cloudwatch")
            self._owns_cw = True
        return self._cw_client

    # Context manager protocol

    def __enter__(self):
        """Support 'with Resources() as ctx:' pattern."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-cleanup when exiting context."""
        if exc_type is None:
            self.commit()  # Only commit on success
        self.close()
        return False  # Don't suppress exceptions
