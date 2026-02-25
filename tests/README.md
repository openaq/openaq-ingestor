# Integration Tests for OpenAQ Ingestor

This directory contains integration tests for the `openaq-ingestor` handler.

## Setup

### Prerequisites

1. **Local Database**: The PostgreSQL database must be running locally
   ```bash
   cd openaq-db
   docker compose up -d
   ```

2. **Test Dependencies**: Install test dependencies with poetry
   ```bash
   cd openaq-ingestor
   poetry install --with dev
   ```

3. **Environment Configuration**: Tests use `.env.local` (configured via `pyproject.toml`)

## Running Tests

### Run all handler tests (integration + unit)
```bash
poetry run pytest tests/test_handler_integration.py tests/test_cronhandler_unit.py -v
```

### Run only integration tests
```bash
poetry run pytest tests/test_handler_integration.py -v -m integration
```

### Run only unit tests
```bash
poetry run pytest tests/test_cronhandler_unit.py -v
```

### Run with coverage
```bash
poetry run pytest tests/test_handler_integration.py tests/test_cronhandler_unit.py --cov=ingest.handler --cov-report=term-missing
```

### Run specific test class
```bash
poetry run pytest tests/test_cronhandler_unit.py::TestCronhandlerTimeout -v
```

### Run specific test
```bash
poetry run pytest tests/test_handler_integration.py::TestHandlerSNSEvents::test_handler_sns_event_single_file -v
```

### Run all tests in the directory
```bash
poetry run pytest tests/ -v
```

## Test Structure

### Test Files
- `test_handler_integration.py` - Integration tests for handler() and cronhandler() functions
- `test_cronhandler_unit.py` - Unit tests for cronhandler() orchestration logic

### Test Classes

**Integration Tests (test_handler_integration.py):**
- `TestHandlerSNSEvents` - Tests for SNS-wrapped S3 events (production path)
- `TestHandlerDirectS3Events` - Tests for direct S3 events (local testing/manual invocations)
- `TestCronhandlerIntegration` - Integration tests for EventBridge cron handler
- `TestGetKeysFromSnsRecord` - Tests for SNS message parsing helper

**Unit Tests (test_cronhandler_unit.py):**
- `TestCronhandlerPaused` - Tests for PAUSE_INGESTING behavior
- `TestCronhandlerFetchlogPattern` - Tests for fetchlogKey pattern processing
- `TestCronhandlerLoaderOrchestration` - Tests for loader function orchestration
- `TestCronhandlerEventOverrides` - Tests for event parameter overrides
- `TestCronhandlerErrorHandling` - Tests for error resilience
- `TestCronhandlerTimeout` - Tests for timeout handling

### Fixtures (conftest.py)
- `db_connection` - Database connection with automatic rollback
- `db_cursor` - Database cursor for queries
- `clean_fetchlogs` - Truncates fetchlogs table before test
- `mock_s3` - Mocked S3 client using moto
- `mock_s3_with_object` - S3 with pre-uploaded test object
- `sample_s3_event` - Mock direct S3 event (single file)
- `sample_batch_s3_event` - Mock direct S3 event (multiple records)
- `sample_sns_event` - Mock SNS event wrapping S3 event
- `sample_batch_sns_event` - Mock SNS event with multiple S3 records
- `lambda_context` - Mock Lambda context object

## Important Notes

### Event Types Supported
The handler supports two event types:

1. **SNS-wrapped S3 events** (production): The Lambda is subscribed to an SNS topic that receives S3 events (see `cdk/lambda_ingest_stack.py:104-116`). This is the production configuration.

2. **Direct S3 events** (local/manual): Direct S3 events are supported for local testing and manual invocations. The handler uses `.get('EventSource')` to safely check for SNS events.

### Test Isolation
Tests use transaction rollback to ensure isolation. Each test:
1. Gets a fresh database connection
2. Performs operations
3. Automatically rolls back all changes

This means tests can run in any order without side effects.

### Database Connection
Tests connect to the local PostgreSQL database on port 5777 (configured in `.env.local`).

## Coverage

Current coverage for ingest/handler.py: **97%**

Coverage breakdown:
- `handler()` function: ~100% (SNS and direct S3 event paths)
- `cronhandler()` function: ~100% (pause, fetchlog pattern, loader orchestration, timeout, error handling)
- `getKeysFromSnsRecord()` helper: 100%
- `getKeysFromS3Record()` helper: 100%

Missing coverage (3 lines):
- Exception handler in `handler()` (lines 79-80)
- Warning log for unknown event types (line 84)

Run coverage report to see detailed metrics:
```bash
poetry run pytest tests/test_handler_integration.py --cov=ingest.handler --cov-report=html
open htmlcov/index.html
```

## Troubleshooting

### Database connection refused
- Verify Docker is running: `docker ps`
- Check port 5777: `netstat -an | grep 5777`
- Verify `.env.local` has `DATABASE_PORT=5777`

### Import errors
- Run `poetry install --with dev`
- Verify pytest-env: `poetry show pytest-env`

### Table does not exist
- Database schema not initialized
- Rebuild Docker: `cd openaq-db && docker compose down -v && docker compose up --build`
