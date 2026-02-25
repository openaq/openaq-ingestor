# OpenAQ Ingestor

Data ingestion pipeline for OpenAQ that processes air quality measurements from various providers into the OpenAQ database.

## Overview

The ingestor handles three main types of data:
1. **LCS Pipeline Data** (lcs-etl-pipeline/) - Low-cost sensor measurements and metadata
2. **Realtime Data** (realtime-gzipped/) - Real-time air quality measurements
3. **Metadata** (lcs-etl-pipeline/stations/) - Sensor station metadata

## Architecture

### Event-Driven Processing

The ingestor runs as an AWS Lambda function triggered by:
- **S3 Events via SNS** (production) - Automatically processes new files uploaded to S3
- **EventBridge/CloudWatch Events** (cron) - Scheduled batch processing
- **Direct invocation** (manual) - For debugging or reprocessing specific files

### Processing Flow

```
S3 Upload → SNS Topic → Lambda Handler → Load to Staging → ETL to Permanent Tables
                           ↓
                      fetchlogs tracking
```

## Data Pipeline: lcsV2 Process

The `lcsV2` module is the primary ingestion pipeline for low-cost sensor data. It processes JSON, NDJSON, and CSV files containing sensor metadata and measurements.

### Process Steps

#### 1. File Selection (`load_fetchlogs`)
- **Function**: `utils.load_fetchlogs(pattern, limit, ascending)`
- **Purpose**: Selects files from `fetchlogs` table for processing
- **Filters**:
  - Pattern match (regex)
  - `init_datetime IS NOT NULL` (initialized records only)
  - `completed_datetime IS NULL` (unprocessed)
  - `has_error = false` (no previous errors)
  - Not loaded in last 30 minutes (prevents duplicate processing)
- **Updates**: Sets `loaded_datetime`, increments `jobs` counter, assigns `batch_uuid`

#### 2. Data Loading (`IngestClient.load_key`)
- **Purpose**: Reads file from S3 or local filesystem and parses data
- **Supported Formats**:
  - JSON - Structured metadata and measurements
  - NDJSON - Newline-delimited JSON (realtime data)
  - CSV - Tabular sensor data
- **Populates**:
  - `self.nodes` - Sensor locations/stations
  - `self.systems` - Sensor systems/instruments
  - `self.sensors` - Individual sensors
  - `self.measurements` - Measurement values
  - `self.flags` - Data quality flags

#### 3. Staging Data Dump (`IngestClient.dump`)

##### 3a. Locations Dump (`dump_locations`)
- **SQL File**: `temp_locations_dump.sql`
- **Purpose**: Creates staging tables and writes location data
- **Tables Created**:
  - `staging_keys` - Tracks files being processed
  - `staging_sensornodes` - Node/location data with unique constraints on `ingest_id` and `(source_name, source_id)`
  - `staging_sensorsystems` - System/instrument data linked to nodes
  - `staging_sensors` - Sensor data linked to systems
  - `staging_flags` - Data quality flags
- **Updates**: Sets `fetchlogs.loaded_datetime = clock_timestamp()` and `last_message = 'load_data'`

##### 3b. Measurements Dump (`dump_measurements`)
- **SQL File**: `temp_measurements_dump.sql`
- **Purpose**: Creates staging table and writes measurement data
- **Table Created**:
  - `staging_measurements` - Raw measurement values with sensor references
- **Data Transfer**: Uses PostgreSQL `COPY` for efficient bulk loading

#### 4. ETL Processing

##### 4a. Process Nodes/Systems/Sensors
- **SQL File**: `etl_process_nodes.sql`
- **Purpose**: Moves data from staging tables to permanent tables
- **Operations**:
  - Matches nodes to existing `sensor_nodes` records or creates new ones
  - Links systems to nodes and creates `sensor_systems` records
  - Links sensors to systems and creates `sensors` records
  - Handles geometry, timezones, countries lookups
  - Resolves measurands and units
  - Merges metadata JSON fields
- **Tables Updated**:
  - `sensor_nodes`
  - `sensor_systems`
  - `sensors`

##### 4b. Process Measurements
- **SQL File**: `etl_process_measurements.sql`
- **Purpose**: Processes measurements into permanent storage
- **Operations**:
  - Links measurements to sensors via ingest_id or (source_name, source_id)
  - Validates and filters measurements
  - Inserts into `measurements` table
  - Handles duplicates and rejects
  - Creates rollup data for aggregations
- **Tables Updated**:
  - `measurements`
  - `rejects` (invalid measurements)

#### 5. Completion
- **Updates**: Sets `fetchlogs.completed_datetime = clock_timestamp()` and `last_message = NULL`
- **Cleanup**: Drops staging tables (if using TEMP TABLEs)

### SQL Files Reference (lcsV2)

| File | Purpose | When Used | Key Operations |
|------|---------|-----------|----------------|
| `temp_locations_dump.sql` | Create staging tables for locations | `dump_locations()` start | CREATE [TEMP] TABLE for nodes, systems, sensors, flags |
| `etl_process_nodes.sql` | ETL staging → permanent tables | `dump_locations()` end | INSERT/UPDATE sensor_nodes, sensor_systems, sensors |
| `temp_measurements_dump.sql` | Create staging table for measurements | `dump_measurements()` start | CREATE [TEMP] TABLE staging_measurements |
| `etl_process_measurements.sql` | ETL measurements → permanent table | `dump_measurements()` end | INSERT measurements, handle rejects, create rollups |

## Data Pipeline: Realtime Process

The realtime process (`fetch.py`) handles high-frequency measurement data in NDJSON format.

### SQL Files Reference (Realtime)

| File | Purpose | When Used |
|------|---------|-----------|
| `fetch_staging.sql` | Create staging table for realtime data | Start of realtime processing |
| `fetch_copy.sql` | Copy data into staging | After data loaded |
| `fetch_ingest_full.sql` | Process staging → measurements table | Final ETL step |

## Orchestration

### Automated Processing (cronhandler)

Triggered by EventBridge on a schedule (e.g., every 5 minutes):

```python
def cronhandler(event, context):
    # Process metadata (stations)
    load_metadata_db(limit=250, ascending=False)

    # Process realtime measurements
    load_db(limit=50, ascending=False)

    # Process pipeline measurements
    load_measurements_db(limit=250, ascending=False)
```

Each function:
1. Calls `load_fetchlogs()` with specific pattern
2. Processes returned records
3. Returns count of processed files

### Manual Processing

For debugging or reprocessing specific files:

```python
# Process specific file by ID
load_measurements_file(fetchlogs_id=123)

# Process by key pattern
load_measurements_pattern(pattern='lcs-etl-pipeline/measures/2024%')

# Process batch by UUID
load_measurements_batch(batch_uuid='abc123')
```

## Database Schema

### Tracking Table: fetchlogs

Tracks all files processed through the system:

| Column | Type | Purpose |
|--------|------|---------|
| `fetchlogs_id` | int | Primary key (auto-generated) |
| `key` | text | S3 object key (unique) |
| `last_modified` | timestamptz | S3 object last modified time |
| `init_datetime` | timestamptz | When record was initialized (required for automated processing) |
| `loaded_datetime` | timestamptz | When file was last loaded for processing |
| `completed_datetime` | timestamptz | When processing completed successfully |
| `has_error` | boolean | Whether file has processing errors |
| `last_message` | text | Status message or error details |
| `jobs` | int | Number of times file has been processed |
| `batch_uuid` | text | UUID linking files processed together |
| `file_size` | bigint | File size in bytes |

### Staging Tables (Temporary)

Created per-session during processing:
- `staging_keys` - Files being processed
- `staging_sensornodes` - Node locations
- `staging_sensorsystems` - Sensor systems
- `staging_sensors` - Individual sensors
- `staging_flags` - Quality flags
- `staging_measurements` - Raw measurements

### Permanent Tables

Final destination for processed data:
- `sensor_nodes` - Sensor locations/stations
- `sensor_systems` - Sensor system/instrument metadata
- `sensors` - Individual sensor configurations
- `measurements` - Time-series measurement data
- `rejects` - Invalid/rejected measurements for review

## Configuration

### Environment Variables

Configured via `.env.local` (local) or environment (production):

```bash
# Database
DATABASE_WRITE_URL=postgresql://user:pass@host:port/database

# S3
FETCH_BUCKET=openaq-fetches

# Processing
USE_TEMP_TABLES=true  # Use temp tables for staging (production=true)
INGEST_TIMEOUT=300    # Timeout for cron processing (seconds)
PAUSE_INGESTING=false # Emergency pause flag

# Limits (records per batch)
PIPELINE_LIMIT=250    # LCS pipeline measurements
REALTIME_LIMIT=50     # Realtime measurements
METADATA_LIMIT=250    # Station metadata

# Order
FETCH_ASCENDING=false # Process newest files first (false) or oldest first (true)
```

### Settings

See `ingest/settings.py` for full configuration options.

## Testing

### Run All Tests

```bash
poetry run pytest tests/ -v
## increase the log level
poetry run pytest tests/ --log-cli-level=DEBUG
```

### Test Categories

**Handler Tests** (`test_handler_integration.py`):
- SNS-wrapped S3 events (production path)
- Direct S3 events (manual invocations)
- EventBridge cron triggers

**Cronhandler Tests** (`test_cronhandler_unit.py`):
- Loader orchestration
- Timeout handling
- Error resilience
- Event parameter overrides

**Load Fetchlogs Tests** (`test_load_fetchlogs.py`):
- Pattern matching
- Ordering (ascending/descending)
- 30-minute loaded_datetime logic
- Duplicate prevention

**lcsV2 Integration Tests** (`test_lcsv2_integration.py`):
- End-to-end data loading
- Staging table verification
- Data integrity checks
- Multiple data formats (JSON, NDJSON, CSV)

### Test with Coverage

```bash
poetry run pytest tests/ --cov=ingest --cov-report=term-missing
```

### Run Specific Test

```bash
poetry run pytest tests/test_handler_integration.py::TestHandlerSNSEvents::test_handler_sns_event_single_file -v
```

## Development Workflow

### Using check.py - File Loading CLI Utility

The `check.py` utility provides a command-line interface for loading and testing individual files from S3 or local filesystem.

#### Basic Usage

```bash
# Full load (staging + ETL to final tables)
python check.py KEY

# Preview mode - see what's in the file without loading to DB
python check.py KEY --preview

# Stage-only mode - load to staging tables, skip ETL
python check.py KEY --stage-only

# Dry run - full workflow with rollback
python check.py KEY --dryrun
```

#### Download Option

The `--download` flag downloads S3 files locally before processing, and can be combined with any mode:

```bash
# Download from S3 and do full load
python check.py s3-key.json --download

# Download and preview (no DB changes)
python check.py s3-key.json --download --preview

# Download and stage-only
python check.py s3-key.json --download --stage-only

# Download and dry run
python check.py s3-key.json --download --dryrun

# Download to specific location
python check.py s3-key.json --download --output ~/my-files/
```

#### Examples with Local Test Files

```bash
# Preview test file contents
python check.py tests/testdata_lcs_clarity.json --preview

# Load test file with full ETL processing
python check.py tests/testdata_lcs_clarity.json

# Dry run - test the full workflow and rollback
python check.py tests/testdata_lcs_clarity.json --dryrun

# Load to staging only (useful for debugging ETL separately)
python check.py tests/testdata_lcs_clarity.json --stage-only
```

#### Examples with S3 Files

```bash
# Preview S3 file without loading to DB (reads directly from S3)
python check.py lcs-etl-pipeline/measures/airgradient/2025-02-14/data.json --preview

# Download S3 file and do full load
python check.py lcs-etl-pipeline/measures/airgradient/2025-02-14/data.json --download

# Download and preview only (saves file locally, no DB changes)
python check.py lcs-etl-pipeline/measures/clarity/2025-02-14/data.json --download --preview

# Full load from S3 to database (without downloading)
python check.py lcs-etl-pipeline/measures/clarity/2025-02-14/measurements.json

# Dry run from S3 (safe testing with rollback)
python check.py lcs-etl-pipeline/measures/purpleair/2025-02-14/data.ndjson --dryrun

# Download and dry run (saves locally + tests load with rollback)
python check.py lcs-etl-pipeline/measures/purpleair/2025-02-14/data.ndjson --download --dryrun

# Load with specific fetchlogs ID
python check.py lcs-etl-pipeline/measures/clarity/data.json --fetchlogs-id 12345
```

#### Options

```bash
# Use specific environment file
python check.py data.json --env .env.staging

# Use AWS profile
python check.py s3-key.json --profile dev-aws

# Enable debug logging
python check.py data.json --debug

# Don't use temp tables (useful for inspecting staging data)
python check.py data.json --keep --stage-only

# Combine options (download + dry run + custom output location)
python check.py s3-key.json --download --dryrun --output ~/Downloads/ --profile production
```

#### Output

The utility provides detailed summaries at each step:

- **Client Summary**: Nodes, systems, sensors, measurements loaded into memory
- **Staging Summary**: Counts and date ranges in staging tables
- **Current Data Summary**: Final table counts after ETL processing (full load mode only)

### Processing a Test File with Python API

1. Place test file in the ingest directory or provide path
2. Run with Python:

```python
from ingest.lcsV2 import IngestClient
from datetime import date

client = IngestClient()
client.load_key('path/to/test.json', fetchlogs_id=1, last_modified=str(date.today()))
client.dump(load=True)
```

### Debugging a Failed File

```python
from ingest.utils import get_logs_from_ids

# Get error details
logs = get_logs_from_ids([fetchlogs_id])
print(logs)

# Reprocess with error handling
from ingest.lcsV2 import load_measurements_file
load_measurements_file(fetchlogs_id)
```

### Checking Processing Status

```python
from ingest.utils import load_errors_summary, load_errors_list

# Summary of recent errors
errors = load_errors_summary(days=7)

# List of recent error details
error_list = load_errors_list(limit=10)
```

## Deployment

The ingestor runs as an AWS Lambda function deployed via CDK (see `../cdk/lambda_ingest_stack.py`).

### Lambda Configuration

- **Trigger**: SNS topic subscribed to S3 bucket notifications
- **Schedule**: EventBridge rule for cron processing
- **Timeout**: Configured based on processing requirements
- **Memory**: Sufficient for large file processing
- **VPC**: Access to RDS PostgreSQL database

### Deployment

```bash
cd ../cdk
cdk deploy lambda-ingest-stack
```

## Monitoring

### CloudWatch Metrics

Custom metrics published:
- Files processed per run
- Processing duration
- Error counts
- Record counts

### Logs

Check CloudWatch Logs for:
- File processing details
- SQL execution notices
- Error tracebacks
- Performance metrics

## Troubleshooting

### Common Issues

**File not processing**:
- Check `fetchlogs.init_datetime IS NOT NULL`
- Verify `has_error = false`
- Check if `completed_datetime IS NULL`
- Ensure not loaded in last 30 minutes

**Staging table not found**:
- Verify `USE_TEMP_TABLES` setting
- Check if processing completed (temp tables dropped)
- For debugging, set `USE_TEMP_TABLES=false`

**Duplicate records**:
- Check unique constraints on staging tables
- Verify `batch_uuid` tracking
- Review 30-minute duplicate prevention logic

**Memory issues**:
- Reduce batch limits (PIPELINE_LIMIT, etc.)
- Increase Lambda memory allocation
- Process files individually with `load_measurements_file()`

### Support

For issues or questions:
- Check test files for examples
- Review SQL files for ETL logic
- See function docstrings for parameter details
