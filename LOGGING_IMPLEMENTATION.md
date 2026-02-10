# Comprehensive Logging Infrastructure Implementation

## Overview
Implemented a comprehensive logging infrastructure in `helpers.py` that captures execution flow, model processing status, metric extraction progress, errors with stack traces, and data quality warnings.

## Implementation Details

### 1. Log File Location
- **Path**: `benchmark/logs/{timestamp}_{pipeline}.log`
- **Timestamp Format**: `YYYYMMDD_HHMMSS_mmm` (milliseconds for multi-run support)
- **Example**: `20240115_143022_123_pipeline_a.log`
- **Implementation**: Lines 534-536 in helpers.py

### 2. Log Format
- **Format String**: `[TIMESTAMP] [LEVEL] [MODULE] message`
- **Timestamp Format**: `[YYYY-MM-DD HH:MM:SS.mmm]` with millisecond precision
- **Example**: `[2024-01-15 14:30:22.123] [INFO] [generate_report] Processing model: stg_trades (1 of 42)`
- **Stack Traces**: Automatically appended for ERROR logs with `exc_info=True`
- **Implementation**: Lines 539-561 (MillisecondFormatter class)

### 3. Log Levels Configuration

#### DEBUG
**Usage**: Model processing details, artifact parsing steps, config values loaded
**Examples**:
```python
logger.debug("Loading manifest.json from target/")
logger.debug("Parsed 42 models from artifact")
```
**Implementation**: Automatic, configured in setup_logging()

#### INFO
**Usage**: Metrics extracted per model, summary statistics, script completion
**Examples**:
```python
logger.info("Extracted 156 metrics from model: stg_customers")
logger.info("Pipeline completed: 42 models, 3 failures")
```
**Implementation**: Automatic, configured in setup_logging()

#### WARNING
**Usage**: Data quality issues (hash mismatches, zero values, missing fields), threshold violations
**Examples**:
```python
logger.warning("Data hash mismatch for model stg_users: expected abc123, got def456")
logger.warning("Zero-value detected in metric revenue_sum for model fact_orders")
logger.warning("Missing field in data quality check: grain validation not found in report")
```
**Implementation**: Automatic, configured in setup_logging()

#### ERROR
**Usage**: Exceptions, failed artifact parsing, validation failures, with full stack traces
**Examples**:
```python
try:
    parse_artifact()
except Exception as e:
    logger.error("Failed to parse artifact", exc_info=True)
```
**Implementation**: Lines 558-559, automatically handles exc_info

### 4. Features Implemented

#### ✓ Timestamp-based Log File Naming
- Uses millisecond precision (`strftime("%Y%m%d_%H%M%S_%f")[:-3]`)
- Ensures no conflicts when multiple runs occur in same second
- Implementation: Line 535

#### ✓ Formatted Log Output
- Custom `MillisecondFormatter` class (Lines 539-561)
- All required fields included: timestamp, level, module, message
- Milliseconds added to every log entry
- Stack traces automatically appended for exceptions

#### ✓ Log Levels
- All 4 levels configured: DEBUG, INFO, WARNING, ERROR
- Configurable via `BENCHMARK_LOG_LEVEL` environment variable
- Default: INFO
- Implementation: Lines 530-532

#### ✓ File and Console Output
- File handler: Writes to timestamped log file (Lines 565-569)
- Console handler: Writes to stdout (Lines 571-575)
- Both handlers use same format for consistency

#### ✓ Log Directory Management
- Automatically creates `benchmark/logs/` directory
- Uses `ensure_logs_directory()` function (Lines 196-205)
- Creates parent directories if needed
- Implementation: Line 520

#### ✓ Error Stack Traces
- Full exception details captured when using `exc_info=True`
- `formatException()` automatically formats traceback
- Implementation: Lines 558-559

#### ✓ Data Quality Warning Support
- WARNING level designed for data quality issues
- Can include detailed context (hash values, metrics, fields)
- Example: Line 502 in docstring

#### ✓ Human-Readable Format
- Bracketed fields for easy scanning
- Clear timestamps for audit trail
- Module names for source identification
- Messages in plain language for actionability

#### ✓ No Log Overwrites
- Millisecond precision in timestamp prevents overwrites
- Multiple runs per day supported
- Latest logs always accessible

## Usage Examples

### Basic Setup
```python
from helpers import setup_logging

# Initialize logging for a pipeline
logger = setup_logging("pipeline_a")

# Logging is now ready for use
logger.info("Starting pipeline execution")
```

### Execution Flow Logging
```python
logger.info("Pipeline started")
logger.debug("Loading artifacts from target/")
logger.info("Processing 42 models")
logger.info("Metric extraction complete: 156 KPIs extracted")
logger.info("Pipeline completed successfully")
```

### Model Processing Status
```python
for i, model in enumerate(models, 1):
    logger.debug(f"Processing model {i}/{len(models)}: {model}")
    try:
        process_model(model)
        logger.info(f"Successfully processed model: {model}")
    except Exception as e:
        logger.error(f"Failed to process model: {model}", exc_info=True)
```

### Metric Extraction Progress
```python
logger.debug("Starting metric extraction for model: stg_users")
metrics = extract_metrics(model_data)
logger.info(f"Extracted {len(metrics)} metrics from model: stg_users")
```

### Error Handling with Stack Traces
```python
try:
    parse_json_artifact("manifest.json")
except Exception as e:
    logger.error("Failed to parse manifest.json", exc_info=True)
```

### Data Quality Warnings
```python
if baseline_hash != candidate_hash:
    logger.warning(f"Data hash mismatch for model {model}: expected {baseline_hash}, got {candidate_hash}")

if metric_value == 0:
    logger.warning(f"Zero-value detected in metric {metric}: {metric_value}")

if required_field not in data:
    logger.warning(f"Missing required field '{required_field}' in data quality report")
```

## Log File Structure

### Directory
```
benchmark/
└── logs/
    ├── 20240115_143022_123_pipeline_a.log
    ├── 20240115_143045_456_pipeline_a.log
    ├── 20240115_150000_789_pipeline_b.log
    └── ...
```

### Sample Log File Content
```
[2024-01-15 14:30:22.123] [INFO] [pipeline_a] Logging initialized for pipeline: pipeline_a
[2024-01-15 14:30:22.124] [DEBUG] [pipeline_a] Log file: /path/to/benchmark/logs/20240115_143022_123_pipeline_a.log
[2024-01-15 14:30:22.125] [DEBUG] [pipeline_a] Log level: INFO
[2024-01-15 14:30:22.126] [INFO] [pipeline_a] Starting pipeline execution
[2024-01-15 14:30:22.127] [DEBUG] [pipeline_a] Loading manifest.json from target/
[2024-01-15 14:30:22.200] [INFO] [pipeline_a] Parsed 42 models from artifact
[2024-01-15 14:30:23.100] [INFO] [pipeline_a] Processing model: stg_users (1 of 42)
[2024-01-15 14:30:23.500] [INFO] [pipeline_a] Extracted 156 metrics from model: stg_users
[2024-01-15 14:30:24.100] [WARNING] [pipeline_a] Data hash mismatch for model stg_orders: expected abc123, got def456
[2024-01-15 14:30:35.000] [ERROR] [pipeline_a] Failed to parse run_results.json
Traceback (most recent call last):
  File "extract_report.py", line 45, in parse_json()
    data = json.load(f)
  File "/usr/lib/python3.9/json/__init__.py", line 357, in load
    obj = raw_decode(fp.read())
json.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
[2024-01-15 14:35:00.000] [INFO] [pipeline_a] Pipeline completed: 42 models processed, 0 failures
```

## Configuration

### Environment Variables
- `BENCHMARK_LOG_LEVEL`: Set logging level (DEBUG, INFO, WARNING, ERROR)
  - Default: INFO
  - Example: `BENCHMARK_LOG_LEVEL=DEBUG python extract_report.py`

### Programmatic Configuration
```python
import os

# Set log level before calling setup_logging()
os.environ["BENCHMARK_LOG_LEVEL"] = "DEBUG"

logger = setup_logging("pipeline_a")
```

## Technical Implementation Details

### MillisecondFormatter Class
Located in `setup_logging()` function (lines 539-561):
- Custom logging.Formatter subclass
- Formats timestamps with milliseconds: `datetime.fromtimestamp()`
- Calculates milliseconds from fractional seconds: `int((record.created - int(record.created)) * 1000)`
- Formats as: `[TIMESTAMP] [LEVEL] [MODULE] message`
- Automatically formats exception stack traces when `exc_info=True`

### File Handler Configuration
- Writes to timestamped file in `benchmark/logs/`
- Uses UTF-8 encoding for international character support
- Inherits log level from logger

### Console Handler Configuration
- Writes to `sys.stdout` for visibility during execution
- Same format as file handler for consistency
- Inherits log level from logger

### Logger Name as Module Identifier
- Logger name is the pipeline name (passed to `setup_logging()`)
- Appears in all log entries as `[module_name]`
- Allows filtering logs by pipeline in multi-pipeline scenarios

## Success Criteria Verification

- ✓ Log files created with correct timestamp and pipeline name format
- ✓ All 4 log levels (DEBUG, INFO, WARNING, ERROR) produce appropriate output
- ✓ Logs capture complete execution flow from start to finish
- ✓ Error logs include full stack traces and context
- ✓ Data quality warnings logged with sufficient detail for investigation
- ✓ Log format is human-readable and scannable (bracketed fields, millisecond timestamps)
- ✓ Multiple runs on same day do not overwrite previous logs (millisecond precision)
- ✓ Logs are created even if metrics extraction partially fails
- ✓ Log directory created automatically (benchmark/logs/)
- ✓ Setup function implemented in helpers.py as `setup_logging()`

## Dependencies Met

- ✓ Task (6): Logging directory (benchmark/logs/) created by ensure_logs_directory()
- ✓ Task (8): setup_logging() function implemented in helpers.py with all requirements

## Testing

A comprehensive test script is available at `./test_logging_setup.py`:
- Tests log file creation with correct naming
- Verifies all 4 log levels
- Validates log format (timestamp, level, module, message)
- Tests error logging with stack traces
- Tests data quality warnings
- Verifies readability and scannability
- Tests multiple runs without conflicts

Run with: `python test_logging_setup.py`

## Integration Notes

When integrating logging into other scripts:

1. Import at the top: `from helpers import setup_logging`
2. Initialize early: `logger = setup_logging("pipeline_name")`
3. Use appropriate levels:
   - DEBUG for development/detailed info
   - INFO for user-facing progress
   - WARNING for issues that don't stop execution
   - ERROR for exceptions (use exc_info=True)
4. Log at key points:
   - Start/end of major operations
   - Model processing status
   - Metric extraction completion
   - Errors with context

## Future Enhancements (Out of Scope)

Potential improvements for future iterations:
- Log rotation by size (e.g., max 10MB per file)
- Structured logging (JSON format for machine parsing)
- Remote log aggregation
- Performance metrics in logs
- Log filtering and search utilities
- Integration with monitoring systems
