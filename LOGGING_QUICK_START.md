# Logging Quick Start Guide

## Basic Usage

### 1. Import and Initialize
```python
from helpers import setup_logging

logger = setup_logging("pipeline_a")
```

### 2. Log Your Operations
```python
logger.debug("Loading configuration...")
logger.info("Processing 42 models")
logger.warning("Unexpected value detected")
logger.error("Operation failed", exc_info=True)
```

## Log Levels Explained

| Level   | Use When | Example |
|---------|----------|---------|
| DEBUG   | Detailed info needed only for debugging | `logger.debug("Parsed 42 models from manifest.json")` |
| INFO    | Major milestone or status update | `logger.info("Extracted 156 metrics from model: stg_customers")` |
| WARNING | Data quality issue or threshold violation | `logger.warning("Hash mismatch detected: expected abc123, got def456")` |
| ERROR   | Exception or critical failure | `logger.error("Failed to parse JSON", exc_info=True)` |

## Common Patterns

### Logging Model Processing
```python
for i, model in enumerate(models, 1):
    logger.info(f"Processing model: {model} ({i} of {len(models)})")
    try:
        # Process model
        process_model(model)
        logger.info(f"Successfully processed: {model}")
    except Exception as e:
        logger.error(f"Failed to process {model}", exc_info=True)
```

### Logging Metric Extraction
```python
logger.debug(f"Extracting metrics for: {model}")
metrics = extract_metrics(model_data)
logger.info(f"Extracted {len(metrics)} metrics from {model}")
```

### Logging Data Quality Issues
```python
if baseline_hash != candidate_hash:
    logger.warning(f"Hash mismatch for {model}: expected {baseline_hash}, got {candidate_hash}")

if value == 0:
    logger.warning(f"Zero-value in {metric}: {value}")

if field not in data:
    logger.warning(f"Missing field: {field}")
```

### Logging Exceptions
```python
try:
    data = load_json_safe("artifact.json")
except Exception as e:
    logger.error("Failed to load artifact", exc_info=True)
    # optionally re-raise
    raise
```

## Log Output Example

```
[2024-01-15 14:30:22.123] [INFO] [pipeline_a] Processing model: stg_users (1 of 42)
[2024-01-15 14:30:22.456] [DEBUG] [pipeline_a] Extracting metrics for: stg_users
[2024-01-15 14:30:23.789] [INFO] [pipeline_a] Extracted 156 metrics from stg_users
[2024-01-15 14:30:24.012] [WARNING] [pipeline_a] Zero-value detected in metric revenue_sum
[2024-01-15 14:30:25.345] [INFO] [pipeline_a] Successfully processed: stg_users (1 of 42)
```

## Log File Locations

Logs are automatically saved to: `benchmark/logs/{timestamp}_{pipeline_name}.log`

Example: `benchmark/logs/20240115_143022_123_pipeline_a.log`

## Configuration

Set logging level via environment variable:
```bash
# DEBUG level
BENCHMARK_LOG_LEVEL=DEBUG python script.py

# INFO level (default)
BENCHMARK_LOG_LEVEL=INFO python script.py

# WARNING level
BENCHMARK_LOG_LEVEL=WARNING python script.py

# ERROR level
BENCHMARK_LOG_LEVEL=ERROR python script.py
```

## Tips for Effective Logging

✓ **DO:**
- Use DEBUG for loop iterations and detailed parsing steps
- Use INFO for major operations and status changes
- Use WARNING for data quality issues
- Use ERROR with `exc_info=True` for exceptions
- Include relevant context (model names, counts, values)
- Log at both start and completion of major operations

✗ **DON'T:**
- Use ERROR for expected conditions (use WARNING instead)
- Include sensitive data (passwords, credentials) in logs
- Log the same message repeatedly in loops
- Use print() instead of logger
- Forget to use `exc_info=True` when logging exceptions

## Viewing Logs

### Last Run
```bash
ls -lt benchmark/logs/ | head -1
cat $(ls -t benchmark/logs/* | head -1)
```

### Filter by Pipeline
```bash
grep "pipeline_a" benchmark/logs/*.log
```

### Filter by Level
```bash
grep "\[ERROR\]" benchmark/logs/*.log
grep "\[WARNING\]" benchmark/logs/*.log
```

### Search for Issues
```bash
grep "hash mismatch" benchmark/logs/*.log
grep "Zero-value" benchmark/logs/*.log
grep "Missing field" benchmark/logs/*.log
```

## Testing Your Logging

Run the comprehensive test:
```bash
python test_logging_setup.py
```

Or test in your script:
```python
from helpers import setup_logging

logger = setup_logging("my_pipeline")
logger.info("Test message")
# Check benchmark/logs/ for output
```

## Troubleshooting

### No log files created
- Check that `benchmark/logs/` directory exists and is writable
- Ensure `setup_logging()` is called before logging

### Logs not appearing in file
- Verify file handler is configured: check that log file exists
- Check log level: use `BENCHMARK_LOG_LEVEL=DEBUG` to see all messages

### Multiple log files for same pipeline
- This is normal! Each run on the same day creates a new file
- Millisecond precision prevents overwrites
- Check the most recent file with: `ls -t benchmark/logs/*pipeline_a* | head -1`

## Reference

For detailed implementation information, see: `LOGGING_IMPLEMENTATION.md`
