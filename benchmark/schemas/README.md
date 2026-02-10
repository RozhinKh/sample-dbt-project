# Report.json Schema

This directory contains the JSON Schema definition for the `report.json` output from the dbt benchmarking system.

## Files

- **report.json.schema**: Complete JSON Schema (Draft 7) for report.json validation
- **example-report.json**: Example report.json file that validates against the schema
- **README.md**: This file

## Schema Overview

The `report.json.schema` file defines the complete structure for benchmark reports, including:

### Top-Level Structure

```json
{
  "schema_version": "1.0.0",
  "metadata": { ... },
  "models": [ ... ],
  "summary": { ... },
  "data_quality_flags": [ ... ],
  "warnings_and_errors": [ ... ]
}
```

### Key Sections

#### 1. Metadata
Audit information and report identification:
- `timestamp`: ISO 8601 timestamp of report generation
- `pipeline_name`: Name of the pipeline (pipeline_a, pipeline_b, pipeline_c)
- `models_processed`: Count of models in report
- `total_duration_seconds`: Total execution time
- `dbt_artifacts_version`: Version of dbt artifacts used
- `dbt_version`: Version of dbt that generated artifacts

#### 2. Models Array
Array of per-model KPI objects containing all 5 KPIs:

**KPI 1: Execution Time**
- `execution_time_seconds`: Query execution time in seconds
- `status`: success, partial, error, or skipped

**KPI 2: Work Metrics**
- `rows_produced`: Number of rows output
- `bytes_scanned`: Estimated bytes scanned

**KPI 3: Data Equivalence**
- `output_hash`: SHA256 hash of model output (or null if unavailable)
- `hash_calculation_method`: Source of hash (run_results_json, snowflake_query, unavailable)

**KPI 4: Query Complexity**
- `join_count`: Number of JOINs in SQL
- `cte_count`: Number of CTEs (Common Table Expressions)
- `window_function_count`: Number of window functions

**KPI 5: Cost Estimation**
- `estimated_credits`: Snowflake credits consumed
- `estimated_cost_usd`: Estimated cost in USD

#### 3. Summary
Aggregated statistics across all models:
- `total_execution_time_seconds`: Sum of all execution times
- `total_estimated_credits`: Sum of all credit estimates
- `data_quality_score`: Overall quality score (0-100)
- `hash_validation_success_rate`: Percentage of successful hashes
- `average_model_complexity`: Average complexity across models

#### 4. Data Quality Flags
Array of validation issues or warnings:
- `flag_type`: missing_hash, missing_execution_time, data_not_scanned, etc
- `severity`: info, warning, or error
- `message`: Human-readable description

#### 5. Warnings and Errors
Array of processing errors encountered:
- `level`: debug, info, warning, or error
- `message`: Error/warning message
- `source`: Component that generated the message

## Usage

### Python Validation

Using the `schema_validator()` function from helpers.py:

```python
from helpers import load_json_safe, schema_validator

# Load report
report = load_json_safe("benchmark/pipeline_b/candidate/report.json")

# Validate against schema
is_valid, errors = schema_validator(report)

if is_valid:
    print("✓ Report is valid")
else:
    print("✗ Validation errors:")
    for error in errors:
        print(f"  - {error}")
```

### Command Line Validation

Using jsonschema tool (if installed):

```bash
# Validate example report
jsonschema -i example-report.json report.json.schema

# Validate actual report
jsonschema -i ../pipeline_b/candidate/report.json report.json.schema
```

## Schema Features

### Type Validation
- Enforces correct data types (string, number, integer, object, array)
- Validates nested object structures
- Checks array items

### Pattern Matching
- `schema_version`: Semantic version pattern (e.g., "1.0.0")
- `timestamp`: ISO 8601 date-time format
- `output_hash`: SHA256 hex string (64 characters)

### Value Constraints
- `minimum`/`maximum`: Numeric ranges (e.g., execution_time >= 0)
- `enum`: Allowed values (e.g., status must be "success", "error", etc)
- `minItems`: Array must have minimum length

### Required Fields
Report must include:
- `schema_version`: For versioning and future compatibility
- `metadata`: Audit and identification information
- `models`: Array of model KPI objects
- `summary`: Aggregated statistics

### Optional Fields
Many fields are optional to support partial reports or graceful degradation:
- `output_hash`: May be null if hash unavailable
- `warnings_and_errors`: Empty array if no issues
- `data_quality_flags`: Empty array if no quality issues

## Schema Versioning

The schema includes a `schema_version` field (required, format "X.Y.Z") for future compatibility:

- **Major version** (X): Breaking changes (removed/renamed fields)
- **Minor version** (Y): New optional fields or relaxed constraints
- **Patch version** (Z): Bug fixes, documentation updates

Example versions:
- `1.0.0`: Initial release
- `1.1.0`: Add new optional field
- `2.0.0`: Remove deprecated field or restructure

## Field Reference

### Model Status Values
- `success`: Model executed successfully and data is available
- `partial`: Model executed but some metrics are missing
- `error`: Model execution failed
- `skipped`: Model was not executed
- `no_sql`: Model type (source/seed) has no SQL

### Model Type Values
- `view`: dbt view materialization
- `table`: dbt table materialization
- `seed`: dbt seed data file
- `source`: External data source
- `ephemeral`: Ephemeral model (only exists in compilation)

### Model Layer Values
- `staging`: Raw data staging models (stg_*)
- `intermediate`: Data transformation models (int_*)
- `marts`: Final analytical models (fact_*, dim_*)
- `report`: Report/dashboard models (report_*)

### Flag Type Values
- `missing_hash`: Hash unavailable for model
- `missing_execution_time`: Execution time not captured
- `data_not_scanned`: Bytes scanned not available
- `zero_rows`: Model produced no output rows
- `hash_mismatch`: Hash differs from baseline
- `outlier_detected`: Metrics detected as outliers

## Examples

See `example-report.json` for a complete valid report with 6 models from pipeline_b.

To validate the example:
```bash
python3 << 'EOF'
from helpers import load_json_safe, schema_validator

report = load_json_safe("example-report.json")
is_valid, errors = schema_validator(report, "report.json.schema")
print(f"Valid: {is_valid}")
if errors:
    for error in errors:
        print(f"  - {error}")
EOF
```

## Implementation Notes

- Schema is JSON Schema Draft 7 compliant
- Validates all 5 KPIs across all models
- Supports partial reports (optional fields)
- Comprehensive error messages with field paths
- Handles nested structures (models array, metadata object)
- Format checking for timestamps and hashes
- Value range validation (non-negative numbers)
- Enum validation for status/type/layer fields

## Future Updates

When updating the schema:

1. Increment the version in this file
2. Update `$id` field in JSON Schema
3. Add breaking changes note if major version bumps
4. Update example-report.json if structure changes
5. Update all validation code
