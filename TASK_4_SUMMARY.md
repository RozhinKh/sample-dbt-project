# Task 4: Parse dbt Artifacts - COMPLETED

## Summary
Successfully implemented comprehensive parsing of dbt artifacts (manifest.json and run_results.json) with robust validation, error handling, and structured data extraction. The implementation includes model metadata extraction, execution telemetry aggregation, and detailed logging of parsing progress.

## Deliverables

### 1. Core Parsing Functions in helpers.py

#### `load_manifest(manifest_path, logger)`
**Lines: 589-651 in helpers.py**

Parses manifest.json from dbt target directory with comprehensive validation:
- Validates JSON structure and required fields (metadata, nodes)
- Checks data types (dict for nodes)
- Extracts and logs model count
- Raises `InvalidSchema` for structural issues
- Raises `MissingArtifact` for missing files

**Returns**: Parsed manifest dictionary with metadata and nodes

**Error Handling**:
- MissingArtifact: File not found or unreadable
- InvalidSchema: Missing required fields or wrong data types

#### `load_run_results(run_results_path, logger)`
**Lines: 654-716 in helpers.py**

Parses run_results.json from dbt target directory:
- Validates JSON structure and required fields (metadata, results)
- Checks that results is a list
- Counts successful model executions
- Logs parsing summary
- Raises `InvalidSchema` for structural issues
- Raises `MissingArtifact` for missing files

**Returns**: Parsed run_results dictionary with metadata and results array

**Error Handling**:
- MissingArtifact: File not found or unreadable
- InvalidSchema: Missing required fields or wrong data types

#### `extract_model_data(model_id, manifest, run_results, logger)`
**Lines: 719-822 in helpers.py**

Extracts comprehensive model-specific data from both artifacts:

**From manifest.json**:
- Model name, database, schema, relation name
- Materialization type (table, view, etc.)
- Tags and metadata
- Raw SQL code
- Compiled SQL code (if compiled=true)
- Dependencies (macros, nodes, refs, sources)

**From run_results.json**:
- Execution time (compile + execute)
- Status (success, error, skipped)
- Adapter response (code, rows_affected, query_id, message)
- Compiled SQL (fallback if not in manifest)

**Defaults for non-executed models**:
- execution_time: 0.0
- status: "not_executed"
- adapter_response: empty structure

**Returns**: Dictionary with complete model metadata and execution metrics

**Error Handling**:
- KeyError: Model not found in manifest

#### `validate_artifact_fields(model_data, logger)`
**Lines: 825-895 in helpers.py**

Validates extracted model data for KPI calculation readiness:

**For executed models (status=success)**:
- Checks required fields: execution_time, model_name, relation_name, status
- Validates execution_time is numeric and non-negative
- Validates rows_affected is integer
- Warns about zero execution times (timing precision issue)

**General validation**:
- Ensures model_data is a dictionary
- Separates errors from warnings
- Returns both validity status and detailed error list

**Returns**: Tuple (is_valid: bool, errors: List[str])

**Error Handling**:
- Returns False for any validation failure
- Includes helpful messages for debugging

### 2. Orchestration Script: benchmark/generate_report.py

**Lines: 1-294**

Main script that orchestrates the complete artifact parsing workflow:

#### ArtifactParser Class

**`__init__(logger)`**
- Initializes parser with logger
- Sets up data structures for parsed models and validation tracking

**`load_artifacts(manifest_path, run_results_path)`**
- Phase 1: Loads manifest.json with error handling
- Phase 2: Loads run_results.json with error handling
- Returns True on success, False on failure
- Logs detailed progress messages

**`parse_models()`**
- Phase 3: Extracts data for all models
- Processes each model with progress logging
- Validates extracted data
- Tracks validation errors and warnings
- Handles per-model exceptions gracefully
- Returns True on completion (even with individual model errors)

**`print_summary()`**
- Logs parsing summary statistics
- Reports model counts by status
- Reports validation results
- Lists validation errors and warnings
- Formatted with section dividers for readability

**`save_parsed_data(output_dir)`**
- Saves parsed model data to JSON
- Creates output directory if needed
- Includes metadata (timestamp, counts, error counts)
- Saves models array and validation details
- Uses default=str for non-serializable types

**`get_parsed_models()`**
- Returns list of parsed model dictionaries

#### Main Function
- Sets up logging (artifact_parser logger)
- Initializes parser
- Orchestrates: load → parse → summarize → save
- Returns 0 on success, 1 on failure
- Catches and logs unexpected exceptions

### 3. Data Structure

#### Model Data Dictionary
```python
{
    "unique_id": "model.project.model_name",
    "model_name": "model_name",
    "database": "DATABASE_NAME",
    "schema": "SCHEMA_NAME",
    "relation_name": "DATABASE.SCHEMA.MODEL",
    "materialization": "table",  # or "view", "incremental", etc.
    "tags": ["tag1", "tag2"],
    "raw_sql": "SELECT ...",
    "compiled_sql": "SELECT ...",  # if compiled
    "meta": {"pipeline": "a", "layer": "staging"},
    "dependencies": {
        "macros": ["macro1", "macro2"],
        "nodes": ["model.project.upstream"],
        "refs": [["upstream_model"]],
        "sources": [["source_name", "table_name"]]
    },
    "execution_time": 2.123,  # seconds
    "status": "success",  # or "not_executed", "error", etc.
    "adapter_response": {
        "code": "SUCCESS",
        "rows_affected": 1000,
        "query_id": "01c22ce4-...",
        "message": "SUCCESS 1"
    },
    "validation_status": "valid"  # or "invalid"
}
```

#### Parsed Output File Structure
**File**: `benchmark/pipeline_a/parsed_artifacts.json`
```json
{
    "metadata": {
        "timestamp": "2024-01-15T14:30:22.123456",
        "parsed_at": "2024-01-15T14:30:22.123456",
        "total_models": 42,
        "successful_models": 40,
        "validation_errors": 0,
        "validation_warnings": 2
    },
    "models": [
        { "model1_data": "..." },
        { "model2_data": "..." }
    ],
    "validation": {
        "errors": ["error1", "error2"],
        "warnings": ["warning1", "warning2"]
    }
}
```

## Error Handling

### MissingArtifact Exception
Raised when artifact files are not found or not readable. Includes:
- File path that was not found
- Expected location information
- Remediation message

**Examples**:
- manifest.json not found in target/
- run_results.json not readable due to permissions

### InvalidSchema Exception
Raised when JSON structure doesn't match expectations. Includes:
- Specific field that is missing or invalid
- Expected structure description
- Suggested remediation

**Examples**:
- manifest.json missing "nodes" field
- run_results.json "results" is not a list
- Malformed JSON (handled by load_json_safe)

### Per-Model Exception Handling
- KeyError: Model not found in manifest
- General Exception: Wrapped with model context
- Errors tracked in validation_errors list
- Processing continues for remaining models

## Logging Implementation

### Log Levels Used

**INFO**:
- Phase transitions: "Phase 1: Loading manifest.json"
- Success messages: "✓ Manifest loaded successfully"
- Model processing completion: "✓ Model N/Total: model_name (status: ..., exec_time: ...s)"
- Parsing summary statistics
- Completion messages

**DEBUG**:
- Loading progress: "Loading manifest from: target/manifest.json"
- Artifact summaries: "Manifest parsed: 15 models found in 42 total nodes"
- Per-model processing: "Processing model 1/15: model.project.stg_users"
- Model extraction: "Extracted data for model: stg_users (status: success)"

**WARNING**:
- Validation issues: "Zero execution time for model 'stg_users' - possible timing precision issue"
- Per-model validation errors
- Validation error summaries

**ERROR**:
- Artifact loading failures: "✗ Failed to load manifest: ..."
- Unexpected exceptions with full stack traces

### Example Log Output
```
[2024-01-15 14:30:22.123] [INFO] [artifact_parser] ================================================================================
[2024-01-15 14:30:22.124] [INFO] [artifact_parser] Starting dbt Artifact Parsing
[2024-01-15 14:30:22.125] [INFO] [artifact_parser] Phase 1: Loading manifest.json
[2024-01-15 14:30:22.126] [DEBUG] [artifact_parser] Loading manifest from: target/manifest.json
[2024-01-15 14:30:22.350] [DEBUG] [artifact_parser] Manifest parsed: 15 models found in 42 total nodes
[2024-01-15 14:30:22.351] [INFO] [artifact_parser] ✓ Manifest loaded successfully
[2024-01-15 14:30:22.352] [INFO] [artifact_parser] Phase 2: Loading run_results.json
[2024-01-15 14:30:22.353] [DEBUG] [artifact_parser] Loading run_results from: target/run_results.json
[2024-01-15 14:30:22.378] [DEBUG] [artifact_parser] Run results parsed: 15/15 models succeeded
[2024-01-15 14:30:22.379] [INFO] [artifact_parser] ✓ Run results loaded successfully
[2024-01-15 14:30:22.380] [INFO] [artifact_parser] Phase 3: Extracting model data from artifacts
[2024-01-15 14:30:22.381] [INFO] [artifact_parser] Found 15 models in manifest
[2024-01-15 14:30:22.382] [DEBUG] [artifact_parser] Processing model 1/15: model.project.stg_users
[2024-01-15 14:30:22.383] [DEBUG] [artifact_parser] Extracted data for model: stg_users (status: success)
[2024-01-15 14:30:22.384] [INFO] [artifact_parser] ✓ Model 1/15: stg_users (status: success, exec_time: 1.2340s)
...
[2024-01-15 14:30:22.500] [INFO] [artifact_parser] Model extraction complete: 15 models parsed
[2024-01-15 14:30:22.501] [INFO] [artifact_parser] ================================================================================
[2024-01-15 14:30:22.502] [INFO] [artifact_parser] PARSING SUMMARY
[2024-01-15 14:30:22.503] [INFO] [artifact_parser] Total models parsed: 15
[2024-01-15 14:30:22.504] [INFO] [artifact_parser] ✓ Parsed data saved to: benchmark/pipeline_a/parsed_artifacts.json
[2024-01-15 14:30:22.505] [INFO] [artifact_parser] Artifact parsing completed successfully
```

## Usage

### Basic Usage
```python
from helpers import load_manifest, load_run_results, extract_model_data
from helpers import setup_logging

# Setup logging
logger = setup_logging("my_app")

# Load artifacts
manifest = load_manifest("target/manifest.json", logger)
run_results = load_run_results("target/run_results.json", logger)

# Extract model data
model_data = extract_model_data("model.project.stg_users", manifest, run_results, logger)
```

### Via Generate Report Script
```bash
# Set log level
export BENCHMARK_LOG_LEVEL=DEBUG

# Run parsing
python benchmark/generate_report.py

# Output location: benchmark/pipeline_a/parsed_artifacts.json
```

## Implementation Details

### Validation Strategy
1. **JSON Loading**: Uses existing `load_json_safe()` for basic JSON validation
2. **Schema Validation**: Checks required top-level fields (metadata, nodes/results)
3. **Type Validation**: Verifies field types match expectations
4. **Model Extraction**: Validates each model has required fields
5. **Graceful Degradation**: Non-critical fields default to empty/zero values

### Data Completeness
- All required fields extracted from manifest and run_results
- Compiled SQL extracted from both sources (manifest primary, run_results fallback)
- Dependencies fully tracked (macros, nodes, refs, sources)
- Execution metrics complete (time, status, rows affected, query ID)

### Edge Cases Handled
1. **Models without execution results**: Defaults to status="not_executed"
2. **Zero execution time**: Warns but doesn't fail
3. **Missing compiled SQL**: Still extracts model data
4. **Models in manifest but not run_results**: Handled gracefully
5. **Non-integer rows_affected**: Validation warning generated

## Success Criteria Met

✓ Both artifact files load without exceptions when valid  
✓ Required fields extracted for all successful models  
✓ Missing or malformed files produce clear error messages  
✓ JSON validation catches structural issues before data extraction  
✓ Parsed data returned in structured format (dict/dataclass) ready for KPI calculation  
✓ Logging captures parsing summary (X models parsed, Y validation warnings)  
✓ Model metadata (definitions, tags, materialization) extracted  
✓ Raw SQL and compiled SQL extracted  
✓ Dependencies and tags preserved  
✓ Execution time, status, rows_affected extracted  
✓ Query ID and adapter response captured  
✓ Relation names preserved for downstream use  

## Files Modified

### helpers.py (1018 lines total)
- Added 4 new functions: load_manifest, load_run_results, extract_model_data, validate_artifact_fields
- Functions added after setup_logging() and before MAIN section
- All existing code preserved

### Files Created

**benchmark/generate_report.py** (294 lines)
- Complete orchestration script
- ArtifactParser class
- Main function for CLI usage
- Ready for integration into larger workflow

## Testing

### Test Coverage
Created `test_artifact_parsing.py` with comprehensive test cases:
- Load manifest validation
- Load run_results validation
- Model data extraction
- Field validation
- All model parsing

### Validation Results
The implementation handles:
- ✓ Valid artifacts from dbt 1.11.2
- ✓ Complex model definitions with multiple dependencies
- ✓ Mixed materialization types (table, view)
- ✓ Successful and non-executed models
- ✓ Models with and without compiled SQL

## Dependencies

### Internal Dependencies
- helpers.py: load_json_safe, setup_logging, custom exceptions
- Existing logging infrastructure from Task 3

### External Dependencies
- Python 3.7+
- json (stdlib)
- logging (stdlib)
- pathlib (stdlib)
- typing (stdlib)
- datetime (stdlib)

## Next Steps

This implementation provides structured data ready for:
1. **KPI Calculation** (Task 5): Execution time analysis
2. **Data Comparison** (Task 6): Baseline vs candidate comparison
3. **Report Generation** (Task 7): Summary reporting
4. **Performance Analysis** (Future): Query optimization metrics

## Code Quality

### Documentation
- Comprehensive docstrings for all functions
- Parameter and return value documentation
- Exception documentation
- Usage examples in docstrings

### Error Messages
- Clear, actionable error messages
- Include context (file paths, expected values)
- Suggest remediation steps

### Logging
- Structured logging with levels
- Progress tracking at each phase
- Summary statistics
- Exception stack traces for debugging
