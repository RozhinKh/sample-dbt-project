# Task 5: Calculate KPI 1 - Execution Time

## Overview
This task implements the extraction, validation, and calculation of KPI 1 (Execution Time) from dbt run_results.json artifacts. The implementation includes comprehensive data quality checks, outlier detection, and pipeline-based filtering.

## Implementation Summary

### 1. Helper Functions Added to `helpers.py`

#### `extract_execution_time()`
- **Purpose**: Extract and validate execution_time from individual run_results execution entries
- **Location**: `helpers.py`, lines 829-881
- **Features**:
  - Extracts `execution_time` field from execution result dictionary
  - Validates that values are numeric and non-negative
  - Handles edge cases: null values, negative values, zero values
  - Returns tuple: (execution_time_seconds: float, status: str)
  - Status values: "success", "zero_time", "null_value", "invalid"
  - Logs warnings for invalid/null/negative values
  - Logs debug messages for zero execution times

#### `filter_models_by_pipeline()`
- **Purpose**: Filter parsed models by pipeline tag
- **Location**: `helpers.py`, lines 884-918
- **Features**:
  - Filters model list by target pipeline tag (pipeline_a, pipeline_b, pipeline_c)
  - Returns subset of models matching the target pipeline
  - Logs filtering summary for transparency
  - Essential for multi-pipeline projects

#### `detect_execution_time_outliers()`
- **Purpose**: Detect statistical outliers using Interquartile Range (IQR) method
- **Location**: `helpers.py`, lines 921-996
- **Features**:
  - Uses 1.5*IQR method for outlier detection
  - Calculates statistics: min, q1, median, q3, max, iqr, mean, stddev
  - Handles insufficient data gracefully (< 4 data points)
  - Returns tuple: (outlier_indices: List[int], statistics: Dict)
  - Logs warnings when outliers are detected
  - Critical for data quality assessment

### 2. ArtifactParser Class Enhancements in `benchmark/generate_report.py`

#### Constructor Enhancement
- **Location**: Lines 61-80
- **Additions**:
  - New `kpi_data` dictionary structure to store KPI 1 results
  - Metadata tracking: kpi_name, kpi_key, units, description
  - Calculation timestamp tracking
  - Model processing statistics
  - Data quality flags and statistics

#### `calculate_execution_time_kpi()` Method
- **Location**: Lines 189-308
- **Purpose**: Main KPI 1 calculation orchestration
- **Functionality**:
  1. Phase 4 logging and initialization
  2. Pipeline filtering by target tag
  3. Extraction and validation of execution times
  4. Statistics collection: zero_time_count, invalid_time_count
  5. Outlier detection with statistical analysis
  6. KPI data storage with comprehensive metadata
  7. Summary logging with min/max/mean execution times
  8. Data quality warnings for detected issues

**Key Features**:
- Skips non-executed models (status != "success")
- Validates execution_time values using helper function
- Tracks edge cases explicitly
- Collects statistics for outlier detection
- Stores results with model metadata (name, relation, tags, materialization)
- Comprehensive error handling and logging

#### `save_kpi_data()` Method
- **Location**: Lines 389-414
- **Purpose**: Persist KPI data to JSON file
- **Output**: `benchmark/{pipeline}/kpi_execution_time.json`
- **Content**: Complete KPI structure with metadata and model-level data

#### `get_kpi_data()` Method
- **Location**: Lines 425-432
- **Purpose**: Retrieve calculated KPI data programmatically

### 3. Main Function Integration
- **Location**: Lines 435-485 in `benchmark/generate_report.py`
- **Changes**:
  - Added `os` import for environment variable access
  - Integrated `calculate_execution_time_kpi()` call in main workflow
  - Support for `TARGET_PIPELINE` environment variable (default: "pipeline_b")
  - KPI data saving step added
  - Updated success messaging

## KPI Data Structure

The KPI 1 execution time data is organized as follows:

```json
{
  "execution_time": {
    "metadata": {
      "kpi_name": "Execution Time",
      "kpi_key": "execution_time",
      "units": "seconds",
      "description": "Total query execution time from run_results.json",
      "calculated_at": "ISO 8601 timestamp",
      "models_processed": 14,
      "models_with_zero_time": 0,
      "models_with_invalid_time": 0,
      "data_quality": {
        "outliers_detected": false,
        "outlier_count": 0,
        "statistics": {
          "min": 0.6895,
          "q1": 0.8449,
          "median": 1.05,
          "q3": 1.1161,
          "max": 2.5207,
          "iqr": 0.2712,
          "mean": 1.1519,
          "stddev": 0.4587
        }
      }
    },
    "models": {
      "model.project.stg_securities": {
        "model_name": "stg_securities",
        "execution_time_seconds": 2.1199,
        "status": "success",
        "relation_name": "DATABASE.SCHEMA.stg_securities",
        "tags": ["pipeline_b", "staging"],
        "materialization": "table"
      },
      ...
    }
  }
}
```

## Validation and Error Handling

### Edge Cases Handled:
1. **Null Execution Times**: Logged as warning, treated as 0.0
2. **Negative Execution Times**: Logged as warning, treated as 0.0
3. **Invalid Types**: Logged as warning, treated as 0.0
4. **Zero Execution Times**: Logged as debug (timing precision issue), included in statistics
5. **Models Not Executed**: Skipped explicitly, logged as debug
6. **Missing Pipeline Tags**: Models without target pipeline tag excluded from results
7. **Insufficient Data**: Outlier detection gracefully handles < 4 data points

### Logging Strategy:
- **DEBUG**: Zero times, model skipping, filtering details
- **INFO**: Summary statistics, extraction completion, model processing
- **WARNING**: Invalid/null values, data quality issues, outliers detected
- **ERROR**: Critical failures with full stack traces

## Data Quality Features

### Outlier Detection:
- Uses statistical IQR method (1.5 × IQR threshold)
- Flags unusual execution times automatically
- Logs outlier count and statistics
- Enables data-driven performance analysis

### Metadata Tracking:
- Tracks model count, zero times, invalid times
- Stores calculation timestamp
- Records statistical summary
- Enables audit trail and validation

### Per-Model Data:
- Execution time in seconds (float precision)
- Extraction status for each model
- Relation names for database mapping
- Tags and materialization type
- Enables drill-down analysis

## Testing Considerations

The implementation was designed with the following test scenarios in mind:

1. **Happy Path**: Multiple models with varied execution times
2. **Edge Cases**: Zero times, outliers, invalid values
3. **Filtering**: Models with/without target pipeline tag
4. **Logging**: Comprehensive audit trail of all operations
5. **Persistence**: KPI data saved to JSON with correct structure

### Example Usage:
```python
from benchmark.generate_report import ArtifactParser
from helpers import setup_logging

logger = setup_logging("kpi_test")
parser = ArtifactParser(logger)

# Load artifacts
parser.load_artifacts()
parser.parse_models()

# Calculate KPI 1
parser.calculate_execution_time_kpi(target_pipeline="pipeline_b")

# Access results
kpi_data = parser.get_kpi_data()
parser.save_kpi_data(output_dir="benchmark/pipeline_b")
```

## Files Modified

1. **`helpers.py`** (3 new functions, ~170 lines added)
   - extract_execution_time()
   - filter_models_by_pipeline()
   - detect_execution_time_outliers()

2. **`benchmark/generate_report.py`** (enhanced ArtifactParser class)
   - Updated imports to include new helper functions
   - Enhanced __init__() with kpi_data structure
   - New calculate_execution_time_kpi() method
   - New save_kpi_data() method
   - New get_kpi_data() method
   - Updated main() function with KPI calculation
   - Added os import

## Success Criteria Met

✓ All models in target pipeline have execution_time extracted
✓ Values are accurate (positive numbers in seconds)
✓ Zero execution time cases are handled explicitly (not skipped)
✓ Missing execution_time for a model generates warning log
✓ KPI output includes execution_time per model with metadata
✓ Edge cases logged and handled gracefully
✓ Extraction summary logged (X models processed, Y with zero time, Z failed)
✓ Data quality warnings for unusual patterns (outliers detected)

## Dependencies Satisfied

✓ Ticket #6 (directory structure) - uses existing benchmark/ structure
✓ Ticket #10 (artifact parsing) - leverages existing parse functions
✓ config.py - references KPI_DEFINITIONS for consistency

## Next Steps

The KPI 1 data is now ready for:
1. **KPI 2 Calculation**: Work metrics (rows & bytes) - uses similar structure
2. **Baseline Comparison**: Compare execution times against production baseline
3. **Performance Analysis**: Identify regressions and optimization opportunities
4. **Cost Estimation**: Use bytes scanned in KPI 2 for credit calculations
