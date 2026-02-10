# dbt Benchmarking System: Comprehensive Documentation

A production-grade benchmarking framework for optimizing dbt data transformation pipelines. This system measures performance across execution time, cost, data equivalence, and query complexity, enabling data engineers to quantify improvements and guide optimization efforts.

**Target Audience:** dbt users, data engineers, optimization teams  
**Framework:** Python 3.8+ with dbt, Snowflake integration  
**Output Format:** JSON reports with detailed KPI metrics, deltas, and recommendations

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Installation & Setup](#installation--setup)
3. [Configuration Guide](#configuration-guide)
4. [Usage Workflows](#usage-workflows)
5. [Output Schemas](#output-schemas)
6. [Troubleshooting](#troubleshooting)
7. [Contributing Guidelines](#contributing-guidelines)
8. [References](#references)

---

## System Overview

### Framework Architecture

The benchmarking system operates on a **4-phase comparison model** designed to isolate and measure optimization impact:

```
┌─────────────────────────────────────────────────────────────────┐
│                   BENCHMARKING SYSTEM WORKFLOW                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PHASE 1: BASELINE                                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Execute current production dbt pipeline                 │   │
│  │ Extract metrics (time, cost, rows, complexity, hash)    │   │
│  │ Output: baseline/report.json                            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            ↓                                     │
│  PHASE 2: OPTIMIZATION                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Apply dbt code changes (refactoring, materialization)   │   │
│  │ Update SQL, modify model config, adjust joins/CTEs      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            ↓                                     │
│  PHASE 3: CANDIDATE GENERATION                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Execute optimized pipeline                              │   │
│  │ Extract same metrics with new code                      │   │
│  │ Output: candidate/report.json                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            ↓                                     │
│  PHASE 4: COMPARISON & ANALYSIS                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Compare baseline vs candidate metrics                   │   │
│  │ Calculate deltas (% change), detect bottlenecks         │   │
│  │ Generate recommendations for further optimization       │   │
│  │ Output: analysis.json with detailed findings            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Key Concepts

**5 Core KPIs (Key Performance Indicators)**

1. **Execution Time** - Total query runtime in seconds
   - Weight: 30% of overall analysis
   - Target: 5% reduction via optimization
   - Regression threshold: >10% increase

2. **Work Metrics** - Rows produced and bytes scanned
   - Weight: 25% of overall analysis
   - Rows should remain consistent (data equivalence)
   - Bytes scanned directly impacts cost

3. **Data Equivalence** - SHA256 hash validation
   - Weight: 20% of overall analysis
   - Critical: Hash mismatch = incorrect optimization
   - Ensures refactoring doesn't change output

4. **Query Complexity** - JOINs, CTEs, window functions
   - Weight: 15% of overall analysis
   - High complexity = harder to maintain and debug
   - Target: 10% simplification

5. **Cost Estimation** - Snowflake credits consumed
   - Weight: 10% of overall analysis
   - Calculated from bytes scanned (1 credit per 10 GB)
   - Regression threshold: >20% increase

### Directory Structure

```
benchmark/
├── README.md (this file)
├── generate_report.py       # Main report generation script
├── compare.py              # Comparison and analysis script
├── logs/                   # Execution logs (auto-created)
├── schemas/
│   ├── report.json.schema  # JSON Schema validation
│   └── example-report.json # Sample output for reference
├── pipeline_a/             # Pipeline A test environment
│   ├── baseline/           # Baseline execution results
│   │   └── report.json
│   └── candidate/          # Candidate execution results
│       └── report.json
├── pipeline_b/             # Pipeline B test environment
│   ├── baseline/
│   │   └── report.json
│   └── candidate/
│       └── report.json
└── pipeline_c/             # Pipeline C test environment
    ├── baseline/
    │   └── report.json
    └── candidate/
        └── report.json
```

---

## Installation & Setup

### Prerequisites

- **Python:** 3.8 or higher
- **dbt:** 1.5+ (requires `manifest.json` and `run_results.json` artifacts)
- **Snowflake:** Account with permissions to run queries (optional, for hash calculation)
- **OS:** Linux, macOS, or Windows with Python environment

### Step 1: Install Python Dependencies

```bash
# Using pip (from project root)
pip install -r requirements.txt

# Or install dependencies directly
pip install pyyaml jsonschema pathlib
```

**Required packages:**
- `pyyaml` - YAML profile parsing for Snowflake credentials
- `jsonschema` - JSON Schema validation for output files
- Standard library: `json`, `logging`, `argparse`, `pathlib`, `hashlib`

### Step 2: Configure dbt Project

Ensure your dbt project has proper tagging structure:

```yaml
# dbt_project.yml
version: '1.0.0'
name: 'my_project'

models:
  my_project:
    staging:
      tags: ['pipeline_a', 'daily']
    intermediate:
      tags: ['pipeline_a', 'pipeline_b']
    marts:
      tags: ['pipeline_b', 'pipeline_c']
```

**Pipeline Tags:** Models must be tagged with `pipeline_a`, `pipeline_b`, or `pipeline_c` to be included in benchmarking reports.

### Step 3: Generate dbt Artifacts

The benchmarking system requires dbt artifacts to analyze models:

```bash
# Parse and compile (creates manifest.json)
dbt parse

# Execute full pipeline (creates run_results.json)
dbt run
```

After running these commands, verify artifacts exist:

```bash
# Check for required files
ls -la target/manifest.json
ls -la target/run_results.json
```

### Step 4: Configure Snowflake Credentials (Optional)

For hash calculation and data validation, configure Snowflake credentials:

```bash
# Standard dbt profile location (~/.dbt/profiles.yml)
# Or in project root as profiles.yml

my_snowflake_profile:
  outputs:
    dev:
      type: snowflake
      account: [account_id]
      user: [user_email]
      password: [password]
      role: [role]
      database: [database_name]
      schema: [schema_name]
      warehouse: [warehouse_name]
      threads: 4
  target: dev
```

---

## Configuration Guide

### Configuration File: `config.py`

The `config.py` file (in project root) contains all benchmarking parameters and thresholds. Each section can be overridden via environment variables.

### KPI Definitions

```python
# From config.py - Edit thresholds to match your requirements

KPI_DEFINITIONS = {
    "execution_time": {
        "name": "Execution Time",
        "description": "Total query execution time in seconds",
        "units": "seconds",
        "weight": 0.30,  # 30% of overall score
        "baseline_expectation": "Current production baseline",
        "acceptable_range": {"min": 0, "max": None}
    },
    "work_metrics": {
        "name": "Work Metrics",
        "description": "Rows returned and bytes scanned",
        "units": "rows, bytes",
        "weight": 0.25,
        "metric_keys": ["row_count", "bytes_scanned"]
    },
    "data_equivalence": {
        "name": "Data Equivalence",
        "description": "SHA256 hash validation",
        "units": "SHA256 hash",
        "weight": 0.20,
        "baseline_expectation": "Must match baseline hash exactly"
    },
    "query_complexity": {
        "name": "Query Complexity",
        "description": "JOINs, CTEs, window functions count",
        "units": "count",
        "weight": 0.15
    },
    "cost_estimation": {
        "name": "Cost Estimation",
        "description": "Snowflake credits and estimated cost",
        "units": "credits, USD",
        "weight": 0.10
    }
}
```

### Bottleneck Detection Thresholds

Configure when regressions should be flagged:

```python
BOTTLENECK_THRESHOLDS = {
    "execution_time": {
        "regression_threshold_percent": 10,  # Flag if >10% slower
        "severity": "HIGH"
    },
    "cost": {
        "regression_threshold_percent": 20,  # Flag if >20% more expensive
        "severity": "MEDIUM"
    },
    "data_equivalence": {
        "mismatch_flag": True,  # Flag any hash mismatch
        "severity": "CRITICAL"
    }
}
```

### Snowflake Pricing Configuration

Default pricing for cost calculation:

```python
SNOWFLAKE_PRICING = {
    "standard": {
        "edition": "Standard Edition",
        "cost_per_credit": 2.0,  # $ per credit
        "cost_per_credit_min": 2.0,
        "cost_per_credit_max": 3.0
    },
    "enterprise": {
        "edition": "Enterprise Edition",
        "cost_per_credit": 3.0,  # Higher cost, more features
        "cost_per_credit_min": 3.0,
        "cost_per_credit_max": 4.0
    },
    "credit_calculation": {
        "formula": "credits = bytes_scanned / (1024^3) / 10",
        "bytes_per_gb": 1073741824,
        "gb_per_credit": 10
    }
}

# Cost calculation example:
# 1 TB bytes_scanned = 1024 GB / 10 = 102.4 credits
# 102.4 credits × $2/credit = $204.80
```

### Environment Variable Overrides

Override configuration values without modifying `config.py`:

```bash
# Set execution time regression threshold
export BOTTLENECK_EXECUTION_TIME_THRESHOLD=15

# Set cost regression threshold
export BOTTLENECK_COST_THRESHOLD=25

# Use enterprise pricing
export SNOWFLAKE_PRICING_EDITION=enterprise

# Set KPI execution_time weight
export KPI_EXECUTION_TIME_WEIGHT=0.40

# Example: Run with enterprise pricing
export SNOWFLAKE_PRICING_EDITION=enterprise
python benchmark/generate_report.py --pipeline a
```

---

## Usage Workflows

### Workflow 1: Generate Baseline Report

**Objective:** Create baseline metrics from current production code

```bash
# Step 1: Ensure dbt artifacts are current
dbt run

# Step 2: Generate baseline report for pipeline_a
cd benchmark
python generate_report.py --pipeline a

# Output: benchmark/pipeline_a/baseline/report.json
```

**Expected Output:**

```json
{
  "schema_version": "1.0.0",
  "metadata": {
    "timestamp": "2024-01-15T14:30:22.123456Z",
    "report_id": "baseline_pipeline_a_123456",
    "pipeline_name": "pipeline_a",
    "models_processed": 12,
    "total_duration_seconds": 45.3
  },
  "models": [
    {
      "model_name": "stg_trades",
      "execution_time_seconds": 2.345,
      "rows_produced": 1000000,
      "bytes_scanned": 536870912,
      "estimated_credits": 0.5,
      "estimated_cost_usd": 1.00,
      "join_count": 2,
      "cte_count": 1,
      "window_function_count": 0
    }
    // ... more models
  ],
  "summary": {
    "total_execution_time_seconds": 19.403,
    "total_estimated_cost_usd": 6.816,
    "data_quality_score": 83
  }
}
```

### Workflow 2: Optimize and Generate Candidate Report

**Objective:** Measure impact of optimization changes

```bash
# Step 1: Make optimization changes to dbt models
# Example: Consolidate JOINs, add materialized views, etc.
# Edit models/marts/fact_trades.sql, models/staging/stg_*.sql, etc.

# Step 2: Execute optimized pipeline
dbt run

# Step 3: Generate candidate report
cd benchmark
python generate_report.py --pipeline a --output pipeline_a/candidate/report.json

# Output: benchmark/pipeline_a/candidate/report.json
```

### Workflow 3: Compare Baseline vs Candidate

**Objective:** Quantify improvements and identify remaining bottlenecks

```bash
# Generate comparison analysis
cd benchmark
python compare.py \
  pipeline_a/baseline/report.json \
  pipeline_a/candidate/report.json \
  --output pipeline_a/analysis.json

# With recommendations flag
python compare.py \
  pipeline_a/baseline/report.json \
  pipeline_a/candidate/report.json \
  --output pipeline_a/analysis.json \
  --recommendations

# With custom log level
python compare.py \
  pipeline_a/baseline/report.json \
  pipeline_a/candidate/report.json \
  --log-level DEBUG
```

**Console Output Example:**

```
================================================================================
BASELINE vs CANDIDATE COMPARISON
================================================================================
Models processed: 12
Comparison date: 2024-01-15 14:35:00
Overall summary: 10 models improved (83.3%)
Total cost delta: $-1.24
================================================================================

================================================================================
SUMMARY STATISTICS
================================================================================
Total models analyzed:       12
✓ Improved:                  10 (83.3%)
✗ Regressed:                 1 (8.3%)
⚠ Neutral:                   1 (8.3%)
Total cost delta:            $-1.24
Average improvement:         -8.50%
================================================================================
```

### Workflow 4: All-Pipelines Comparison

**Objective:** Benchmark all three pipelines in one run

```bash
#!/bin/bash
# benchmark_all_pipelines.sh

for pipeline in a b c; do
  echo "Generating baseline for pipeline_$pipeline..."
  python benchmark/generate_report.py --pipeline $pipeline
  
  # After optimization...
  echo "Generating candidate for pipeline_$pipeline..."
  python benchmark/generate_report.py --pipeline $pipeline \
    --output benchmark/pipeline_${pipeline}/candidate/report.json
  
  echo "Comparing pipeline_$pipeline..."
  python benchmark/compare.py \
    benchmark/pipeline_${pipeline}/baseline/report.json \
    benchmark/pipeline_${pipeline}/candidate/report.json \
    --output benchmark/pipeline_${pipeline}/analysis.json \
    --recommendations
done
```

Run the script:

```bash
chmod +x benchmark_all_pipelines.sh
./benchmark_all_pipelines.sh
```

### CLI Flags Reference

#### `generate_report.py` Flags

```bash
# Required flag
--pipeline {a|b|c}         Pipeline identifier to benchmark

# Optional flags
--output PATH              Custom output path (default: benchmark/pipeline_{pipeline}/baseline/report.json)
--config PATH              Config file path (default: config.py)
--log-level {DEBUG|INFO|WARNING|ERROR}  Logging verbosity (default: INFO)
```

**Examples:**

```bash
# Minimal (uses all defaults)
python generate_report.py --pipeline b

# With custom output
python generate_report.py --pipeline c --output results/my_report.json

# With verbose logging
python generate_report.py --pipeline a --log-level DEBUG

# Full options
python generate_report.py --pipeline b --output custom/path/report.json --log-level INFO
```

#### `compare.py` Flags

```bash
# Required positional arguments
baseline_report.json       Path to baseline report (or use --baseline)
candidate_report.json      Path to candidate report (or use --candidate)

# Optional flags
--baseline PATH            Path to baseline report
--candidate PATH           Path to candidate report
--output PATH              Output file for analysis.json
--recommendations          Generate optimization recommendations
--log-level {DEBUG|INFO|WARNING|ERROR}  Logging verbosity (default: INFO)
```

**Examples:**

```bash
# Positional arguments
python compare.py baseline.json candidate.json

# Named arguments
python compare.py --baseline baseline.json --candidate candidate.json

# With recommendations and custom output
python compare.py baseline.json candidate.json \
  --recommendations \
  --output custom/analysis.json

# Verbose logging
python compare.py baseline.json candidate.json --log-level DEBUG
```

---

## Output Schemas

### Report Schema: `report.json`

The `report.json` file contains metrics for a single pipeline execution.

#### File Structure

```
report.json (top level)
├── schema_version: "1.0.0"
├── metadata: {...}          # Execution context and timing
├── models: [...]            # Per-model metrics array
├── summary: {...}           # Aggregated statistics
├── data_quality_flags: [...] # Validation warnings
└── warnings_and_errors: [...] # Processing issues
```

#### Metadata Section

```json
{
  "metadata": {
    "timestamp": "2024-01-15T14:30:22.123456Z",  // ISO 8601 timestamp
    "report_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",  // Unique report ID
    "pipeline_name": "pipeline_a",  // Which pipeline (a/b/c)
    "models_processed": 12,  // Total models extracted
    "total_duration_seconds": 45.3,  // Report generation time
    "dbt_artifacts_version": "1.6",
    "dbt_version": "1.6.2"
  }
}
```

#### Models Array

Each model in the `models` array contains:

```json
{
  "model_name": "stg_trades",
  "model_id": "model.portfolio_analytics.stg_trades",
  "model_type": "table",  // "table", "view", "snapshot"
  "model_layer": "staging",  // dbt layer/subdirectory
  "status": "success",  // "success" or "error"
  
  // KPI 1: Execution Time
  "execution_time_seconds": 2.345,
  
  // KPI 2: Work Metrics
  "rows_produced": 1000000,
  "bytes_scanned": 536870912,
  
  // KPI 3: Data Equivalence
  "output_hash": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "hash_calculation_method": "snowflake_query",  // or "unavailable"
  
  // KPI 4: Query Complexity
  "join_count": 2,
  "cte_count": 1,
  "window_function_count": 0,
  
  // KPI 5: Cost Estimation
  "estimated_credits": 0.5,
  "estimated_cost_usd": 1.00,
  
  // Metadata
  "materialization": "table",
  "tags": ["pipeline_a", "staging"],
  "dependencies": 2  // Number of upstream models
}
```

#### Summary Section

Aggregated statistics across all models:

```json
{
  "summary": {
    "total_execution_time_seconds": 19.403,
    "total_models_processed": 6,
    "models_with_errors": 0,
    "models_with_incomplete_data": 1,
    "total_rows_produced": 5100100,
    "total_bytes_scanned": 3630893824,
    "total_estimated_credits": 3.408,
    "total_estimated_cost_usd": 6.816,
    "average_execution_time_seconds": 3.234,
    "average_model_complexity": 2.833,
    "data_quality_score": 83,  // 0-100, based on hash validation
    "hash_validation_success_rate": 0.833
  }
}
```

### Analysis Schema: `analysis.json`

Generated by `compare.py`, contains baseline vs candidate comparison.

#### File Structure

```
analysis.json
├── metadata: {...}           # Execution context
├── baseline_report: {...}    # Full baseline report (snapshot)
├── candidate_report: {...}   # Full candidate report (snapshot)
├── model_comparisons: [...]  # Per-model deltas
├── summary_statistics: {...} # Aggregate improvement metrics
├── bottlenecks: [...]        # Detected regressions
├── recommendations: [...]    # Optimization suggestions
├── data_quality_warnings: [...] // Issues detected
└── warnings_and_errors: [...]
```

#### Model Comparisons

Per-model KPI deltas:

```json
{
  "model_comparisons": [
    {
      "model_name": "stg_trades",
      "baseline_metrics": {
        "execution_time_seconds": 2.345,
        "estimated_cost_usd": 1.00,
        "bytes_scanned": 536870912,
        "rows_produced": 1000000
      },
      "candidate_metrics": {
        "execution_time_seconds": 1.890,
        "estimated_cost_usd": 0.81,
        "bytes_scanned": 430046720,
        "rows_produced": 1000000
      },
      "deltas": {
        "execution_time_seconds": -19.4,  // % change, negative = improvement
        "estimated_cost_usd": -19.0,
        "bytes_scanned": -19.8,
        "rows_produced": 0.0
      },
      "status": "improved",  // "improved", "regressed", "neutral"
      "data_hash_match": true  // Critical for correctness
    }
  ]
}
```

#### Summary Statistics

```json
{
  "summary_statistics": {
    "total_models": 12,
    "improved_count": 10,
    "improved_percent": 83.3,
    "regressed_count": 1,
    "regressed_percent": 8.3,
    "neutral_count": 1,
    "neutral_percent": 8.3,
    "total_cost_delta": -1.24,  // $ change
    "avg_improvement_percent": -8.5,
    "highest_improvement_model": "stg_trades",
    "highest_improvement_percent": -19.4,
    "highest_regression_model": "fact_trades",
    "highest_regression_percent": 5.2
  }
}
```

#### Bottlenecks Detection

Models that regressed beyond thresholds:

```json
{
  "bottlenecks": [
    {
      "model_name": "fact_trades",
      "metric": "execution_time",
      "baseline_value": 5.678,
      "candidate_value": 5.962,
      "delta_percent": 5.0,
      "threshold_percent": 10.0,
      "severity": "LOW",  // Below threshold but notable
      "recommendation": "Monitor execution time; within acceptable range"
    },
    {
      "model_name": "int_trade_metrics",
      "metric": "data_equivalence",
      "data_hash_baseline": "b2c3d4e5f6g7...",
      "data_hash_candidate": "x2y3z4a5b6c7...",
      "severity": "CRITICAL",
      "message": "Data hash mismatch - candidate output differs from baseline",
      "recommendation": "REVIEW OPTIMIZATION: Output changed. Verify business logic correctness."
    }
  ]
}
```

#### Recommendations

Optimization suggestions based on analysis:

```json
{
  "recommendations": [
    {
      "rule_id": "HIGH_JOIN_COUNT",
      "model_name": "fact_trades",
      "severity": "MEDIUM",
      "current_value": 7,
      "threshold": 5,
      "recommendation": "Consider consolidating JOINs or breaking into multiple queries.",
      "action_items": [
        "Identify redundant JOINs",
        "Consider temporary tables or materialized views",
        "Review JOIN order and conditions",
        "Profile query execution plan"
      ],
      "potential_savings_percent": 15.0
    }
  ]
}
```

### Sample Output Table

**Example: Baseline vs Candidate Comparison**

| Model Name | Metric | Baseline | Candidate | Delta | Status |
|---|---|---|---|---|---|
| stg_trades | execution_time_seconds | 2.35s | 1.89s | -19.4% | ✓ |
| stg_trades | estimated_cost_usd | $1.00 | $0.81 | -19.0% | ✓ |
| stg_securities | execution_time_seconds | 1.23s | 1.15s | -6.5% | ✓ |
| int_trades_enriched | estimated_cost_usd | $1.50 | $1.53 | +2.0% | ✗ |
| fact_trades | execution_time_seconds | 5.68s | 5.96s | +4.9% | ⚠ |
| fact_trades | rows_produced | 2,000,000 | 2,000,000 | 0.0% | ⚠ |

---

## Troubleshooting

### Error 1: Missing dbt Artifacts

**Symptom:**
```
ERROR: Missing artifact (CRITICAL): Required artifact not found: target/manifest.json
  Expected location: /path/to/project/target/manifest.json
  Remediation: Run 'dbt parse' or 'dbt run' to generate dbt artifacts
```

**Cause:** dbt artifacts have not been generated.

**Solution:**

```bash
# Regenerate dbt artifacts
dbt parse        # Generates manifest.json
dbt run          # Generates run_results.json

# Verify files exist
ls -la target/manifest.json
ls -la target/run_results.json

# Retry report generation
python benchmark/generate_report.py --pipeline a
```

### Error 2: Invalid JSON / Schema Validation Failure

**Symptom:**
```
WARNING: Schema validation errors: 5
  - models[0].execution_time_seconds: required property missing
  - summary.total_models_processed: type mismatch (expected integer, got string)
```

**Cause:** Generated report doesn't conform to schema (usually from corrupted artifact data).

**Solution:**

```bash
# Verify artifact integrity
python -c "import json; json.load(open('target/manifest.json'))"  # Should not error
python -c "import json; json.load(open('target/run_results.json'))"

# Run with verbose logging to see what failed
python benchmark/generate_report.py --pipeline a --log-level DEBUG

# Check benchmark/logs/ for detailed error messages
tail -f benchmark/logs/*pipeline_a*.log
```

### Error 3: Snowflake Authentication Failure

**Symptom:**
```
WARNING: Could not load Snowflake credentials - using default row width estimates
  Message: Snowflake account credentials not found in profiles.yml
```

**Cause:** Snowflake credentials not configured (optional feature, has fallback).

**Solution:**

```bash
# If you want Snowflake integration, configure profiles.yml

# Option A: Project root (recommended for local dev)
cat > profiles.yml << 'EOF'
my_profile:
  outputs:
    dev:
      type: snowflake
      account: xy12345
      user: user@domain.com
      password: [password_or_env_var]
      role: TRANSFORMER
      database: ANALYTICS
      schema: STAGING
      warehouse: COMPUTE_WH
  target: dev
EOF

# Option B: User home directory
mkdir -p ~/.dbt
# Copy profiles.yml to ~/.dbt/profiles.yml

# Verify credentials work
dbt debug

# Retry report generation
python benchmark/generate_report.py --pipeline a
```

### Error 4: Configuration Error or Missing Sections

**Symptom:**
```
ERROR: Configuration missing required sections: kpi_definitions, bottleneck_thresholds
  Ensure config.py contains all required definitions.
  Remediation: Check that config.py has not been corrupted or modified.
```

**Cause:** `config.py` is missing required configuration dictionaries.

**Solution:**

```bash
# Verify config.py has all required sections
python config.py  # Should output test results

# If test fails, check config.py contains:
# - KPI_DEFINITIONS
# - BOTTLENECK_THRESHOLDS
# - SNOWFLAKE_PRICING
# - OPTIMIZATION_RULES
# - IMPROVEMENT_TARGETS

# Test config loading directly
python << 'EOF'
from config import load_config
config = load_config()
print(f"✓ Config loaded: {list(config.keys())}")
EOF

# If still failing, restore backup or regenerate config.py
```

### Error 5: Data Hash Mismatch

**Symptom:**
```
CRITICAL: Data hash mismatch for model stg_trades
  Baseline: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6...
  Candidate: x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m7n8...
  Remediation: REVIEW OPTIMIZATION: Output changed. Verify business logic correctness.
```

**Cause:** Optimization changed the output data, which is a correctness issue.

**Solution:**

```bash
# First: Determine if the change is intentional
# 1. Review the SQL changes made during optimization
# 2. Verify the new output is still correct

git diff models/staging/stg_trades.sql  # Review SQL changes

# Option A: Change was a mistake, revert
git checkout models/staging/stg_trades.sql
dbt run
python benchmark/generate_report.py --pipeline a --output pipeline_a/candidate/report.json

# Option B: Change is correct, update baseline
# Accept the new output as correct and re-run as baseline
python benchmark/generate_report.py --pipeline a

# Option C: Investigate the difference
dbt snapshot  # Create snapshots of both versions for comparison
# Query the snapshots to understand what changed
```

### Validation Success Checklist

Before considering optimization complete:

```bash
# ✓ All models extracted successfully
grep "✓ Extracted" benchmark/logs/*pipeline_a*.log

# ✓ No critical data quality issues
grep "CRITICAL" benchmark/logs/*pipeline_a*.log | wc -l

# ✓ Data hash matches (if applicable)
grep "hash_validation_success_rate" pipeline_a/baseline/report.json

# ✓ Cost improvement >5%
python << 'EOF'
import json
analysis = json.load(open("pipeline_a/analysis.json"))
print(f"Cost delta: {analysis['summary_statistics']['total_cost_delta']}")
EOF

# ✓ No regressions beyond thresholds
grep "BOTTLENECK" pipeline_a/analysis.json | grep -v "LOW"
```

---

## Contributing Guidelines

### Adding New KPIs

To extend the benchmarking system with custom metrics:

**Step 1:** Define KPI in `config.py`

```python
KPI_DEFINITIONS["custom_metric"] = {
    "name": "Custom Metric",
    "description": "What this metric measures",
    "units": "measurement units",
    "metric_key": "custom_metric_value",
    "weight": 0.05,  # Sum of all weights should be ≤ 1.0
    "baseline_expectation": "Expected behavior",
    "acceptable_range": {"min": 0, "max": 100}
}
```

**Step 2:** Implement extraction in `generate_report.py`

```python
def extract_custom_metric(model: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Extract custom metric from model and manifest."""
    # Implementation
    return {
        "custom_metric_value": computed_value,
        "custom_metric_status": "success"
    }

# Add to extract_kpi_metrics()
custom_data = extract_custom_metric(model, manifest)
kpi_metrics.update(custom_data)
```

**Step 3:** Update schema validation

Edit `benchmark/schemas/report.json.schema` to include new metric fields.

**Step 4:** Test with integration tests

```python
# test_custom_metric.py
def test_extract_custom_metric():
    manifest = load_manifest("target/manifest.json")
    model = manifest["nodes"]["model.my_project.test_model"]
    result = extract_custom_metric(model, manifest)
    assert "custom_metric_value" in result
    assert result["custom_metric_status"] == "success"
```

### Adding New Optimization Rules

To add automatic optimization recommendations:

**Step 1:** Define rule in `config.py`

```python
OPTIMIZATION_RULES.append({
    "rule_id": "CUSTOM_PATTERN_DETECTION",
    "name": "Custom Pattern Rule",
    "description": "Detects custom anti-pattern in SQL",
    "metric": "join_count",  # Or custom metric
    "threshold": 5,
    "comparison_operator": "greater_than",
    "severity": "MEDIUM",
    "recommendation": "Recommendation text here",
    "action_items": [
        "Action 1",
        "Action 2"
    ],
    "optimization_technique": "Technique name",
    "sql_pattern_suggestion": [
        "before_pattern",
        "after_pattern"
    ]
})
```

**Step 2:** Test in `compare.py` recommendation generation

```bash
python << 'EOF'
from config import load_config
from recommendation import generate_recommendations

config = load_config()
metrics = {"join_count": 7}  # Exceeds threshold of 5
recommendations = generate_recommendations({"models": [{"kpis": metrics}]}, config)
assert any(r["rule_id"] == "CUSTOM_PATTERN_DETECTION" for r in recommendations)
EOF
```

### Testing Changes

Run existing tests to verify changes don't break functionality:

```bash
# Run all benchmarking tests
pytest test_*.py -v

# Test specific functionality
pytest test_delta_calculation.py -v
pytest test_bottleneck_detection.py -v
pytest test_recommendation_engine.py -v

# Run with coverage
pytest test_*.py --cov=benchmark --cov-report=html
```

---

## References

### dbt Documentation

- [dbt Models Documentation](https://docs.getdbt.com/docs/build/models)
- [dbt Tags and Selectors](https://docs.getdbt.com/reference/dbt-jinja-functions/modules#tags)
- [dbt Profiles.yml](https://docs.getdbt.com/dbt-cli/configure-your-profile)
- [dbt Artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)

### Snowflake Integration

- [Snowflake dbt Adapter](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
- [Snowflake Pricing Calculator](https://www.snowflake.com/en/pricing-calculator/)
- [Snowflake Query Profile Analysis](https://docs.snowflake.com/en/user-guide/query-history)

### JSON Schema Validation

- [JSON Schema Draft 7](https://json-schema.org/draft/2019-09/json-schema-validation.html)
- [jsonschema Python Library](https://python-jsonschema.readthedocs.io/)

### Project Files

- Main scripts: `benchmark/generate_report.py`, `benchmark/compare.py`
- Configuration: `config.py` (project root)
- Utilities: `helpers.py` (project root)
- Analysis modules: `bottleneck.py`, `delta.py`, `recommendation.py`
- Schemas: `benchmark/schemas/report.json.schema`

### Quick Command Reference

```bash
# Full benchmarking workflow
dbt run
python benchmark/generate_report.py --pipeline a
# [Make optimization changes]
dbt run
python benchmark/generate_report.py --pipeline a --output pipeline_a/candidate/report.json
python benchmark/compare.py pipeline_a/baseline/report.json pipeline_a/candidate/report.json --recommendations

# Troubleshooting
tail -f benchmark/logs/*.log
python config.py
python -c "import json; json.load(open('target/manifest.json'))"
```

---

## Additional Resources

### Sample Optimization Techniques

1. **JOIN Consolidation:** Combine multiple JOINs into single operation
2. **CTE Materialization:** Convert frequently-used CTEs to persistent tables
3. **Window Function Optimization:** Pre-aggregate window function results
4. **Partition Pruning:** Add date filters to reduce scanned data
5. **Column Selection:** Select only required columns, not `SELECT *`

### Metrics Interpretation

| Metric | Improvement | Neutral | Regression |
|--------|-----------|---------|-----------|
| Execution Time | -10% | ±5% | +10% |
| Bytes Scanned | -10% | ±5% | +10% |
| Cost | -15% | ±10% | +20% |
| Data Hash | Match | - | Mismatch |

### Support

For issues or questions:

1. Check the **Troubleshooting** section above
2. Review log files in `benchmark/logs/`
3. Examine sample report: `benchmark/schemas/example-report.json`
4. Verify configuration: `python config.py`

---

**Document Version:** 1.0.0  
**Last Updated:** 2024  
**Framework:** dbt Benchmarking System  
**Status:** Production Ready

