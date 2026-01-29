# Benchmark Report Generator

This folder contains Python scripts to generate benchmark reports for each dbt pipeline. Each script runs the pipeline, compiles models, and generates a report with real execution metrics.

## Scripts

### Individual Pipeline Scripts

- **`gen_report_a.py`** - Generates benchmark for Pipeline A (Cashflow Analytics)
  ```bash
  python gen_report_a.py
  ```
  Output: `pipeline_a/candidate/report.json`

- **`gen_report_b.py`** - Generates benchmark for Pipeline B (Trading Analytics)
  ```bash
  python gen_report_b.py
  ```
  Output: `pipeline_b/candidate/report.json`

- **`gen_report_c.py`** - Generates benchmark for Pipeline C (Portfolio Analytics)
  ```bash
  python gen_report_c.py
  ```
  Output: `pipeline_c/candidate/report.json`

### Master Script

- **`run_all_benchmarks.py`** - Runs all three pipeline benchmarks sequentially
  ```bash
  python run_all_benchmarks.py
  ```
  This will:
  1. Run Pipeline A benchmark
  2. Run Pipeline B benchmark
  3. Run Pipeline C benchmark
  4. Display summary of results

## Workflow

### For Testing & Optimization

1. **Optimize your dbt models** in the `models/` folder
2. **Run the benchmark script** for that pipeline:
   ```bash
   cd benchmark
   python gen_report_b.py  # or gen_report_a.py / gen_report_c.py
   ```
3. **Compare metrics** in `pipeline_b/candidate/report.json`
4. **Repeat** - Make improvements and re-run to measure progress

### For All Pipelines

```bash
cd benchmark
python run_all_benchmarks.py
```

## Reports Structure

Each pipeline generates a `candidate/report.json` with:

```json
{
  "metadata": {
    "timestamp": "2026-01-29T...",
    "pipeline": "Pipeline B - Trading Analytics",
    "pipeline_code": "B",
    "target_table": "FACT_TRADES",
    "model_count": 12
  },
  "kpi_1_execution": {
    "runtime_seconds": 12.5,
    "description": "End-to-end query execution time"
  },
  "kpi_2_work_metrics": {
    "rows_returned": 18234,
    "bytes_scanned": 2648000,
    "description": "Rows and bytes scanned"
  },
  "kpi_3_output_validation": {
    "row_count": 18234,
    "output_hash": "d177e11...",
    "hash_algorithm": "SHA256"
  },
  "kpi_4_complexity": {
    "num_joins": 8,
    "num_ctes": 15,
    "complexity_score": 18.5
  },
  "kpi_5_cost_estimation": {
    "credits_estimated": 0.05,
    "runtime_seconds": 12.5
  }
}
```

## What Happens When You Run a Script

1. **Runs `dbt build`** for the pipeline (compiles and executes all models)
2. **Extracts execution metrics** from dbt's `run_results.json`:
   - Total runtime per model
   - Model execution times
3. **Queries Snowflake** for table statistics:
   - Row count in fact table
   - Bytes scanned (storage metrics)
4. **Generates SHA256 hash** from execution timestamp
5. **Computes estimated Snowflake credits** based on runtime
6. **Writes report** to `pipeline_x/candidate/report.json`

## Metrics Captured

### KPI 1: Execution Time
- Total runtime to build and execute all models in the pipeline

### KPI 2: Work Metrics
- Rows returned from the fact table
- Bytes scanned (from Snowflake storage metrics or estimated)

### KPI 3: Output Validation
- Row count of fact table
- SHA256 hash of execution (changes with each run for uniqueness)

### KPI 4: Complexity
- Number of JOINs in SQL
- Number of CTEs (WITH clauses)
- Number of window functions
- Overall complexity score

### KPI 5: Cost Estimation
- Estimated Snowflake credits based on runtime
- Warehouse size (M = Medium)
- Credits per second rate

## Environment Requirements

- Python 3.6+
- dbt installed and configured
- Snowflake credentials (optional - script uses fallback estimates if unavailable)

## Notes

- Each run **overwrites** the previous candidate report
- Use `baseline/` folder to store baseline metrics for comparison
- Reports are timestamped for tracking changes over time
- All scripts run independently and can be executed in parallel
