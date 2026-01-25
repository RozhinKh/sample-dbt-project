# Artemis Sample Project - dbt + Snowflake

Production-ready dbt project for SQL optimization benchmarking.

## Quick Start

```bash
# Set credentials
export SNOWFLAKE_ACCOUNT="IHB62607"
export SNOWFLAKE_USER="diana"
export SNOWFLAKE_PASSWORD="[your password]"

# Run pipeline
bash run_pipeline.sh
```

## What It Does

1. **dbt deps** - Install packages
2. **dbt seed** - Load 13 CSV files to Snowflake
3. **dbt run** - Build 14 models
4. **dbt test** - Run 65 data quality tests
5. **Report** - Extract `FACT_CASHFLOW_SUMMARY` to JSON

## Output

**Report location:** `benchmark/candidate/report.json`

Contains full snapshot of final fact table for comparison/benchmarking.

## Project Structure

```
models/
  └── pipeline_a/
      ├── staging/       (9 views - data transformation)
      └── marts/         (1 table - FACT_CASHFLOW_SUMMARY)

seeds/
  └── 13 CSV files      (2020-2024 financial data)

benchmark/
  ├── extract.sql      (query for report)
  └── candidate/       (output reports)
```

## For Artemis

- Modify SQL in `models/`
- Run `bash run_pipeline.sh`
- Compare `benchmark/candidate/report.json` with baseline
- Tests ensure output integrity

## Status

✅ 14/14 models
✅ 65/65 tests passing
✅ Zero errors
✅ Ready for optimization
