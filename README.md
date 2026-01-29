# Portfolio Analytics - Production dbt Project

Real-world production dbt project with 3 pipelines of increasing complexity.

## Project Structure

**Pipeline A (Simple):** 4 models
```
Staging: stg_portfolios, stg_cashflows
Intermediate: int_portfolio_attributes, int_cashflow_aggregated
```

**Pipeline B (Medium):** 9 models
```
Staging: stg_trades, stg_securities, stg_market_prices, stg_brokers
Intermediate: int_trades_enriched, int_trade_metrics, int_trade_summary
Marts: fact_trades, report_trading_performance
```

**Pipeline C (Complex):** 16 models
```
Staging: stg_positions_daily, stg_valuations, stg_benchmarks, stg_benchmark_returns, stg_portfolio_benchmarks
Intermediate: int_position_enriched, int_position_returns, int_portfolio_returns,
             int_benchmark_metrics, int_relative_performance, int_sector_allocation, int_risk_metrics
Marts: fact_portfolio_performance, fact_position_snapshot, fact_sector_performance, report_portfolio_analytics
```

## Characteristics

- **Linear Dependencies:** Clean staging → intermediate → marts → reports flow
- **Production-Ready:** Real-world patterns including intentional optimization opportunities
- **Portable Design:** Focus on maintainability over early optimization
- **Snowflake Target:** Optimized for Snowflake with window functions, CTEs, and advanced analytics

## Key Optimization Opportunities

These patterns demonstrate typical challenges companies face:
- Heavy joins at fact layer (can be pushed upstream)
- Redundant window function calculations
- Late aggregation (pre-aggregate in intermediate layer)
- Repeated calculations across pipelines

## Quick Start

```bash
dbt deps
dbt seed
dbt run
dbt test
```

## Running

```bash
# Build all pipelines
dbt run

# Build specific pipeline
dbt run --select tag:pipeline_a
dbt run --select tag:pipeline_b
dbt run --select tag:pipeline_c

# Build specific layer across all pipelines
dbt run --select tag:staging
dbt run --select tag:intermediate
dbt run --select tag:marts

# Run tests
dbt test

# Generate docs
dbt docs generate && dbt docs serve
```

## Benchmarking

The project includes benchmarking support for testing optimizations across 3 pipelines. Each benchmark captures 5 KPIs:

1. **KPI 1:** Execution time (seconds)
2. **KPI 2:** Work metrics (rows returned, bytes scanned)
3. **KPI 3:** Output validation (SHA256 hash for equivalence checking)
4. **KPI 4:** Query complexity (JOINs, CTEs, window functions)
5. **KPI 5:** Cost estimation (Snowflake credits)

### Benchmark Workflow

```bash
# 1. Capture baseline metrics (unoptimized)
python extract_report.py --pipeline a --output benchmark/pipeline_a/baseline/report.json
python extract_report.py --pipeline b --output benchmark/pipeline_b/baseline/report.json
python extract_report.py --pipeline c --output benchmark/pipeline_c/baseline/report.json

# 2. Apply optimizations to models
# (Edit models in models/pipeline_*/intermediate or models/pipeline_*/marts)

# 3. Capture candidate metrics (optimized)
python extract_report.py --pipeline a --output benchmark/pipeline_a/candidate/report.json
python extract_report.py --pipeline b --output benchmark/pipeline_b/candidate/report.json
python extract_report.py --pipeline c --output benchmark/pipeline_c/candidate/report.json

# 4. Compare baseline vs candidate
python benchmark/compare_kpis.py
```

### Benchmark Directory Structure

```
benchmark/
├── pipeline_a/
│   ├── baseline/
│   │   └── report.json         # Baseline metrics
│   ├── candidate/
│   │   └── report.json         # Optimized metrics
│   └── pipeline.yaml           # Pipeline configuration
├── pipeline_b/
│   ├── baseline/
│   │   └── report.json
│   ├── candidate/
│   │   └── report.json
│   └── pipeline.yaml
├── pipeline_c/
│   ├── baseline/
│   │   └── report.json
│   ├── candidate/
│   │   └── report.json
│   └── pipeline.yaml
├── compare.py                  # Legacy comparison script
├── compare_kpis.py            # KPI comparison tool
└── extract.sql                # Helper queries
```

## Total Models

- **Staging:** 11 models (stg_*)
- **Intermediate:** 12 models (int_*)
- **Marts:** 6 models (fact_*, report_*)

**Total:** 29 models across 3 pipelines
