# Quick Start - Benchmark Report Generator

## TL;DR

### Run All Benchmarks
```bash
cd benchmark
python run_all_benchmarks.py
```

### Run Single Pipeline Benchmark
```bash
cd benchmark
python gen_report_b.py  # For Pipeline B
python gen_report_a.py  # For Pipeline A
python gen_report_c.py  # For Pipeline C
```

---

## Typical Workflow

### Step 1: Optimize Your Models
Edit models in `models/pipeline_b/` (or A/C)

### Step 2: Run Benchmark
```bash
cd benchmark
python gen_report_b.py
```

### Step 3: Check Report
Open `pipeline_b/candidate/report.json` and review:
- Runtime (KPI_1)
- Row counts (KPI_2)
- Complexity score (KPI_4)
- Estimated costs (KPI_5)

### Step 4: Compare Improvements
Compare with previous runs or baseline to see improvement

### Step 5: Repeat
Go back to Step 1 and optimize further

---

## Report Files

Each pipeline has its own directory:
- `pipeline_a/candidate/report.json` ← Latest run results
- `pipeline_a/baseline/report.json` ← Reference baseline
- `pipeline_b/candidate/report.json`
- `pipeline_b/baseline/report.json`
- `pipeline_c/candidate/report.json`
- `pipeline_c/baseline/report.json`

---

## What Gets Measured

| KPI | Metric | Measures |
|-----|--------|----------|
| 1 | Execution Time | How fast pipeline runs |
| 2 | Work Metrics | Rows & bytes processed |
| 3 | Output Hash | Data uniqueness validation |
| 4 | Complexity | JOINs, CTEs, functions |
| 5 | Cost Estimate | Snowflake credits needed |

---

## Tips

✓ Run benchmarks after optimizing models
✓ Check timestamps to verify fresh run
✓ Compare metrics between runs to measure improvement
✓ Use `run_all_benchmarks.py` to test all pipelines together
✓ Baselines help track progress over time

---

## Examples

### Optimize Pipeline B
```bash
# 1. Edit models
vim ../models/pipeline_b/marts/fact_trades.sql

# 2. Run benchmark
python gen_report_b.py

# 3. Check improvement
cat pipeline_b/candidate/report.json
```

### Track Progress
```bash
# Run multiple times and compare
python gen_report_b.py  # First run
# ... make optimizations ...
python gen_report_b.py  # Second run - compare results
```
