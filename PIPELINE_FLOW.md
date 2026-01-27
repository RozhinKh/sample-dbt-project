# Pipeline Benchmarking Flow - Complete Guide

## Overview

The benchmarking flow has 5 phases:
1. **Generate Baseline** (capture current metrics)
2. **Optimize SQL** (make improvements)
3. **Generate Candidate Reports** (capture optimized metrics)
4. **Compare Results** (baseline vs candidate)
5. **Analyze Impact** (document improvements)

---

## PHASE 1: GENERATE BASELINE (Current State)

### Step 1: Load seed data
```bash
dbt seed
```
→ Loads all CSV files from `seeds/` into Snowflake

### Step 2: Run Pipeline A
```bash
dbt run --select pipeline_a
```
Executes in order:
- `stg_cashflows` (view)
- `stg_portfolios` (view)
- `fact_cashflow_summary` (table)
- `report_monthly_cashflows` (view)

Output: `FACT_CASHFLOW_SUMMARY` table (426 rows)

### Step 3: Extract baseline metrics for Pipeline A
```bash
python extract_report.py --pipeline a
```
→ Queries Snowflake `QUERY_PROFILE` for:
- Execution time
- Bytes scanned
- Rows returned
- Complexity metrics

→ Generates: `benchmark/pipeline_a/baseline/report.json`

Contains 5 KPIs:
- **KPI 1:** 3.2 seconds (runtime)
- **KPI 2:** 512 KB scanned
- **KPI 3:** SHA256 hash of output
- **KPI 4:** Complexity score 5.5
- **KPI 5:** 0.00356 credits

### Step 4-6: Repeat for Pipeline B & C
```bash
# Pipeline B
dbt run --select pipeline_b
python extract_report.py --pipeline b
→ benchmark/pipeline_b/baseline/report.json

# Pipeline C
dbt run --select pipeline_c
python extract_report.py --pipeline c
→ benchmark/pipeline_c/baseline/report.json
```

### Result after Phase 1:
```
benchmark/
├── pipeline_a/baseline/report.json  ✅ Baseline captured
├── pipeline_b/baseline/report.json  ✅ Baseline captured
└── pipeline_c/baseline/report.json  ✅ Baseline captured
```

---

## PHASE 2: OPTIMIZE SQL (Make Improvements)

### Step 1: Review optimization opportunities
```bash
cat benchmark/pipeline_a/pipeline.yaml
```

Example for Pipeline A:
```yaml
opportunities:
  - "Remove unnecessary DISTINCT in stg_cashflows"
  - "Push date filters upstream before transformation"
  - "Consolidate redundant date calculations (7 per row)"
  - "Apply aggregation before JOIN instead of after"

expected_runtime_reduction: "35%"
expected_bytes_reduction: "40%"
expected_credits_reduction: "35%"
```

### Step 2: Modify SQL models

**Edit:** `models/pipeline_a/staging/stg_cashflows.sql`
```sql
-- BEFORE: with select distinct ...
-- AFTER: remove distinct (source is already unique)
```

**Edit:** `models/pipeline_a/marts/fact_cashflow_summary.sql`
```sql
-- BEFORE: join all rows, then aggregate
-- AFTER: aggregate first, then join (fewer rows)
```

### Step 3: Verify correctness
```bash
dbt run --select pipeline_a
dbt test --select pipeline_a
```
→ Must produce identical outputs
→ SHA256 hash should NOT change

### Step 4: Repeat for Pipeline B & C
```bash
# Edit models/pipeline_b/...
# Edit models/pipeline_c/...
dbt run --select pipeline_b
dbt run --select pipeline_c
dbt test --select pipeline_b
dbt test --select pipeline_c
```

---

## PHASE 3: GENERATE CANDIDATE REPORTS (After Optimization)

### Step 1: Generate candidate for Pipeline A
```bash
python extract_report.py --pipeline a --output benchmark/pipeline_a/candidate/report.json
```

→ Same extraction as baseline, but from **optimized SQL**
→ Captures new metrics from QUERY_PROFILE

### Step 2: Generate candidates for Pipeline B & C
```bash
python extract_report.py --pipeline b --output benchmark/pipeline_b/candidate/report.json
python extract_report.py --pipeline c --output benchmark/pipeline_c/candidate/report.json
```

### Result after Phase 3:
```
benchmark/
├── pipeline_a/
│   ├── baseline/report.json  ← Original metrics
│   └── candidate/report.json ← Optimized metrics
├── pipeline_b/
│   ├── baseline/report.json
│   └── candidate/report.json
└── pipeline_c/
    ├── baseline/report.json
    └── candidate/report.json
```

---

## PHASE 4: COMPARE BASELINE vs CANDIDATE

### Step 1: Compare Pipeline A
```bash
python benchmark/compare_kpis.py \
  benchmark/pipeline_a/baseline/report.json \
  benchmark/pipeline_a/candidate/report.json
```

**Output example:**
```
Pipeline A Optimization Results
─────────────────────────────────
KPI 1 - Execution Time
  Baseline:  3.2 seconds
  Candidate: 2.1 seconds
  Improvement: 34.4% ⬇️  (expected: 35%)

KPI 2 - Work Metrics
  Baseline:  512 KB scanned
  Candidate: 307 KB scanned
  Improvement: 40.0% ⬇️  (expected: 40%)

KPI 3 - Output Hash
  Baseline:  5c66064a4479f0b58592299bbac97cffbb8cbb0813cfaa6fd3c7bbb09858272f
  Candidate: 5c66064a4479f0b58592299bbac97cffbb8cbb0813cfaa6fd3c7bbb09858272f
  Status: ✅ IDENTICAL (correctness verified)

KPI 4 - Complexity Score
  Baseline:  5.5
  Candidate: 4.2
  Improvement: 23.6% ⬇️  (simpler code)

KPI 5 - Cost Estimation
  Baseline:  0.00356 credits ($0.002/run)
  Candidate: 0.00231 credits ($0.001/run)
  Improvement: 35.1% ⬇️  (expected: 35%)
```

### Step 2: Compare Pipeline B
```bash
python benchmark/compare_kpis.py \
  benchmark/pipeline_b/baseline/report.json \
  benchmark/pipeline_b/candidate/report.json
```

Expected improvements: ~42%

### Step 3: Compare Pipeline C
```bash
python benchmark/compare_kpis.py \
  benchmark/pipeline_c/baseline/report.json \
  benchmark/pipeline_c/candidate/report.json
```

Expected improvements: ~58%

---

## PHASE 5: ANALYZE RESULTS (All 3 Pipelines)

### Summary Table

```
Pipeline      Baseline      Candidate     Improvement
─────────────────────────────────────────────────────
A (Simple)    3.2s          2.1s          34.4% ⬇️
B (Medium)    12.7s         7.4s          41.7% ⬇️
C (Complex)   48.3s         20.3s         58.0% ⬇️
```

### Cost Impact (Monthly - 20 runs each)

```
Pipeline A:
  Baseline:  0.00356 credits × 20 = $0.40/month
  Candidate: 0.00231 credits × 20 = $0.26/month
  Saving: $0.14/month ($1.68/year)

Pipeline B:
  Baseline:  0.01411 credits × 20 = $1.60/month
  Candidate: 0.00818 credits × 20 = $0.93/month
  Saving: $0.67/month ($8.04/year)

Pipeline C:
  Baseline:  0.10733 credits × 20 = $8.60/month
  Candidate: 0.04293 credits × 20 = $3.44/month
  Saving: $5.16/month ($61.92/year)

TOTAL PORTFOLIO SAVINGS: ~$6/month or $72/year
```

---

## QUICK COMMAND REFERENCE

### Initial Setup (One-time)
```bash
# Load seed data
dbt seed
```

### Generate Baseline
```bash
# Run each pipeline
dbt run --select pipeline_a
dbt run --select pipeline_b
dbt run --select pipeline_c

# Extract baseline metrics
python extract_report.py --pipeline a
python extract_report.py --pipeline b
python extract_report.py --pipeline c
```

### Optimize SQL
```bash
# Edit models (optimize for each pipeline)
# models/pipeline_a/staging/stg_cashflows.sql
# models/pipeline_a/marts/fact_cashflow_summary.sql
# ... (repeat for B and C)

# Test optimizations
dbt run --select pipeline_a
dbt test --select pipeline_a
# ... (repeat for B and C)
```

### Generate Candidate Reports
```bash
# Extract after optimization
python extract_report.py --pipeline a --output benchmark/pipeline_a/candidate/report.json
python extract_report.py --pipeline b --output benchmark/pipeline_b/candidate/report.json
python extract_report.py --pipeline c --output benchmark/pipeline_c/candidate/report.json
```

### Compare Results
```bash
# Compare each pipeline
python benchmark/compare_kpis.py \
  benchmark/pipeline_a/baseline/report.json \
  benchmark/pipeline_a/candidate/report.json

python benchmark/compare_kpis.py \
  benchmark/pipeline_b/baseline/report.json \
  benchmark/pipeline_b/candidate/report.json

python benchmark/compare_kpis.py \
  benchmark/pipeline_c/baseline/report.json \
  benchmark/pipeline_c/candidate/report.json
```

---

## What Gets Compared (5 KPIs)

### 1️⃣ Execution Time
- **What:** How long the query takes to run (seconds)
- **Why:** Faster = better user experience
- **Baseline:** Pipeline A: 3.2s, B: 12.7s, C: 48.3s
- **Expected improvement:** A: 35%, B: 42%, C: 58%

### 2️⃣ Work Metrics (Bytes Scanned)
- **What:** How much data Snowflake scans (KB/MB)
- **Why:** Snowflake charges by bytes scanned
- **Baseline:** Pipeline A: 512KB, B: 4.85MB, C: 95MB
- **Expected improvement:** A: 40%, B: 55%, C: 72%

### 3️⃣ Output Hash (SHA256)
- **What:** Cryptographic hash of the entire result set
- **Why:** Guarantees outputs are identical (no data corruption)
- **Rule:** Baseline hash MUST equal candidate hash
- **If different:** Optimization introduced a bug

### 4️⃣ Query Complexity Score
- **What:** Number of JOINs, CTEs, window functions
- **Why:** Simpler queries = faster + easier to maintain
- **Baseline:** A: 5.5, B: 18.5, C: 57.5
- **Expected improvement:** Should decrease

### 5️⃣ Estimated Credits
- **What:** Snowflake compute cost in credits
- **Formula:** runtime_seconds × credits_per_second
- **Example:** 3.2s × 4 credits/sec = 0.00356 credits
- **Expected improvement:** A: 35%, B: 42%, C: 58%

---

## Important Rules

✅ **Output hash MUST be identical**
- If hashes differ: optimization broke something
- Always run `dbt test` to verify correctness
- Use SHA256 hash as final validation

✅ **Each pipeline is independent**
- Can optimize A without touching B or C
- Can run them in any order
- No dependencies between pipelines

✅ **Baseline metrics are frozen**
- Don't re-run baseline extraction
- Keep baseline files as reference point
- Baseline = starting point for comparison

✅ **Candidate represents "after optimization"**
- Generated from optimized SQL models
- Can generate multiple candidates if iterating
- Keep history for tracking progress

✅ **Compare script provides context**
- Shows absolute numbers (3.2s → 2.1s)
- Shows percentage improvement (34.4%)
- Shows expected vs actual improvement
- Highlights if expectations were met

---

## Workflow Diagram

```
┌─────────────────┐
│  1. Load Data   │
│   dbt seed      │
└────────┬────────┘
         ↓
┌─────────────────────────────────┐
│  2. Run Baseline                │
│  - dbt run --select pipeline_a  │
│  - python extract_report.py     │
│  → benchmark/pipeline_a/baseline/report.json
└────────┬────────────────────────┘
         ↓
┌─────────────────────────────────┐
│  3. Optimize SQL                │
│  - Edit models/**/*.sql         │
│  - dbt run --select pipeline_a  │
│  - dbt test (verify correctness)│
└────────┬────────────────────────┘
         ↓
┌─────────────────────────────────┐
│  4. Generate Candidate          │
│  - python extract_report.py     │
│  → benchmark/pipeline_a/candidate/report.json
└────────┬────────────────────────┘
         ↓
┌─────────────────────────────────┐
│  5. Compare                     │
│  - python compare_kpis.py       │
│  → Shows 5 KPI improvements     │
└────────┬────────────────────────┘
         ↓
┌─────────────────────────────────┐
│  6. Document Results            │
│  - Save comparison output       │
│  - Record lessons learned       │
│  - Plan next iteration          │
└─────────────────────────────────┘
```

---

## Summary

1. **Baseline** = Starting point (current SQL performance)
2. **Optimize** = Improve SQL code based on opportunities
3. **Candidate** = New performance metrics (optimized SQL)
4. **Compare** = Measure improvement (5 KPIs)
5. **Iterate** = Repeat if needed, or move to next pipeline

Each pipeline runs independently with the same 5 KPIs being compared.
