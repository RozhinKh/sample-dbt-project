# Artemis Custom Runner Commands

Quick reference for running dbt compilation, testing, and benchmarking with Artemis.

---

## Configuration File

The `artemis.yaml` file in the project root defines the complete build and benchmark workflow.

---

## Core Commands

### 1. Compile, Test, and Generate Baseline (All Pipelines)

```bash
artemis build --config artemis.yaml
```

This command:
1. ✅ Parses dbt project (`dbt parse`)
2. ✅ Compiles all models to SQL (`dbt compile`)
3. ✅ Installs dependencies (`dbt deps`)
4. ✅ Runs data quality tests (`dbt test`)
5. ✅ Loads seed data (`dbt seed`)
6. ✅ Runs Pipeline A, B, C (`dbt run --select pipeline_a|b|c`)
7. ✅ Extracts baseline metrics (`python extract_report.py`)
8. ✅ Generates reports (`artemis_output/`)

**Duration:** ~15-20 minutes (depending on Snowflake warehouse)

**Output:**
```
benchmark/pipeline_a/baseline/report.json  ← 5 KPIs
benchmark/pipeline_b/baseline/report.json  ← 5 KPIs
benchmark/pipeline_c/baseline/report.json  ← 5 KPIs
artemis_output/benchmark_summary.json
artemis_output/kpi_comparison.html
```

---

### 2. Compile and Test Only (No Benchmarking)

```bash
artemis build --config artemis.yaml --skip-benchmark
```

Runs only:
- `dbt parse`
- `dbt compile`
- `dbt test`

**Use case:** Quick validation that SQL is syntactically correct

**Duration:** ~2-3 minutes

---

### 3. Generate Baseline Only (Without Testing)

```bash
artemis build --config artemis.yaml --target baseline
```

Runs:
- `dbt compile`
- `dbt seed`
- `dbt run --select pipeline_a|b|c`
- `python extract_report.py`

**Use case:** Capture baseline metrics without data quality tests

**Duration:** ~10-15 minutes

---

### 4. Run Specific Pipeline Only

```bash
# Pipeline A (Simple - 4 models)
artemis build --config artemis.yaml --pipeline a

# Pipeline B (Medium - 12 models)
artemis build --config artemis.yaml --pipeline b

# Pipeline C (Complex - 20 models)
artemis build --config artemis.yaml --pipeline c
```

**Use case:** Test one pipeline without running all three

**Duration:**
- A: 3-5 minutes
- B: 5-10 minutes
- C: 10-15 minutes

---

### 5. Generate Candidate Reports (After Optimization)

```bash
artemis build --config artemis.yaml --target candidate
```

First, enable candidate generation in `artemis.yaml`:
```yaml
benchmark:
  candidate:
    enabled: true  # Change from false
```

Then run:
```bash
artemis build --config artemis.yaml --target candidate
```

**Use case:** After optimizing SQL, capture new metrics for comparison

---

### 6. Full Comparison (Baseline vs Candidate)

```bash
artemis build --config artemis.yaml --include-comparison
```

Requires candidate reports to exist. Runs:
- `dbt run --select pipeline_a|b|c`
- Extract candidate metrics
- Compare baseline vs candidate
- Generate comparison reports

**Output:**
```
artemis_output/kpi_comparison.html  ← Visual comparison
artemis_output/optimization_recommendations.md
```

---

## Configuration Options in artemis.yaml

### Enable/Disable Sections

```yaml
build:
  before: [...]    # Pre-build checks
  main: [...]      # Compile and test
  after: [...]     # Run dbt test

benchmark:
  enabled: true    # Set to false to skip benchmarking
  baseline: [...]  # Baseline generation
  candidate: [...]  # Candidate generation
    enabled: false  # Set to true to enable
  comparison: [...]  # KPI comparison

validation:
  enabled: true    # Data quality validation
  output_hash:
    enabled: true  # SHA256 hash validation

reporting:
  enabled: true    # Generate reports
```

### Timeout Settings

```yaml
build:
  main:
    - command: "dbt compile"
      timeout: 600  # 10 minutes

benchmark:
  baseline:
    - command: "dbt run --select pipeline_c"
      timeout: 900  # 15 minutes for complex pipeline
```

---

## Full Workflow Example

### Step 1: Generate and Test Baseline

```bash
# Initial setup - compile, test, generate baseline
artemis build --config artemis.yaml --target baseline
```

**Output:**
- ✅ `benchmark/pipeline_a/baseline/report.json`
- ✅ `benchmark/pipeline_b/baseline/report.json`
- ✅ `benchmark/pipeline_c/baseline/report.json`

### Step 2: Optimize SQL Models

```bash
# Edit models
vim models/pipeline_a/staging/stg_cashflows.sql
vim models/pipeline_a/marts/fact_cashflow_summary.sql
# ... (repeat for pipeline_b and pipeline_c)

# Quick compile check
dbt compile
dbt test
```

### Step 3: Enable Candidate and Generate Reports

```bash
# Edit artemis.yaml
# Change: benchmark.candidate.enabled: false → true

# Run candidate generation
artemis build --config artemis.yaml --target candidate
```

**Output:**
- ✅ `benchmark/pipeline_a/candidate/report.json`
- ✅ `benchmark/pipeline_b/candidate/report.json`
- ✅ `benchmark/pipeline_c/candidate/report.json`

### Step 4: Compare Results

```bash
# Full comparison with report generation
artemis build --config artemis.yaml --include-comparison
```

**Output:**
- ✅ `artemis_output/kpi_comparison.html` (visual comparison)
- ✅ `artemis_output/benchmark_summary.json`
- ✅ `artemis_output/optimization_recommendations.md`

---

## Advanced Options

### Run with Specific Threads

```bash
artemis build --config artemis.yaml --threads 8
```

### Run with Detailed Logging

```bash
artemis build --config artemis.yaml --verbosity detailed
```

### Save All Artifacts

```bash
artemis build --config artemis.yaml --save-artifacts artemis_output/
```

### Dry Run (Show commands without executing)

```bash
artemis build --config artemis.yaml --dry-run
```

### Run Specific Build Section

```bash
# Only pre-build checks
artemis build --config artemis.yaml --only before

# Only main build (compile/test)
artemis build --config artemis.yaml --only main

# Only post-build (dbt test)
artemis build --config artemis.yaml --only after
```

---

## Output Structure

After running Artemis, the output directory contains:

```
artemis_output/
├── benchmark_summary.json          ← Summary of all baseline metrics
├── kpi_comparison.html             ← Visual comparison (if candidate exists)
├── optimization_recommendations.md ← Artemis recommendations
├── dbt_compile_output/
│   ├── manifest.json
│   ├── run_results.json
│   └── compiled/
├── benchmark/
│   ├── pipeline_a/
│   │   └── baseline/report.json
│   ├── pipeline_b/
│   │   └── baseline/report.json
│   └── pipeline_c/
│       └── baseline/report.json
└── logs/
    └── build.log
```

---

## Troubleshooting

### Issue: "profiles.yml not found"

```bash
# Make sure profiles.yml exists in project root
ls -la profiles.yml

# Or create a minimal profiles.yml:
cat > profiles.yml << 'EOF'
bain_capital:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: [ACCOUNT_ID]
      user: [USERNAME]
      password: [PASSWORD]
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: BAIN_ANALYTICS
      schema: DEV
      threads: 4
EOF
```

### Issue: "dbt version mismatch"

```bash
# Check dbt version
dbt --version

# Expected: dbt-core >=1.5.0, dbt-snowflake >=1.5.0
```

### Issue: "Snowflake connection failed"

```bash
# Test connection
dbt debug --config-dir

# Should show "All checks passed!"
```

### Issue: "extract_report.py not found"

```bash
# Make sure extract_report.py exists in project root
ls -la extract_report.py

# If missing, check it's in the repository
git status | grep extract_report.py
```

---

## Quick Reference Table

| Task | Command | Duration | Output |
|------|---------|----------|--------|
| Compile & test | `artemis build --config artemis.yaml --skip-benchmark` | 2-3m | Compiled SQL |
| Baseline (all) | `artemis build --config artemis.yaml --target baseline` | 10-15m | 3 baseline reports |
| Baseline (A) | `artemis build --config artemis.yaml --pipeline a` | 3-5m | 1 baseline report |
| Candidate | `artemis build --config artemis.yaml --target candidate` | 10-15m | 3 candidate reports |
| Comparison | `artemis build --config artemis.yaml --include-comparison` | 5m | HTML comparison |
| Full build | `artemis build --config artemis.yaml` | 15-20m | All artifacts |

---

## Understanding the 5 KPIs

Each report (baseline and candidate) contains:

```json
{
  "kpi_1_execution": {
    "runtime_seconds": 3.2
  },
  "kpi_2_work_metrics": {
    "bytes_scanned": 512000
  },
  "kpi_3_output_validation": {
    "output_hash": "5c66064a..."
  },
  "kpi_4_complexity": {
    "complexity_score": 5.5
  },
  "kpi_5_cost_estimation": {
    "credits_estimated": 0.003556
  }
}
```

Artemis compares these 5 KPIs between baseline and candidate to show improvements.

---

## Next Steps

1. **Set up Snowflake connection:** `profiles.yml` with valid credentials
2. **Run initial build:** `artemis build --config artemis.yaml --target baseline`
3. **Review baseline metrics:** Check `benchmark/pipeline_a|b|c/baseline/report.json`
4. **Optimize SQL models:** Edit models based on `benchmark/pipeline_*/pipeline.yaml` recommendations
5. **Enable candidates:** Set `candidate.enabled: true` in `artemis.yaml`
6. **Run comparison:** `artemis build --config artemis.yaml --include-comparison`
7. **Review results:** Check `artemis_output/kpi_comparison.html`

---

For more details, see:
- [Artemis Custom Runner Docs](https://docs.artemis.turintech.ai/features/artemis-custom-runner)
- [Artemis Settings Documentation](https://docs.artemis.turintech.ai/features/settings-build)
- [PIPELINE_FLOW.md](./PIPELINE_FLOW.md) - Detailed benchmarking workflow
