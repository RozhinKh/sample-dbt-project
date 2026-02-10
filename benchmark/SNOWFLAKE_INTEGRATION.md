# Snowflake Integration Guide

## Overview

This guide explains how to enhance the dbt benchmarking system with direct Snowflake query access for more accurate performance metrics and data equivalence validation.

## Current Limitations

The benchmark system currently has the following limitations:

1. **Data Equivalence Validation**: Cannot calculate SHA256 hashes of model outputs without Snowflake access
2. **Bytes Scanned Accuracy**: Uses estimation (rows × 500 bytes) instead of actual bytes scanned from query metadata
3. **Cost Estimation Accuracy**: Approximates costs based on estimated bytes rather than actual Snowflake credit consumption

## Benefits of Snowflake Integration

### 1. Data Equivalence Validation (Critical)

**Current State**: Hash validation unavailable (0% success rate)

**With Snowflake**:
- Calculate SHA256 hashes of model output using `HASH(*)` or `SHA2()` functions
- Verify that optimized code produces **identical results** to baseline
- Prevent silent data corruption during optimization work

**Impact**: Enables safe optimization by guaranteeing correctness

### 2. Accurate Metrics

**Current State**: Bytes scanned estimated, cost approximated

**With Snowflake**:
- Query `INFORMATION_SCHEMA.QUERY_HISTORY` for actual:
  - `BYTES_SCANNED`
  - `PARTITIONS_SCANNED`
  - `CREDITS_USED_CLOUD_SERVICES`
  - `EXECUTION_TIME`
  - `WAREHOUSE_SIZE`

**Impact**: Production-grade cost analysis and performance tracking

### 3. Enhanced Insights

Additional metrics available:
- Query execution plan analysis
- Cache hit rates
- Spilling to disk indicators
- Warehouse contention metrics

## Implementation Roadmap

### Phase 1: Basic Snowflake Connection (2-4 hours)

**Goal**: Establish Snowflake connectivity for hash calculation

**Files to Modify**:
- `helpers.py` - Add Snowflake connection functions
- `benchmark/generate_report.py` - Integrate hash calculation

**Steps**:

1. Add Snowflake connector dependency:
   ```bash
   pip install snowflake-connector-python
   ```

2. Create connection helper in `helpers.py`:
   ```python
   def get_snowflake_connection(profile_name: str = "default"):
       """
       Create Snowflake connection from dbt profiles.yml.

       Returns:
           snowflake.connector.connection: Active Snowflake connection
       """
       import snowflake.connector

       # Parse profiles.yml
       profile = parse_profiles_yml(profile_name)

       conn = snowflake.connector.connect(
           user=profile['user'],
           password=profile.get('password') or os.getenv('SNOWFLAKE_PASSWORD'),
           account=profile['account'],
           warehouse=profile['warehouse'],
           database=profile['database'],
           schema=profile['schema'],
           role=profile.get('role')
       )

       return conn
   ```

3. Add hash calculation function:
   ```python
   def calculate_model_hash(model_name: str, database: str, schema: str, conn) -> Optional[str]:
       """
       Calculate SHA256 hash of model output using Snowflake query.

       Args:
           model_name: Name of the dbt model (e.g., 'stg_trades')
           database: Snowflake database name
           schema: Snowflake schema name
           conn: Active Snowflake connection

       Returns:
           SHA256 hash string or None if calculation fails
       """
       query = f"""
       SELECT SHA2_HEX(
           TO_VARCHAR(
               OBJECT_AGG(*)
           )
       ) as output_hash
       FROM {database}.{schema}.{model_name}
       ORDER BY 1  -- Ensure consistent ordering
       """

       try:
           cursor = conn.cursor()
           cursor.execute(query)
           result = cursor.fetchone()
           return result[0] if result else None
       except Exception as e:
           logger.warning(f"Hash calculation failed for {model_name}: {str(e)}")
           return None
   ```

4. Integrate into KPI extraction:
   - Update `extract_kpi_metrics()` in `helpers.py`
   - Add optional `snowflake_conn` parameter
   - Calculate hash when connection available

**Verification**:
```bash
# Test hash calculation
python -c "
from helpers import get_snowflake_connection, calculate_model_hash

conn = get_snowflake_connection()
hash_value = calculate_model_hash('stg_trades', 'YOUR_DB', 'YOUR_SCHEMA', conn)
print(f'Hash: {hash_value}')
"
```

**Expected Result**: Non-null SHA256 hash for existing models

### Phase 2: Query History Integration (3-5 hours)

**Goal**: Extract actual bytes scanned and credits from Snowflake query history

**Steps**:

1. Add query history lookup function:
   ```python
   def get_query_metrics(query_id: str, conn) -> Dict[str, Any]:
       """
       Fetch actual execution metrics from QUERY_HISTORY.

       Returns:
           Dict with: bytes_scanned, partitions_scanned, credits_used, execution_time_ms
       """
       query = f"""
       SELECT
           BYTES_SCANNED,
           PARTITIONS_SCANNED,
           CREDITS_USED_CLOUD_SERVICES,
           TOTAL_ELAPSED_TIME as execution_time_ms,
           WAREHOUSE_SIZE,
           COMPILATION_TIME,
           EXECUTION_STATUS
       FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
       WHERE QUERY_ID = %s
       """

       cursor = conn.cursor()
       cursor.execute(query, (query_id,))
       result = cursor.fetchone()

       if not result:
           return {}

       return {
           'bytes_scanned': result[0],
           'partitions_scanned': result[1],
           'credits_used': result[2],
           'execution_time_ms': result[3],
           'warehouse_size': result[4],
           'compilation_time': result[5],
           'execution_status': result[6]
       }
   ```

2. Modify dbt execution to capture query IDs:
   - Add `--log-format json` to dbt commands
   - Parse query_id from logs
   - Store mapping: model_name → query_id

3. Update report generation:
   - After dbt run completes, lookup query metrics
   - Replace estimated bytes with actual
   - Add actual credits to report

**Verification**:
```bash
# Run pipeline with query tracking
python benchmark/generate_report.py --pipeline a --use-snowflake

# Check report for actual metrics
python -c "
import json
with open('benchmark/pipeline_a/baseline/report.json') as f:
    report = json.load(f)

for model in report['models'][:3]:
    print(f\"{model['model_name']}: {model['bytes_scanned']:,} bytes (actual)\")
"
```

**Expected Result**: Real bytes scanned instead of estimates

### Phase 3: Automated Hash Validation (2-3 hours)

**Goal**: Automatically compare baseline vs candidate hashes in `compare.py`

**Steps**:

1. Update `compare.py` to check hash equivalence:
   ```python
   def validate_data_equivalence(baseline_model, candidate_model, logger):
       """
       Verify baseline and candidate produce identical data.

       Returns:
           Tuple[bool, Optional[str]]: (is_equivalent, error_message)
       """
       baseline_hash = baseline_model.get('output_hash')
       candidate_hash = candidate_model.get('output_hash')

       # Both must have hashes
       if not baseline_hash or not candidate_hash:
           return False, "Hash unavailable for comparison (requires Snowflake access)"

       # Hashes must match
       if baseline_hash != candidate_hash:
           return False, f"Data mismatch detected! Baseline hash {baseline_hash[:16]}... != Candidate hash {candidate_hash[:16]}..."

       return True, None
   ```

2. Add to comparison workflow:
   - Run hash validation before performance comparison
   - **FAIL FAST** if hashes don't match (data corruption detected)
   - Only proceed to performance analysis if data is equivalent

**Verification**:
```bash
# Compare baseline vs candidate
python benchmark/compare.py --pipeline a --baseline baseline --candidate candidate

# Expected output includes:
# "✓ Data equivalence validated: All 4 models produce identical output"
```

## Configuration

### Environment Variables

Add to your shell profile or `.env` file:

```bash
# Snowflake connection (if not in profiles.yml)
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="benchmark_user"
export SNOWFLAKE_PASSWORD="<secure_password>"
export SNOWFLAKE_WAREHOUSE="BENCHMARK_WH"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_SCHEMA="DBT_PROD"
export SNOWFLAKE_ROLE="BENCHMARK_ROLE"

# Enable Snowflake integration
export BENCHMARK_USE_SNOWFLAKE="true"
```

### Permissions Required

The Snowflake user needs:

```sql
-- Read access to dbt models
GRANT USAGE ON DATABASE ANALYTICS TO ROLE BENCHMARK_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.DBT_PROD TO ROLE BENCHMARK_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.DBT_PROD TO ROLE BENCHMARK_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS.DBT_PROD TO ROLE BENCHMARK_ROLE;

-- Read access to query history
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE BENCHMARK_ROLE;

-- Warehouse usage for hash calculation queries
GRANT USAGE ON WAREHOUSE BENCHMARK_WH TO ROLE BENCHMARK_ROLE;
```

## Testing Strategy

### Unit Tests

Create `tests/test_snowflake_integration.py`:

```python
import pytest
from helpers import get_snowflake_connection, calculate_model_hash

@pytest.fixture
def snowflake_conn():
    """Provide Snowflake connection for tests."""
    conn = get_snowflake_connection('test')
    yield conn
    conn.close()

def test_connection(snowflake_conn):
    """Verify Snowflake connection works."""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    result = cursor.fetchone()
    assert result is not None

def test_hash_calculation(snowflake_conn):
    """Verify hash calculation produces consistent results."""
    hash1 = calculate_model_hash('stg_trades', 'DB', 'SCHEMA', snowflake_conn)
    hash2 = calculate_model_hash('stg_trades', 'DB', 'SCHEMA', snowflake_conn)

    assert hash1 is not None
    assert hash1 == hash2  # Deterministic
    assert len(hash1) == 64  # SHA256 is 64 hex chars
```

### Integration Tests

```bash
# 1. Generate baseline with Snowflake
python benchmark/generate_report.py --pipeline a --use-snowflake

# 2. Verify all hashes calculated
python -c "
import json
with open('benchmark/pipeline_a/baseline/report.json') as f:
    report = json.load(f)

hash_rate = report['summary']['hash_validation_success_rate']
assert hash_rate == 1.0, f'Expected 100% hash success, got {hash_rate*100}%'
print('✓ All models hashed successfully')
"

# 3. Generate candidate (unchanged code)
python benchmark/generate_report.py --pipeline a --use-snowflake --output benchmark/pipeline_a/candidate/report.json

# 4. Compare - should show 100% data equivalence
python benchmark/compare.py --pipeline a
```

## Cost Considerations

### Hash Calculation Cost

- Each hash query scans the full model output
- For large models (GB-TB scale), this costs Snowflake credits
- Estimated cost: ~$0.01-$0.10 per model per run

**Optimization**: Cache hashes based on model content hash to avoid recomputation

### Query History Queries

- Minimal cost (<$0.01 per query)
- Reads from `ACCOUNT_USAGE` system tables

**Total**: ~$1-5 per full benchmark run across all pipelines

## Security Best Practices

1. **Never commit credentials**: Use environment variables or secret management
2. **Principle of least privilege**: Grant only required permissions
3. **Dedicated warehouse**: Use separate warehouse for benchmarking (avoid production impact)
4. **Read-only access**: Benchmark user should NOT have write permissions
5. **Audit logging**: Monitor benchmark queries via Snowflake query history

## Troubleshooting

### Issue: "Hash calculation failed"

**Symptoms**: Hashes are null in report

**Causes**:
- Snowflake connection failed
- Model doesn't exist in Snowflake
- Insufficient permissions
- Model contains unsupported data types for hashing

**Solutions**:
1. Verify connection: `python -c "from helpers import get_snowflake_connection; get_snowflake_connection()"`
2. Check model exists: `SELECT * FROM db.schema.model_name LIMIT 1`
3. Verify permissions (see Permissions Required section)
4. Check model for VARIANT/OBJECT types (may need custom hash logic)

### Issue: "Bytes scanned still estimated"

**Symptoms**: Reports show `bytes_scanned = rows × 500`

**Causes**:
- Query ID not captured from dbt logs
- Query history lookup failed
- `--use-snowflake` flag not set

**Solutions**:
1. Ensure `--use-snowflake` flag is used
2. Check dbt log format is JSON
3. Verify query ID mapping is populated
4. Check ACCOUNT_USAGE permissions

## Next Steps

After implementing Snowflake integration:

1. **Baseline regeneration**: Re-run all baseline reports with Snowflake integration enabled
2. **CI/CD integration**: Add automated benchmark comparisons to pull request workflow
3. **Dashboard creation**: Build visualization dashboard for trend analysis
4. **Alerting**: Set up Slack/email alerts for performance regressions

## Resources

- [Snowflake Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Snowflake QUERY_HISTORY Reference](https://docs.snowflake.com/en/sql-reference/functions/query_history.html)
- [dbt Snowflake Adapter](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
- [SHA2 Hash Functions in Snowflake](https://docs.snowflake.com/en/sql-reference/functions/sha2.html)
