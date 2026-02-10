# KPI Extraction Unit Tests

Comprehensive unit tests for all KPI extraction functions used in the dbt benchmarking system.

## Test Structure

### Test Files

- **test_kpi_extraction.py**: Main test suite with 50+ test cases
  - KPI 1: Execution time extraction (9 tests)
  - KPI 2: Rows and bytes calculation (10 tests)
  - KPI 3: SHA256 hashing (7 tests)
  - KPI 4: SQL complexity counting (10 tests)
  - KPI 5: Cost estimation (13 tests)
  - Integration tests (3 tests)

- **conftest.py**: Pytest fixtures and configuration
  - Sample manifest.json fixture
  - Sample run_results.json fixture
  - Model fixtures (simple, complex, edge cases)
  - Pricing configuration fixtures

### Fixture Files

- **fixtures/sample_manifest.json**: Mock dbt manifest with 5 test models
- **fixtures/sample_run_results.json**: Mock dbt run results with execution data

## Running Tests

### Run all tests
```bash
pytest tests/test_kpi_extraction.py -v
```

### Run specific test class
```bash
pytest tests/test_kpi_extraction.py::TestExecutionTimeExtraction -v
```

### Run specific test
```bash
pytest tests/test_kpi_extraction.py::TestCostEstimation::test_credits_calculation_1tb -v
```

### Run with coverage analysis
```bash
pytest tests/test_kpi_extraction.py \
  --cov=benchmark.generate_report \
  --cov=config \
  --cov-report=term-missing \
  --cov-report=html
```

### Run with timing information
```bash
pytest tests/test_kpi_extraction.py -v --durations=10
```

## Test Coverage

Target: >90% coverage of KPI extraction module

The test suite covers:

### KPI 1: Execution Time Extraction
- ✓ Positive values
- ✓ Zero values
- ✓ Large values
- ✓ Missing values (defaults to 0.0)
- ✓ Negative values (normalized to 0.0)
- ✓ Invalid data types (defaults to 0.0)
- ✓ Float precision (3+ decimal places)

### KPI 2: Rows and Bytes Calculation
- ✓ Row count extraction (simple, complex, zero)
- ✓ Missing row data (defaults to 0)
- ✓ Negative row counts (normalized)
- ✓ Invalid row types (defaults to 0)
- ✓ Bytes estimation with standard row width (500 bytes)
- ✓ Large dataset handling (1M+ rows)
- ✓ Zero rows handling
- ✓ Realistic data size validation

### KPI 3: SHA256 Hashing
- ✓ Hash generation from data
- ✓ Hash consistency (same data → same hash)
- ✓ Hash uniqueness (different data → different hash)
- ✓ Empty data handling
- ✓ None/null handling
- ✓ Format validation (64 char hex string)
- ✓ Case sensitivity

### KPI 4: SQL Complexity Counting
- ✓ JOIN counting (INNER, LEFT, RIGHT, FULL)
- ✓ Multiple JOIN handling
- ✓ Case-insensitive matching
- ✓ Zero JOIN detection
- ✓ CTE counting (WITH clauses)
- ✓ Multiple CTE handling
- ✓ Window function counting (OVER clauses)
- ✓ Multiple window function handling
- ✓ Complex real-world SQL parsing

### KPI 5: Cost Estimation
- ✓ Credit calculation (0 bytes → 0 credits)
- ✓ 10 GB = 1 credit calculation
- ✓ 100 GB = 10 credits calculation
- ✓ 1 TB = 100 credits calculation
- ✓ Large dataset (10 TB) handling
- ✓ Fractional credit calculations
- ✓ Standard edition cost ($2/credit)
- ✓ Enterprise edition cost ($3/credit)
- ✓ Invalid edition handling (defaults to standard)
- ✓ Cost precision (2 decimal places)
- ✓ End-to-end bytes→credits→cost pipeline
- ✓ Large dataset cost calculation
- ✓ Consistency across multiple calls

### Integration Tests
- ✓ Complete pipeline (simple model)
- ✓ Complete pipeline (complex model)
- ✓ Edge case handling

## Test Fixtures

### sample_manifest()
Provides a complete dbt manifest.json with:
- simple_model: No JOINs/CTEs/window functions
- join_model: 2 JOINs, 2 CTEs
- complex_model: 4 JOINs, 4 CTEs, 3 window functions
- view_model: View materialization
- no_code_model: Ephemeral materialization

### sample_run_results()
Provides execution results for all models:
- Varied execution times (0.567s - 12.456s)
- Varied row counts (0 - 50,000)
- Success and skipped statuses
- Timing breakdowns

### Model-specific fixtures
- sample_model_simple: Basic execution data
- sample_model_complex: High row count, long execution
- sample_model_with_zero_values: Edge case (0 exec time, 0 rows)
- sample_model_with_missing_values: Edge case (None values)
- sample_model_with_negative_values: Edge case (negative values)
- sample_model_with_invalid_types: Edge case (invalid data types)
- sample_model_large_values: Edge case (1M rows, 60s execution)

## Edge Case Coverage

The test suite explicitly handles:

1. **Zero Values**
   - 0.0 execution time
   - 0 rows produced
   - 0 bytes scanned
   - 0 credits/cost

2. **Missing Data**
   - Missing execution_time (defaults to 0.0)
   - Missing rows_affected (defaults to 0)
   - Missing adapter_response (empty dict)

3. **Invalid Data Types**
   - String values for numeric fields
   - None/null values
   - Wrong data types in adapter_response

4. **Negative Values**
   - Negative execution time
   - Negative row counts
   - Behavior: normalized to 0

5. **Large Values**
   - 1M+ row counts
   - Multiple TB of data
   - 1000+ credits
   - Cost calculations >$200

6. **SQL Parsing**
   - Case-insensitive keyword matching
   - Multiple JOINs (up to 4)
   - Multiple CTEs (up to 4)
   - Multiple window functions (up to 4)
   - Missing code fields

## Performance

Test execution target: <5 seconds total

All tests use lightweight mock data and in-memory fixtures with no I/O operations.

## Assertions

Each test includes clear, specific assertions with:
- Type validation (isinstance checks)
- Value validation (equality, ranges)
- Format validation (hash format, precision)
- Consistency validation (same inputs → same outputs)

## Mocking Strategy

Tests use:
- pytest fixtures for sample data
- In-memory data structures (no file I/O)
- Regular expressions for SQL parsing (same as production)
- Direct function calls (no external dependencies)

No external services (Snowflake, file system) are required to run tests.

## Debugging

To debug a specific test:

```bash
pytest tests/test_kpi_extraction.py::TestCostEstimation::test_cost_standard_edition_100_credits -vv -s
```

To see full traceback:

```bash
pytest tests/test_kpi_extraction.py --tb=long
```

To run with logging:

```bash
pytest tests/test_kpi_extraction.py --log-cli-level=DEBUG
```
