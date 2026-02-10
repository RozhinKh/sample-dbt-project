# Quick Start Guide - KPI Extraction Unit Tests

## Installation

### Prerequisites
- Python 3.7+
- pytest
- pytest-cov (for coverage analysis)

### Install Dependencies
```bash
pip install pytest pytest-cov
```

## Running Tests

### Quick Test Run (No Coverage)
```bash
# Run all KPI extraction tests with verbose output
pytest tests/test_kpi_extraction.py -v

# Expected output:
# ===== 52 passed in 2.45s =====
```

### Run with Coverage Analysis
```bash
# Generate coverage report with missing lines shown
pytest tests/test_kpi_extraction.py \
  --cov=config \
  --cov-report=term-missing \
  --cov-report=html
```

### Automated Test Runner (Recommended)
```bash
bash run_tests.sh
```
This script will:
1. Install dependencies if needed
2. Run all tests
3. Generate coverage report
4. Create HTML coverage report in `htmlcov/index.html`

## Specific Test Runs

### Run Single Test Class
```bash
# Test cost estimation functions
pytest tests/test_kpi_extraction.py::TestCostEstimation -v

# Test execution time extraction
pytest tests/test_kpi_extraction.py::TestExecutionTimeExtraction -v
```

### Run Single Test
```bash
# Test 1 TB credit calculation
pytest tests/test_kpi_extraction.py::TestCostEstimation::test_credits_calculation_1tb -v
```

### Run Only Edge Case Tests
```bash
# Run all tests that handle edge cases
pytest tests/test_kpi_extraction.py::TestExecutionTimeExtraction::test_execution_time_missing_value -v
pytest tests/test_kpi_extraction.py::TestExecutionTimeExtraction::test_execution_time_zero_value -v
```

## Test Organization

### Test Classes
1. **TestExecutionTimeExtraction** - 9 tests
2. **TestRowsBytesCalculation** - 10 tests
3. **TestSHA256Hashing** - 7 tests
4. **TestComplexityCounting** - 10 tests
5. **TestCostEstimation** - 13 tests
6. **TestKPIIntegration** - 3 tests

### Key Test Scenarios

#### KPI 1: Execution Time
```bash
pytest tests/test_kpi_extraction.py::TestExecutionTimeExtraction -v
```
- ✓ Positive values (1.234s)
- ✓ Zero values (0.0s)
- ✓ Missing values (None)
- ✓ Negative values
- ✓ Invalid types

#### KPI 2: Rows and Bytes
```bash
pytest tests/test_kpi_extraction.py::TestRowsBytesCalculation -v
```
- ✓ Row count extraction
- ✓ Bytes calculation (rows × 500)
- ✓ Large datasets (1M+ rows)
- ✓ Zero values
- ✓ Missing data

#### KPI 3: Hashing
```bash
pytest tests/test_kpi_extraction.py::TestSHA256Hashing -v
```
- ✓ Hash generation
- ✓ Consistency (same data → same hash)
- ✓ Uniqueness (different data → different hash)
- ✓ Format validation
- ✓ Case sensitivity

#### KPI 4: Complexity
```bash
pytest tests/test_kpi_extraction.py::TestComplexityCounting -v
```
- ✓ JOIN counting
- ✓ CTE counting
- ✓ Window function counting
- ✓ Complex SQL parsing
- ✓ Case-insensitive matching

#### KPI 5: Cost Estimation
```bash
pytest tests/test_kpi_extraction.py::TestCostEstimation -v
```
- ✓ Credit calculation (0 bytes → 1000+ credits)
- ✓ Standard pricing ($2/credit)
- ✓ Enterprise pricing ($3/credit)
- ✓ Cost precision
- ✓ Large datasets

## Expected Output

### Successful Test Run
```
tests/test_kpi_extraction.py::TestExecutionTimeExtraction::test_execution_time_positive_value PASSED
tests/test_kpi_extraction.py::TestExecutionTimeExtraction::test_execution_time_zero_value PASSED
tests/test_kpi_extraction.py::TestExecutionTimeExtraction::test_execution_time_large_value PASSED
...
tests/test_kpi_extraction.py::TestKPIIntegration::test_kpi_extraction_with_edge_cases PASSED

===== 52 passed in 2.45s =====
```

### Coverage Report
```
Name                                    Stmts   Miss  Cover   Missing
---------------------------------------------------------------------------
config.py                                 200    10   95%    
benchmark/generate_report.py              500    30   94%    
helpers.py                                300    15   95%    
---------------------------------------------------------------------------
TOTAL                                    1000    55   94%
```

## Interpreting Results

### All Tests Pass ✓
Green lights indicate the KPI extraction functions are working correctly across all scenarios.

### Some Tests Fail ✗
Check the specific test output:
- Test name indicates which function failed
- Assertion message shows expected vs. actual values
- Compare against the test documentation in `tests/README.md`

### Coverage Below Target (<90%)
Review the "Missing" column to see which lines aren't tested:
1. Check if the missing code is reachable
2. Add test cases to cover that code path
3. Re-run coverage analysis

## Debugging

### Enable Verbose Output
```bash
pytest tests/test_kpi_extraction.py -vv
```

### Show Print Statements
```bash
pytest tests/test_kpi_extraction.py -s
```

### Full Traceback
```bash
pytest tests/test_kpi_extraction.py --tb=long
```

### Specific Test with Debugging
```bash
pytest tests/test_kpi_extraction.py::TestCostEstimation::test_credits_calculation_1tb -vv -s
```

## Performance

### Expected Execution Time
- **All 52 tests**: 2-3 seconds
- **Single test**: <100ms
- **With coverage analysis**: 3-5 seconds

### If Tests are Slow
1. Check if Snowflake/external services are being called
2. Verify no file I/O is occurring
3. Profile with: `pytest --durations=10`

## Fixtures Overview

All test data is provided by pytest fixtures in `conftest.py`:

### Available Fixtures
- `sample_manifest()` - Complete dbt manifest
- `sample_run_results()` - Execution results
- `sample_model_simple` - Simple model with 1000 rows
- `sample_model_complex` - Complex model with 50000 rows
- `sample_model_with_zero_values` - Zero rows and execution time
- `sample_model_with_missing_values` - Missing data fields
- `sample_model_with_negative_values` - Negative values
- `sample_model_with_invalid_types` - Invalid data types
- `sample_model_large_values` - 1M rows
- `mock_logger()` - Mock logger instance

### Using Fixtures in Custom Tests
```python
def test_my_feature(sample_model_simple, sample_manifest):
    # Access fixture data
    execution_time = sample_model_simple.get("execution_time")
    manifest = sample_manifest
    
    # Run your test
    assert execution_time > 0
```

## Files Reference

- **tests/test_kpi_extraction.py** - Main test suite
- **tests/conftest.py** - Pytest fixtures
- **tests/fixtures/sample_manifest.json** - Mock manifest data
- **tests/fixtures/sample_run_results.json** - Mock execution results
- **tests/README.md** - Detailed test documentation
- **pytest.ini** - Pytest configuration
- **run_tests.sh** - Automated test runner
- **TEST_COVERAGE_REPORT.md** - Detailed coverage analysis
- **TASK_21_IMPLEMENTATION_SUMMARY.md** - Full implementation details

## Support

For detailed information, see:
- **TEST_COVERAGE_REPORT.md** - Coverage analysis and detailed test breakdown
- **tests/README.md** - Test structure and usage
- **TASK_21_IMPLEMENTATION_SUMMARY.md** - Implementation details

## Common Issues

### Issue: pytest not found
**Solution**: 
```bash
pip install pytest
```

### Issue: Tests fail with import errors
**Solution**: Ensure you're running from the project root:
```bash
cd /path/to/project
pytest tests/test_kpi_extraction.py -v
```

### Issue: Coverage report not generated
**Solution**: Install pytest-cov:
```bash
pip install pytest-cov
```

### Issue: Some tests skipped
**Solution**: Check if all fixtures are available. Run with verbose output:
```bash
pytest tests/test_kpi_extraction.py -vv
```

---

## Next Steps

After running the tests successfully:

1. **Review Coverage Report**
   - Open `htmlcov/index.html` in a browser
   - Identify any uncovered code paths

2. **Examine Test Results**
   - Check which tests pass/fail
   - Review any warnings or deprecations

3. **Integrate into CI/CD**
   - Add test command to your CI pipeline
   - Set minimum coverage threshold
   - Block merges if coverage drops

4. **Monitor Performance**
   - Track test execution time over time
   - Optimize slow tests
   - Add performance benchmarks

---

**Happy Testing! 🚀**
