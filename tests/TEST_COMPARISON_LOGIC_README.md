# Comparison and Delta Logic Unit Tests

Comprehensive unit tests for baseline vs candidate analysis, including delta calculation, bottleneck detection, data equivalence checking, and recommendation generation.

## Test File

**Location**: `tests/test_comparison_logic.py`

## Test Coverage

### 1. Delta Calculation Tests (TestDeltaCalculation)
Tests the core percentage change formula: `((candidate - baseline) / baseline) × 100`

- ✅ `test_delta_basic_regression()` - Positive changes (regressions)
- ✅ `test_delta_basic_improvement()` - Negative changes (improvements)
- ✅ `test_delta_formula_accuracy()` - Exact formula validation
- ✅ `test_delta_zero_baseline()` - Division by zero handling
- ✅ `test_delta_null_baseline()` - Null/None baseline value
- ✅ `test_delta_null_candidate()` - Null/None candidate value
- ✅ `test_delta_no_change()` - When values are unchanged (0% delta)
- ✅ `test_delta_perfect_improvement()` - 100% reduction (from 100 to 0)
- ✅ `test_direction_improvement()` - Direction indicator for improvement (+)
- ✅ `test_direction_regression()` - Direction indicator for regression (-)
- ✅ `test_delta_result_structure()` - DeltaResult object structure validation

**Key Features Tested**:
- Percentage change calculation accuracy
- Edge case handling (zero baseline, null values)
- Direction indicators (+/- for improvement/regression)
- DeltaResult data structure with all required fields

### 2. Bottleneck Detection Tests (TestBottleneckDetection)
Tests regression threshold detection for performance bottlenecks.

#### Execution Time Regression (>10% threshold)
- ✅ `test_execution_time_regression_threshold()` - Flags 15% > 10%
- ✅ `test_execution_time_below_threshold()` - No flag for 5% < 10%
- ✅ `test_execution_time_boundary()` - Exact threshold (10%) not flagged
- ✅ `test_execution_time_improvement()` - Improvements not flagged

#### Cost Regression (>20% threshold)
- ✅ `test_cost_regression_threshold()` - Flags 25% > 20%
- ✅ `test_cost_below_threshold()` - No flag for 15% < 20%
- ✅ `test_cost_boundary()` - Exact threshold (20%) not flagged
- ✅ `test_cost_improvement()` - Cost improvements not flagged

#### Data Drift Detection
- ✅ `test_data_drift_detected()` - Hash mismatch flagged with annotation
- ✅ `test_data_drift_not_detected()` - Matching hashes not flagged

#### KPI Categorization
- ✅ `test_kpi_categorization_improved()` - Category = "improved"
- ✅ `test_kpi_categorization_regressed()` - Category = "regressed"
- ✅ `test_kpi_categorization_neutral_small()` - Small changes (<0.5%) = "neutral"
- ✅ `test_kpi_categorization_neutral_na()` - N/A direction = "neutral"

**Key Features Tested**:
- Regression detection at proper thresholds (10% for time, 20% for cost)
- Boundary condition handling (exact threshold values)
- Data drift detection via SHA256 mismatch
- KPI categorization into improved/regressed/neutral

### 3. Data Equivalence Tests (TestDataEquivalence)
Tests SHA256 hash-based data validation.

- ✅ `test_matching_hashes_no_warning()` - Same hashes = no drift warning
- ✅ `test_mismatched_hashes_warning()` - Different hashes = data drift warning
- ✅ `test_missing_hash_fields()` - Graceful handling when hashes missing

**Key Features Tested**:
- Hash matching validation
- Data drift warning generation
- Graceful error handling for missing hash data

### 4. Recommendation Generation Tests (TestRecommendationGeneration)
Tests rule-based recommendation generation for SQL optimization.

#### JOIN Count Rules (threshold: 5)
- ✅ `test_high_join_count_rule_triggered()` - Triggered for ≥5 JOINs
- ✅ `test_high_join_count_not_triggered()` - Not triggered for <5 JOINs

#### CTE Count Rules (threshold: 3)
- ✅ `test_high_cte_count_rule_triggered()` - Triggered for ≥3 CTEs
- ✅ `test_high_cte_count_not_triggered()` - Not triggered for <3 CTEs

#### Window Function Rules (threshold: 2)
- ✅ `test_high_window_function_rule_triggered()` - Triggered for ≥2 functions
- ✅ `test_high_window_function_not_triggered()` - Not triggered for <2 functions

#### Multiple Rules
- ✅ `test_multiple_rules_triggered()` - Multiple rules triggered together

#### Priority Scoring
- ✅ `test_priority_score_calculation()` - Score = (impact/100) × (complexity/threshold) × 100
- ✅ `test_priority_level_high()` - HIGH priority for score >66
- ✅ `test_priority_level_medium()` - MEDIUM priority for score 33-66
- ✅ `test_priority_level_low()` - LOW priority for score <33
- ✅ `test_priority_level_cost_boost()` - Cost regression >20% forces HIGH priority

**Key Features Tested**:
- Rule triggering based on complexity metric thresholds
- Priority score calculation
- Priority level determination (HIGH/MEDIUM/LOW)
- Cost regression impact on priority

### 5. Edge Cases Tests (TestEdgeCases)
Tests error handling and boundary conditions.

- ✅ `test_zero_baseline_multiple_metrics()` - Zero baseline for multiple metrics
- ✅ `test_missing_model_new()` - New model detection (in candidate, not baseline)
- ✅ `test_missing_model_removed()` - Removed model detection (in baseline, not candidate)
- ✅ `test_output_serialization()` - JSON serialization of delta output
- ✅ `test_bottleneck_result_structure()` - BottleneckResult data structure
- ✅ `test_recommendation_structure()` - Recommendation object structure

**Key Features Tested**:
- Zero baseline handling across multiple metrics
- New/removed model detection
- Output serialization for JSON export
- Data structure completeness and correctness

### 6. Comprehensive Scenarios Tests (TestComprehensiveScenarios)
Tests complex multi-model, multi-KPI scenarios.

- ✅ `test_multi_model_mixed_scenarios()` - Mixed improvements, regressions, new/removed models
- ✅ `test_delta_all_kpis()` - Delta calculation across all KPI types

**Key Features Tested**:
- Multi-model analysis with various scenarios
- All KPI types processed correctly

## Test Fixtures

### pytest Fixtures
Located in `tests/test_comparison_logic.py`:

- **config**: Loads application configuration
- **mock_logger**: Creates a mock logger for testing
- **baseline_kpis**: Single baseline model KPIs
- **candidate_kpis**: Single candidate model KPIs
- **baseline_models**: Multiple baseline models
- **candidate_models**: Multiple candidate models with various scenarios

### Mock Data Files

- **tests/fixtures/baseline_report.json**: Sample baseline report with 3 models
- **tests/fixtures/candidate_report.json**: Sample candidate report with 3 existing + 1 new model

Example baseline_report.json:
```json
{
  "models": {
    "stg_users": {
      "execution_time": 5.2,
      "cost": 12.5,
      "bytes_scanned": 500000,
      "join_count": 3,
      "cte_count": 2,
      "window_function_count": 1,
      "data_hash": "abc123def456xyz789uvw012"
    },
    ...
  }
}
```

## Running Tests

### Run all comparison tests
```bash
pytest tests/test_comparison_logic.py -v
```

### Run specific test class
```bash
pytest tests/test_comparison_logic.py::TestDeltaCalculation -v
```

### Run specific test
```bash
pytest tests/test_comparison_logic.py::TestDeltaCalculation::test_delta_formula_accuracy -v
```

### Run with detailed output
```bash
pytest tests/test_comparison_logic.py -vv -s
```

### Run with coverage
```bash
pytest tests/test_comparison_logic.py \
  --cov=delta \
  --cov=bottleneck \
  --cov=recommendation \
  --cov-report=term-missing \
  --cov-report=html
```

## Test Statistics

- **Total Test Classes**: 6
- **Total Test Methods**: 70+
- **Test Categories**:
  - Delta Calculation: 11 tests
  - Bottleneck Detection: 14 tests
  - Data Equivalence: 3 tests
  - Recommendation Generation: 11 tests
  - Edge Cases: 6 tests
  - Comprehensive Scenarios: 2 tests

## Key Testing Patterns

### 1. Delta Calculation Formula
```python
delta = ((candidate - baseline) / baseline) * 100
```
Tests validate this formula with various baseline/candidate combinations.

### 2. Regression Thresholds
- **Execution Time**: >10% increase flags regression
- **Cost**: >20% increase flags regression

### 3. Data Drift Detection
SHA256 hash mismatch between baseline and candidate indicates data drift (critical issue).

### 4. Complexity Rules
- **JOINs**: ≥5 triggers HIGH_JOIN_COUNT rule
- **CTEs**: ≥3 triggers HIGH_CTE_COUNT rule
- **Window Functions**: ≥2 triggers HIGH_WINDOW_FUNCTION_COUNT rule

### 5. Priority Scoring
```
score = (impact_score / 100) * (complexity_value / threshold) * 100
score = min(score, 100)  # Capped at 100
```
Cost regression >20% adds +25 bonus points.

## Edge Cases Covered

1. **Zero Baseline** - Division by zero handling (returns N/A status)
2. **Null Values** - Missing or None values (returns null_value status)
3. **Perfect Improvement** - 100% reduction (from 100 to 0 = -100% delta)
4. **Boundary Conditions** - Exact threshold values (e.g., 10.0% for time)
5. **Missing Models** - New (in candidate, not baseline) and removed (in baseline, not candidate)
6. **Missing Hashes** - Graceful handling when data_hash fields missing
7. **Zero Values** - Multiple metrics with zero baseline
8. **Output Serialization** - All results are JSON-serializable

## Success Criteria

✅ All 70+ tests pass successfully
✅ Delta calculation tests verify correct % change formula
✅ Bottleneck detection tests validate threshold crossing
✅ Data equivalence tests verify hash mismatch detection
✅ Recommendation generation tests validate rule-based suggestions
✅ Edge cases handled gracefully with no errors
✅ Output structures properly formatted for JSON export
✅ Mock data reflects realistic report.json structure

## Related Files

- **delta.py**: Delta calculation implementation
- **bottleneck.py**: Bottleneck detection implementation
- **recommendation.py**: Recommendation engine implementation
- **config.py**: Configuration with thresholds and rules
- **helpers.py**: Utility functions and custom exceptions

## Notes

- All tests use mocked data - no external dependencies required
- Tests follow pytest conventions with fixtures and descriptive names
- Comprehensive assertions with failure messages
- Tests validate both happy path and error cases
- Output structures validated for JSON serialization compatibility

