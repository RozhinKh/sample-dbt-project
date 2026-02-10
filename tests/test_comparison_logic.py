#!/usr/bin/env python3
"""
Comprehensive unit tests for comparison and delta calculation logic.

Tests all aspects of baseline vs candidate analysis:
- Delta calculation: ((candidate - baseline) / baseline) × 100
- Bottleneck detection: >10% execution time, >20% cost regression
- Data equivalence: SHA256 hash mismatch detection
- Recommendation generation: JOIN, CTE, window function rules
- Edge cases: zero baselines, perfect improvements, missing models
- Output validation: all data structures are properly formed
"""

import pytest
import sys
import logging
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from delta import (
    calculate_delta,
    determine_direction,
    create_delta_result,
    calculate_all_deltas,
    calculate_model_deltas,
    format_delta_output,
    DeltaResult,
    get_improvement_metrics
)
from bottleneck import (
    check_execution_time_regression,
    check_cost_regression,
    check_data_drift,
    categorize_kpi,
    BottleneckResult,
    KPICategorization
)
from recommendation import (
    calculate_priority_score,
    get_priority_level,
    find_matching_rules,
    Recommendation
)
from config import load_config


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def config():
    """Load configuration for tests."""
    return load_config()


@pytest.fixture
def mock_logger():
    """Create a mock logger for tests."""
    logger = logging.getLogger("test_comparison")
    logger.setLevel(logging.DEBUG)
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    # Add null handler
    handler = logging.NullHandler()
    logger.addHandler(handler)
    return logger


@pytest.fixture
def baseline_kpis():
    """Fixture for baseline KPIs."""
    return {
        "execution_time": 10.0,
        "cost": 25.0,
        "bytes_scanned": 1000000,
        "join_count": 3,
        "cte_count": 2,
        "window_function_count": 1,
        "data_hash": "baseline_hash_abc123"
    }


@pytest.fixture
def candidate_kpis():
    """Fixture for candidate KPIs."""
    return {
        "execution_time": 9.0,  # 10% improvement
        "cost": 30.0,  # 20% regression
        "bytes_scanned": 900000,
        "join_count": 3,
        "cte_count": 2,
        "window_function_count": 1,
        "data_hash": "baseline_hash_abc123"  # Same = no drift
    }


@pytest.fixture
def baseline_models():
    """Fixture for multiple baseline models."""
    return {
        "model_a": {
            "execution_time": 10.0,
            "cost": 25.0,
            "bytes_scanned": 1000000,
            "join_count": 5,
            "cte_count": 3,
            "window_function_count": 2,
            "data_hash": "hash_a_v1"
        },
        "model_b": {
            "execution_time": 8.0,
            "cost": 15.0,
            "bytes_scanned": 500000,
            "join_count": 2,
            "cte_count": 1,
            "window_function_count": 0,
            "data_hash": "hash_b_v1"
        }
    }


@pytest.fixture
def candidate_models():
    """Fixture for multiple candidate models."""
    return {
        "model_a": {
            "execution_time": 12.0,  # +20% regression
            "cost": 35.0,  # +40% regression
            "bytes_scanned": 1200000,
            "join_count": 5,
            "cte_count": 3,
            "window_function_count": 2,
            "data_hash": "hash_a_v1"  # Same = no drift
        },
        "model_b": {
            "execution_time": 7.0,  # -12.5% improvement
            "cost": 12.0,  # -20% improvement
            "bytes_scanned": 400000,
            "join_count": 2,
            "cte_count": 1,
            "window_function_count": 0,
            "data_hash": "hash_b_v2"  # Different = drift!
        },
        "model_c": {
            "execution_time": 5.0,  # New model
            "cost": 10.0,
            "bytes_scanned": 300000,
            "join_count": 3,
            "cte_count": 1,
            "window_function_count": 1,
            "data_hash": "hash_c_v1"
        }
    }


# ============================================================================
# TEST: DELTA CALCULATION
# ============================================================================

class TestDeltaCalculation:
    """Test suite for delta calculation logic."""
    
    def test_delta_basic_regression(self):
        """Test delta calculation for regression (positive change)."""
        delta, status = calculate_delta(100, 150, "execution_time")
        assert delta == 50.0, f"Expected 50.0%, got {delta}"
        assert status == "success"
    
    def test_delta_basic_improvement(self):
        """Test delta calculation for improvement (negative change)."""
        delta, status = calculate_delta(100, 50, "execution_time")
        assert delta == -50.0, f"Expected -50.0%, got {delta}"
        assert status == "success"
    
    def test_delta_formula_accuracy(self):
        """Validate exact formula: ((candidate - baseline) / baseline) × 100."""
        # baseline=10, candidate=15 => ((15-10)/10) * 100 = 50%
        delta, status = calculate_delta(10, 15, "bytes_scanned")
        assert delta == 50.0, f"Formula validation failed: {delta}"
    
    def test_delta_zero_baseline(self):
        """Test division by zero handling when baseline is 0."""
        delta, status = calculate_delta(0, 100, "cost")
        assert delta is None, "Delta should be None for zero baseline"
        assert status == "N/A - baseline zero", f"Got status: {status}"
    
    def test_delta_null_baseline(self):
        """Test null/missing baseline value."""
        delta, status = calculate_delta(None, 50, "execution_time")
        assert delta is None
        assert status == "null_value"
    
    def test_delta_null_candidate(self):
        """Test null/missing candidate value."""
        delta, status = calculate_delta(100, None, "cost")
        assert delta is None
        assert status == "null_value"
    
    def test_delta_no_change(self):
        """Test delta when values are unchanged."""
        delta, status = calculate_delta(100, 100, "cost")
        assert delta == 0.0, f"Expected 0.0%, got {delta}"
        assert status == "success"
    
    def test_delta_perfect_improvement(self):
        """Test delta for perfect improvement (100% reduction)."""
        delta, status = calculate_delta(100, 0, "execution_time")
        assert delta == -100.0, f"Expected -100.0%, got {delta}"
        assert status == "success"
    
    def test_direction_improvement(self):
        """Test direction indicator for improvement."""
        improvement_metrics = get_improvement_metrics()
        direction = determine_direction(-10.0, "execution_time", improvement_metrics)
        assert direction == "+", "Negative delta on lower-is-better metric should be '+'"
    
    def test_direction_regression(self):
        """Test direction indicator for regression."""
        improvement_metrics = get_improvement_metrics()
        direction = determine_direction(20.0, "cost", improvement_metrics)
        assert direction == "-", "Positive delta on lower-is-better metric should be '-'"
    
    def test_delta_result_structure(self):
        """Test DeltaResult object contains all required fields."""
        result = DeltaResult(
            delta=-10.5,
            direction="+",
            status="success",
            annotation=None
        )
        assert result.delta == -10.5
        assert result.direction == "+"
        assert result.status == "success"
        assert result.annotation is None


# ============================================================================
# TEST: BOTTLENECK DETECTION
# ============================================================================

class TestBottleneckDetection:
    """Test suite for bottleneck detection logic."""
    
    def test_execution_time_regression_threshold(self):
        """Test flagging execution time regression >10%."""
        is_regression = check_execution_time_regression(15.0, 10.0)
        assert is_regression is True, "Should flag 15% > 10% threshold"
    
    def test_execution_time_below_threshold(self):
        """Test no flag when execution time regression is below 10%."""
        is_regression = check_execution_time_regression(5.0, 10.0)
        assert is_regression is False, "Should not flag 5% < 10% threshold"
    
    def test_execution_time_boundary(self):
        """Test boundary case: exactly at threshold (10%)."""
        is_regression = check_execution_time_regression(10.0, 10.0)
        assert is_regression is False, "Exact threshold should not be flagged"
    
    def test_execution_time_improvement(self):
        """Test improvements are not flagged as regression."""
        is_regression = check_execution_time_regression(-10.0, 10.0)
        assert is_regression is False, "Improvements should not be flagged"
    
    def test_cost_regression_threshold(self):
        """Test flagging cost regression >20%."""
        is_regression = check_cost_regression(25.0, 20.0)
        assert is_regression is True, "Should flag 25% > 20% threshold"
    
    def test_cost_below_threshold(self):
        """Test no flag when cost regression is below 20%."""
        is_regression = check_cost_regression(15.0, 20.0)
        assert is_regression is False, "Should not flag 15% < 20% threshold"
    
    def test_cost_boundary(self):
        """Test boundary case: exactly at cost threshold (20%)."""
        is_regression = check_cost_regression(20.0, 20.0)
        assert is_regression is False, "Exact threshold should not be flagged"
    
    def test_cost_improvement(self):
        """Test cost improvements are not flagged."""
        is_regression = check_cost_regression(-15.0, 20.0)
        assert is_regression is False, "Cost improvements should not be flagged"
    
    def test_data_drift_detected(self):
        """Test data drift detection with mismatched hashes."""
        result = DeltaResult(
            delta=5.0,
            direction="-",
            status="success",
            annotation="⚠ data drift detected"
        )
        has_drift = check_data_drift(result)
        assert has_drift is True, "Should detect data drift annotation"
    
    def test_data_drift_not_detected(self):
        """Test no drift when hashes match."""
        result = DeltaResult(
            delta=5.0,
            direction="-",
            status="success",
            annotation=None
        )
        has_drift = check_data_drift(result)
        assert has_drift is False, "Should not detect drift when hashes match"
    
    def test_kpi_categorization_improved(self):
        """Test KPI categorized as 'improved'."""
        result = DeltaResult(delta=-10.0, direction="+", status="success")
        cat = categorize_kpi("execution_time", result)
        assert cat.category == "improved", f"Expected 'improved', got '{cat.category}'"
    
    def test_kpi_categorization_regressed(self):
        """Test KPI categorized as 'regressed'."""
        result = DeltaResult(delta=25.0, direction="-", status="success")
        cat = categorize_kpi("cost", result)
        assert cat.category == "regressed", f"Expected 'regressed', got '{cat.category}'"
    
    def test_kpi_categorization_neutral_small(self):
        """Test KPI categorized as 'neutral' for small changes (<0.5%)."""
        result = DeltaResult(delta=0.2, direction="+", status="success")
        cat = categorize_kpi("execution_time", result)
        assert cat.category == "neutral", f"Expected 'neutral', got '{cat.category}'"
    
    def test_kpi_categorization_neutral_na(self):
        """Test KPI categorized as 'neutral' when direction is N/A."""
        result = DeltaResult(delta=None, direction="N/A", status="baseline_zero")
        cat = categorize_kpi("bytes_scanned", result)
        assert cat.category == "neutral", f"Expected 'neutral', got '{cat.category}'"


# ============================================================================
# TEST: DATA EQUIVALENCE DETECTION
# ============================================================================

class TestDataEquivalence:
    """Test suite for data equivalence detection via hashing."""
    
    def test_matching_hashes_no_warning(self):
        """Test matching hashes produce NO data drift warning."""
        baseline = {
            "execution_time": 10.0,
            "data_hash": "abc123def456"
        }
        candidate = {
            "execution_time": 9.0,
            "data_hash": "abc123def456"  # Same hash
        }
        
        deltas = calculate_all_deltas(baseline, candidate, check_data_hash=True)
        result = deltas.get("execution_time")
        
        assert result.annotation != "⚠ data drift detected", "Should not flag matching hashes"
    
    def test_mismatched_hashes_warning(self):
        """Test mismatched hashes ARE flagged with data drift warning."""
        baseline = {
            "execution_time": 10.0,
            "data_hash": "abc123def456"
        }
        candidate = {
            "execution_time": 9.0,
            "data_hash": "xyz789uvw012"  # Different hash
        }
        
        deltas = calculate_all_deltas(baseline, candidate, check_data_hash=True)
        result = deltas.get("execution_time")
        
        assert result.annotation == "⚠ data drift detected", "Should flag hash mismatch"
    
    def test_missing_hash_fields(self):
        """Test graceful handling when hash fields are missing."""
        baseline = {"execution_time": 10.0}  # No data_hash
        candidate = {"execution_time": 9.0}  # No data_hash
        
        # Should process without error
        deltas = calculate_all_deltas(baseline, candidate, check_data_hash=True)
        assert "execution_time" in deltas
        assert deltas["execution_time"].status == "success"


# ============================================================================
# TEST: RECOMMENDATION GENERATION
# ============================================================================

class TestRecommendationGeneration:
    """Test suite for recommendation generation."""
    
    def test_high_join_count_rule_triggered(self, config):
        """Test HIGH_JOIN_COUNT rule triggered for ≥5 JOINs."""
        metrics = {"join_count": 6}  # Greater than threshold (5)
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_JOIN_COUNT" in rule_ids, "Should trigger HIGH_JOIN_COUNT rule"
    
    def test_high_join_count_not_triggered(self, config):
        """Test HIGH_JOIN_COUNT rule NOT triggered for <5 JOINs."""
        metrics = {"join_count": 4}  # Less than threshold
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_JOIN_COUNT" not in rule_ids, "Should not trigger at 4 JOINs"
    
    def test_high_cte_count_rule_triggered(self, config):
        """Test HIGH_CTE_COUNT rule triggered for ≥3 CTEs."""
        metrics = {"cte_count": 4}  # Greater than threshold (3)
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_CTE_COUNT" in rule_ids, "Should trigger HIGH_CTE_COUNT rule"
    
    def test_high_cte_count_not_triggered(self, config):
        """Test HIGH_CTE_COUNT rule NOT triggered for <3 CTEs."""
        metrics = {"cte_count": 2}  # Less than threshold
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_CTE_COUNT" not in rule_ids, "Should not trigger at 2 CTEs"
    
    def test_high_window_function_rule_triggered(self, config):
        """Test HIGH_WINDOW_FUNCTION_COUNT rule triggered for ≥2 functions."""
        metrics = {"window_function_count": 3}  # Greater than threshold (2)
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_WINDOW_FUNCTION_COUNT" in rule_ids, "Should trigger window function rule"
    
    def test_high_window_function_not_triggered(self, config):
        """Test HIGH_WINDOW_FUNCTION_COUNT NOT triggered for <2 functions."""
        metrics = {"window_function_count": 1}  # Less than threshold
        rules = find_matching_rules(metrics, config)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_WINDOW_FUNCTION_COUNT" not in rule_ids, "Should not trigger at 1 function"
    
    def test_multiple_rules_triggered(self, config):
        """Test multiple rules triggered together."""
        metrics = {
            "join_count": 7,
            "cte_count": 5,
            "window_function_count": 3
        }
        rules = find_matching_rules(metrics, config)
        assert len(rules) >= 3, f"Expected ≥3 rules, got {len(rules)}"
    
    def test_priority_score_calculation(self):
        """Test priority score calculation."""
        score = calculate_priority_score(80.0, 7, 5)
        assert 0 <= score <= 100, "Priority score must be 0-100"
    
    def test_priority_level_high(self):
        """Test priority level HIGH (score >66)."""
        level = get_priority_level(75.0)
        assert level == "HIGH", f"Expected HIGH, got {level}"
    
    def test_priority_level_medium(self):
        """Test priority level MEDIUM (33-66)."""
        level = get_priority_level(50.0)
        assert level == "MEDIUM", f"Expected MEDIUM, got {level}"
    
    def test_priority_level_low(self):
        """Test priority level LOW (score <33)."""
        level = get_priority_level(20.0)
        assert level == "LOW", f"Expected LOW, got {level}"
    
    def test_priority_level_cost_boost(self):
        """Test cost regression >20% forces HIGH priority."""
        level = get_priority_level(10.0, cost_regression=25.0)
        assert level == "HIGH", "Cost regression >20% should force HIGH"


# ============================================================================
# TEST: EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test suite for edge cases and error handling."""
    
    def test_zero_baseline_multiple_metrics(self):
        """Test zero baseline handling across multiple metrics."""
        baseline = {
            "execution_time": 0,
            "cost": 0,
            "bytes_scanned": 100
        }
        candidate = {
            "execution_time": 5,
            "cost": 10,
            "bytes_scanned": 120
        }
        
        deltas = calculate_all_deltas(baseline, candidate)
        
        assert deltas["execution_time"].status == "N/A - baseline zero"
        assert deltas["cost"].status == "N/A - baseline zero"
        assert deltas["bytes_scanned"].status == "success"
    
    def test_missing_model_new(self, baseline_models, candidate_models):
        """Test new model detection (in candidate but not baseline)."""
        deltas = calculate_model_deltas(baseline_models, candidate_models)
        
        assert "_status" in deltas["model_c"]
        assert deltas["model_c"]["_status"] == "new_model"
    
    def test_missing_model_removed(self, baseline_models, candidate_models):
        """Test removed model detection (in baseline but not candidate)."""
        deltas = calculate_model_deltas(baseline_models, candidate_models)
        
        # Create reversed scenario for removed models
        deltas_reversed = calculate_model_deltas(candidate_models, baseline_models)
        assert "_status" in deltas_reversed["model_c"]
    
    def test_output_serialization(self):
        """Test delta output is serializable for JSON."""
        model_deltas = {
            "model_1": {
                "execution_time": DeltaResult(delta=-10.0, direction="+", status="success"),
                "cost": DeltaResult(delta=20.0, direction="-", status="success")
            }
        }
        
        formatted = format_delta_output(model_deltas)
        
        assert isinstance(formatted["model_1"]["execution_time"], dict)
        assert "delta" in formatted["model_1"]["execution_time"]
        assert "direction" in formatted["model_1"]["execution_time"]
        assert "status" in formatted["model_1"]["execution_time"]
    
    def test_bottleneck_result_structure(self):
        """Test BottleneckResult has required fields."""
        bottleneck = BottleneckResult(
            model_name="test_model",
            impact_score=50.0,
            regression_flags=["EXECUTION_TIME_REGRESSION"],
            severity="HIGH"
        )
        
        assert bottleneck.model_name == "test_model"
        assert bottleneck.impact_score == 50.0
        assert "EXECUTION_TIME_REGRESSION" in bottleneck.regression_flags
    
    def test_recommendation_structure(self):
        """Test Recommendation object structure."""
        rec = Recommendation(
            model_name="model_a",
            rule_id="HIGH_JOIN_COUNT",
            rule_name="High JOIN Count",
            priority="HIGH",
            priority_score=85.0,
            optimization_technique="JOIN Consolidation",
            sql_pattern_suggestion=["Use temp tables"],
            rationale="Multiple JOINs reduce optimizer effectiveness"
        )
        
        assert rec.model_name == "model_a"
        assert rec.priority == "HIGH"
        assert rec.priority_score == 85.0


# ============================================================================
# TEST: COMPREHENSIVE SCENARIOS
# ============================================================================

class TestComprehensiveScenarios:
    """Test comprehensive multi-model, multi-KPI scenarios."""
    
    def test_multi_model_mixed_scenarios(self, baseline_models, candidate_models):
        """Test complex scenario with improvements, regressions, and new/removed models."""
        deltas = calculate_model_deltas(baseline_models, candidate_models)
        
        # Verify all expected models present
        assert len(deltas) >= 3
        
        # model_a should have calculations (exists in both)
        assert isinstance(deltas["model_a"].get("execution_time"), DeltaResult)
        
        # model_c should be marked as new
        assert deltas["model_c"]["_status"] == "new_model"
    
    def test_delta_all_kpis(self, baseline_kpis, candidate_kpis):
        """Test delta calculation across all KPI types."""
        deltas = calculate_all_deltas(baseline_kpis, candidate_kpis)
        
        # Should have calculated deltas for numeric KPIs
        assert "execution_time" in deltas
        assert "cost" in deltas
        assert "bytes_scanned" in deltas
        
        # All should be DeltaResult objects
        for kpi, result in deltas.items():
            assert isinstance(result, DeltaResult) or isinstance(result, (type(None)))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
