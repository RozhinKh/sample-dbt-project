#!/usr/bin/env python3
"""
Test suite for delta calculation module.

Validates delta calculation logic with comprehensive test cases covering:
- Basic delta calculation with positive and negative changes
- Edge cases: zero baseline, null values, type errors
- Direction indicators: improvement vs regression for different metric types
- Data drift detection
- New/removed model handling
- Multiple models and KPIs
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

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
from helpers import setup_logging


def test_basic_delta_calculation():
    """Test basic delta formula: ((candidate - baseline) / baseline) × 100"""
    print("\n" + "=" * 80)
    print("TEST 1: Basic Delta Calculation")
    print("=" * 80)
    
    tests = [
        (100, 50, "execution_time", -50.0, "success"),     # 50% reduction = -50%
        (100, 150, "execution_time", 50.0, "success"),     # 50% increase = +50%
        (10, 9, "execution_time", -10.0, "success"),       # 10% reduction
        (50, 100, "cost", 100.0, "success"),               # 100% increase
        (1000, 1000, "execution_time", 0.0, "success"),    # No change
    ]
    
    for baseline, candidate, metric, expected_delta, expected_status in tests:
        delta, status = calculate_delta(baseline, candidate, metric)
        assert delta == expected_delta, f"Expected {expected_delta}, got {delta}"
        assert status == expected_status, f"Expected status {expected_status}, got {status}"
        print(f"✓ {metric}: {baseline} → {candidate} = {delta}% ({status})")
    
    print("\n✓ All basic delta calculation tests passed")


def test_zero_baseline_handling():
    """Test handling of zero baseline (division by zero)"""
    print("\n" + "=" * 80)
    print("TEST 2: Zero Baseline Handling")
    print("=" * 80)
    
    delta, status = calculate_delta(0, 50, "bytes_scanned")
    assert delta is None, "Delta should be None for zero baseline"
    assert status == "N/A - baseline zero", f"Expected 'N/A - baseline zero', got '{status}'"
    print("✓ Zero baseline correctly returns (None, 'N/A - baseline zero')")
    
    print("\n✓ Zero baseline handling tests passed")


def test_null_value_handling():
    """Test handling of null/missing values"""
    print("\n" + "=" * 80)
    print("TEST 3: Null Value Handling")
    print("=" * 80)
    
    tests = [
        (None, 50, "metric1"),
        (100, None, "metric2"),
        (None, None, "metric3"),
    ]
    
    for baseline, candidate, metric in tests:
        delta, status = calculate_delta(baseline, candidate, metric)
        assert delta is None, f"Delta should be None for null value"
        assert status == "null_value", f"Expected 'null_value', got '{status}'"
        print(f"✓ {metric}: null values handled correctly")
    
    print("\n✓ Null value handling tests passed")


def test_direction_indicators():
    """Test direction indicators for improvement vs regression"""
    print("\n" + "=" * 80)
    print("TEST 4: Direction Indicators")
    print("=" * 80)
    
    improvement_metrics = get_improvement_metrics()
    
    # Test metrics where lower is better (improvement_on_reduction)
    print("\nMetrics where lower is better (time, cost, complexity):")
    tests_reduction = [
        (-10.0, "execution_time", "+"),  # 10% reduction = improvement
        (5.0, "execution_time", "-"),    # 5% increase = regression
        (-20.0, "cost", "+"),            # 20% reduction = improvement
        (15.0, "bytes_scanned", "-"),    # 15% increase = regression
        (0.0, "cte_count", "N/A"),       # No change (special case)
    ]
    
    for delta, metric, expected_direction in tests_reduction:
        direction = determine_direction(delta, metric, improvement_metrics)
        assert direction == expected_direction, f"Expected '{expected_direction}', got '{direction}'"
        print(f"✓ {metric}: delta={delta}% → direction={direction}")
    
    print("\n✓ Direction indicator tests passed")


def test_data_drift_detection():
    """Test data drift flag for hash mismatches"""
    print("\n" + "=" * 80)
    print("TEST 5: Data Drift Detection")
    print("=" * 80)
    
    # Case 1: Hash mismatch (data drift)
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
    assert result is not None, "Delta result should exist"
    assert result.annotation == "⚠ data drift detected", "Should flag data drift"
    print(f"✓ Data drift detected and flagged: {result.annotation}")
    
    # Case 2: Matching hash (no data drift)
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
    assert result.annotation is None or "data drift" not in str(result.annotation), "Should NOT flag data drift"
    print(f"✓ Matching hashes: no data drift flagged")
    
    print("\n✓ Data drift detection tests passed")


def test_new_removed_models():
    """Test handling of new and removed models"""
    print("\n" + "=" * 80)
    print("TEST 6: New/Removed Model Handling")
    print("=" * 80)
    
    baseline_models = {
        "model_a": {"execution_time": 10.0, "cost": 25.0},
        "model_b": {"execution_time": 15.0, "cost": 35.0},
    }
    
    candidate_models = {
        "model_a": {"execution_time": 9.0, "cost": 30.0},
        "model_c": {"execution_time": 5.0, "cost": 12.0},  # New model
    }
    
    deltas = calculate_model_deltas(baseline_models, candidate_models)
    
    # Check model_a (exists in both)
    assert "execution_time" in deltas["model_a"], "model_a should have delta calculations"
    assert isinstance(deltas["model_a"]["execution_time"], DeltaResult), "Should be DeltaResult"
    print(f"✓ model_a (existing): deltas calculated ({len(deltas['model_a'])} KPIs)")
    
    # Check model_b (removed)
    assert deltas["model_b"]["_status"] == "removed_model", "model_b should be marked as removed"
    print(f"✓ model_b (removed): status='removed_model'")
    
    # Check model_c (new)
    assert deltas["model_c"]["_status"] == "new_model", "model_c should be marked as new"
    print(f"✓ model_c (new): status='new_model'")
    
    print("\n✓ New/removed model tests passed")


def test_structured_output():
    """Test structured delta result format"""
    print("\n" + "=" * 80)
    print("TEST 7: Structured Output Format")
    print("=" * 80)
    
    delta, status = calculate_delta(100, 85, "execution_time")
    result = create_delta_result(delta, status, "execution_time")
    
    assert isinstance(result, DeltaResult), "Should return DeltaResult object"
    assert result.delta == -15.0, f"Delta should be -15.0, got {result.delta}"
    assert result.direction == "+", f"Direction should be '+', got '{result.direction}'"
    assert result.status == "success", f"Status should be 'success', got '{result.status}'"
    print(f"✓ DeltaResult created: delta={result.delta}%, direction={result.direction}, status={result.status}")
    
    # Test serialization
    model_deltas = {
        "model_1": {
            "execution_time": result,
            "cost": create_delta_result(25.0, "success", "cost")
        }
    }
    
    formatted = format_delta_output(model_deltas)
    
    # Verify serializable format
    assert isinstance(formatted["model_1"]["execution_time"], dict), "Should be dict"
    assert "delta" in formatted["model_1"]["execution_time"], "Should have delta key"
    assert "direction" in formatted["model_1"]["execution_time"], "Should have direction key"
    print(f"✓ Output formatted for JSON serialization")
    
    print("\n✓ Structured output tests passed")


def test_comprehensive_scenario():
    """Test comprehensive scenario with multiple models and KPIs"""
    print("\n" + "=" * 80)
    print("TEST 8: Comprehensive Multi-Model/Multi-KPI Scenario")
    print("=" * 80)
    
    baseline_models = {
        "stg_users": {
            "execution_time": 5.0,
            "cost": 10.0,
            "bytes_scanned": 1000000,
            "join_count": 3,
            "data_hash": "hash_v1"
        },
        "stg_orders": {
            "execution_time": 8.0,
            "cost": 15.0,
            "bytes_scanned": 2000000,
            "join_count": 2,
            "data_hash": "hash_v2"
        },
    }
    
    candidate_models = {
        "stg_users": {
            "execution_time": 4.5,  # Improvement
            "cost": 12.0,  # Regression
            "bytes_scanned": 900000,  # Improvement
            "join_count": 3,  # Same
            "data_hash": "hash_v1"  # Same
        },
        "stg_orders": {
            "execution_time": 9.0,  # Regression
            "cost": 16.0,  # Regression
            "bytes_scanned": 2200000,  # Regression
            "join_count": 2,  # Same
            "data_hash": "hash_v2_updated"  # Data drift!
        },
        "stg_products": {  # New model
            "execution_time": 3.0,
            "cost": 8.0,
            "join_count": 1,
            "data_hash": "hash_v3"
        }
    }
    
    deltas = calculate_model_deltas(baseline_models, candidate_models)
    
    # Check stg_users results
    stg_users_deltas = deltas["stg_users"]
    assert "execution_time" in stg_users_deltas, "stg_users should have execution_time"
    exec_time_result = stg_users_deltas["execution_time"]
    assert exec_time_result.delta == -10.0, "execution_time should show -10% (improvement)"
    assert exec_time_result.direction == "+", "execution_time should show + (improvement)"
    print(f"✓ stg_users execution_time: -10% (improvement)")
    
    cost_result = stg_users_deltas["cost"]
    assert cost_result.delta == 20.0, "cost should show +20% (regression)"
    assert cost_result.direction == "-", "cost should show - (regression)"
    print(f"✓ stg_users cost: +20% (regression)")
    
    # Check stg_orders (with data drift)
    stg_orders_deltas = deltas["stg_orders"]
    stg_orders_exec = stg_orders_deltas["execution_time"]
    assert "data drift detected" in str(stg_orders_exec.annotation), "Should flag data drift"
    print(f"✓ stg_orders: data drift detected")
    
    # Check new model
    assert deltas["stg_products"]["_status"] == "new_model", "stg_products should be marked as new"
    print(f"✓ stg_products: new_model detected")
    
    print("\n✓ Comprehensive scenario tests passed")


def run_all_tests():
    """Run all test suites"""
    print("\n" + "=" * 80)
    print("DELTA CALCULATION MODULE TEST SUITE")
    print("=" * 80)
    
    try:
        test_basic_delta_calculation()
        test_zero_baseline_handling()
        test_null_value_handling()
        test_direction_indicators()
        test_data_drift_detection()
        test_new_removed_models()
        test_structured_output()
        test_comprehensive_scenario()
        
        print("\n" + "=" * 80)
        print("✓ ALL TESTS PASSED")
        print("=" * 80 + "\n")
        return True
    
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
