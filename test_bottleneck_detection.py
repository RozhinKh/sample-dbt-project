#!/usr/bin/env python3
"""
Test suite for bottleneck detection and model classification module.

Validates bottleneck detection logic with comprehensive test cases covering:
- Regression threshold checking (>10% execution time, >20% cost)
- Data drift detection from delta annotations
- Model categorization (improved/regressed/neutral)
- Impact score calculation with weighted factors
- Bottleneck ranking and top-N summary generation
- Edge cases: no regressions, all improved, single model, critical issues
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from bottleneck import (
    check_execution_time_regression,
    check_cost_regression,
    check_data_drift,
    categorize_kpi,
    categorize_model_kpis,
    calculate_impact_score,
    detect_bottlenecks,
    rank_bottlenecks_by_impact,
    generate_bottleneck_summary,
    format_bottleneck_output,
    KPICategorization,
    BottleneckResult
)
from delta import DeltaResult
from helpers import setup_logging


def test_execution_time_regression_threshold():
    """Test execution time regression threshold checking"""
    print("\n" + "=" * 80)
    print("TEST 1: Execution Time Regression Threshold")
    print("=" * 80)
    
    # Test cases: (delta, threshold, expected_result)
    tests = [
        (15.0, 10.0, True),   # 15% > 10% threshold
        (5.0, 10.0, False),   # 5% < 10% threshold
        (10.0, 10.0, False),  # 10% = 10% (boundary, not regression)
        (10.01, 10.0, True),  # 10.01% > 10% threshold
        (-5.0, 10.0, False),  # Negative = improvement, not regression
        (None, 10.0, False),  # None = no regression
    ]
    
    for delta, threshold, expected in tests:
        result = check_execution_time_regression(delta, threshold)
        assert result == expected, f"Expected {expected}, got {result} for delta={delta}"
        print(f"✓ delta={delta}%, threshold={threshold}% → regression={result}")
    
    print("\n✓ All execution time regression tests passed")


def test_cost_regression_threshold():
    """Test cost regression threshold checking"""
    print("\n" + "=" * 80)
    print("TEST 2: Cost Regression Threshold")
    print("=" * 80)
    
    # Test cases: (delta, threshold, expected_result)
    tests = [
        (25.0, 20.0, True),   # 25% > 20% threshold
        (15.0, 20.0, False),  # 15% < 20% threshold
        (20.0, 20.0, False),  # 20% = 20% (boundary, not regression)
        (20.01, 20.0, True),  # 20.01% > 20% threshold
        (-10.0, 20.0, False), # Negative = improvement
        (None, 20.0, False),  # None = no regression
    ]
    
    for delta, threshold, expected in tests:
        result = check_cost_regression(delta, threshold)
        assert result == expected, f"Expected {expected}, got {result} for delta={delta}"
        print(f"✓ delta={delta}%, threshold={threshold}% → regression={result}")
    
    print("\n✓ All cost regression tests passed")


def test_data_drift_detection():
    """Test data drift detection from delta results"""
    print("\n" + "=" * 80)
    print("TEST 3: Data Drift Detection")
    print("=" * 80)
    
    # Case 1: Data drift flag present
    result_with_drift = DeltaResult(
        delta=-10.0,
        direction="+",
        status="success",
        annotation="⚠ data drift detected"
    )
    assert check_data_drift(result_with_drift) == True, "Should detect data drift"
    print("✓ Data drift flag detected")
    
    # Case 2: No drift flag
    result_no_drift = DeltaResult(
        delta=-10.0,
        direction="+",
        status="success",
        annotation=None
    )
    assert check_data_drift(result_no_drift) == False, "Should NOT detect drift"
    print("✓ No drift flag: correctly identified")
    
    # Case 3: None input
    assert check_data_drift(None) == False, "Should handle None"
    print("✓ None input handled correctly")
    
    print("\n✓ All data drift detection tests passed")


def test_kpi_categorization():
    """Test KPI categorization (improved/regressed/neutral)"""
    print("\n" + "=" * 80)
    print("TEST 4: KPI Categorization")
    print("=" * 80)
    
    # Test improvement
    result_improved = DeltaResult(
        delta=-15.0,
        direction="+",
        status="success"
    )
    cat = categorize_kpi("execution_time", result_improved)
    assert cat.category == "improved", f"Expected 'improved', got '{cat.category}'"
    print(f"✓ Improvement: delta=-15.0, direction=+ → category='improved'")
    
    # Test regression
    result_regressed = DeltaResult(
        delta=25.0,
        direction="-",
        status="success"
    )
    cat = categorize_kpi("cost", result_regressed)
    assert cat.category == "regressed", f"Expected 'regressed', got '{cat.category}'"
    print(f"✓ Regression: delta=25.0, direction=- → category='regressed'")
    
    # Test neutral (small delta)
    result_neutral = DeltaResult(
        delta=0.2,
        direction="+",
        status="success"
    )
    cat = categorize_kpi("execution_time", result_neutral)
    assert cat.category == "neutral", f"Expected 'neutral', got '{cat.category}'"
    print(f"✓ Neutral: delta=0.2% (small) → category='neutral'")
    
    # Test neutral (N/A direction)
    result_na = DeltaResult(
        delta=None,
        direction="N/A",
        status="baseline_zero"
    )
    cat = categorize_kpi("bytes_scanned", result_na)
    assert cat.category == "neutral", f"Expected 'neutral', got '{cat.category}'"
    print(f"✓ Neutral: direction=N/A → category='neutral'")
    
    print("\n✓ All KPI categorization tests passed")


def test_model_kpi_categorization():
    """Test categorization of all KPIs for a model"""
    print("\n" + "=" * 80)
    print("TEST 5: Model KPI Categorization")
    print("=" * 80)
    
    model_deltas = {
        "execution_time": DeltaResult(delta=-10.0, direction="+", status="success"),
        "cost": DeltaResult(delta=25.0, direction="-", status="success"),
        "bytes_scanned": DeltaResult(delta=5.0, direction="-", status="success"),
        "_status": "not_counted"  # Should be skipped
    }
    
    categorizations = categorize_model_kpis("test_model", model_deltas)
    
    assert len(categorizations) == 3, f"Expected 3 categorizations, got {len(categorizations)}"
    assert categorizations["execution_time"].category == "improved"
    assert categorizations["cost"].category == "regressed"
    assert categorizations["bytes_scanned"].category == "regressed"
    assert "_status" not in categorizations, "Should skip _status field"
    
    print(f"✓ execution_time: improved")
    print(f"✓ cost: regressed")
    print(f"✓ bytes_scanned: regressed")
    print(f"✓ Special fields (_status) skipped")
    
    print("\n✓ All model KPI categorization tests passed")


def test_impact_score_calculation():
    """Test impact score calculation with weighted factors"""
    print("\n" + "=" * 80)
    print("TEST 6: Impact Score Calculation")
    print("=" * 80)
    
    # Test 1: Execution time only
    score = calculate_impact_score(15.0, None, False)
    # 15% / 100 * 0.4 * 100 = 6.0
    assert score == 6.0, f"Expected 6.0, got {score}"
    print(f"✓ Execution time 15%: score={score}")
    
    # Test 2: Cost only
    score = calculate_impact_score(None, 25.0, False)
    # 25% / 100 * 0.4 * 100 = 10.0
    assert score == 10.0, f"Expected 10.0, got {score}"
    print(f"✓ Cost 25%: score={score}")
    
    # Test 3: Both metrics
    score = calculate_impact_score(15.0, 25.0, False)
    # (15% * 0.4 + 25% * 0.4) / 100 * 100 = 16.0
    assert score == 16.0, f"Expected 16.0, got {score}"
    print(f"✓ Execution time 15% + Cost 25%: score={score}")
    
    # Test 4: With data drift
    score = calculate_impact_score(15.0, 25.0, True)
    # (15% * 0.4 + 25% * 0.4 + 1 * 0.2) / 100 * 100 = 36.0
    assert score == 36.0, f"Expected 36.0, got {score}"
    print(f"✓ Both metrics + data drift: score={score}")
    
    # Test 5: No regressions
    score = calculate_impact_score(-5.0, -10.0, False)
    assert score == 0.0, f"Expected 0.0, got {score}"
    print(f"✓ All improvements (negative deltas): score={score}")
    
    # Test 6: Cap at 100% per metric
    score = calculate_impact_score(200.0, 200.0, False)
    # max(200%/100, 1.0) * 0.4 + max(200%/100, 1.0) * 0.4 = 0.4 + 0.4 = 0.8, * 100 = 80
    assert score == 80.0, f"Expected 80.0, got {score}"
    print(f"✓ Over-cap handling (200% each): score={score}")
    
    print("\n✓ All impact score calculation tests passed")


def test_bottleneck_detection():
    """Test bottleneck detection with various scenarios"""
    print("\n" + "=" * 80)
    print("TEST 7: Bottleneck Detection")
    print("=" * 80)
    
    # Create model deltas
    model_deltas = {
        "model_a": {  # Critical: execution time regression + data drift
            "execution_time": DeltaResult(
                delta=15.0,
                direction="-",
                status="success",
                annotation="⚠ data drift detected"
            ),
            "cost": DeltaResult(delta=-5.0, direction="+", status="success")
        },
        "model_b": {  # High: execution time regression only
            "execution_time": DeltaResult(delta=12.0, direction="-", status="success"),
            "cost": DeltaResult(delta=5.0, direction="-", status="success")
        },
        "model_c": {  # Medium: cost regression only
            "execution_time": DeltaResult(delta=5.0, direction="-", status="success"),
            "cost": DeltaResult(delta=25.0, direction="-", status="success")
        },
        "model_d": {  # Low: no regressions
            "execution_time": DeltaResult(delta=-10.0, direction="+", status="success"),
            "cost": DeltaResult(delta=-5.0, direction="+", status="success")
        },
        "model_new": {  # New model - should be skipped
            "_status": "new_model"
        }
    }
    
    config = {
        "bottleneck_thresholds": {
            "execution_time": {"regression_threshold_percent": 10.0},
            "cost": {"regression_threshold_percent": 20.0}
        }
    }
    
    bottlenecks = detect_bottlenecks(model_deltas, config)
    
    # Verify results
    assert len(bottlenecks) == 4, f"Expected 4 bottlenecks, got {len(bottlenecks)}"
    assert "model_new" not in bottlenecks, "New model should be skipped"
    
    # Check severities
    assert bottlenecks["model_a"].severity == "CRITICAL", "model_a should be CRITICAL"
    assert bottlenecks["model_b"].severity == "HIGH", "model_b should be HIGH"
    assert bottlenecks["model_c"].severity == "MEDIUM", "model_c should be MEDIUM"
    assert bottlenecks["model_d"].severity == "LOW", "model_d should be LOW"
    
    # Check flags
    assert "EXECUTION_TIME_REGRESSION" in bottlenecks["model_a"].regression_flags
    assert "DATA_DRIFT" in bottlenecks["model_a"].regression_flags
    assert "COST_REGRESSION" in bottlenecks["model_c"].regression_flags
    
    print(f"✓ model_a: CRITICAL (exec_time + data_drift)")
    print(f"✓ model_b: HIGH (exec_time regression)")
    print(f"✓ model_c: MEDIUM (cost regression)")
    print(f"✓ model_d: LOW (improved metrics)")
    print(f"✓ model_new: skipped (new model)")
    
    print("\n✓ All bottleneck detection tests passed")


def test_bottleneck_ranking():
    """Test bottleneck ranking by impact score"""
    print("\n" + "=" * 80)
    print("TEST 8: Bottleneck Ranking")
    print("=" * 80)
    
    bottlenecks = {
        "model_a": BottleneckResult("model_a", impact_score=35.0, severity="HIGH"),
        "model_b": BottleneckResult("model_b", impact_score=10.0, severity="MEDIUM"),
        "model_c": BottleneckResult("model_c", impact_score=50.0, severity="CRITICAL"),
        "model_d": BottleneckResult("model_d", impact_score=0.0, severity="LOW"),
    }
    
    ranked = rank_bottlenecks_by_impact(bottlenecks)
    
    # Should be ordered by impact score descending
    assert ranked[0].model_name == "model_c", f"Expected model_c first, got {ranked[0].model_name}"
    assert ranked[1].model_name == "model_a", f"Expected model_a second, got {ranked[1].model_name}"
    assert ranked[2].model_name == "model_b", f"Expected model_b third, got {ranked[2].model_name}"
    assert ranked[3].model_name == "model_d", f"Expected model_d last, got {ranked[3].model_name}"
    
    print(f"✓ Ranking 1: {ranked[0].model_name} (score={ranked[0].impact_score})")
    print(f"✓ Ranking 2: {ranked[1].model_name} (score={ranked[1].impact_score})")
    print(f"✓ Ranking 3: {ranked[2].model_name} (score={ranked[2].impact_score})")
    print(f"✓ Ranking 4: {ranked[3].model_name} (score={ranked[3].impact_score})")
    
    print("\n✓ All bottleneck ranking tests passed")


def test_bottleneck_summary_generation():
    """Test top-N bottleneck summary generation"""
    print("\n" + "=" * 80)
    print("TEST 9: Bottleneck Summary Generation")
    print("=" * 80)
    
    # Create bottlenecks
    bottlenecks = {
        f"model_{i}": BottleneckResult(
            f"model_{i}",
            impact_score=100 - (i * 10),
            regression_flags=["EXECUTION_TIME_REGRESSION"],
            regression_amounts={"execution_time": 15.0},
            severity="HIGH" if i < 3 else "MEDIUM"
        )
        for i in range(15)  # 15 bottlenecks
    }
    
    # Generate top 10
    summary = generate_bottleneck_summary(bottlenecks, top_n=10)
    
    assert len(summary) == 10, f"Expected 10 items in summary, got {len(summary)}"
    
    # Verify ordering
    for i, item in enumerate(summary):
        expected_score = 100 - (i * 10)
        assert item["impact_score"] == expected_score, \
            f"Expected score {expected_score} at position {i}, got {item['impact_score']}"
        print(f"✓ Position {i+1}: {item['model_name']}: {item['impact_score']:.2f}")
    
    # Verify structure
    assert "model_name" in summary[0]
    assert "impact_score" in summary[0]
    assert "severity" in summary[0]
    assert "regression_flags" in summary[0]
    assert "kpi_categorizations" in summary[0]
    
    print("\n✓ All summary generation tests passed")


def test_edge_cases():
    """Test edge cases: no regressions, all improved, single model"""
    print("\n" + "=" * 80)
    print("TEST 10: Edge Cases")
    print("=" * 80)
    
    config = {
        "bottleneck_thresholds": {
            "execution_time": {"regression_threshold_percent": 10.0},
            "cost": {"regression_threshold_percent": 20.0}
        }
    }
    
    # Case 1: All models improved
    all_improved = {
        "model_1": {
            "execution_time": DeltaResult(delta=-15.0, direction="+", status="success"),
            "cost": DeltaResult(delta=-10.0, direction="+", status="success")
        },
        "model_2": {
            "execution_time": DeltaResult(delta=-5.0, direction="+", status="success"),
            "cost": DeltaResult(delta=-8.0, direction="+", status="success")
        }
    }
    
    bottlenecks = detect_bottlenecks(all_improved, config)
    assert len(bottlenecks) == 2, "Should include all models"
    for b in bottlenecks.values():
        assert b.severity == "LOW", "All improved models should be LOW severity"
    print("✓ All improved: correctly identified as LOW severity")
    
    # Case 2: Single model with regression
    single_bottleneck = {
        "model_critical": {
            "execution_time": DeltaResult(delta=25.0, direction="-", status="success"),
            "cost": DeltaResult(delta=35.0, direction="-", status="success",
                              annotation="⚠ data drift detected")
        }
    }
    
    bottlenecks = detect_bottlenecks(single_bottleneck, config)
    assert len(bottlenecks) == 1, "Should have 1 bottleneck"
    assert bottlenecks["model_critical"].severity == "CRITICAL"
    print("✓ Single model: correctly identified as CRITICAL")
    
    # Case 3: Empty input
    bottlenecks = detect_bottlenecks({}, config)
    assert len(bottlenecks) == 0, "Empty input should return empty results"
    print("✓ Empty input: handled correctly")
    
    # Case 4: Only new/removed models
    only_status = {
        "model_new": {"_status": "new_model"},
        "model_removed": {"_status": "removed_model"}
    }
    
    bottlenecks = detect_bottlenecks(only_status, config)
    assert len(bottlenecks) == 0, "New/removed models should be skipped"
    print("✓ Only status models: correctly skipped")
    
    print("\n✓ All edge case tests passed")


def test_output_formatting():
    """Test complete output formatting for JSON export"""
    print("\n" + "=" * 80)
    print("TEST 11: Output Formatting")
    print("=" * 80)
    
    bottlenecks = {
        "model_a": BottleneckResult(
            "model_a",
            impact_score=45.0,
            kpi_categorizations={
                "execution_time": KPICategorization("execution_time", "regressed", 15.0),
                "cost": KPICategorization("cost", "neutral", 2.0)
            },
            regression_flags=["EXECUTION_TIME_REGRESSION"],
            regression_amounts={"execution_time": 15.0},
            data_drift_detected=False,
            severity="HIGH"
        )
    }
    
    summary = generate_bottleneck_summary(bottlenecks, top_n=10)
    output = format_bottleneck_output(bottlenecks, summary)
    
    # Verify structure
    assert "total_models_analyzed" in output
    assert "models_with_bottlenecks" in output
    assert "critical_bottlenecks" in output
    assert "summary" in output
    assert "all_bottlenecks" in output
    
    assert output["total_models_analyzed"] == 1
    assert output["models_with_bottlenecks"] == 1
    assert output["critical_bottlenecks"] == []  # No CRITICAL models
    assert len(output["summary"]) == 1
    assert len(output["all_bottlenecks"]) == 1
    
    # Verify JSON serializability
    import json
    json_str = json.dumps(output)
    assert len(json_str) > 0, "Output should be JSON serializable"
    
    print("✓ Output structure verified")
    print("✓ JSON serialization validated")
    print("\n✓ All output formatting tests passed")


def run_all_tests():
    """Run all test cases"""
    print("\n" + "=" * 80)
    print("BOTTLENECK DETECTION TEST SUITE")
    print("=" * 80)
    
    tests = [
        ("Execution Time Regression Threshold", test_execution_time_regression_threshold),
        ("Cost Regression Threshold", test_cost_regression_threshold),
        ("Data Drift Detection", test_data_drift_detection),
        ("KPI Categorization", test_kpi_categorization),
        ("Model KPI Categorization", test_model_kpi_categorization),
        ("Impact Score Calculation", test_impact_score_calculation),
        ("Bottleneck Detection", test_bottleneck_detection),
        ("Bottleneck Ranking", test_bottleneck_ranking),
        ("Bottleneck Summary Generation", test_bottleneck_summary_generation),
        ("Edge Cases", test_edge_cases),
        ("Output Formatting", test_output_formatting),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            failed += 1
            print(f"\n✗ {test_name} FAILED: {e}")
        except Exception as e:
            failed += 1
            print(f"\n✗ {test_name} ERROR: {e}")
    
    print("\n" + "=" * 80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
