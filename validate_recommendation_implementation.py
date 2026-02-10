#!/usr/bin/env python3
"""
Validation script for recommendation engine implementation.
Verifies all success criteria are met.
"""

import json
import logging
from typing import Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "="*70)
    print(f"  {text}")
    print("="*70)

def print_success(text: str):
    """Print a success message."""
    print(f"  ✅ {text}")

def print_failure(text: str):
    """Print a failure message."""
    print(f"  ❌ {text}")

def validate_config_rules():
    """Validate config.py has SQL pattern templates."""
    print_header("Validating config.py Optimization Rules")
    
    try:
        from config import load_config
        config = load_config()
        
        rules = config.get('optimization_rules', [])
        print_success(f"Loaded {len(rules)} optimization rules from config.py")
        
        # Check each rule has required fields
        required_fields = [
            'rule_id', 'name', 'metric', 'threshold', 'severity',
            'optimization_technique', 'sql_pattern_suggestion', 'rationale'
        ]
        
        all_valid = True
        for rule in rules:
            missing = [f for f in required_fields if f not in rule]
            if missing:
                print_failure(f"Rule {rule.get('rule_id')} missing: {missing}")
                all_valid = False
        
        if all_valid:
            print_success("All rules have required fields (including SQL patterns)")
        
        # Show rules
        for rule in rules:
            print(f"    - {rule['rule_id']}: {rule['name']}")
        
        return all_valid
    except Exception as e:
        print_failure(f"Failed to load config: {e}")
        return False

def validate_recommendation_module():
    """Validate recommendation.py module."""
    print_header("Validating recommendation.py Module")
    
    try:
        from recommendation import (
            Recommendation,
            calculate_priority_score,
            get_priority_level,
            find_matching_rules,
            generate_recommendations_for_model,
            generate_recommendations,
            rank_recommendations_by_priority,
            generate_recommendation_summary
        )
        print_success("All recommendation functions imported successfully")
        return True
    except Exception as e:
        print_failure(f"Failed to import recommendation module: {e}")
        return False

def validate_recommendation_dataclass():
    """Validate Recommendation dataclass structure."""
    print_header("Validating Recommendation Data Structure")
    
    try:
        from recommendation import Recommendation
        
        rec = Recommendation(
            model_name="test_model",
            rule_id="HIGH_JOIN_COUNT",
            rule_name="Test Rule",
            priority="HIGH",
            priority_score=85.5,
            optimization_technique="Test Technique",
            sql_pattern_suggestion=["Pattern 1", "Pattern 2"],
            rationale="Test rationale",
            impact_score=80.0,
            complexity_metric="join_count",
            complexity_value=7,
            threshold_value=5
        )
        
        print_success("Recommendation dataclass created successfully")
        
        # Verify to_dict() works
        rec_dict = rec.to_dict()
        print_success("to_dict() method works")
        
        # Verify JSON serialization
        json_str = json.dumps(rec_dict)
        print_success("JSON serialization works")
        
        return True
    except Exception as e:
        print_failure(f"Recommendation dataclass validation failed: {e}")
        return False

def validate_priority_calculation():
    """Validate priority calculation logic."""
    print_header("Validating Priority Calculation")
    
    try:
        from recommendation import calculate_priority_score, get_priority_level
        
        # Test 1: Basic calculation
        score = calculate_priority_score(80.0, 7, 5)
        assert score > 0, "Priority score should be positive"
        assert score <= 100, "Priority score should be capped at 100"
        print_success(f"Basic calculation: impact=80, complexity=7/5 → score={score:.2f}")
        
        # Test 2: Zero impact
        score = calculate_priority_score(0.0, 7, 5)
        assert score == 0.0, "Zero impact should give zero score"
        print_success("Zero impact correctly returns 0")
        
        # Test 3: Cost regression boost
        score_before = calculate_priority_score(30.0, 6, 3)
        score_after = calculate_priority_score(30.0, 6, 3, cost_regression=25.0)
        assert score_after > score_before, "Cost regression should boost score"
        print_success(f"Cost regression boost: {score_before:.2f} → {score_after:.2f}")
        
        # Test 4: Priority levels
        assert get_priority_level(75.0) == "HIGH"
        assert get_priority_level(50.0) == "MEDIUM"
        assert get_priority_level(20.0) == "LOW"
        assert get_priority_level(10.0, cost_regression=25.0) == "HIGH"
        print_success("Priority level determination works correctly")
        
        return True
    except Exception as e:
        print_failure(f"Priority calculation validation failed: {e}")
        return False

def validate_rule_matching():
    """Validate rule matching functionality."""
    print_header("Validating Rule Matching")
    
    try:
        from recommendation import find_matching_rules
        from config import load_config
        
        config = load_config()
        
        # Test 1: High JOIN count
        rules = find_matching_rules({"join_count": 7}, config)
        assert any(r['rule_id'] == 'HIGH_JOIN_COUNT' for r in rules)
        print_success("HIGH_JOIN_COUNT rule triggered for join_count=7")
        
        # Test 2: High CTE count
        rules = find_matching_rules({"cte_count": 5}, config)
        assert any(r['rule_id'] == 'HIGH_CTE_COUNT' for r in rules)
        print_success("HIGH_CTE_COUNT rule triggered for cte_count=5")
        
        # Test 3: Multiple rules
        metrics = {"join_count": 7, "cte_count": 5, "window_function_count": 4}
        rules = find_matching_rules(metrics, config)
        assert len(rules) >= 3
        print_success(f"Multiple rules triggered: {len(rules)} rules found")
        
        # Test 4: No rules triggered
        rules = find_matching_rules(
            {"join_count": 2, "cte_count": 1, "window_function_count": 1},
            config
        )
        assert len(rules) == 0
        print_success("No rules triggered for simple metrics")
        
        return True
    except Exception as e:
        print_failure(f"Rule matching validation failed: {e}")
        return False

def validate_recommendation_generation():
    """Validate recommendation generation."""
    print_header("Validating Recommendation Generation")
    
    try:
        from recommendation import generate_recommendations_for_model
        from bottleneck import BottleneckResult
        from config import load_config
        
        config = load_config()
        
        # Create a bottleneck result
        bottleneck = BottleneckResult(
            model_name="fact_trades",
            impact_score=80.0,
            kpi_categorizations={},
            regression_flags=["EXECUTION_TIME_REGRESSION", "COST_REGRESSION"],
            data_drift_detected=False,
            regression_amounts={"execution_time": 15.0, "cost": 25.0},
            severity="HIGH"
        )
        
        metrics = {
            "join_count": 7,
            "cte_count": 5,
            "window_function_count": 4
        }
        
        recs = generate_recommendations_for_model("fact_trades", bottleneck, metrics, config)
        
        assert len(recs) > 0, "Should generate recommendations for complex model"
        print_success(f"Generated {len(recs)} recommendations for bottleneck model")
        
        # Verify recommendations are sorted
        scores = [r.priority_score for r in recs]
        assert scores == sorted(scores, reverse=True)
        print_success("Recommendations sorted by priority score")
        
        # Verify each has required fields
        for rec in recs:
            assert rec.model_name, "Missing model_name"
            assert rec.rule_id, "Missing rule_id"
            assert rec.priority in ["HIGH", "MEDIUM", "LOW"], f"Invalid priority: {rec.priority}"
            assert rec.optimization_technique, "Missing optimization_technique"
            assert rec.sql_pattern_suggestion, "Missing SQL patterns"
            assert rec.rationale, "Missing rationale"
        
        print_success("All recommendations have required fields")
        
        return True
    except Exception as e:
        print_failure(f"Recommendation generation validation failed: {e}")
        return False

def validate_multi_model():
    """Validate multi-model recommendation generation."""
    print_header("Validating Multi-Model Recommendation Generation")
    
    try:
        from recommendation import generate_recommendations, rank_recommendations_by_priority
        from bottleneck import BottleneckResult
        from config import load_config
        
        config = load_config()
        
        # Create multiple bottlenecks
        bottlenecks = {
            "model_a": BottleneckResult(
                model_name="model_a",
                impact_score=85.0,
                kpi_categorizations={},
                regression_flags=["EXECUTION_TIME_REGRESSION", "COST_REGRESSION"],
                data_drift_detected=False,
                regression_amounts={"execution_time": 15.0, "cost": 25.0},
                severity="HIGH"
            ),
            "model_b": BottleneckResult(
                model_name="model_b",
                impact_score=50.0,
                kpi_categorizations={},
                regression_flags=["COST_REGRESSION"],
                data_drift_detected=False,
                regression_amounts={"cost": 25.0},
                severity="MEDIUM"
            )
        }
        
        metrics = {
            "model_a": {"join_count": 8, "cte_count": 6, "window_function_count": 3},
            "model_b": {"join_count": 4, "cte_count": 5, "window_function_count": 1}
        }
        
        all_recs = generate_recommendations(bottlenecks, metrics, config)
        
        assert len(all_recs) > 0, "Should generate recommendations"
        print_success(f"Generated recommendations for {len(all_recs)} models")
        
        # Rank all
        ranked = rank_recommendations_by_priority(all_recs)
        print_success(f"Ranked {len(ranked)} total recommendations globally")
        
        # Verify sorted
        scores = [r.priority_score for r in ranked]
        assert scores == sorted(scores, reverse=True)
        print_success("Global ranking sorted by priority score")
        
        return True
    except Exception as e:
        print_failure(f"Multi-model validation failed: {e}")
        return False

def validate_json_serialization():
    """Validate JSON serialization of recommendations."""
    print_header("Validating JSON Serialization")
    
    try:
        from recommendation import generate_recommendations, generate_recommendation_summary
        from bottleneck import BottleneckResult
        from config import load_config
        
        config = load_config()
        
        bottleneck = BottleneckResult(
            model_name="model_x",
            impact_score=75.0,
            kpi_categorizations={},
            regression_flags=["EXECUTION_TIME_REGRESSION"],
            data_drift_detected=False,
            regression_amounts={"execution_time": 15.0},
            severity="HIGH"
        )
        
        all_recs = generate_recommendations(
            {"model_x": bottleneck},
            {"model_x": {"join_count": 7, "cte_count": 4}},
            config
        )
        
        # Test individual recommendation serialization
        for model_recs in all_recs.values():
            for rec in model_recs:
                rec_dict = rec.to_dict()
                json_str = json.dumps(rec_dict)
                assert isinstance(json_str, str)
        print_success("Individual recommendations JSON serializable")
        
        # Test summary serialization
        summary = generate_recommendation_summary(all_recs, top_n=10)
        summary_json = json.dumps(summary)
        assert isinstance(summary_json, str)
        assert "total_recommendations" in summary_json
        assert "top_recommendations" in summary_json
        print_success("Summary JSON serializable with all required fields")
        
        # Verify structure
        assert "priority_breakdown" in summary
        assert "HIGH" in summary["priority_breakdown"]
        print_success("Summary has priority breakdown")
        
        return True
    except Exception as e:
        print_failure(f"JSON serialization validation failed: {e}")
        return False

def validate_edge_cases():
    """Validate edge case handling."""
    print_header("Validating Edge Case Handling")
    
    try:
        from recommendation import generate_recommendations_for_model
        from bottleneck import BottleneckResult
        from config import load_config
        
        config = load_config()
        
        # Test 1: No recommendations for improved model
        bottleneck = BottleneckResult(
            model_name="improved_model",
            impact_score=0.0,
            kpi_categorizations={},
            regression_flags=[],
            data_drift_detected=False,
            regression_amounts={},
            severity="LOW"
        )
        
        recs = generate_recommendations_for_model(
            "improved_model",
            bottleneck,
            {"join_count": 2, "cte_count": 1, "window_function_count": 1},
            config
        )
        
        assert len(recs) == 0, "Should not recommend for simple improved models"
        print_success("No recommendations for improved simple models")
        
        # Test 2: Cost regression forces HIGH
        bottleneck = BottleneckResult(
            model_name="costly",
            impact_score=15.0,  # Low impact
            kpi_categorizations={},
            regression_flags=["COST_REGRESSION"],
            data_drift_detected=False,
            regression_amounts={"cost": 25.0},
            severity="MEDIUM"
        )
        
        recs = generate_recommendations_for_model(
            "costly",
            bottleneck,
            {"join_count": 7},
            config
        )
        
        high_priority_recs = [r for r in recs if r.priority == "HIGH"]
        assert len(high_priority_recs) > 0, "Cost regression >20% should force HIGH"
        print_success("Cost regression >20% forces HIGH priority")
        
        return True
    except Exception as e:
        print_failure(f"Edge case validation failed: {e}")
        return False

def main():
    """Run all validation checks."""
    print("\n" + "="*70)
    print("  RECOMMENDATION ENGINE IMPLEMENTATION VALIDATION")
    print("="*70)
    
    validations = [
        ("Config Rules", validate_config_rules),
        ("Recommendation Module", validate_recommendation_module),
        ("Recommendation Dataclass", validate_recommendation_dataclass),
        ("Priority Calculation", validate_priority_calculation),
        ("Rule Matching", validate_rule_matching),
        ("Recommendation Generation", validate_recommendation_generation),
        ("Multi-Model Generation", validate_multi_model),
        ("JSON Serialization", validate_json_serialization),
        ("Edge Case Handling", validate_edge_cases),
    ]
    
    results = []
    for name, validator in validations:
        try:
            result = validator()
            results.append((name, result))
        except Exception as e:
            print_failure(f"Unexpected error in {name}: {e}")
            results.append((name, False))
    
    # Print summary
    print_header("VALIDATION SUMMARY")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\n  Result: {passed}/{total} validations passed")
    
    if passed == total:
        print_success("All validations passed! ✅")
        print("\n" + "="*70)
        return 0
    else:
        print_failure(f"{total - passed} validations failed")
        print("\n" + "="*70)
        return 1

if __name__ == "__main__":
    exit(main())
