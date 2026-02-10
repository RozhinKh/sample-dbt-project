#!/usr/bin/env python3
"""
Quick integration test to verify recommendation module imports and basic functionality.
"""

import json
import logging

# Test imports
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
    print("✓ Successfully imported all recommendation functions")
except ImportError as e:
    print(f"✗ Failed to import recommendation module: {e}")
    exit(1)

try:
    from bottleneck import BottleneckResult
    print("✓ Successfully imported BottleneckResult")
except ImportError as e:
    print(f"✗ Failed to import BottleneckResult: {e}")
    exit(1)

try:
    from config import load_config
    print("✓ Successfully imported load_config")
except ImportError as e:
    print(f"✗ Failed to import load_config: {e}")
    exit(1)

# Test basic functionality
print("\n" + "="*60)
print("Testing Basic Functionality")
print("="*60)

# Load config
config = load_config()
print(f"✓ Config loaded with {len(config['optimization_rules'])} optimization rules")

# Test priority calculation
score = calculate_priority_score(80.0, 7, 5)
print(f"✓ Priority score calculation: 80.0 impact × 7/5 complexity = {score:.2f}")

# Test priority level
level = get_priority_level(85.0)
print(f"✓ Priority level for score 85.0: {level}")

# Test rule matching
metrics = {"join_count": 7, "cte_count": 5, "window_function_count": 4}
rules = find_matching_rules(metrics, config)
print(f"✓ Found {len(rules)} matching rules for high complexity metrics")
for rule in rules:
    print(f"  - {rule['rule_id']}: {rule['name']}")

# Test recommendation data structure
rec = Recommendation(
    model_name="test_model",
    rule_id="HIGH_JOIN_COUNT",
    rule_name="High JOIN Count Detection",
    priority="HIGH",
    priority_score=85.5,
    optimization_technique="JOIN Consolidation & Materialization",
    sql_pattern_suggestion=["Create materialized view for JOIN result"],
    rationale="Multiple JOINs increase query complexity",
    impact_score=80.0,
    complexity_metric="join_count",
    complexity_value=7,
    threshold_value=5
)

print(f"✓ Created recommendation object for {rec.model_name}")

# Test JSON serialization
try:
    rec_dict = rec.to_dict()
    json_str = json.dumps(rec_dict, indent=2)
    print(f"✓ Recommendation serializable to JSON ({len(json_str)} bytes)")
except Exception as e:
    print(f"✗ JSON serialization failed: {e}")
    exit(1)

# Test single model recommendations
bottleneck = BottleneckResult(
    model_name="model_a",
    impact_score=80.0,
    kpi_categorizations={},
    regression_flags=["EXECUTION_TIME_REGRESSION", "COST_REGRESSION"],
    data_drift_detected=False,
    regression_amounts={"execution_time": 15.0, "cost": 25.0},
    severity="HIGH"
)

model_metrics = {"join_count": 7, "cte_count": 5, "window_function_count": 4}

logger = logging.getLogger("test")
recs = generate_recommendations_for_model("model_a", bottleneck, model_metrics, config, logger)
print(f"✓ Generated {len(recs)} recommendations for model_a")

# Test multi-model recommendations
bottlenecks = {
    "model_a": bottleneck,
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

all_metrics = {
    "model_a": {"join_count": 7, "cte_count": 5, "window_function_count": 4},
    "model_b": {"join_count": 3, "cte_count": 5, "window_function_count": 2}
}

all_recs = generate_recommendations(bottlenecks, all_metrics, config, logger)
print(f"✓ Generated recommendations for {len(all_recs)} bottleneck models")

# Test ranking
ranked = rank_recommendations_by_priority(all_recs)
print(f"✓ Ranked {len(ranked)} total recommendations by priority")

# Test summary
summary = generate_recommendation_summary(all_recs, top_n=10)
print(f"✓ Generated summary:")
print(f"  - Total recommendations: {summary['total_recommendations']}")
print(f"  - HIGH priority: {summary['high_priority_count']}")
print(f"  - MEDIUM priority: {summary['medium_priority_count']}")
print(f"  - LOW priority: {summary['low_priority_count']}")
print(f"  - Top recommendations: {len(summary['top_recommendations'])}")

# Verify summary is JSON serializable
try:
    summary_json = json.dumps(summary, indent=2)
    print(f"✓ Summary serializable to JSON ({len(summary_json)} bytes)")
except Exception as e:
    print(f"✗ Summary JSON serialization failed: {e}")
    exit(1)

print("\n" + "="*60)
print("✓ All integration tests passed!")
print("="*60)
