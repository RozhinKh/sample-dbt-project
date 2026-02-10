#!/usr/bin/env python3
"""
Tests for the Recommendation Engine

Comprehensive test suite for SQL optimization recommendation generation
based on bottleneck detection and query complexity metrics.
"""

import logging
import json
from typing import Dict, Any
import pytest

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
from bottleneck import BottleneckResult
from config import load_config


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def config():
    """Load configuration for tests."""
    return load_config()


@pytest.fixture
def logger():
    """Create a test logger."""
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger("test_recommendation")


def create_bottleneck_result(
    model_name: str = "test_model",
    impact_score: float = 75.0,
    exec_time_delta: float = 15.0,
    cost_delta: float = 25.0,
    data_drift: bool = False,
    severity: str = "HIGH"
) -> BottleneckResult:
    """Helper to create a BottleneckResult for testing."""
    regression_amounts = {}
    if exec_time_delta is not None:
        regression_amounts["execution_time"] = abs(exec_time_delta)
    if cost_delta is not None:
        regression_amounts["cost"] = abs(cost_delta)
    
    return BottleneckResult(
        model_name=model_name,
        impact_score=impact_score,
        kpi_categorizations={},
        regression_flags=["EXECUTION_TIME_REGRESSION", "COST_REGRESSION"] if (exec_time_delta or cost_delta) else [],
        data_drift_detected=data_drift,
        regression_amounts=regression_amounts,
        severity=severity
    )


# ============================================================================
# PRIORITY CALCULATION TESTS
# ============================================================================

class TestPriorityCalculation:
    """Tests for priority score calculation."""
    
    def test_priority_score_basic(self):
        """Test basic priority score calculation."""
        # impact=80, complexity=7, threshold=5 -> (80/100) * (7/5) * 100 = 112 capped at 100
        score = calculate_priority_score(80.0, 7, 5)
        assert 99 < score <= 100, f"Expected ~100, got {score}"
    
    def test_priority_score_below_threshold(self):
        """Test priority score when complexity is below threshold."""
        # impact=80, complexity=2, threshold=5 -> (80/100) * (2/5) * 100 = 32
        score = calculate_priority_score(80.0, 2, 5)
        assert 30 < score < 35, f"Expected ~32, got {score}"
    
    def test_priority_score_zero_impact(self):
        """Test priority score with zero impact."""
        score = calculate_priority_score(0.0, 7, 5)
        assert score == 0.0, f"Expected 0, got {score}"
    
    def test_priority_score_with_cost_boost(self):
        """Test priority score receives boost for high cost regression."""
        # Base: (50/100) * (6/3) * 100 = 100, with cost 25% -> +25 capped at 100
        score = calculate_priority_score(50.0, 6, 3, cost_regression=25.0)
        assert score == 100.0, f"Expected 100 (capped), got {score}"
    
    def test_priority_score_with_cost_boost_below_cap(self):
        """Test priority score with cost boost not exceeding cap."""
        # Base: (30/100) * (6/3) * 100 = 60, with cost 25% -> +25 = 85
        score = calculate_priority_score(30.0, 6, 3, cost_regression=25.0)
        assert 84 < score < 86, f"Expected ~85, got {score}"


# ============================================================================
# PRIORITY LEVEL TESTS
# ============================================================================

class TestPriorityLevel:
    """Tests for priority level determination."""
    
    def test_high_priority_by_score(self):
        """Test HIGH priority from score > 66."""
        level = get_priority_level(75.0)
        assert level == "HIGH", f"Expected HIGH, got {level}"
    
    def test_medium_priority(self):
        """Test MEDIUM priority for score 33-66."""
        level = get_priority_level(50.0)
        assert level == "MEDIUM", f"Expected MEDIUM, got {level}"
    
    def test_low_priority(self):
        """Test LOW priority for score < 33."""
        level = get_priority_level(20.0)
        assert level == "LOW", f"Expected LOW, got {level}"
    
    def test_high_priority_by_cost_regression(self):
        """Test HIGH priority forced by cost regression > 20%."""
        # Even with low score, should be HIGH
        level = get_priority_level(10.0, cost_regression=25.0)
        assert level == "HIGH", f"Expected HIGH (cost boost), got {level}"
    
    def test_no_high_priority_for_low_cost(self):
        """Test cost regression <= 20% doesn't force HIGH."""
        level = get_priority_level(10.0, cost_regression=15.0)
        assert level == "LOW", f"Expected LOW, got {level}"


# ============================================================================
# RULE MATCHING TESTS
# ============================================================================

class TestRuleMatching:
    """Tests for finding matching optimization rules."""
    
    def test_high_join_count_rule(self, config, logger):
        """Test HIGH_JOIN_COUNT rule is triggered."""
        metrics = {"join_count": 7}
        rules = find_matching_rules(metrics, config, logger)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_JOIN_COUNT" in rule_ids, "HIGH_JOIN_COUNT rule not triggered"
    
    def test_high_cte_count_rule(self, config, logger):
        """Test HIGH_CTE_COUNT rule is triggered."""
        metrics = {"cte_count": 5}
        rules = find_matching_rules(metrics, config, logger)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_CTE_COUNT" in rule_ids, "HIGH_CTE_COUNT rule not triggered"
    
    def test_high_window_function_rule(self, config, logger):
        """Test HIGH_WINDOW_FUNCTION_COUNT rule is triggered."""
        metrics = {"window_function_count": 4}
        rules = find_matching_rules(metrics, config, logger)
        
        rule_ids = [r["rule_id"] for r in rules]
        assert "HIGH_WINDOW_FUNCTION_COUNT" in rule_ids, "HIGH_WINDOW_FUNCTION_COUNT rule not triggered"
    
    def test_no_rules_triggered(self, config, logger):
        """Test no rules are triggered for normal metrics."""
        metrics = {"join_count": 2, "cte_count": 1, "window_function_count": 1}
        rules = find_matching_rules(metrics, config, logger)
        
        assert len(rules) == 0, f"Expected 0 rules, got {len(rules)}"
    
    def test_multiple_rules_triggered(self, config, logger):
        """Test multiple rules can be triggered together."""
        metrics = {"join_count": 7, "cte_count": 5, "window_function_count": 4}
        rules = find_matching_rules(metrics, config, logger)
        
        assert len(rules) >= 3, f"Expected at least 3 rules, got {len(rules)}"


# ============================================================================
# RECOMMENDATION GENERATION TESTS
# ============================================================================

class TestRecommendationGeneration:
    """Tests for recommendation generation."""
    
    def test_single_model_recommendations(self, config, logger):
        """Test recommendation generation for a single model."""
        bottleneck = create_bottleneck_result(
            model_name="model_a",
            impact_score=80.0,
            exec_time_delta=15.0,
            cost_delta=25.0
        )
        metrics = {
            "join_count": 7,
            "cte_count": 5,
            "window_function_count": 4
        }
        
        recs = generate_recommendations_for_model("model_a", bottleneck, metrics, config, logger)
        
        assert len(recs) > 0, "Expected recommendations for high complexity model"
        assert all(isinstance(r, Recommendation) for r in recs), "All results should be Recommendation objects"
    
    def test_recommendations_sorted_by_priority(self, config, logger):
        """Test recommendations are sorted by priority score."""
        bottleneck = create_bottleneck_result(model_name="model_a", impact_score=80.0)
        metrics = {
            "join_count": 7,
            "cte_count": 5,
            "window_function_count": 4
        }
        
        recs = generate_recommendations_for_model("model_a", bottleneck, metrics, config, logger)
        
        # Check sorted descending by priority score
        scores = [r.priority_score for r in recs]
        assert scores == sorted(scores, reverse=True), "Recommendations not sorted by priority"
    
    def test_no_recommendations_for_simple_model(self, config, logger):
        """Test no recommendations for simple (non-bottleneck) models."""
        bottleneck = create_bottleneck_result(
            model_name="model_b",
            impact_score=0.0,
            exec_time_delta=None,
            cost_delta=None
        )
        metrics = {
            "join_count": 2,
            "cte_count": 1,
            "window_function_count": 1
        }
        
        recs = generate_recommendations_for_model("model_b", bottleneck, metrics, config, logger)
        
        assert len(recs) == 0, "Expected no recommendations for simple model"
    
    def test_recommendation_includes_sql_patterns(self, config, logger):
        """Test recommendations include SQL pattern suggestions."""
        bottleneck = create_bottleneck_result(model_name="model_a", impact_score=80.0)
        metrics = {"join_count": 7}
        
        recs = generate_recommendations_for_model("model_a", bottleneck, metrics, config, logger)
        
        assert len(recs) > 0, "Expected recommendations"
        for rec in recs:
            assert rec.sql_pattern_suggestion, f"Recommendation {rec.rule_id} missing SQL patterns"
            assert len(rec.sql_pattern_suggestion) > 0, "SQL pattern list should not be empty"


# ============================================================================
# MULTI-MODEL RECOMMENDATION TESTS
# ============================================================================

class TestMultiModelRecommendations:
    """Tests for recommendation generation across multiple models."""
    
    def test_generate_for_multiple_bottlenecks(self, config, logger):
        """Test recommendation generation for multiple bottleneck models."""
        bottlenecks = {
            "model_a": create_bottleneck_result("model_a", 80.0),
            "model_b": create_bottleneck_result("model_b", 50.0),
            "model_c": create_bottleneck_result("model_c", 0.0)  # No bottleneck
        }
        
        metrics = {
            "model_a": {"join_count": 7, "cte_count": 5, "window_function_count": 4},
            "model_b": {"join_count": 3, "cte_count": 5, "window_function_count": 2},
            "model_c": {"join_count": 2, "cte_count": 1, "window_function_count": 1}
        }
        
        all_recs = generate_recommendations(bottlenecks, metrics, config, logger)
        
        # Should have recommendations for models with bottlenecks/complexity
        assert len(all_recs) > 0, "Expected recommendations for bottleneck models"
    
    def test_rank_all_recommendations(self, config, logger):
        """Test ranking all recommendations globally."""
        bottlenecks = {
            "model_a": create_bottleneck_result("model_a", 80.0, 15.0, 25.0),
            "model_b": create_bottleneck_result("model_b", 50.0, 5.0, 15.0)
        }
        
        metrics = {
            "model_a": {"join_count": 7, "cte_count": 5},
            "model_b": {"join_count": 6, "cte_count": 2}
        }
        
        all_recs = generate_recommendations(bottlenecks, metrics, config, logger)
        ranked = rank_recommendations_by_priority(all_recs)
        
        # Verify sorted globally
        scores = [r.priority_score for r in ranked]
        assert scores == sorted(scores, reverse=True), "Global ranking not sorted by priority"


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and special scenarios."""
    
    def test_all_improved_model(self, config, logger):
        """Test model with all improved metrics."""
        bottleneck = create_bottleneck_result(
            model_name="improved_model",
            impact_score=0.0,
            exec_time_delta=-10.0,  # Improvement
            cost_delta=-15.0  # Improvement
        )
        metrics = {"join_count": 2, "cte_count": 1, "window_function_count": 1}
        
        recs = generate_recommendations_for_model("improved_model", bottleneck, metrics, config, logger)
        assert len(recs) == 0, "No recommendations for improved model"
    
    def test_model_with_missing_metrics(self, config, logger):
        """Test handling of models with missing complexity metrics."""
        bottleneck = create_bottleneck_result(model_name="incomplete_model", impact_score=75.0)
        metrics = {"join_count": 7}  # Only one metric provided
        
        recs = generate_recommendations_for_model("incomplete_model", bottleneck, metrics, config, logger)
        # Should only get recommendations for the metric provided
        rule_ids = [r.rule_id for r in recs]
        assert "HIGH_JOIN_COUNT" in rule_ids, "Should have JOIN recommendation"
    
    def test_cost_regression_forces_high_priority(self, config, logger):
        """Test that cost regression >20% forces HIGH priority."""
        bottleneck = create_bottleneck_result(
            model_name="costly_model",
            impact_score=10.0,  # Low impact
            exec_time_delta=2.0,  # Minor time regression
            cost_delta=25.0  # High cost regression
        )
        metrics = {"join_count": 7}
        
        recs = generate_recommendations_for_model("costly_model", bottleneck, metrics, config, logger)
        
        for rec in recs:
            if rec.rule_id == "HIGH_JOIN_COUNT":
                assert rec.priority == "HIGH", f"Expected HIGH priority for high cost, got {rec.priority}"
    
    def test_data_drift_model(self, config, logger):
        """Test recommendations for model with data drift."""
        bottleneck = create_bottleneck_result(
            model_name="drift_model",
            impact_score=85.0,
            data_drift=True,
            severity="CRITICAL"
        )
        metrics = {"join_count": 7, "cte_count": 4}
        
        recs = generate_recommendations_for_model("drift_model", bottleneck, metrics, config, logger)
        
        assert len(recs) > 0, "Should have recommendations for data drift model"


# ============================================================================
# JSON SERIALIZATION TESTS
# ============================================================================

class TestJsonSerialization:
    """Tests for JSON serialization of recommendations."""
    
    def test_recommendation_to_dict(self):
        """Test Recommendation converts to dict."""
        rec = Recommendation(
            model_name="test_model",
            rule_id="HIGH_JOIN_COUNT",
            rule_name="High JOIN Count",
            priority="HIGH",
            priority_score=85.5,
            optimization_technique="JOIN Consolidation",
            sql_pattern_suggestion=["Use materialized view"],
            rationale="Multiple JOINs impact performance",
            impact_score=80.0,
            complexity_metric="join_count",
            complexity_value=7,
            threshold_value=5
        )
        
        rec_dict = rec.to_dict()
        
        assert isinstance(rec_dict, dict), "Should convert to dict"
        assert rec_dict["model_name"] == "test_model"
        assert rec_dict["rule_id"] == "HIGH_JOIN_COUNT"
        assert rec_dict["priority"] == "HIGH"
    
    def test_recommendation_json_serializable(self):
        """Test Recommendation is JSON serializable."""
        rec = Recommendation(
            model_name="test_model",
            rule_id="HIGH_CTE_COUNT",
            rule_name="High CTE Count",
            priority="MEDIUM",
            priority_score=50.0,
            optimization_technique="CTE Materialization",
            sql_pattern_suggestion=["Create temp table"],
            rationale="CTE recomputation overhead"
        )
        
        # Should not raise exception
        json_str = json.dumps(rec.to_dict())
        assert isinstance(json_str, str)
        assert "test_model" in json_str
    
    def test_summary_json_serializable(self, config, logger):
        """Test recommendation summary is JSON serializable."""
        bottleneck = create_bottleneck_result(model_name="model_a", impact_score=80.0)
        metrics = {"model_a": {"join_count": 7, "cte_count": 5}}
        
        all_recs = generate_recommendations(
            {"model_a": bottleneck},
            metrics,
            config,
            logger
        )
        
        summary = generate_recommendation_summary(all_recs, top_n=10)
        
        # Should be JSON serializable
        json_str = json.dumps(summary)
        assert isinstance(json_str, str)
        assert "total_recommendations" in json_str


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Integration tests combining multiple components."""
    
    def test_full_recommendation_workflow(self, config, logger):
        """Test complete workflow from bottlenecks to recommendations."""
        # Simulate bottleneck detection results
        bottlenecks = {
            "model_trades": create_bottleneck_result(
                "model_trades", 85.0, 20.0, 30.0
            ),
            "model_portfolio": create_bottleneck_result(
                "model_portfolio", 60.0, 8.0, 25.0
            ),
            "model_reports": create_bottleneck_result(
                "model_reports", 0.0, -5.0, -10.0
            )
        }
        
        # Complexity metrics
        complexity_metrics = {
            "model_trades": {
                "join_count": 8,
                "cte_count": 6,
                "window_function_count": 3
            },
            "model_portfolio": {
                "join_count": 6,
                "cte_count": 4,
                "window_function_count": 2
            },
            "model_reports": {
                "join_count": 2,
                "cte_count": 1,
                "window_function_count": 0
            }
        }
        
        # Generate recommendations
        all_recommendations = generate_recommendations(
            bottlenecks, complexity_metrics, config, logger
        )
        
        # Verify results
        assert "model_trades" in all_recommendations, "Should have recommendations for model_trades"
        assert "model_portfolio" in all_recommendations, "Should have recommendations for model_portfolio"
        assert "model_reports" not in all_recommendations, "Should not recommend for improved model"
        
        # Generate summary
        summary = generate_recommendation_summary(all_recommendations, top_n=5)
        assert summary["total_recommendations"] > 0
        assert summary["high_priority_count"] > 0 or summary["medium_priority_count"] > 0
        assert len(summary["top_recommendations"]) <= 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
