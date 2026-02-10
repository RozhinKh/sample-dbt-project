#!/usr/bin/env python3
"""
End-to-end integration tests for the complete comparison workflow.

This test suite validates the entire benchmarking pipeline:
1. Loading baseline and candidate reports from fixtures
2. Computing delta calculations across all KPIs
3. Detecting bottlenecks (execution time, cost, data drift)
4. Generating optimization recommendations
5. Creating analysis output with all validation checks

Test scenarios include:
- Normal case: 5 models with mixed improvements/regressions
- All models improved
- All models regressed
- No changes (identical reports)
- Data drift detection
- New/removed models
"""

import pytest
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from delta import calculate_model_deltas, format_delta_output, DeltaResult
from bottleneck import detect_bottlenecks, BottleneckResult
from recommendation import (
    generate_recommendations,
    Recommendation
)
from config import load_config


# ============================================================================
# FIXTURES: LOAD BASELINE AND CANDIDATE REPORTS
# ============================================================================

@pytest.fixture
def baseline_report():
    """Load baseline report from fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "baseline_report.json"
    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def candidate_report():
    """Load candidate report from fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "candidate_report.json"
    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def config():
    """Load configuration."""
    return load_config()


@pytest.fixture
def logger():
    """Create logger for tests."""
    logger = logging.getLogger("test_integration")
    logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add a null handler for testing
    handler = logging.NullHandler()
    logger.addHandler(handler)
    
    return logger


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def run_comparison_workflow(
    baseline_models: Dict[str, Dict[str, Any]],
    candidate_models: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Execute the complete comparison workflow orchestrating delta, bottleneck,
    and recommendation analysis.
    
    Args:
        baseline_models: Baseline model KPIs
        candidate_models: Candidate model KPIs
        config: Configuration with thresholds
        logger: Logger instance
    
    Returns:
        Dict with analysis structure containing model_comparisons, bottlenecks,
        recommendations, and overall_statistics
    """
    # Calculate deltas
    model_deltas = calculate_model_deltas(
        baseline_models, candidate_models, config, logger=logger
    )
    
    # Format output
    formatted_deltas = format_delta_output(model_deltas)
    
    # Detect bottlenecks
    bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
    
    # Extract complexity metrics from baseline models for recommendations
    complexity_metrics = {}
    for model_name, kpis in baseline_models.items():
        complexity_metrics[model_name] = {
            "join_count": kpis.get("join_count", 0),
            "cte_count": kpis.get("cte_count", 0),
            "window_function_count": kpis.get("window_function_count", 0)
        }
    
    # Generate recommendations
    recommendations_dict = generate_recommendations(
        bottlenecks, complexity_metrics, config, logger=logger
    ) if bottlenecks else {}
    
    # Flatten recommendations to list
    recommendations = []
    for model_recs in recommendations_dict.values():
        recommendations.extend(model_recs)
    
    # Calculate overall statistics
    total_models = len(baseline_models)
    improved_count = 0
    regressed_count = 0
    
    for model_name, deltas in formatted_deltas.items():
        if model_name in baseline_models:  # Only count existing models
            status = deltas.get("_status")
            if status != "new_model" and status != "removed_model":
                # Count as improved if execution_time improved
                exec_time = deltas.get("execution_time")
                if isinstance(exec_time, dict):
                    direction = exec_time.get("direction", "N/A")
                    if direction == "+":
                        improved_count += 1
                    elif direction == "-":
                        regressed_count += 1
    
    return {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "comparison_date": datetime.now().isoformat(),
            "baseline_timestamp": baseline_models.get("_generated_at", "unknown"),
            "candidate_timestamp": candidate_models.get("_generated_at", "unknown")
        },
        "model_comparisons": formatted_deltas,
        "bottleneck_summary": {
            model_name: {
                "impact_score": bn.impact_score,
                "severity": bn.severity,
                "regression_flags": bn.regression_flags,
                "data_drift_detected": bn.data_drift_detected
            }
            for model_name, bn in bottlenecks.items()
        },
        "optimization_recommendations": recommendations,
        "data_equivalence_warnings": {
            model_name: True
            for model_name, bn in bottlenecks.items()
            if bn.data_drift_detected
        },
        "overall_statistics": {
            "total_models": total_models,
            "improved_count": improved_count,
            "regressed_count": regressed_count,
            "percent_improved": (improved_count / total_models * 100) if total_models > 0 else 0
        }
    }


# ============================================================================
# TEST: NORMAL CASE - 5 MODELS WITH MIXED RESULTS
# ============================================================================

class TestNormalCase:
    """Test the normal case with 5 models having mixed improvements and regressions."""
    
    def test_baseline_has_5_models(self, baseline_report):
        """Verify baseline fixture has exactly 5 models."""
        models = baseline_report.get("models", {})
        assert len(models) == 5, f"Expected 5 models, got {len(models)}"
        assert "stg_users" in models
        assert "stg_orders" in models
        assert "fct_sales" in models
        assert "dim_products" in models
        assert "fct_inventory" in models
    
    def test_candidate_has_5_models(self, candidate_report):
        """Verify candidate fixture has exactly 5 models."""
        models = candidate_report.get("models", {})
        assert len(models) == 5, f"Expected 5 models, got {len(models)}"
    
    def test_all_models_have_all_kpis(self, baseline_report):
        """Verify all models have all 5 KPI metrics."""
        required_kpis = {
            "execution_time", "cost", "bytes_scanned",
            "join_count", "cte_count", "window_function_count", "data_hash"
        }
        
        for model_name, kpis in baseline_report["models"].items():
            assert required_kpis.issubset(kpis.keys()), \
                f"Model {model_name} missing KPIs: {required_kpis - kpis.keys()}"
    
    def test_run_full_workflow(self, baseline_report, candidate_report, config, logger):
        """Execute complete comparison workflow."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        assert analysis is not None
        assert "model_comparisons" in analysis
        assert "bottleneck_summary" in analysis
        assert "overall_statistics" in analysis
    
    def test_metadata_structure(self, baseline_report, candidate_report, config, logger):
        """Validate metadata has required fields."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        metadata = analysis["metadata"]
        assert "timestamp" in metadata
        assert "comparison_date" in metadata
        assert "baseline_timestamp" in metadata
        assert "candidate_timestamp" in metadata
    
    def test_model_comparisons_count(self, baseline_report, candidate_report, config, logger):
        """Verify model_comparisons has all 5 models."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        comparisons = analysis["model_comparisons"]
        assert len(comparisons) == 5, f"Expected 5 model comparisons, got {len(comparisons)}"
    
    def test_delta_calculation_formula(self, baseline_report, candidate_report, config, logger):
        """Verify delta calculations use correct formula: ((candidate - baseline) / baseline) * 100."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        # Check stg_users execution time: (4.8 - 5.2) / 5.2 * 100 = -7.69%
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        
        stg_users_deltas = model_deltas["stg_users"]
        execution_time_delta = stg_users_deltas["execution_time"]
        
        # Should be negative (improvement) and approximately -7.69%
        expected_delta = ((4.8 - 5.2) / 5.2) * 100
        assert abs(execution_time_delta.delta - expected_delta) < 0.01, \
            f"Expected delta ~{expected_delta:.2f}%, got {execution_time_delta.delta}%"
    
    def test_bottleneck_detection(self, baseline_report, candidate_report, config, logger):
        """Verify bottleneck detection identifies models exceeding thresholds."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # fct_sales should be a bottleneck (execution time: (18.2-15.4)/15.4 = 18.18% > 10%)
        # fct_inventory should be a bottleneck (execution time: (27.9-22.5)/22.5 = 24% > 10%)
        assert "fct_sales" in bottlenecks
        assert "fct_inventory" in bottlenecks
        assert bottlenecks["fct_sales"].impact_score > 0
        assert bottlenecks["fct_inventory"].impact_score > 0
    
    def test_data_drift_detection(self, baseline_report, candidate_report, config, logger):
        """Verify data drift is detected when hashes mismatch."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # stg_orders has different hash in candidate
        # fct_inventory has different hash in candidate
        assert bottlenecks["stg_orders"].data_drift_detected
        assert bottlenecks["fct_inventory"].data_drift_detected
    
    def test_recommendations_generation(self, baseline_report, candidate_report, config, logger):
        """Verify recommendations are generated for bottleneck models."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # Extract complexity metrics
        complexity_metrics = {}
        for model_name, kpis in baseline_models.items():
            complexity_metrics[model_name] = {
                "join_count": kpis.get("join_count", 0),
                "cte_count": kpis.get("cte_count", 0),
                "window_function_count": kpis.get("window_function_count", 0)
            }
        
        recommendations_dict = generate_recommendations(
            bottlenecks, complexity_metrics, config, logger=logger
        )
        
        # Should have recommendations for bottleneck models
        total_recs = sum(len(recs) for recs in recommendations_dict.values())
        assert total_recs > 0, "Should generate recommendations for bottlenecks"
        
        # Check structure - flatten recommendations to check
        all_recs = []
        for model_recs in recommendations_dict.values():
            all_recs.extend(model_recs)
        
        for rec in all_recs:
            assert hasattr(rec, "model_name")
            assert hasattr(rec, "rule_id")
            assert hasattr(rec, "priority")
            assert hasattr(rec, "priority_score")
    
    def test_overall_statistics(self, baseline_report, candidate_report, config, logger):
        """Verify overall_statistics calculations."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        stats = analysis["overall_statistics"]
        assert "total_models" in stats
        assert "improved_count" in stats
        assert "regressed_count" in stats
        assert "percent_improved" in stats
        assert stats["total_models"] == 5


# ============================================================================
# TEST: EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test edge case scenarios."""
    
    def test_all_models_improved(self, baseline_report, config, logger):
        """Test scenario where all models show improvement."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with all improvements (lower execution time, cost, bytes)
        candidate_models = {}
        for model_name, model_data in baseline_models.items():
            candidate_models[model_name] = {
                "execution_time": model_data["execution_time"] * 0.9,  # 10% improvement
                "cost": model_data["cost"] * 0.9,
                "bytes_scanned": model_data["bytes_scanned"] * 0.9,
                "join_count": model_data["join_count"],
                "cte_count": model_data["cte_count"],
                "window_function_count": model_data["window_function_count"],
                "data_hash": model_data["data_hash"]  # Same hash
            }
        
        analysis = run_comparison_workflow(
            baseline_models, candidate_models, config, logger
        )
        
        stats = analysis["overall_statistics"]
        assert stats["improved_count"] == 5
        assert stats["regressed_count"] == 0
        assert stats["percent_improved"] == 100.0
    
    def test_all_models_regressed(self, baseline_report, config, logger):
        """Test scenario where all models show regression."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with all regressions (higher execution time, cost, bytes)
        candidate_models = {}
        for model_name, model_data in baseline_models.items():
            candidate_models[model_name] = {
                "execution_time": model_data["execution_time"] * 1.15,  # 15% regression
                "cost": model_data["cost"] * 1.25,  # 25% regression
                "bytes_scanned": model_data["bytes_scanned"] * 1.15,
                "join_count": model_data["join_count"],
                "cte_count": model_data["cte_count"],
                "window_function_count": model_data["window_function_count"],
                "data_hash": model_data["data_hash"]  # Same hash
            }
        
        analysis = run_comparison_workflow(
            baseline_models, candidate_models, config, logger
        )
        
        stats = analysis["overall_statistics"]
        assert stats["improved_count"] == 0
        assert stats["regressed_count"] == 5
        assert stats["percent_improved"] == 0.0
    
    def test_no_changes(self, baseline_report, config, logger):
        """Test scenario with identical baseline and candidate."""
        baseline_models = baseline_report["models"]
        candidate_models = {
            model_name: dict(model_data)
            for model_name, model_data in baseline_models.items()
        }
        
        analysis = run_comparison_workflow(
            baseline_models, candidate_models, config, logger
        )
        
        bottlenecks = analysis["bottleneck_summary"]
        assert len(bottlenecks) == 0, "No bottlenecks should be detected when there are no changes"
    
    def test_execution_time_threshold_boundary(self, baseline_report, config, logger):
        """Test execution time regression at exactly 10% threshold."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with exactly 10% execution time increase
        candidate_models = {}
        for model_name, model_data in baseline_models.items():
            candidate_models[model_name] = {
                "execution_time": model_data["execution_time"] * 1.10,  # Exactly 10%
                "cost": model_data["cost"],
                "bytes_scanned": model_data["bytes_scanned"],
                "join_count": model_data["join_count"],
                "cte_count": model_data["cte_count"],
                "window_function_count": model_data["window_function_count"],
                "data_hash": model_data["data_hash"]
            }
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # At exactly 10% threshold, should NOT be flagged as regression (> not >=)
        # Verify via threshold check (delta > threshold_percent)
        for model_name in baseline_models:
            if model_name in bottlenecks:
                # Check if execution time regression was flagged
                assert "EXECUTION_TIME_REGRESSION" not in bottlenecks[model_name].regression_flags, \
                    f"{model_name} should not flag at exactly 10% threshold"
    
    def test_cost_threshold_boundary(self, baseline_report, config, logger):
        """Test cost regression at exactly 20% threshold."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with exactly 20% cost increase
        candidate_models = {}
        for model_name, model_data in baseline_models.items():
            candidate_models[model_name] = {
                "execution_time": model_data["execution_time"],
                "cost": model_data["cost"] * 1.20,  # Exactly 20%
                "bytes_scanned": model_data["bytes_scanned"],
                "join_count": model_data["join_count"],
                "cte_count": model_data["cte_count"],
                "window_function_count": model_data["window_function_count"],
                "data_hash": model_data["data_hash"]
            }
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # At exactly 20% threshold, should NOT be flagged (> not >=)
        for model_name in baseline_models:
            if model_name in bottlenecks:
                assert "COST_REGRESSION" not in bottlenecks[model_name].regression_flags, \
                    f"{model_name} should not flag at exactly 20% cost threshold"


# ============================================================================
# TEST: DELTA CALCULATION DETAILS
# ============================================================================

class TestDeltaCalculations:
    """Test detailed delta calculation scenarios."""
    
    def test_delta_zero_baseline_handling(self, config, logger):
        """Test delta calculation with zero baseline value."""
        baseline = {
            "execution_time": 0.0,
            "cost": 100.0
        }
        candidate = {
            "execution_time": 5.0,
            "cost": 150.0
        }
        
        from delta import calculate_all_deltas
        deltas = calculate_all_deltas(baseline, candidate, config, logger=logger)
        
        # Zero baseline should return N/A
        assert deltas["execution_time"].delta is None
        assert "baseline_zero" in deltas["execution_time"].status
        
        # Non-zero baseline should calculate
        assert deltas["cost"].delta is not None
        assert deltas["cost"].status == "success"
    
    def test_negative_delta(self, config, logger):
        """Test that negative deltas (improvements) are handled correctly."""
        baseline = {
            "execution_time": 10.0,
            "cost": 50.0
        }
        candidate = {
            "execution_time": 5.0,
            "cost": 50.0
        }
        
        from delta import calculate_all_deltas
        deltas = calculate_all_deltas(baseline, candidate, config, logger=logger)
        
        # -50% execution time should be improvement
        assert deltas["execution_time"].delta == -50.0
        assert deltas["execution_time"].direction == "+"


# ============================================================================
# TEST: BOTTLENECK DETECTION DETAILS
# ============================================================================

class TestBottleneckDetection:
    """Test bottleneck detection details."""
    
    def test_execution_time_regression_above_threshold(self, baseline_report, config, logger):
        """Test execution time regression > 10% is detected."""
        baseline_models = baseline_report["models"]
        
        # fct_sales: (18.2 - 15.4) / 15.4 = 18.18% > 10%
        candidate_models = baseline_report["models"].copy()
        candidate_models["fct_sales"] = {
            "execution_time": 18.2,  # 18.18% increase
            "cost": 35.8,
            "bytes_scanned": 2000000,
            "join_count": 5,
            "cte_count": 3,
            "window_function_count": 2,
            "data_hash": "xyz789uvw012abc123def456"
        }
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        assert "fct_sales" in bottlenecks
        assert "EXECUTION_TIME_REGRESSION" in bottlenecks["fct_sales"].regression_flags
    
    def test_cost_regression_above_threshold(self, baseline_report, config, logger):
        """Test cost regression > 20% is detected."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with 25% cost increase
        candidate_models = {
            name: dict(data) for name, data in baseline_models.items()
        }
        candidate_models["stg_orders"]["cost"] = 18.3 * 1.25  # 25% increase
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        assert "stg_orders" in bottlenecks
        assert "COST_REGRESSION" in bottlenecks["stg_orders"].regression_flags
    
    def test_bottleneck_severity_scoring(self, baseline_report, config, logger):
        """Test bottleneck severity scoring."""
        baseline_models = baseline_report["models"]
        
        # Create candidate with both execution time and cost regressions
        candidate_models = {
            name: dict(data) for name, data in baseline_models.items()
        }
        candidate_models["fct_inventory"] = {
            "execution_time": 22.5 * 1.25,  # 25% regression
            "cost": 52.3 * 1.30,  # 30% regression
            "bytes_scanned": 3500000,
            "join_count": 6,
            "cte_count": 4,
            "window_function_count": 3,
            "data_hash": "inv_baseline_xyz789"
        }
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # Model with multiple regressions should have high impact score
        assert "fct_inventory" in bottlenecks
        assert bottlenecks["fct_inventory"].impact_score > 0


# ============================================================================
# TEST: ERROR HANDLING
# ============================================================================

class TestErrorHandling:
    """Test error handling for malformed inputs."""
    
    def test_malformed_json_handling(self, config, logger):
        """Test handling of malformed JSON fixtures."""
        # Create obviously malformed data
        malformed_baseline = {"models": {"bad_model": {"invalid": "data"}}}
        malformed_candidate = {"models": {"bad_model": {"invalid": "data"}}}
        
        # Should not crash, but may produce empty or N/A results
        model_deltas = calculate_model_deltas(
            malformed_baseline["models"],
            malformed_candidate["models"],
            config,
            logger=logger
        )
        
        assert isinstance(model_deltas, dict)
    
    def test_missing_kpi_in_candidate(self, baseline_report, config, logger):
        """Test handling when candidate is missing KPIs."""
        baseline_models = baseline_report["models"]
        
        # Candidate missing some KPIs
        candidate_models = {
            "stg_users": {
                "execution_time": 4.8,
                "cost": 12.0
                # Missing other KPIs
            }
        }
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        
        # stg_users should still be processed
        assert "stg_users" in model_deltas


# ============================================================================
# TEST: CONSOLE OUTPUT FORMATTING
# ============================================================================

class TestConsoleOutput:
    """Test console output formatting and readability."""
    
    def test_formatted_deltas_serializable(self, baseline_report, candidate_report, config, logger):
        """Verify formatted deltas are JSON serializable."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        formatted = format_delta_output(model_deltas)
        
        # Should be serializable to JSON
        json_str = json.dumps(formatted, default=str)
        assert isinstance(json_str, str)
        assert len(json_str) > 0
    
    def test_model_comparison_output_structure(self, baseline_report, candidate_report, config, logger):
        """Verify model comparison output has required columns."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        # Each model should have delta information
        for model_name, comparison in analysis["model_comparisons"].items():
            # Should have KPI fields
            assert isinstance(comparison, dict)
            
            # Should have standard KPIs
            kpi_fields = [
                "execution_time", "cost", "bytes_scanned",
                "join_count", "cte_count", "window_function_count"
            ]
            
            for kpi in kpi_fields:
                if kpi in comparison:
                    kpi_data = comparison[kpi]
                    if isinstance(kpi_data, dict):
                        # Should have delta, direction, status
                        assert "delta" in kpi_data or "_status" in comparison
                        assert "direction" in kpi_data or "delta" not in kpi_data
    
    def test_bottleneck_summary_completeness(self, baseline_report, candidate_report, config, logger):
        """Verify bottleneck summary has all required fields."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        for model_name, bottleneck_info in analysis["bottleneck_summary"].items():
            assert "impact_score" in bottleneck_info
            assert "severity" in bottleneck_info
            assert "regression_flags" in bottleneck_info
            assert "data_drift_detected" in bottleneck_info
            
            # Severity should be valid
            assert bottleneck_info["severity"] in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
            
            # Impact score should be numeric
            assert isinstance(bottleneck_info["impact_score"], (int, float))
            assert 0 <= bottleneck_info["impact_score"] <= 100
    
    def test_recommendation_summary_output(self, baseline_report, candidate_report, config, logger):
        """Verify recommendation output is properly formatted."""
        baseline_models = baseline_report["models"]
        candidate_models = candidate_report["models"]
        
        model_deltas = calculate_model_deltas(
            baseline_models, candidate_models, config, logger=logger
        )
        bottlenecks = detect_bottlenecks(model_deltas, config, logger=logger)
        
        # Extract complexity metrics
        complexity_metrics = {}
        for model_name, kpis in baseline_models.items():
            complexity_metrics[model_name] = {
                "join_count": kpis.get("join_count", 0),
                "cte_count": kpis.get("cte_count", 0),
                "window_function_count": kpis.get("window_function_count", 0)
            }
        
        recommendations_dict = generate_recommendations(
            bottlenecks, complexity_metrics, config, logger=logger
        )
        
        # Flatten and check recommendations
        for model_recs in recommendations_dict.values():
            for rec in model_recs:
                # Should be serializable
                rec_dict = rec.to_dict()
                json_str = json.dumps(rec_dict, default=str)
                assert len(json_str) > 0
    
    def test_overall_statistics_output(self, baseline_report, candidate_report, config, logger):
        """Verify overall statistics are properly formatted."""
        analysis = run_comparison_workflow(
            baseline_report["models"],
            candidate_report["models"],
            config,
            logger
        )
        
        stats = analysis["overall_statistics"]
        
        # Should have percentage calculations
        assert stats["total_models"] >= 0
        assert stats["improved_count"] >= 0
        assert stats["regressed_count"] >= 0
        assert 0 <= stats["percent_improved"] <= 100
        
        # Should be JSON serializable
        json_str = json.dumps(stats)
        assert len(json_str) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
