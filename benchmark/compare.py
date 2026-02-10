#!/usr/bin/env python3
"""
Compare Baseline and Candidate Reports

This script loads and validates baseline and candidate reports before comparison
proceeds. It ensures both reports conform to the expected schema and checks for
model name consistency.

Key responsibilities:
1. Load baseline_report.json and candidate_report.json from file system
2. Validate both reports against schema (required fields, data types)
3. Verify field consistency between baseline and candidate (same KPI fields)
4. Check model name consistency between reports
5. Log detailed validation results with pass/fail summary
6. Exit with code 1 on critical validation failure, 0 on success

Usage:
    python benchmark/compare.py baseline.json candidate.json
    python benchmark/compare.py --baseline baseline.json --candidate candidate.json
    python benchmark/compare.py baseline.json candidate.json --log-level DEBUG

CLI Arguments:
    baseline_report.json    (positional) Path to baseline report file
    candidate_report.json   (positional) Path to candidate report file
    --baseline PATH         (optional) Path to baseline report (alternative to positional)
    --candidate PATH        (optional) Path to candidate report (alternative to positional)
    --log-level {DEBUG|INFO|WARNING|ERROR}  (optional) Log level (default: INFO)
    --output PATH           (optional) Output file path for comparison results

Returns:
    0 on successful validation (including non-critical warnings)
    1 on critical validation failure (missing artifacts, schema errors)
"""

import sys
import json
import logging
import os
import argparse
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import (
    load_json_safe,
    validate_report_schema,
    setup_logging,
    ensure_logs_directory,
    get_project_root,
    MissingArtifact,
    InvalidSchema,
    ConfigError
)

# Import optional analysis modules
try:
    from bottleneck import detect_bottlenecks, generate_bottleneck_summary, BottleneckResult
    HAS_BOTTLENECK = True
except ImportError:
    HAS_BOTTLENECK = False

try:
    from recommendation import generate_recommendations
    HAS_RECOMMENDATIONS = True
except ImportError:
    HAS_RECOMMENDATIONS = False

try:
    from config import load_config
    HAS_CONFIG = True
except ImportError:
    HAS_CONFIG = False


# ============================================================================
# CONSOLE OUTPUT FORMATTING FUNCTIONS
# ============================================================================
# Functions for formatting baseline vs candidate comparison with visual status
# indicators and summary statistics for console and log output

def get_status_indicator(delta: Optional[float], is_improvement_on_reduction: bool = True) -> Tuple[str, str]:
    """
    Determine status indicator and color based on delta value and metric type.
    
    For metrics where lower is better (e.g., time, cost):
    - Negative delta (reduction) = improvement ✓
    - Positive delta (increase) = regression ✗
    
    For metrics where higher is better (e.g., rows):
    - Positive delta (increase) = improvement ✓
    - Negative delta (decrease) = regression ✗
    
    Args:
        delta (Optional[float]): Percentage change
        is_improvement_on_reduction (bool): Whether metric improves on reduction
        
    Returns:
        Tuple[str, str]: (indicator_symbol, status_description)
    """
    if delta is None:
        return "⚠", "neutral"
    
    # For metrics where lower is better (time, cost), negative delta is improvement
    if is_improvement_on_reduction:
        if delta < -0.01:
            return "✓", "improvement"
        elif delta > 0.01:
            return "✗", "regression"
        else:
            return "⚠", "neutral"
    else:
        # For metrics where higher is better (rows, output), positive delta is improvement
        if delta > 0.01:
            return "✓", "improvement"
        elif delta < -0.01:
            return "✗", "regression"
        else:
            return "⚠", "neutral"


def format_delta_percentage(delta: Optional[float]) -> str:
    """Format delta value as a percentage string with sign and 2 decimals."""
    if delta is None:
        return "N/A"
    
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.2f}%"


def _format_bytes(byte_value: float) -> str:
    """Format bytes into human-readable format (B, KB, MB, GB, TB)."""
    if isinstance(byte_value, str):
        try:
            byte_value = float(byte_value)
        except (ValueError, TypeError):
            return str(byte_value)
    
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if byte_value < 1024:
            return f"{byte_value:.2f} {unit}"
        byte_value /= 1024
    
    return f"{byte_value:.2f} PB"


def format_number_with_units(value: Any, metric_name: str) -> str:
    """Format numeric value with appropriate units based on metric type."""
    if value is None:
        return "N/A"
    
    try:
        # Time metrics
        if "time" in metric_name.lower():
            return f"{float(value):.2f}s"
        
        # Size metrics (bytes)
        if "bytes" in metric_name.lower() or "scanned" in metric_name.lower():
            return _format_bytes(float(value))
        
        # Cost metrics
        if "cost" in metric_name.lower() or "credit" in metric_name.lower():
            return f"${float(value):.2f}"
        
        # Count metrics
        if any(x in metric_name.lower() for x in ["count", "rows", "produced"]):
            return f"{int(float(value)):,}"
        
        # Default: show as float with 2 decimals
        return f"{float(value):.2f}"
    except (ValueError, TypeError):
        return str(value)


def prepare_comparison_table_data(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Prepare comparison data into a structured format for table rendering.
    
    Extracts models and KPI data from baseline/candidate reports.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (Optional[logging.Logger]): Logger for debug info
        
    Returns:
        Dict[str, Any]: Structured comparison data
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    # Build lookup dicts
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    # Get all model names
    all_models = sorted(set(baseline_by_name.keys()) | set(candidate_by_name.keys()))
    
    if logger:
        logger.debug(f"Preparing comparison for {len(all_models)} models")
    
    return {
        "baseline_by_name": baseline_by_name,
        "candidate_by_name": candidate_by_name,
        "all_models": all_models,
        "total_models": len(all_models),
        "comparison_date": datetime.now().isoformat(),
        "model_count": len(baseline_models)
    }


def generate_comparison_summary_stats(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Calculate aggregated summary statistics across all models.
    
    Computes improvement/regression percentages and cost deltas.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (Optional[logging.Logger]): Logger for debug info
        
    Returns:
        Dict[str, Any]: Summary statistics
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    all_models = set(baseline_by_name.keys()) & set(candidate_by_name.keys())
    total_models = len(all_models)
    
    improved_count = 0
    regressed_count = 0
    neutral_count = 0
    total_cost_delta = 0.0
    all_deltas = []
    
    for model_name in all_models:
        baseline_model = baseline_by_name.get(model_name, {})
        candidate_model = candidate_by_name.get(model_name, {})
        
        # Check cost delta
        baseline_cost = baseline_model.get("estimated_cost_usd")
        candidate_cost = candidate_model.get("estimated_cost_usd")
        
        if baseline_cost and candidate_cost and isinstance(baseline_cost, (int, float)) and isinstance(candidate_cost, (int, float)):
            if baseline_cost != 0:
                cost_delta = ((candidate_cost - baseline_cost) / baseline_cost * 100)
                total_cost_delta += (candidate_cost - baseline_cost)
                all_deltas.append(abs(cost_delta))
                
                if cost_delta < -0.01:
                    improved_count += 1
                elif cost_delta > 0.01:
                    regressed_count += 1
                else:
                    neutral_count += 1
    
    # Calculate percentages
    if total_models > 0:
        improved_pct = (improved_count / total_models * 100)
        regressed_pct = (regressed_count / total_models * 100)
        neutral_pct = (neutral_count / total_models * 100)
        avg_improvement = sum(all_deltas) / len(all_deltas) if all_deltas else 0.0
    else:
        improved_pct = regressed_pct = neutral_pct = avg_improvement = 0.0
    
    if logger:
        logger.debug(
            f"Summary stats: {improved_count} improved ({improved_pct:.1f}%), "
            f"{regressed_count} regressed ({regressed_pct:.1f}%), "
            f"cost delta: ${total_cost_delta:.2f}"
        )
    
    return {
        "total_models": total_models,
        "improved_count": improved_count,
        "improved_percent": round(improved_pct, 1),
        "regressed_count": regressed_count,
        "regressed_percent": round(regressed_pct, 1),
        "neutral_count": neutral_count,
        "neutral_percent": round(neutral_pct, 1),
        "total_cost_delta": round(total_cost_delta, 2),
        "avg_improvement_percent": round(avg_improvement, 2)
    }


def format_comparison_header(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    summary_stats: Dict[str, Any],
    logger: logging.Logger
) -> None:
    """Log a formatted header section with comparison metadata."""
    logger.info("\n" + "=" * 80)
    logger.info("BASELINE vs CANDIDATE COMPARISON")
    logger.info("=" * 80)
    
    total_models = summary_stats.get("total_models", 0)
    improved_count = summary_stats.get("improved_count", 0)
    improved_pct = summary_stats.get("improved_percent", 0)
    total_cost_delta = summary_stats.get("total_cost_delta", 0)
    
    logger.info(f"Models processed: {total_models}")
    logger.info(f"Comparison date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Overall summary: {improved_count} models improved ({improved_pct}%)")
    logger.info(f"Total cost delta: ${total_cost_delta:+.2f}")
    logger.info("=" * 80 + "\n")


def format_comparison_summary_table(summary_stats: Dict[str, Any], logger: logging.Logger) -> None:
    """Log a summary statistics table with aggregated metrics."""
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    
    total = summary_stats.get("total_models", 0)
    improved = summary_stats.get("improved_count", 0)
    improved_pct = summary_stats.get("improved_percent", 0)
    regressed = summary_stats.get("regressed_count", 0)
    regressed_pct = summary_stats.get("regressed_percent", 0)
    neutral = summary_stats.get("neutral_count", 0)
    neutral_pct = summary_stats.get("neutral_percent", 0)
    total_cost_delta = summary_stats.get("total_cost_delta", 0)
    avg_improvement = summary_stats.get("avg_improvement_percent", 0)
    
    logger.info(f"Total models analyzed:       {total}")
    logger.info(f"✓ Improved:                  {improved} ({improved_pct:.1f}%)")
    logger.info(f"✗ Regressed:                 {regressed} ({regressed_pct:.1f}%)")
    logger.info(f"⚠ Neutral:                   {neutral} ({neutral_pct:.1f}%)")
    logger.info(f"Total cost delta:            ${total_cost_delta:+.2f}")
    logger.info(f"Average improvement:         {avg_improvement:+.2f}%")
    logger.info("=" * 80 + "\n")


def format_model_comparison_rows(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> List[List[str]]:
    """Format model-by-model comparison data into table rows."""
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    all_models = sorted(set(baseline_by_name.keys()) & set(candidate_by_name.keys()))
    
    rows = []
    key_metrics = [
        "execution_time_seconds",
        "estimated_cost_usd",
        "bytes_scanned",
        "rows_produced"
    ]
    
    # Metrics where lower is better
    improvement_on_reduction = {
        "execution_time_seconds", "estimated_cost_usd", "bytes_scanned"
    }
    
    for model_name in all_models:
        baseline_model = baseline_by_name[model_name]
        candidate_model = candidate_by_name[model_name]
        
        for metric in key_metrics:
            baseline_val = baseline_model.get(metric)
            candidate_val = candidate_model.get(metric)
            
            if (baseline_val is not None and candidate_val is not None and 
                isinstance(baseline_val, (int, float)) and isinstance(candidate_val, (int, float))):
                
                # Calculate delta
                if baseline_val != 0:
                    delta = ((candidate_val - baseline_val) / baseline_val) * 100
                else:
                    delta = None
                
                # Determine status
                is_reduction_metric = metric in improvement_on_reduction
                indicator, status = get_status_indicator(delta, is_reduction_metric)
                
                # Format row
                baseline_str = format_number_with_units(baseline_val, metric)
                candidate_str = format_number_with_units(candidate_val, metric)
                delta_str = format_delta_percentage(delta)
                
                rows.append([
                    model_name,
                    metric,
                    baseline_str,
                    candidate_str,
                    delta_str,
                    indicator,
                    status
                ])
    
    return rows


# ============================================================================
# ANALYSIS JSON REPORT GENERATION FUNCTIONS
# ============================================================================
# Functions for generating comprehensive baseline vs candidate analysis reports
# with deltas, bottleneck summaries, recommendations, and data quality flags

def calculate_model_deltas(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Calculate percentage deltas for all KPI metrics per model.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (Optional[logging.Logger]): Logger for progress tracking
    
    Returns:
        Dict[str, Dict[str, Optional[float]]]: {model_name: {metric: delta_percent or None}}
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    # Only process models in both reports
    common_models = set(baseline_by_name.keys()) & set(candidate_by_name.keys())
    
    result = {}
    key_metrics = [
        "execution_time_seconds",
        "estimated_cost_usd",
        "bytes_scanned",
        "rows_produced"
    ]
    
    for model_name in sorted(common_models):
        result[model_name] = {}
        baseline_model = baseline_by_name[model_name]
        candidate_model = candidate_by_name[model_name]
        
        for metric in key_metrics:
            baseline_val = baseline_model.get(metric)
            candidate_val = candidate_model.get(metric)
            
            if (baseline_val is not None and candidate_val is not None and 
                isinstance(baseline_val, (int, float)) and isinstance(candidate_val, (int, float))):
                if baseline_val != 0:
                    delta = ((candidate_val - baseline_val) / baseline_val) * 100
                    result[model_name][metric] = round(delta, 2)
                else:
                    result[model_name][metric] = None
            else:
                result[model_name][metric] = None
    
    return result


def detect_data_equivalence_warnings(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Detect models with data hash mismatches (data equivalence issues).
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (Optional[logging.Logger]): Logger for warnings
    
    Returns:
        List[Dict[str, Any]]: List of models with hash mismatches
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    warnings = []
    
    for model_name in set(baseline_by_name.keys()) & set(candidate_by_name.keys()):
        baseline_model = baseline_by_name[model_name]
        candidate_model = candidate_by_name[model_name]
        
        baseline_hash = baseline_model.get("output_hash")
        candidate_hash = candidate_model.get("output_hash")
        
        if baseline_hash and candidate_hash and baseline_hash != candidate_hash:
            warnings.append({
                "model_name": model_name,
                "baseline_hash": str(baseline_hash)[:16] + "..." if baseline_hash else None,
                "candidate_hash": str(candidate_hash)[:16] + "..." if candidate_hash else None,
                "potential_causes": [
                    "Data distribution changes",
                    "Query logic modification",
                    "Upstream dependency changes",
                    "Random seed or non-deterministic operations"
                ]
            })
            if logger:
                logger.warning(f"Data equivalence mismatch for {model_name}")
    
    return warnings


def generate_model_comparisons(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    deltas: Dict[str, Dict[str, Optional[float]]],
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Generate model comparison objects with baseline, candidate, and delta KPIs.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        deltas (Dict[str, Dict[str, Optional[float]]]): Calculated deltas per model
        logger (Optional[logging.Logger]): Logger for progress
    
    Returns:
        List[Dict[str, Any]]: List of model comparison objects
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    comparisons = []
    
    for model_name in sorted(deltas.keys()):
        baseline_model = baseline_by_name.get(model_name, {})
        candidate_model = candidate_by_name.get(model_name, {})
        model_deltas = deltas.get(model_name, {})
        
        comparison = {
            "model_name": model_name,
            "baseline_kpis": {
                "execution_time_seconds": baseline_model.get("execution_time_seconds"),
                "estimated_cost_usd": baseline_model.get("estimated_cost_usd"),
                "bytes_scanned": baseline_model.get("bytes_scanned"),
                "rows_produced": baseline_model.get("rows_produced")
            },
            "candidate_kpis": {
                "execution_time_seconds": candidate_model.get("execution_time_seconds"),
                "estimated_cost_usd": candidate_model.get("estimated_cost_usd"),
                "bytes_scanned": candidate_model.get("bytes_scanned"),
                "rows_produced": candidate_model.get("rows_produced")
            },
            "delta_metrics": model_deltas,
            "data_equivalence": {
                "baseline_hash": baseline_model.get("output_hash"),
                "candidate_hash": candidate_model.get("output_hash"),
                "hash_match": baseline_model.get("output_hash") == candidate_model.get("output_hash")
            }
        }
        comparisons.append(comparison)
    
    return comparisons


def calculate_overall_statistics(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    deltas: Dict[str, Dict[str, Optional[float]]],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Calculate aggregated overall statistics across all models.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        deltas (Dict[str, Dict[str, Optional[float]]]): Model deltas
        logger (Optional[logging.Logger]): Logger
    
    Returns:
        Dict[str, Any]: Overall statistics
    """
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_by_name = {m.get("model_name"): m for m in baseline_models if m.get("model_name")}
    candidate_by_name = {m.get("model_name"): m for m in candidate_models if m.get("model_name")}
    
    total_models = len(deltas)
    improved = 0
    regressed = 0
    neutral = 0
    total_execution_time_delta = 0.0
    total_cost_delta = 0.0
    all_cost_deltas = []
    
    for model_name in deltas.keys():
        baseline_model = baseline_by_name.get(model_name, {})
        candidate_model = candidate_by_name.get(model_name, {})
        model_deltas = deltas[model_name]
        
        # Check cost delta for improvement/regression categorization
        cost_delta = model_deltas.get("estimated_cost_usd")
        if cost_delta is not None:
            if cost_delta < -0.01:
                improved += 1
            elif cost_delta > 0.01:
                regressed += 1
            else:
                neutral += 1
            all_cost_deltas.append(cost_delta)
            
            # Add to totals
            baseline_cost = baseline_model.get("estimated_cost_usd")
            candidate_cost = candidate_model.get("estimated_cost_usd")
            if baseline_cost and candidate_cost:
                total_cost_delta += (candidate_cost - baseline_cost)
        
        # Track execution time delta
        exec_delta = model_deltas.get("execution_time_seconds")
        if exec_delta is not None:
            baseline_time = baseline_model.get("execution_time_seconds")
            candidate_time = candidate_model.get("execution_time_seconds")
            if baseline_time and candidate_time:
                total_execution_time_delta += (candidate_time - baseline_time)
    
    # Calculate averages
    avg_improvement = 0.0
    if all_cost_deltas:
        avg_improvement = sum(all_cost_deltas) / len(all_cost_deltas)
    
    return {
        "total_models": total_models,
        "models_improved": improved,
        "models_regressed": regressed,
        "models_neutral": neutral,
        "improved_percent": round((improved / total_models * 100) if total_models > 0 else 0, 2),
        "regressed_percent": round((regressed / total_models * 100) if total_models > 0 else 0, 2),
        "neutral_percent": round((neutral / total_models * 100) if total_models > 0 else 0, 2),
        "total_execution_time_delta_seconds": round(total_execution_time_delta, 4),
        "total_cost_delta_usd": round(total_cost_delta, 2),
        "average_improvement_percentage": round(avg_improvement, 2)
    }


def generate_analysis_report(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Generate comprehensive analysis report with all sections.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (Optional[logging.Logger]): Logger for progress
    
    Returns:
        Dict[str, Any]: Complete analysis report
    """
    if logger:
        logger.info("Generating analysis report...")
    
    # Calculate deltas
    deltas = calculate_model_deltas(baseline, candidate, logger)
    
    # Detect data equivalence issues
    data_warnings = detect_data_equivalence_warnings(baseline, candidate, logger)
    
    # Generate model comparisons
    model_comparisons = generate_model_comparisons(baseline, candidate, deltas, logger)
    
    # Calculate overall statistics
    overall_stats = calculate_overall_statistics(baseline, candidate, deltas, logger)
    
    # Create report
    report = {
        "metadata": {
            "baseline_timestamp": baseline.get("timestamp"),
            "candidate_timestamp": candidate.get("timestamp"),
            "comparison_date": datetime.now().isoformat(),
            "config_version": "1.0.0",
            "pipeline_name": baseline.get("pipeline_name", "unknown")
        },
        "model_comparisons": model_comparisons,
        "bottleneck_summary": {
            "top_models": [],
            "note": "Bottleneck detection requires Task #21 integration"
        },
        "optimization_recommendations": {
            "recommendations": [],
            "note": "Recommendations require Task #22 integration"
        },
        "overall_statistics": overall_stats,
        "data_equivalence_warnings": data_warnings
    }
    
    if logger:
        logger.info(f"✓ Analysis report generated ({len(model_comparisons)} models)")
    
    return report


def validate_analysis_schema(
    report: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Tuple[bool, List[str]]:
    """
    Validate analysis report against schema.
    
    Args:
        report (Dict[str, Any]): Analysis report to validate
        logger (Optional[logging.Logger]): Logger for errors
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
    """
    errors = []
    
    # Check required top-level sections
    required_sections = [
        "metadata", "model_comparisons", "overall_statistics",
        "data_equivalence_warnings"
    ]
    
    for section in required_sections:
        if section not in report:
            errors.append(f"Missing required section: {section}")
    
    # Validate metadata
    if "metadata" in report:
        metadata = report["metadata"]
        required_metadata = [
            "baseline_timestamp", "candidate_timestamp", "comparison_date",
            "config_version", "pipeline_name"
        ]
        for field in required_metadata:
            if field not in metadata:
                errors.append(f"Missing metadata field: {field}")
    
    # Validate model_comparisons
    if "model_comparisons" in report:
        if not isinstance(report["model_comparisons"], list):
            errors.append("model_comparisons must be a list")
        else:
            for i, comp in enumerate(report["model_comparisons"]):
                required_fields = [
                    "model_name", "baseline_kpis", "candidate_kpis",
                    "delta_metrics", "data_equivalence"
                ]
                for field in required_fields:
                    if field not in comp:
                        errors.append(f"Model comparison {i} missing field: {field}")
    
    # Validate overall_statistics
    if "overall_statistics" in report:
        stats = report["overall_statistics"]
        required_stats = [
            "total_models", "models_improved", "models_regressed",
            "models_neutral", "total_execution_time_delta_seconds",
            "total_cost_delta_usd", "average_improvement_percentage"
        ]
        for field in required_stats:
            if field not in stats:
                errors.append(f"overall_statistics missing field: {field}")
    
    # Validate data_equivalence_warnings
    if "data_equivalence_warnings" in report:
        if not isinstance(report["data_equivalence_warnings"], list):
            errors.append("data_equivalence_warnings must be a list")
    
    is_valid = len(errors) == 0
    
    if logger:
        if is_valid:
            logger.info("✓ Analysis report schema validation passed")
        else:
            logger.error(f"✗ Analysis report validation failed with {len(errors)} error(s)")
            for error in errors[:5]:
                logger.error(f"  - {error}")
    
    return is_valid, errors


def write_analysis_report(
    report: Dict[str, Any],
    output_path: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Write analysis report to file, creating parent directories as needed.
    
    Args:
        report (Dict[str, Any]): Analysis report to write
        output_path (str): Path for output file
        logger (Optional[logging.Logger]): Logger for progress
    
    Returns:
        bool: True if write succeeded, False otherwise
    """
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        if logger:
            logger.info(f"✓ Analysis report written to: {output_path}")
        
        return True
    except Exception as e:
        if logger:
            logger.error(f"✗ Failed to write analysis report: {str(e)}", exc_info=True)
        else:
            print(f"Error: {str(e)}", file=sys.stderr)
        return False


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the report comparison script.
    
    Supports both positional arguments (baseline candidate) and flag arguments
    (--baseline, --candidate).
    
    Returns:
        Parsed arguments with baseline, candidate, log_level, output, recommendations, and config paths
        
    Raises:
        SystemExit: If required arguments are missing
    """
    parser = argparse.ArgumentParser(
        prog="compare.py",
        description="Load and validate baseline and candidate reports for comparison with optional optimization recommendations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark/compare.py baseline.json candidate.json
  python benchmark/compare.py --baseline baseline.json --candidate candidate.json
  python benchmark/compare.py baseline.json candidate.json --log-level DEBUG
  python benchmark/compare.py baseline.json candidate.json --recommendations
  python benchmark/compare.py baseline.json candidate.json --config benchmark/config.py --output analysis.json
        """
    )
    
    # Positional arguments (optional if using flags)
    parser.add_argument(
        "baseline",
        nargs="?",
        default=None,
        help="Path to baseline report (positional argument)"
    )
    
    parser.add_argument(
        "candidate",
        nargs="?",
        default=None,
        help="Path to candidate report (positional argument)"
    )
    
    # Flag arguments
    parser.add_argument(
        "--baseline",
        type=str,
        default=None,
        help="Path to baseline report (flag argument)"
    )
    
    parser.add_argument(
        "--candidate",
        type=str,
        default=None,
        help="Path to candidate report (flag argument)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path for analysis results (optional, default: benchmark/analysis.json)"
    )
    
    parser.add_argument(
        "--recommendations",
        action="store_true",
        default=False,
        help="Include detailed optimization recommendations in analysis output (requires config)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config file (default: benchmark/config.py)"
    )
    
    args = parser.parse_args()
    
    # Resolve baseline path (prefer flag over positional)
    baseline = None
    if hasattr(args, 'baseline') and isinstance(args.baseline, str):
        baseline = args.baseline
    
    # Resolve candidate path (prefer flag over positional)
    candidate = None
    if hasattr(args, 'candidate') and isinstance(args.candidate, str):
        candidate = args.candidate
    
    # Handle positional args from sys.argv
    positional_args = [arg for arg in sys.argv[1:] if not arg.startswith('--')]
    
    if not baseline and positional_args:
        baseline = positional_args[0]
    if not candidate and len(positional_args) > 1:
        candidate = positional_args[1]
    
    args.baseline = baseline
    args.candidate = candidate
    args.config_path = args.config or "benchmark/config.py"
    
    return args


def load_report(file_path: str, logger: logging.Logger) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Load a single report JSON file with error handling.
    
    Args:
        file_path (str): Path to report JSON file
        logger (logging.Logger): Logger instance
    
    Returns:
        Tuple[Optional[Dict[str, Any]], bool]: (report_data, success_flag)
            - report_data: Parsed report dictionary or None if failed
            - success_flag: True if load succeeded, False otherwise
    
    Examples:
        >>> logger = setup_logging("test")
        >>> data, success = load_report("baseline.json", logger)
        >>> if success:
        ...     print(f"Loaded {len(data['models'])} models")
    """
    try:
        logger.info(f"Loading report from: {file_path}")
        data = load_json_safe(file_path)
        logger.info(f"✓ Successfully loaded report: {file_path}")
        return data, True
    except MissingArtifact as e:
        logger.error(f"✗ Failed to load report: {str(e)}")
        return None, False
    except InvalidSchema as e:
        logger.error(f"✗ Malformed JSON in report: {str(e)}")
        return None, False
    except Exception as e:
        logger.error(f"✗ Unexpected error loading report: {str(e)}", exc_info=True)
        return None, False


def check_model_consistency(baseline: Dict[str, Any], candidate: Dict[str, Any], logger: logging.Logger) -> Tuple[bool, List[str]]:
    """
    Check model name consistency between baseline and candidate reports.
    
    Allows baseline models to not exist in candidate (subset differences), but
    flags discrepancies in detailed logs.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (logging.Logger): Logger instance
    
    Returns:
        Tuple[bool, List[str]]: (is_consistent, warning_messages)
            - is_consistent: True if models are compatible (False only for critical mismatches)
            - warning_messages: List of warning messages about discrepancies
    
    Examples:
        >>> baseline = {"models": [{"model_name": "a"}, {"model_name": "b"}]}
        >>> candidate = {"models": [{"model_name": "a"}]}
        >>> is_ok, warnings = check_model_consistency(baseline, candidate, logger)
        >>> print(is_ok, len(warnings))
        True 1
    """
    warnings = []
    
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    baseline_names = {m.get("model_name") for m in baseline_models if m.get("model_name")}
    candidate_names = {m.get("model_name") for m in candidate_models if m.get("model_name")}
    
    logger.info(f"Model inventory:")
    logger.info(f"  Baseline: {len(baseline_names)} models")
    logger.info(f"  Candidate: {len(candidate_names)} models")
    
    # Find models in baseline but not in candidate
    only_in_baseline = baseline_names - candidate_names
    if only_in_baseline:
        msg = f"Models in baseline but not in candidate ({len(only_in_baseline)}): {', '.join(sorted(only_in_baseline)[:5])}"
        if len(only_in_baseline) > 5:
            msg += f" and {len(only_in_baseline) - 5} more"
        warnings.append(msg)
        logger.warning(f"  - {msg}")
    
    # Find models in candidate but not in baseline
    only_in_candidate = candidate_names - baseline_names
    if only_in_candidate:
        msg = f"Models in candidate but not in baseline ({len(only_in_candidate)}): {', '.join(sorted(only_in_candidate)[:5])}"
        if len(only_in_candidate) > 5:
            msg += f" and {len(only_in_candidate) - 5} more"
        warnings.append(msg)
        logger.warning(f"  - {msg}")
    
    # Models in both
    common_models = baseline_names & candidate_names
    logger.info(f"  Models in both: {len(common_models)}")
    
    # This is not a critical failure - we can proceed with comparison
    # even if models differ between reports
    is_consistent = True
    
    return is_consistent, warnings


def check_kpi_field_consistency(baseline: Dict[str, Any], candidate: Dict[str, Any], logger: logging.Logger) -> Tuple[bool, List[str]]:
    """
    Verify KPI field names and types are consistent between baseline and candidate.
    
    Args:
        baseline (Dict[str, Any]): Baseline report dictionary
        candidate (Dict[str, Any]): Candidate report dictionary
        logger (logging.Logger): Logger instance
    
    Returns:
        Tuple[bool, List[str]]: (is_consistent, error_messages)
            - is_consistent: False if critical field differences found
            - error_messages: List of error messages
    
    Examples:
        >>> baseline = {"models": [{"model_name": "a", "execution_time_seconds": 1.0}]}
        >>> candidate = {"models": [{"model_name": "a", "execution_time_seconds": 1.0}]}
        >>> is_ok, errors = check_kpi_field_consistency(baseline, candidate, logger)
        >>> print(is_ok)
        True
    """
    errors = []
    
    baseline_models = baseline.get("models", [])
    candidate_models = candidate.get("models", [])
    
    if not baseline_models or not candidate_models:
        logger.warning("Cannot check KPI consistency: one or both reports have no models")
        return True, []
    
    # Extract fields from first model of each report
    baseline_model = baseline_models[0]
    candidate_model = candidate_models[0]
    
    baseline_fields = set(baseline_model.keys())
    candidate_fields = set(candidate_model.keys())
    
    # Required KPI fields
    required_kpi_fields = {
        "execution_time_seconds",
        "rows_produced",
        "bytes_scanned",
        "output_hash",
        "join_count",
        "cte_count",
        "window_function_count",
        "estimated_credits",
        "estimated_cost_usd"
    }
    
    # Check baseline has all required KPI fields
    missing_in_baseline = required_kpi_fields - baseline_fields
    if missing_in_baseline:
        msg = f"Baseline missing KPI fields: {', '.join(sorted(missing_in_baseline))}"
        errors.append(msg)
        logger.error(f"  - {msg}")
    
    # Check candidate has all required KPI fields
    missing_in_candidate = required_kpi_fields - candidate_fields
    if missing_in_candidate:
        msg = f"Candidate missing KPI fields: {', '.join(sorted(missing_in_candidate))}"
        errors.append(msg)
        logger.error(f"  - {msg}")
    
    # Check field type consistency for numeric fields
    for field in required_kpi_fields:
        if field in baseline_fields and field in candidate_fields:
            baseline_val = baseline_model.get(field)
            candidate_val = candidate_model.get(field)
            
            baseline_type = type(baseline_val).__name__
            candidate_type = type(candidate_val).__name__
            
            # Allow None for output_hash
            if field == "output_hash" and (baseline_val is None or candidate_val is None):
                continue
            
            # Check type consistency (allow int/float interchangeably for numeric fields)
            if field in ["execution_time_seconds", "estimated_credits", "estimated_cost_usd"]:
                if not isinstance(baseline_val, (int, float)) or not isinstance(candidate_val, (int, float)):
                    msg = f"Type mismatch for '{field}': baseline={baseline_type}, candidate={candidate_type}"
                    errors.append(msg)
                    logger.warning(f"  - {msg}")
            elif field == "output_hash":
                if not isinstance(baseline_val, (str, type(None))) or not isinstance(candidate_val, (str, type(None))):
                    msg = f"Type mismatch for '{field}': baseline={baseline_type}, candidate={candidate_type}"
                    errors.append(msg)
                    logger.warning(f"  - {msg}")
            else:
                # Integer fields must be integers
                if not isinstance(baseline_val, int) or not isinstance(candidate_val, int):
                    msg = f"Type mismatch for '{field}': baseline={baseline_type}, candidate={candidate_type}"
                    errors.append(msg)
                    logger.warning(f"  - {msg}")
    
    return len(errors) == 0, errors


def load_config_safe(config_path: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Load configuration from file with error handling.
    
    Args:
        config_path (str): Path to config file or module
        logger (logging.Logger): Logger instance
    
    Returns:
        Optional[Dict[str, Any]]: Configuration dictionary or None if failed
    """
    if not HAS_CONFIG:
        logger.warning("Config loading not available - recommendation features may be limited")
        return None
    
    try:
        # Try loading as Python module first
        if config_path.endswith('.py'):
            import importlib.util
            spec = importlib.util.spec_from_file_location("config", config_path)
            if spec is None or spec.loader is None:
                logger.warning(f"Could not load config from {config_path}")
                return None
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            
            # Extract config data
            config = {}
            for attr in dir(config_module):
                if attr.isupper():  # Convention: config constants are UPPERCASE
                    config[attr] = getattr(config_module, attr)
            
            if config:
                logger.info(f"✓ Configuration loaded from: {config_path}")
                logger.debug(f"Loaded {len(config)} configuration items")
                return config
        
        # Fallback: try loading as JSON
        config = load_json_safe(config_path)
        logger.info(f"✓ Configuration loaded from: {config_path}")
        return config
        
    except Exception as e:
        logger.warning(f"Failed to load configuration from {config_path}: {str(e)}")
        return None


def main() -> int:
    """
    Main orchestration function for report loading, validation, and analysis.
    
    Orchestration flow:
    1. Parse CLI arguments
    2. Setup logging
    3. Load configuration (if --config provided)
    4. Load and validate baseline report
    5. Load and validate candidate report
    6. Check model consistency
    7. Check KPI field consistency
    8. Generate console comparison output
    9. Calculate model deltas (Task #20)
    10. Detect bottlenecks (Task #21)
    11. Generate recommendations (Task #22, if --recommendations flag)
    12. Generate analysis.json report (Task #24)
    13. Log execution summary
    
    Returns:
        0 on successful completion
        1 on critical failure
    """
    start_time = time.time()
    
    # Parse arguments
    args = parse_arguments()
    
    # Validate required arguments
    if not args.baseline or not args.candidate:
        print("Error: Both baseline and candidate report paths are required")
        print("Usage: python benchmark/compare.py baseline.json candidate.json")
        return 1
    
    # Setup logging
    try:
        # Set log level environment variable before creating logger
        os.environ["BENCHMARK_LOG_LEVEL"] = args.log_level
        logger = setup_logging("compare")
    except Exception as e:
        print(f"Error setting up logging: {str(e)}", file=sys.stderr)
        return 1
    
    # Load configuration
    config = None
    if args.config_path:
        logger.info("\n" + "=" * 80)
        logger.info("Loading Configuration")
        logger.info("=" * 80)
        config = load_config_safe(args.config_path, logger)
    
    logger.info("=" * 80)
    logger.info("Report Loading and Validation")
    logger.info("=" * 80)
    logger.info(f"Baseline report: {args.baseline}")
    logger.info(f"Candidate report: {args.candidate}")
    
    # Track validation results
    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "baseline_path": str(args.baseline),
        "candidate_path": str(args.candidate),
        "baseline_valid": False,
        "candidate_valid": False,
        "baseline_errors": [],
        "candidate_errors": [],
        "model_consistency_warnings": [],
        "kpi_field_warnings": [],
        "overall_status": "FAILED"
    }
    
    # 1. Load baseline report
    logger.info("\n" + "=" * 80)
    logger.info("Loading Baseline Report")
    logger.info("=" * 80)
    baseline, baseline_loaded = load_report(args.baseline, logger)
    
    if not baseline_loaded:
        logger.error("✗ Failed to load baseline report - aborting validation")
        logger.info(f"\nValidation Results: CRITICAL FAILURE")
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(validation_results, f, indent=2)
            logger.info(f"Validation results written to: {args.output}")
        return 1
    
    # 2. Load candidate report
    logger.info("\n" + "=" * 80)
    logger.info("Loading Candidate Report")
    logger.info("=" * 80)
    candidate, candidate_loaded = load_report(args.candidate, logger)
    
    if not candidate_loaded:
        logger.error("✗ Failed to load candidate report - aborting validation")
        logger.info(f"\nValidation Results: CRITICAL FAILURE")
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(validation_results, f, indent=2)
            logger.info(f"Validation results written to: {args.output}")
        return 1
    
    # 3. Validate baseline report schema
    logger.info("\n" + "=" * 80)
    logger.info("Validating Baseline Report Schema")
    logger.info("=" * 80)
    baseline_valid, baseline_errors = validate_report_schema(baseline, logger)
    validation_results["baseline_valid"] = baseline_valid
    validation_results["baseline_errors"] = baseline_errors
    
    if not baseline_valid:
        logger.error(f"✗ Baseline report schema validation failed with {len(baseline_errors)} error(s)")
        logger.info(f"\nValidation Results: CRITICAL FAILURE")
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(validation_results, f, indent=2)
            logger.info(f"Validation results written to: {args.output}")
        return 1
    else:
        logger.info(f"✓ Baseline report schema validation passed")
    
    # 4. Validate candidate report schema
    logger.info("\n" + "=" * 80)
    logger.info("Validating Candidate Report Schema")
    logger.info("=" * 80)
    candidate_valid, candidate_errors = validate_report_schema(candidate, logger)
    validation_results["candidate_valid"] = candidate_valid
    validation_results["candidate_errors"] = candidate_errors
    
    if not candidate_valid:
        logger.error(f"✗ Candidate report schema validation failed with {len(candidate_errors)} error(s)")
        logger.info(f"\nValidation Results: CRITICAL FAILURE")
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(validation_results, f, indent=2)
            logger.info(f"Validation results written to: {args.output}")
        return 1
    else:
        logger.info(f"✓ Candidate report schema validation passed")
    
    # 5. Check model name consistency (non-critical)
    logger.info("\n" + "=" * 80)
    logger.info("Checking Model Name Consistency")
    logger.info("=" * 80)
    _, model_warnings = check_model_consistency(baseline, candidate, logger)
    validation_results["model_consistency_warnings"] = model_warnings
    
    # 6. Check KPI field consistency (non-critical)
    logger.info("\n" + "=" * 80)
    logger.info("Checking KPI Field Consistency")
    logger.info("=" * 80)
    _, kpi_warnings = check_kpi_field_consistency(baseline, candidate, logger)
    validation_results["kpi_field_warnings"] = kpi_warnings
    
    # 7. Generate console comparison output
    logger.info("\n" + "=" * 80)
    logger.info("Generating Comparison Output")
    logger.info("=" * 80)
    try:
        # Generate summary statistics
        summary_stats = generate_comparison_summary_stats(baseline, candidate, logger)
        
        # Log formatted header with metadata
        format_comparison_header(baseline, candidate, summary_stats, logger)
        
        # Log model-by-model comparison details
        comparison_rows = format_model_comparison_rows(baseline, candidate, logger)
        if comparison_rows:
            logger.info("\n" + "=" * 80)
            logger.info("MODEL-BY-MODEL COMPARISON DETAILS")
            logger.info("=" * 80)
            
            # Log rows with status indicators
            for row in comparison_rows:
                model_name, metric, baseline_val, candidate_val, delta_pct, indicator, status = row
                logger.info(f"{indicator} {model_name:30} {metric:25} {baseline_val:>15} → {candidate_val:>15} ({delta_pct:>8}) [{status}]")
        
        # Log summary statistics table
        format_comparison_summary_table(summary_stats, logger)
        
        # Store comparison results in validation_results for JSON output
        validation_results["comparison_summary"] = summary_stats
        validation_results["comparison_rows_count"] = len(comparison_rows)
        
        logger.info("✓ Comparison output generated successfully")
    except Exception as e:
        logger.warning(f"Failed to generate comparison output: {str(e)}", exc_info=True)
    
    # 8. Generate analysis.json report
    logger.info("\n" + "=" * 80)
    logger.info("Generating Analysis Report")
    logger.info("=" * 80)
    try:
        # Generate analysis report with all sections
        analysis_report = generate_analysis_report(baseline, candidate, logger)
        
        # Validate analysis report schema
        is_valid, errors = validate_analysis_schema(analysis_report, logger)
        validation_results["analysis_report_valid"] = is_valid
        validation_results["analysis_report_errors"] = errors
        
        if is_valid:
            # Determine output path for analysis.json
            # Use --output flag if provided, otherwise default to benchmark/analysis.json
            if args.output:
                # If output ends with .json, use it as analysis report path
                if args.output.endswith('.json'):
                    analysis_output_path = args.output
                else:
                    # If output is a directory or path without extension, add analysis.json
                    analysis_output_path = str(Path(args.output).parent / "analysis.json")
            else:
                # Default location
                analysis_output_path = "benchmark/analysis.json"
            
            # Write analysis report to file
            if write_analysis_report(analysis_report, analysis_output_path, logger):
                validation_results["analysis_report_path"] = analysis_output_path
                logger.info("✓ Analysis report generated and written successfully")
            else:
                logger.error("✗ Failed to write analysis report")
                validation_results["analysis_report_path"] = None
        else:
            logger.error(f"✗ Analysis report schema validation failed with {len(errors)} error(s)")
            validation_results["analysis_report_path"] = None
    except Exception as e:
        logger.warning(f"Failed to generate analysis report: {str(e)}", exc_info=True)
        validation_results["analysis_report_valid"] = False
        validation_results["analysis_report_path"] = None
    
    # 9. Optional: Detect bottlenecks and generate recommendations
    bottleneck_results = {}
    recommendation_results = []
    
    if HAS_BOTTLENECK and config:
        logger.info("\n" + "=" * 80)
        logger.info("Detecting Bottlenecks (Task #21)")
        logger.info("=" * 80)
        try:
            # Get deltas from analysis report
            deltas = analysis_report.get("model_comparisons", []) if 'analysis_report' in locals() else []
            if deltas:
                # Prepare delta dict for bottleneck detection
                delta_dict = {comp.get("model_name"): comp.get("delta_metrics", {}) for comp in deltas}
                bottleneck_results = detect_bottlenecks(delta_dict, config, logger)
                logger.info(f"✓ Bottleneck detection completed ({len(bottleneck_results)} models analyzed)")
            else:
                logger.warning("No model deltas available for bottleneck detection")
        except Exception as e:
            logger.warning(f"Failed to detect bottlenecks: {str(e)}", exc_info=True)
    
    if HAS_RECOMMENDATIONS and args.recommendations and config:
        logger.info("\n" + "=" * 80)
        logger.info("Generating Optimization Recommendations (Task #22)")
        logger.info("=" * 80)
        try:
            if bottleneck_results:
                # Extract complexity metrics from models
                complexity_metrics = {}
                for model in baseline.get("models", []):
                    model_name = model.get("model_name")
                    if model_name:
                        complexity_metrics[model_name] = {
                            "join_count": model.get("join_count", 0),
                            "cte_count": model.get("cte_count", 0),
                            "window_function_count": model.get("window_function_count", 0)
                        }
                
                recommendation_results = generate_recommendations(
                    bottleneck_results, complexity_metrics, config, logger
                )
                logger.info(f"✓ Generated {len(recommendation_results)} recommendations")
            else:
                logger.info("No bottlenecks detected - skipping recommendations")
        except Exception as e:
            logger.warning(f"Failed to generate recommendations: {str(e)}", exc_info=True)
    elif args.recommendations and not HAS_RECOMMENDATIONS:
        logger.warning("--recommendations flag specified but recommendation module not available")
    
    # Summary with execution metrics
    end_time = time.time()
    execution_time_seconds = round(end_time - start_time, 2)
    
    logger.info("\n" + "=" * 80)
    logger.info("Execution Summary")
    logger.info("=" * 80)
    logger.info(f"Baseline report: {'✓ VALID' if baseline_valid else '✗ INVALID'}")
    logger.info(f"Candidate report: {'✓ VALID' if candidate_valid else '✗ INVALID'}")
    logger.info(f"Model consistency: {len(model_warnings)} warning(s)")
    logger.info(f"KPI fields consistency: {len(kpi_warnings)} warning(s)")
    if bottleneck_results:
        logger.info(f"Bottlenecks detected: {len(bottleneck_results)} models")
    if recommendation_results:
        logger.info(f"Recommendations generated: {len(recommendation_results)} recommendations")
    logger.info(f"Execution time: {execution_time_seconds}s")
    
    # All critical checks passed
    validation_results["overall_status"] = "SUCCESS"
    validation_results["execution_time_seconds"] = execution_time_seconds
    validation_results["bottleneck_count"] = len(bottleneck_results)
    validation_results["recommendation_count"] = len(recommendation_results)
    
    logger.info(f"\n✓ All critical validation checks passed")
    logger.info(f"Reports are ready for comparison")
    logger.info(f"Analysis report: {analysis_output_path if 'analysis_output_path' in locals() else 'benchmark/analysis.json'}")
    
    # Write validation results if output path specified
    if args.output:
        try:
            output_dir = Path(args.output).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(args.output, 'w') as f:
                json.dump(validation_results, f, indent=2)
            logger.info(f"Validation results written to: {args.output}")
        except Exception as e:
            logger.warning(f"Failed to write validation results: {str(e)}")
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
