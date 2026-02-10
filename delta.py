#!/usr/bin/env python3
"""
Delta Calculation Module for Benchmarking System

This module calculates percentage changes (deltas) between baseline and candidate 
KPI values with robust edge case handling, direction indicators, and data drift 
detection.

Core Features:
- Calculate deltas using formula: ((candidate - baseline) / baseline) × 100
- Direction indicators: + for improvement, - for regression
- Edge case handling: division by zero, missing models, data drift
- Structured output format with comprehensive logging
- Support for all KPI types with improvement-direction awareness

Usage:
    from delta import calculate_delta, calculate_all_deltas
    from config import load_config
    
    baseline_kpi = {"execution_time": 10.5, "cost": 25.0}
    candidate_kpi = {"execution_time": 9.5, "cost": 30.0}
    
    # Single delta
    delta_result = calculate_delta(baseline_kpi, candidate_kpi, "execution_time")
    
    # All deltas for a model
    config = load_config()
    model_deltas = calculate_all_deltas(baseline_kpi, candidate_kpi, config)
"""

import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass


# ============================================================================
# DELTA RESULT DATA STRUCTURE
# ============================================================================

@dataclass
class DeltaResult:
    """
    Structured result for a single delta calculation.
    
    Attributes:
        delta: Percentage change (or None if baseline=0 or error)
        direction: "+" for improvement, "-" for regression, "N/A" for special cases
        status: One of "success", "baseline_zero", "new_model", "removed_model", "error"
        annotation: Optional message (e.g., "⚠ data drift detected")
    """
    delta: Optional[float]
    direction: str
    status: str
    annotation: Optional[str] = None


# ============================================================================
# CORE DELTA CALCULATION FUNCTIONS
# ============================================================================

def calculate_delta(
    baseline_value: Optional[float],
    candidate_value: Optional[float],
    metric_name: str = "unknown",
    logger: Optional[logging.Logger] = None
) -> Tuple[Optional[float], str]:
    """
    Calculate percentage change delta between baseline and candidate values.
    
    Formula: ((candidate - baseline) / baseline) × 100
    
    Handles edge cases:
    - Null/missing values: returns (None, "null_value")
    - Zero baseline: returns (None, "N/A - baseline zero")
    - Type errors: returns (None, "error: <message>")
    
    Args:
        baseline_value (Optional[float]): Baseline metric value
        candidate_value (Optional[float]): Candidate/current metric value
        metric_name (str): Name of the metric for logging
        logger (Optional[logging.Logger]): Logger for warnings/errors
    
    Returns:
        Tuple[Optional[float], str]: (delta_value_or_none, status_message)
            - delta_value: Percentage change as float, or None if error
            - status_message: "success", "N/A - baseline zero", "null_value", or error description
    
    Examples:
        >>> delta, status = calculate_delta(10.0, 15.0, "execution_time")
        >>> print(f"{delta}% ({status})")
        50.0% (success)
        
        >>> delta, status = calculate_delta(100.0, 50.0, "execution_time")
        >>> print(f"{delta}% ({status})")
        -50.0% (success)
    """
    # Handle null/missing values
    if baseline_value is None or candidate_value is None:
        if logger:
            logger.debug(f"Metric '{metric_name}' has null value(s): baseline={baseline_value}, candidate={candidate_value}")
        return None, "null_value"
    
    # Handle zero baseline
    if baseline_value == 0:
        if logger:
            logger.warning(f"Metric '{metric_name}' delta skipped due to zero baseline")
        return None, "N/A - baseline zero"
    
    # Calculate delta using formula: ((candidate - baseline) / baseline) × 100
    try:
        delta = ((candidate_value - baseline_value) / baseline_value) * 100
        return round(delta, 2), "success"
    except (TypeError, ValueError, ZeroDivisionError) as e:
        if logger:
            logger.error(f"Error calculating delta for '{metric_name}': {str(e)}")
        return None, f"error: {str(e)}"


def determine_direction(
    delta: Optional[float],
    metric_name: str,
    improvement_on_reduction: Optional[list] = None
) -> str:
    """
    Determine direction indicator (+ for improvement, - for regression).
    
    Direction logic depends on metric type:
    - For metrics where lower is better (improvement_on_reduction):
      - Negative delta = improvement (marked with +)
      - Positive delta = regression (marked with -)
    - For other metrics where higher is better:
      - Positive delta = improvement (marked with +)
      - Negative delta = regression (marked with -)
    
    Args:
        delta (Optional[float]): Calculated delta percentage
        metric_name (str): Name of the metric
        improvement_on_reduction (Optional[list]): List of metrics where lower is better
    
    Returns:
        str: "+" for improvement, "-" for regression, "N/A" if delta is None
    
    Examples:
        >>> determine_direction(-10.5, "execution_time", ["execution_time", "cost"])
        '+'
        >>> determine_direction(5.0, "row_count", ["execution_time", "cost"])
        '-'
        >>> determine_direction(None, "unknown")
        'N/A'
    """
    if delta is None:
        return "N/A"
    
    if improvement_on_reduction is None:
        improvement_on_reduction = []
    
    # Determine if this metric improves on reduction
    metric_improves_on_reduction = metric_name in improvement_on_reduction
    
    if metric_improves_on_reduction:
        # Lower is better: negative delta = improvement (+), positive delta = regression (-)
        return "+" if delta < 0 else "-"
    else:
        # Higher is better: positive delta = improvement (+), negative delta = regression (-)
        return "+" if delta > 0 else "-"


def create_delta_result(
    delta: Optional[float],
    status: str,
    metric_name: str,
    improvement_on_reduction: Optional[list] = None,
    data_drift_detected: bool = False
) -> DeltaResult:
    """
    Create a structured DeltaResult object with all relevant information.
    
    Args:
        delta (Optional[float]): Calculated delta percentage
        status (str): Status indicator ("success", "baseline_zero", etc.)
        metric_name (str): Name of the metric
        improvement_on_reduction (Optional[list]): Metrics where lower is better
        data_drift_detected (bool): Whether data drift was detected
    
    Returns:
        DeltaResult: Structured result object
    
    Examples:
        >>> result = create_delta_result(-10.5, "success", "execution_time")
        >>> print(f"{result.delta}% ({result.direction})")
        -10.5% (+)
    """
    # Determine direction if delta exists and status is success
    if delta is not None and status == "success":
        direction = determine_direction(delta, metric_name, improvement_on_reduction)
    else:
        direction = "N/A"
    
    # Create annotation for special cases
    annotation = None
    if data_drift_detected:
        annotation = "⚠ data drift detected"
    elif status != "success":
        annotation = f"Status: {status}"
    
    return DeltaResult(
        delta=delta,
        direction=direction,
        status=status,
        annotation=annotation
    )


# ============================================================================
# KPI DELTA CALCULATION
# ============================================================================

def calculate_all_deltas(
    baseline_kpis: Dict[str, Any],
    candidate_kpis: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    check_data_hash: bool = True,
    logger: Optional[logging.Logger] = None
) -> Dict[str, DeltaResult]:
    """
    Calculate deltas for all KPIs comparing baseline vs candidate.
    
    Processes each KPI metric and handles edge cases:
    - Zero baselines (returns null delta with reason)
    - Data hash mismatches (flags as data drift)
    - Missing values (logs and skips gracefully)
    - Both single-metric and multi-metric KPIs
    
    Args:
        baseline_kpis (Dict[str, Any]): Baseline KPI dictionary with metric values
        candidate_kpis (Dict[str, Any]): Candidate KPI dictionary with metric values
        config (Optional[Dict[str, Any]]): Configuration with KPI definitions
        check_data_hash (bool): Check data_hash for mismatch and flag data drift
        logger (Optional[logging.Logger]): Logger for progress and edge cases
    
    Returns:
        Dict[str, DeltaResult]: {metric_name: DeltaResult} for all metrics processed
    
    Examples:
        >>> baseline = {"execution_time": 10.0, "cost": 25.0}
        >>> candidate = {"execution_time": 9.0, "cost": 30.0}
        >>> config = {"kpi_definitions": {...}}
        >>> deltas = calculate_all_deltas(baseline, candidate, config)
        >>> for kpi, result in deltas.items():
        ...     print(f"{kpi}: {result.delta}% {result.direction}")
    """
    deltas = {}
    
    # Define metrics where lower is better
    improvement_on_reduction = [
        "execution_time",
        "cost",
        "bytes_scanned",
        "credits_consumed",
        "estimated_cost_usd",
        "join_count",
        "cte_count",
        "window_function_count"
    ]
    
    # Check for data drift (hash mismatch)
    data_drift = False
    if check_data_hash:
        baseline_hash = baseline_kpis.get("data_hash")
        candidate_hash = candidate_kpis.get("data_hash")
        if baseline_hash and candidate_hash and baseline_hash != candidate_hash:
            data_drift = True
            if logger:
                logger.warning(
                    f"Data drift detected: hash mismatch "
                    f"(baseline: {str(baseline_hash)[:8]}..., "
                    f"candidate: {str(candidate_hash)[:8]}...)"
                )
    
    # Get KPI definitions from config
    if config is None:
        config = {}
    kpi_definitions = config.get("kpi_definitions", {})
    
    # If no config provided, use default metric names for calculation
    if not kpi_definitions:
        # Use all keys in baseline/candidate that look like metrics
        metric_names = set()
        metric_names.update(baseline_kpis.keys())
        metric_names.update(candidate_kpis.keys())
        
        # Skip special fields
        skip_fields = {"data_hash", "timestamp", "model_name", "query_hash"}
        for metric_name in sorted(metric_names - skip_fields):
            baseline_val = baseline_kpis.get(metric_name)
            candidate_val = candidate_kpis.get(metric_name)
            
            if isinstance(baseline_val, (int, float)) and isinstance(candidate_val, (int, float)):
                delta, status = calculate_delta(
                    baseline_val, candidate_val, metric_name, logger=logger
                )
                deltas[metric_name] = create_delta_result(
                    delta, status, metric_name, improvement_on_reduction, data_drift
                )
    else:
        # Calculate deltas for each KPI defined in config
        for kpi_name, kpi_def in kpi_definitions.items():
            if logger:
                logger.debug(f"Calculating delta for KPI: {kpi_name}")
            
            # Get metric keys for this KPI
            metric_key = kpi_def.get("metric_key")
            metric_keys = kpi_def.get("metric_keys", [])
            
            # Handle single-metric KPIs
            if metric_key:
                baseline_val = baseline_kpis.get(metric_key)
                candidate_val = candidate_kpis.get(metric_key)
                
                if isinstance(baseline_val, (int, float)) and isinstance(candidate_val, (int, float)):
                    delta, status = calculate_delta(
                        baseline_val, candidate_val, kpi_name, logger=logger
                    )
                    deltas[metric_key] = create_delta_result(
                        delta, status, kpi_name, improvement_on_reduction, data_drift
                    )
                else:
                    if logger:
                        logger.debug(
                            f"Skipping {kpi_name}: invalid types or missing values "
                            f"(baseline={type(baseline_val).__name__}, "
                            f"candidate={type(candidate_val).__name__})"
                        )
            
            # Handle multi-metric KPIs
            for mkey in metric_keys:
                baseline_val = baseline_kpis.get(mkey)
                candidate_val = candidate_kpis.get(mkey)
                
                if isinstance(baseline_val, (int, float)) and isinstance(candidate_val, (int, float)):
                    delta, status = calculate_delta(
                        baseline_val, candidate_val, mkey, logger=logger
                    )
                    deltas[mkey] = create_delta_result(
                        delta, status, mkey, improvement_on_reduction, data_drift
                    )
                else:
                    if logger:
                        logger.debug(
                            f"Skipping {mkey}: invalid types or missing values "
                            f"(baseline={type(baseline_val).__name__}, "
                            f"candidate={type(candidate_val).__name__})"
                        )
    
    return deltas


# ============================================================================
# MODEL DELTA CALCULATION
# ============================================================================

def calculate_model_deltas(
    baseline_models: Dict[str, Dict[str, Any]],
    candidate_models: Dict[str, Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Calculate deltas for all models, handling new/removed models.
    
    Processes models and identifies:
    - New models (in candidate only): marked with status="new_model", delta=null
    - Removed models (in baseline only): marked with status="removed_model", delta=null
    - Models in both: normal delta calculation for all KPIs
    
    Args:
        baseline_models (Dict[str, Dict]): {model_name: {metric: value}}
        candidate_models (Dict[str, Dict]): {model_name: {metric: value}}
        config (Optional[Dict[str, Any]]): Configuration
        logger (Optional[logging.Logger]): Logger for progress tracking
    
    Returns:
        Dict[str, Dict[str, Any]]: {model_name: {kpi: DeltaResult or status}}
            Format allows for both normal DeltaResult objects and special status fields
    
    Examples:
        >>> baseline = {"model_a": {"execution_time": 10.0}}
        >>> candidate = {"model_a": {"execution_time": 9.0}, "model_b": {"execution_time": 5.0}}
        >>> deltas = calculate_model_deltas(baseline, candidate)
        >>> print(deltas["model_a"]["execution_time"].delta)  # -10.0
        >>> print(deltas["model_b"]["_status"])  # new_model
    """
    result = {}
    
    # Get all model names (union of baseline and candidate models)
    all_models = set(baseline_models.keys()) | set(candidate_models.keys())
    
    if logger:
        logger.info(
            f"Processing {len(all_models)} models "
            f"(baseline: {len(baseline_models)}, candidate: {len(candidate_models)})"
        )
    
    for model_name in sorted(all_models):
        baseline_kpis = baseline_models.get(model_name, {})
        candidate_kpis = candidate_models.get(model_name, {})
        
        # Check if model is new, removed, or in both
        if model_name not in baseline_models:
            # New model
            if logger:
                logger.info(f"New model detected: {model_name}")
            result[model_name] = {"_status": "new_model"}
        
        elif model_name not in candidate_models:
            # Removed model
            if logger:
                logger.info(f"Removed model detected: {model_name}")
            result[model_name] = {"_status": "removed_model"}
        
        else:
            # Calculate deltas for models in both baseline and candidate
            if logger:
                logger.debug(f"Calculating deltas for model: {model_name}")
            
            model_deltas = calculate_all_deltas(
                baseline_kpis, candidate_kpis, config, logger=logger
            )
            result[model_name] = model_deltas
    
    return result


# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

def format_delta_output(
    model_deltas: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    Format delta results into a serializable dictionary for JSON output.
    
    Converts DeltaResult dataclass objects to dictionaries for JSON serialization.
    Preserves special status fields like "_status" for new/removed models.
    
    Args:
        model_deltas (Dict[str, Dict]): Raw delta result objects from calculate_model_deltas
    
    Returns:
        Dict[str, Dict[str, Any]]: Serializable format suitable for JSON export
    
    Structure example:
        {
            "model_a": {
                "execution_time": {
                    "delta": -10.5,
                    "direction": "+",
                    "status": "success",
                    "annotation": null
                },
                "cost": {...}
            },
            "model_b": {
                "_status": "new_model"
            }
        }
    
    Examples:
        >>> result = calculate_model_deltas(...)
        >>> formatted = format_delta_output(result)
        >>> json.dumps(formatted)  # Valid JSON output
    """
    formatted = {}
    
    for model_name, deltas in model_deltas.items():
        formatted[model_name] = {}
        
        for kpi_name, result in deltas.items():
            if isinstance(result, DeltaResult):
                # Convert DeltaResult dataclass to dictionary
                formatted[model_name][kpi_name] = {
                    "delta": result.delta,
                    "direction": result.direction,
                    "status": result.status,
                    "annotation": result.annotation
                }
            else:
                # Preserve non-DeltaResult values (like "_status" field)
                formatted[model_name][kpi_name] = result
    
    return formatted


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_improvement_metrics() -> list:
    """
    Get list of metrics where improvement is achieved by reduction (lower values).
    
    Returns:
        list: Metric names where lower is better
    """
    return [
        "execution_time",
        "cost",
        "bytes_scanned",
        "credits_consumed",
        "estimated_cost_usd",
        "join_count",
        "cte_count",
        "window_function_count"
    ]


def summarize_deltas(
    model_deltas: Dict[str, Dict[str, DeltaResult]],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Create a summary of delta statistics across all models and KPIs.
    
    Computes min, max, mean, and count of deltas to provide high-level overview.
    
    Args:
        model_deltas (Dict[str, Dict]): Results from calculate_model_deltas
        logger (Optional[logging.Logger]): Logger for summary output
    
    Returns:
        Dict[str, Any]: Summary statistics with improvements and regressions
    """
    improvements = []
    regressions = []
    errors = []
    
    for model_name, deltas in model_deltas.items():
        for kpi_name, result in deltas.items():
            if isinstance(result, DeltaResult):
                if result.status == "success" and result.delta is not None:
                    if result.direction == "+":
                        improvements.append((model_name, kpi_name, result.delta))
                    elif result.direction == "-":
                        regressions.append((model_name, kpi_name, result.delta))
                elif result.status != "success":
                    errors.append((model_name, kpi_name, result.status))
    
    summary = {
        "total_models": len(model_deltas),
        "improvements": len(improvements),
        "regressions": len(regressions),
        "errors": len(errors),
        "total_metrics_processed": len(improvements) + len(regressions) + len(errors)
    }
    
    if improvements:
        improvement_values = [v for _, _, v in improvements]
        summary["improvement_avg_delta"] = round(sum(improvement_values) / len(improvement_values), 2)
        summary["improvement_best"] = round(min(improvement_values), 2)
    
    if regressions:
        regression_values = [v for _, _, v in regressions]
        summary["regression_avg_delta"] = round(sum(regression_values) / len(regression_values), 2)
        summary["regression_worst"] = round(max(regression_values), 2)
    
    if logger:
        logger.info(f"Delta Summary: {improvements_count} improvements, {regressions_count} regressions, {errors_count} errors")
    
    return summary
