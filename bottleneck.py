#!/usr/bin/env python3
"""
Bottleneck Detection and Model Classification Module

This module identifies performance regressions, data equivalence issues, and generates
impact rankings for prioritized optimization recommendations. It provides:

- Regression threshold detection (>10% execution time, >20% cost)
- Data drift detection (SHA256 mismatch)
- Model categorization (improved/regressed/neutral per KPI)
- Impact scoring (weighted: 40% execution_time, 40% cost, 20% data_drift)
- Bottleneck ranking and top-N summary generation
- Comprehensive logging of threshold crossings and ranking decisions

Usage:
    from bottleneck import (
        detect_bottlenecks,
        categorize_model_kpis,
        calculate_impact_score,
        generate_bottleneck_summary
    )
    from config import load_config
    
    # Detect bottlenecks in model deltas
    config = load_config()
    bottlenecks = detect_bottlenecks(model_deltas, config)
    
    # Generate top 10 bottleneck models by impact
    summary = generate_bottleneck_summary(bottlenecks, top_n=10)
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from delta import DeltaResult


# ============================================================================
# BOTTLENECK RESULT DATA STRUCTURES
# ============================================================================

@dataclass
class KPICategorization:
    """
    Categorization result for a single KPI.
    
    Attributes:
        metric_name: Name of the KPI/metric
        category: "improved", "regressed", or "neutral"
        delta: Percentage change value
        is_regression: True if crosses regression threshold
        regression_amount: If regressed, the absolute delta amount
    """
    metric_name: str
    category: str  # "improved", "regressed", or "neutral"
    delta: Optional[float]
    is_regression: bool = False
    regression_amount: Optional[float] = None


@dataclass
class BottleneckResult:
    """
    Complete bottleneck analysis result for a model.
    
    Attributes:
        model_name: Name of the model
        impact_score: Weighted impact score (0-100)
        kpi_categorizations: Dict of KPI categorizations
        regression_flags: List of regression types flagged
        data_drift_detected: Whether data drift was detected
        regression_amounts: Dict of regression metric amounts
        severity: "CRITICAL", "HIGH", "MEDIUM", or "LOW"
    """
    model_name: str
    impact_score: float
    kpi_categorizations: Dict[str, KPICategorization] = field(default_factory=dict)
    regression_flags: List[str] = field(default_factory=list)
    data_drift_detected: bool = False
    regression_amounts: Dict[str, float] = field(default_factory=dict)
    severity: str = "LOW"


# ============================================================================
# THRESHOLD CHECKING FUNCTIONS
# ============================================================================

def check_execution_time_regression(
    delta: Optional[float],
    threshold_percent: float = 10.0,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Check if execution time delta exceeds regression threshold.
    
    A regression is flagged when execution time increases by more than the threshold.
    For metrics where lower is better, a positive delta indicates regression.
    
    Args:
        delta (Optional[float]): Percentage change in execution time
        threshold_percent (float): Threshold percentage (default: 10%)
        logger (Optional[logging.Logger]): Logger for decision logging
    
    Returns:
        bool: True if execution time regression detected
    
    Examples:
        >>> check_execution_time_regression(15.0)  # 15% increase
        True
        >>> check_execution_time_regression(5.0)   # 5% increase
        False
        >>> check_execution_time_regression(-10.0) # 10% improvement
        False
    """
    if delta is None:
        return False
    
    # Positive delta = execution time increased (regression for lower-is-better metric)
    is_regression = delta > threshold_percent
    
    if is_regression and logger:
        logger.warning(
            f"Execution time regression detected: {delta:.2f}% > {threshold_percent}% threshold"
        )
    
    return is_regression


def check_cost_regression(
    delta: Optional[float],
    threshold_percent: float = 20.0,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Check if cost delta exceeds regression threshold.
    
    A regression is flagged when cost increases by more than the threshold.
    For metrics where lower is better, a positive delta indicates regression.
    
    Args:
        delta (Optional[float]): Percentage change in cost
        threshold_percent (float): Threshold percentage (default: 20%)
        logger (Optional[logging.Logger]): Logger for decision logging
    
    Returns:
        bool: True if cost regression detected
    
    Examples:
        >>> check_cost_regression(25.0)  # 25% increase
        True
        >>> check_cost_regression(15.0)  # 15% increase
        False
        >>> check_cost_regression(-5.0)  # 5% improvement
        False
    """
    if delta is None:
        return False
    
    # Positive delta = cost increased (regression for lower-is-better metric)
    is_regression = delta > threshold_percent
    
    if is_regression and logger:
        logger.warning(
            f"Cost regression detected: {delta:.2f}% > {threshold_percent}% threshold"
        )
    
    return is_regression


def check_data_drift(
    delta_result: Optional[DeltaResult],
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Check if data drift is indicated by delta result annotation.
    
    Data drift is detected when the delta result contains data drift annotation
    from hash mismatch detection.
    
    Args:
        delta_result (Optional[DeltaResult]): Delta result object
        logger (Optional[logging.Logger]): Logger for decision logging
    
    Returns:
        bool: True if data drift detected
    """
    if delta_result is None:
        return False
    
    has_drift = delta_result.annotation and "data drift" in delta_result.annotation
    
    if has_drift and logger:
        logger.warning(f"Data drift detected: {delta_result.annotation}")
    
    return has_drift


# ============================================================================
# KPI CATEGORIZATION FUNCTIONS
# ============================================================================

def categorize_kpi(
    metric_name: str,
    delta_result: Optional[DeltaResult],
    logger: Optional[logging.Logger] = None
) -> KPICategorization:
    """
    Categorize a single KPI as improved, regressed, or neutral.
    
    Categorization is based on the delta direction:
    - Improvement: delta direction is "+" (improvement for this metric)
    - Regression: delta direction is "-" (regression for this metric)
    - Neutral: delta is very small (< 0.5%) or N/A
    
    Args:
        metric_name (str): Name of the metric/KPI
        delta_result (Optional[DeltaResult]): Delta calculation result
        logger (Optional[logging.Logger]): Logger for categorization details
    
    Returns:
        KPICategorization: Categorization result
    """
    if delta_result is None or delta_result.delta is None:
        return KPICategorization(
            metric_name=metric_name,
            category="neutral",
            delta=None
        )
    
    delta = delta_result.delta
    direction = delta_result.direction
    
    # Small changes (<0.5%) are considered neutral
    if abs(delta) < 0.5:
        return KPICategorization(
            metric_name=metric_name,
            category="neutral",
            delta=delta
        )
    
    # Categorize based on direction indicator from delta calculation
    if direction == "+":
        category = "improved"
    elif direction == "-":
        category = "regressed"
    else:  # "N/A"
        category = "neutral"
    
    categorization = KPICategorization(
        metric_name=metric_name,
        category=category,
        delta=delta
    )
    
    if logger:
        logger.debug(f"KPI {metric_name}: {category} (delta={delta:.2f}%, direction={direction})")
    
    return categorization


def categorize_model_kpis(
    model_name: str,
    model_deltas: Dict[str, DeltaResult],
    logger: Optional[logging.Logger] = None
) -> Dict[str, KPICategorization]:
    """
    Categorize all KPIs for a model.
    
    Args:
        model_name (str): Name of the model
        model_deltas (Dict[str, DeltaResult]): Delta results for all KPIs
        logger (Optional[logging.Logger]): Logger for categorization
    
    Returns:
        Dict[str, KPICategorization]: Categorization for each KPI
    
    Examples:
        >>> model_deltas = {
        ...     "execution_time": DeltaResult(delta=-10.0, direction="+", ...),
        ...     "cost": DeltaResult(delta=25.0, direction="-", ...)
        ... }
        >>> categorizations = categorize_model_kpis("model_a", model_deltas)
        >>> print(categorizations["execution_time"].category)  # "improved"
        >>> print(categorizations["cost"].category)  # "regressed"
    """
    categorizations = {}
    
    if logger:
        logger.debug(f"Categorizing {len(model_deltas)} KPIs for model: {model_name}")
    
    for metric_name, delta_result in model_deltas.items():
        # Skip special fields
        if metric_name.startswith("_"):
            continue
        
        categorizations[metric_name] = categorize_kpi(metric_name, delta_result, logger)
    
    return categorizations


# ============================================================================
# IMPACT SCORING FUNCTIONS
# ============================================================================

def calculate_impact_score(
    execution_time_delta: Optional[float],
    cost_delta: Optional[float],
    data_drift_present: bool = False,
    weights: Optional[Dict[str, float]] = None,
    logger: Optional[logging.Logger] = None
) -> float:
    """
    Calculate weighted impact score for a model's bottleneck severity.
    
    Impact score combines three factors with configurable weights:
    - Execution time delta (weight: 40%, default)
    - Cost delta (weight: 40%, default)
    - Data drift presence (weight: 20%, default)
    
    Score is normalized to 0-100 scale. Only considers regressions
    (positive deltas for lower-is-better metrics).
    
    Args:
        execution_time_delta (Optional[float]): Percentage change in execution time
        cost_delta (Optional[float]): Percentage change in cost
        data_drift_present (bool): Whether data drift was detected
        weights (Optional[Dict[str, float]]): Custom weights
            Default: {"execution_time": 0.4, "cost": 0.4, "data_drift": 0.2}
        logger (Optional[logging.Logger]): Logger for scoring details
    
    Returns:
        float: Impact score (0-100)
    
    Examples:
        >>> score = calculate_impact_score(15.0, 25.0, False)
        >>> print(f"Impact: {score:.2f}")
        Impact: 20.00
        
        >>> score = calculate_impact_score(15.0, 25.0, True)  # With data drift
        >>> print(f"Impact: {score:.2f}")
        Impact: 25.00
    """
    if weights is None:
        weights = {
            "execution_time": 0.4,
            "cost": 0.4,
            "data_drift": 0.2
        }
    
    # Start with base score of 0
    score = 0.0
    
    # Execution time contribution (only positive deltas = regressions)
    if execution_time_delta is not None and execution_time_delta > 0:
        # Cap at 100% to avoid oversized contributions
        exec_contribution = min(execution_time_delta / 100.0, 1.0) * weights["execution_time"]
        score += exec_contribution
        if logger:
            logger.debug(f"Execution time contribution: {exec_contribution:.4f} (delta={execution_time_delta:.2f}%)")
    
    # Cost contribution (only positive deltas = regressions)
    if cost_delta is not None and cost_delta > 0:
        # Cap at 100% to avoid oversized contributions
        cost_contribution = min(cost_delta / 100.0, 1.0) * weights["cost"]
        score += cost_contribution
        if logger:
            logger.debug(f"Cost contribution: {cost_contribution:.4f} (delta={cost_delta:.2f}%)")
    
    # Data drift contribution (binary: present or not)
    if data_drift_present:
        drift_contribution = weights["data_drift"]
        score += drift_contribution
        if logger:
            logger.debug(f"Data drift contribution: {drift_contribution:.4f}")
    
    # Normalize to 0-100 scale
    normalized_score = score * 100
    
    if logger:
        logger.debug(f"Impact score: {normalized_score:.2f} (raw={score:.4f})")
    
    return round(normalized_score, 2)


# ============================================================================
# BOTTLENECK DETECTION FUNCTIONS
# ============================================================================

def detect_bottlenecks(
    model_deltas: Dict[str, Dict[str, DeltaResult]],
    config: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, BottleneckResult]:
    """
    Detect bottleneck models from delta results.
    
    Analyzes all models and identifies bottlenecks based on:
    - Execution time regression (>10% by default)
    - Cost regression (>20% by default)
    - Data drift detection (SHA256 mismatch)
    
    For each model, calculates impact score and categorizes all KPIs.
    Flags models with critical issues for prioritized attention.
    
    Args:
        model_deltas (Dict[str, Dict]): Model delta results from calculate_model_deltas
        config (Optional[Dict[str, Any]]): Configuration with thresholds
        logger (Optional[logging.Logger]): Logger for detection details
    
    Returns:
        Dict[str, BottleneckResult]: Bottleneck results indexed by model name
            Only includes models that are neither new nor removed
    
    Examples:
        >>> model_deltas = calculate_model_deltas(baseline_models, candidate_models)
        >>> config = load_config()
        >>> bottlenecks = detect_bottlenecks(model_deltas, config)
        >>> for model, result in bottlenecks.items():
        ...     if result.regression_flags:
        ...         print(f"{model}: {result.impact_score:.2f}")
    """
    if config is None:
        config = {}
    
    thresholds = config.get("bottleneck_thresholds", {})
    exec_time_threshold = thresholds.get("execution_time", {}).get("regression_threshold_percent", 10.0)
    cost_threshold = thresholds.get("cost", {}).get("regression_threshold_percent", 20.0)
    
    bottlenecks = {}
    
    if logger:
        logger.info(
            f"Detecting bottlenecks across {len(model_deltas)} models "
            f"(execution_time_threshold={exec_time_threshold}%, cost_threshold={cost_threshold}%)"
        )
    
    for model_name, model_kpis in model_deltas.items():
        # Skip new/removed models
        if "_status" in model_kpis:
            if logger:
                logger.debug(f"Skipping {model_name}: {model_kpis.get('_status')}")
            continue
        
        if logger:
            logger.debug(f"Analyzing model: {model_name}")
        
        # Categorize all KPIs
        categorizations = categorize_model_kpis(model_name, model_kpis, logger)
        
        # Extract key metrics for impact scoring
        exec_time_result = model_kpis.get("execution_time")
        cost_result = model_kpis.get("cost") or model_kpis.get("estimated_cost_usd")
        
        exec_time_delta = exec_time_result.delta if exec_time_result else None
        cost_delta = cost_result.delta if cost_result else None
        
        # Check for regressions
        exec_time_regression = check_execution_time_regression(
            exec_time_delta, exec_time_threshold, logger
        )
        cost_regression = check_cost_regression(
            cost_delta, cost_threshold, logger
        )
        
        # Check for data drift
        data_drift = False
        for delta_result in model_kpis.values():
            if check_data_drift(delta_result, logger):
                data_drift = True
                break
        
        # Calculate impact score
        impact_score = calculate_impact_score(
            exec_time_delta, cost_delta, data_drift, logger=logger
        )
        
        # Build regression flags
        regression_flags = []
        regression_amounts = {}
        
        if exec_time_regression:
            regression_flags.append("EXECUTION_TIME_REGRESSION")
            regression_amounts["execution_time"] = abs(exec_time_delta) if exec_time_delta else 0
        
        if cost_regression:
            regression_flags.append("COST_REGRESSION")
            regression_amounts["cost"] = abs(cost_delta) if cost_delta else 0
        
        if data_drift:
            regression_flags.append("DATA_DRIFT")
        
        # Determine severity
        if "EXECUTION_TIME_REGRESSION" in regression_flags or data_drift:
            severity = "CRITICAL" if data_drift else "HIGH"
        elif "COST_REGRESSION" in regression_flags:
            severity = "MEDIUM"
        else:
            severity = "LOW"
        
        # Create bottleneck result
        bottleneck = BottleneckResult(
            model_name=model_name,
            impact_score=impact_score,
            kpi_categorizations=categorizations,
            regression_flags=regression_flags,
            data_drift_detected=data_drift,
            regression_amounts=regression_amounts,
            severity=severity
        )
        
        bottlenecks[model_name] = bottleneck
        
        if logger and (exec_time_regression or cost_regression or data_drift):
            flags_str = ", ".join(regression_flags) if regression_flags else "none"
            logger.warning(
                f"Model {model_name} bottleneck detected: "
                f"score={impact_score:.2f}, severity={severity}, flags=[{flags_str}]"
            )
    
    return bottlenecks


# ============================================================================
# BOTTLENECK RANKING AND SUMMARIZATION
# ============================================================================

def rank_bottlenecks_by_impact(
    bottlenecks: Dict[str, BottleneckResult],
    logger: Optional[logging.Logger] = None
) -> List[BottleneckResult]:
    """
    Rank bottleneck models by impact score (descending).
    
    Args:
        bottlenecks (Dict[str, BottleneckResult]): Bottleneck results
        logger (Optional[logging.Logger]): Logger for ranking details
    
    Returns:
        List[BottleneckResult]: Sorted list by impact score (highest first)
    """
    ranked = sorted(
        bottlenecks.values(),
        key=lambda b: b.impact_score,
        reverse=True
    )
    
    if logger:
        logger.info(f"Ranked {len(ranked)} bottleneck models by impact score")
        for i, bottleneck in enumerate(ranked[:5], 1):
            logger.info(
                f"  {i}. {bottleneck.model_name}: {bottleneck.impact_score:.2f} "
                f"({bottleneck.severity})"
            )
    
    return ranked


def generate_bottleneck_summary(
    bottlenecks: Dict[str, BottleneckResult],
    top_n: int = 10,
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Generate top N bottleneck summary with categorization and scores.
    
    Produces a ranked summary suitable for consumption by optimization
    recommendation engine, ordered by impact score.
    
    Args:
        bottlenecks (Dict[str, BottleneckResult]): Bottleneck detection results
        top_n (int): Number of top bottlenecks to include (default: 10)
        logger (Optional[logging.Logger]): Logger for summary generation
    
    Returns:
        List[Dict[str, Any]]: Top N bottleneck summaries with structure:
            {
                "model_name": str,
                "impact_score": float,
                "severity": str,
                "regression_flags": List[str],
                "regression_amounts": Dict[str, float],
                "data_drift_detected": bool,
                "kpi_categorizations": {
                    "metric_name": {
                        "category": str,
                        "delta": float
                    },
                    ...
                }
            }
    
    Examples:
        >>> bottlenecks = detect_bottlenecks(model_deltas, config)
        >>> summary = generate_bottleneck_summary(bottlenecks, top_n=10)
        >>> for item in summary:
        ...     print(f"{item['model_name']}: {item['impact_score']:.2f}")
    """
    # Rank by impact score
    ranked = rank_bottlenecks_by_impact(bottlenecks, logger)
    
    # Take top N
    top_bottlenecks = ranked[:top_n]
    
    # Format for output
    summary = []
    for bottleneck in top_bottlenecks:
        # Format categorizations
        categorizations = {}
        for metric_name, cat in bottleneck.kpi_categorizations.items():
            categorizations[metric_name] = {
                "category": cat.category,
                "delta": cat.delta
            }
        
        summary_item = {
            "model_name": bottleneck.model_name,
            "impact_score": bottleneck.impact_score,
            "severity": bottleneck.severity,
            "regression_flags": bottleneck.regression_flags,
            "regression_amounts": bottleneck.regression_amounts,
            "data_drift_detected": bottleneck.data_drift_detected,
            "kpi_categorizations": categorizations
        }
        summary.append(summary_item)
    
    if logger:
        logger.info(f"Generated bottleneck summary for top {len(summary)} models")
    
    return summary


def format_bottleneck_output(
    bottlenecks: Dict[str, BottleneckResult],
    summary: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Format complete bottleneck detection output.
    
    Combines detailed bottleneck results with top-N summary for JSON export.
    
    Args:
        bottlenecks (Dict[str, BottleneckResult]): Full bottleneck results
        summary (List[Dict[str, Any]]): Top-N summary
    
    Returns:
        Dict[str, Any]: Formatted output suitable for JSON serialization
    
    Structure:
        {
            "total_models_analyzed": int,
            "models_with_bottlenecks": int,
            "critical_bottlenecks": List[str],
            "summary": List[Dict],  # Top-N models
            "all_bottlenecks": Dict[str, Dict]  # Detailed results
        }
    """
    # Count bottleneck types
    critical_models = [
        name for name, b in bottlenecks.items()
        if b.severity == "CRITICAL"
    ]
    
    # Format all bottleneck details
    all_bottlenecks = {}
    for model_name, bottleneck in bottlenecks.items():
        categorizations = {}
        for metric_name, cat in bottleneck.kpi_categorizations.items():
            categorizations[metric_name] = {
                "category": cat.category,
                "delta": cat.delta
            }
        
        all_bottlenecks[model_name] = {
            "impact_score": bottleneck.impact_score,
            "severity": bottleneck.severity,
            "regression_flags": bottleneck.regression_flags,
            "regression_amounts": bottleneck.regression_amounts,
            "data_drift_detected": bottleneck.data_drift_detected,
            "kpi_categorizations": categorizations
        }
    
    return {
        "total_models_analyzed": len(bottlenecks),
        "models_with_bottlenecks": len(bottlenecks),
        "critical_bottlenecks": critical_models,
        "summary": summary,
        "all_bottlenecks": all_bottlenecks
    }
