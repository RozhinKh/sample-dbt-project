#!/usr/bin/env python3
"""
Recommendation Engine for SQL Optimization

This module generates rule-based optimization recommendations for bottleneck models
based on query complexity metrics and performance deltas from baseline vs candidate
comparison.

Core Features:
- Rule-based recommendation generation from bottleneck detection results
- Priority calculation based on impact score × complexity metric
- Model-specific SQL pattern suggestions
- Rationale for each recommendation explaining the benefit
- Structured output compatible with JSON serialization

Usage:
    from recommendation import generate_recommendations, generate_recommendations_for_model
    from bottleneck import detect_bottlenecks
    from config import load_config
    
    config = load_config()
    bottlenecks = detect_bottlenecks(model_deltas, config)
    complexity_metrics = {"model_a": {"join_count": 7, ...}, ...}
    
    # Generate recommendations for all bottleneck models
    recommendations = generate_recommendations(bottlenecks, complexity_metrics, config)
    
    # Or for a single model
    model_recs = generate_recommendations_for_model(
        "model_a", bottlenecks["model_a"], complexity_metrics["model_a"], config
    )
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from bottleneck import BottleneckResult


# ============================================================================
# RECOMMENDATION DATA STRUCTURE
# ============================================================================

@dataclass
class Recommendation:
    """
    A single optimization recommendation for a model.
    
    Attributes:
        model_name: Name of the model receiving recommendation
        rule_id: Rule identifier that triggered this recommendation
        rule_name: Human-readable name of the rule
        priority: Priority level ("HIGH", "MEDIUM", "LOW")
        priority_score: Numerical score 0-100 for ranking
        optimization_technique: Specific optimization approach
        sql_pattern_suggestion: List of SQL patterns or strategies
        rationale: Explanation of why this recommendation helps
        impact_score: Weighted impact score from bottleneck detection
        complexity_metric: The metric that triggered (join_count, etc)
        complexity_value: Current value of the metric
        threshold_value: Threshold for the rule
    """
    model_name: str
    rule_id: str
    rule_name: str
    priority: str
    priority_score: float
    optimization_technique: str
    sql_pattern_suggestion: List[str] = field(default_factory=list)
    rationale: str = ""
    impact_score: float = 0.0
    complexity_metric: Optional[str] = None
    complexity_value: Optional[float] = None
    threshold_value: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert recommendation to JSON-serializable dictionary."""
        return asdict(self)


# ============================================================================
# RECOMMENDATION ENGINE FUNCTIONS
# ============================================================================

def calculate_priority_score(
    impact_score: float,
    complexity_value: float,
    rule_threshold: float,
    cost_regression: Optional[float] = None,
    logger: Optional[logging.Logger] = None
) -> float:
    """
    Calculate numerical priority score based on impact and complexity metrics.
    
    Priority score formula:
    - Base: (impact_score / 100) × (complexity_value / rule_threshold) × 100
    - Cost adjustment: If cost_regression > 20%, add 25 points
    - Result: Capped at 100
    
    Args:
        impact_score (float): Bottleneck impact score (0-100)
        complexity_value (float): Current complexity metric value
        rule_threshold (float): Threshold for the rule
        cost_regression (Optional[float]): Cost regression percentage
        logger (Optional[logging.Logger]): Logger for scoring details
    
    Returns:
        float: Priority score 0-100
    
    Examples:
        >>> score = calculate_priority_score(80.0, 7, 5)
        >>> print(f"Score: {score:.2f}")
        Score: 112.00
    """
    if rule_threshold <= 0:
        return 0.0
    
    # Base score: impact normalized × complexity ratio
    complexity_ratio = complexity_value / rule_threshold
    base_score = (impact_score / 100.0) * complexity_ratio * 100.0
    
    # Cost adjustment: high cost regression always gets boost
    adjusted_score = base_score
    if cost_regression is not None and cost_regression > 20.0:
        adjusted_score = base_score + 25.0
        if logger:
            logger.debug(f"Cost regression boost (+25): {base_score:.2f} → {adjusted_score:.2f}")
    
    # Cap at 100
    final_score = min(adjusted_score, 100.0)
    
    if logger:
        logger.debug(f"Priority score: {final_score:.2f} (impact={impact_score:.2f}, complexity_ratio={complexity_ratio:.2f})")
    
    return round(final_score, 2)


def get_priority_level(priority_score: float, cost_regression: Optional[float] = None) -> str:
    """
    Determine priority level from numerical score.
    
    Priority determination:
    - HIGH: score > 66 OR cost_regression > 20%
    - MEDIUM: score 33-66
    - LOW: score < 33
    
    Args:
        priority_score (float): Numerical priority score (0-100)
        cost_regression (Optional[float]): Cost regression percentage
    
    Returns:
        str: Priority level ("HIGH", "MEDIUM", or "LOW")
    
    Examples:
        >>> get_priority_level(75.0)
        'HIGH'
        >>> get_priority_level(50.0)
        'MEDIUM'
        >>> get_priority_level(20.0, 25.0)
        'HIGH'
    """
    # Cost regression >20% always HIGH priority
    if cost_regression is not None and cost_regression > 20.0:
        return "HIGH"
    
    # Score-based determination
    if priority_score > 66.0:
        return "HIGH"
    elif priority_score >= 33.0:
        return "MEDIUM"
    else:
        return "LOW"


def find_matching_rules(
    complexity_metrics: Dict[str, float],
    config: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> List[Dict[str, Any]]:
    """
    Find all optimization rules that are triggered by complexity metrics.
    
    Args:
        complexity_metrics (Dict[str, float]): Model metrics (join_count, cte_count, etc)
        config (Dict[str, Any]): Configuration with optimization_rules
        logger (Optional[logging.Logger]): Logger for rule matching
    
    Returns:
        List[Dict[str, Any]]: List of triggered rules with full details
    
    Examples:
        >>> metrics = {"join_count": 7, "cte_count": 4}
        >>> config = load_config()
        >>> rules = find_matching_rules(metrics, config)
        >>> print(len(rules))  # Number of triggered rules
        2
    """
    triggered_rules = []
    optimization_rules = config.get("optimization_rules", [])
    
    for rule in optimization_rules:
        metric_name = rule.get("metric")
        threshold = rule.get("threshold")
        operator = rule.get("comparison_operator", "greater_than")
        
        if metric_name not in complexity_metrics:
            continue
        
        metric_value = complexity_metrics[metric_name]
        
        # Check if rule is triggered
        is_triggered = False
        if operator == "greater_than" and metric_value > threshold:
            is_triggered = True
        elif operator == "percent_greater_than" and metric_value > threshold:
            is_triggered = True
        
        if is_triggered:
            triggered_rules.append(rule)
            if logger:
                logger.debug(f"Rule triggered: {rule['rule_id']} ({metric_name}={metric_value} > {threshold})")
    
    return triggered_rules


def generate_recommendations_for_model(
    model_name: str,
    bottleneck_result: BottleneckResult,
    complexity_metrics: Dict[str, float],
    config: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> List[Recommendation]:
    """
    Generate recommendations for a single bottleneck model.
    
    Evaluates the model against all optimization rules in config:
    1. Find rules triggered by complexity metrics
    2. Calculate priority based on impact_score × complexity ratio
    3. Create recommendation object for each triggered rule
    4. Sort by priority score (highest first)
    
    Args:
        model_name (str): Name of the model
        bottleneck_result (BottleneckResult): Bottleneck detection result
        complexity_metrics (Dict[str, float]): Model's complexity metrics
        config (Dict[str, Any]): Configuration with rules and templates
        logger (Optional[logging.Logger]): Logger for recommendation generation
    
    Returns:
        List[Recommendation]: List of recommendations sorted by priority
    
    Examples:
        >>> bottleneck = BottleneckResult(model_name="model_a", impact_score=75.0, ...)
        >>> metrics = {"join_count": 7, "cte_count": 4}
        >>> recs = generate_recommendations_for_model("model_a", bottleneck, metrics, config)
        >>> print(f"Generated {len(recs)} recommendations")
    """
    recommendations = []
    
    if logger:
        logger.info(f"Generating recommendations for model: {model_name}")
    
    # Find all triggered rules for this model
    triggered_rules = find_matching_rules(complexity_metrics, config, logger)
    
    if not triggered_rules:
        if logger:
            logger.debug(f"No optimization rules triggered for {model_name}")
        return recommendations
    
    # Create recommendation for each triggered rule
    for rule in triggered_rules:
        metric_name = rule.get("metric")
        metric_value = complexity_metrics.get(metric_name, 0)
        threshold = rule.get("threshold", 1)
        
        # Calculate priority
        priority_score = calculate_priority_score(
            bottleneck_result.impact_score,
            metric_value,
            threshold,
            bottleneck_result.regression_amounts.get("cost"),
            logger
        )
        
        # Determine priority level
        priority_level = get_priority_level(
            priority_score,
            bottleneck_result.regression_amounts.get("cost")
        )
        
        # Create recommendation
        recommendation = Recommendation(
            model_name=model_name,
            rule_id=rule.get("rule_id"),
            rule_name=rule.get("name"),
            priority=priority_level,
            priority_score=priority_score,
            optimization_technique=rule.get("optimization_technique", ""),
            sql_pattern_suggestion=rule.get("sql_pattern_suggestion", []),
            rationale=rule.get("rationale", ""),
            impact_score=bottleneck_result.impact_score,
            complexity_metric=metric_name,
            complexity_value=metric_value,
            threshold_value=threshold
        )
        
        recommendations.append(recommendation)
        
        if logger:
            logger.debug(
                f"Created recommendation: {rule['rule_id']} "
                f"(priority={priority_level}, score={priority_score:.2f})"
            )
    
    # Sort by priority score (highest first)
    recommendations.sort(key=lambda r: r.priority_score, reverse=True)
    
    return recommendations


def generate_recommendations(
    bottlenecks: Dict[str, BottleneckResult],
    complexity_metrics: Dict[str, Dict[str, float]],
    config: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, List[Recommendation]]:
    """
    Generate recommendations for all bottleneck models.
    
    Processes each bottleneck model and generates prioritized recommendations
    based on its complexity metrics and impact score.
    
    Args:
        bottlenecks (Dict[str, BottleneckResult]): Bottleneck detection results
        complexity_metrics (Dict[str, Dict[str, float]]): Complexity metrics per model
        config (Dict[str, Any]): Configuration with rules
        logger (Optional[logging.Logger]): Logger for generation
    
    Returns:
        Dict[str, List[Recommendation]]: {model_name: [recommendations]}
            Models are included only if they have at least one recommendation
    
    Examples:
        >>> bottlenecks = {"model_a": BottleneckResult(...), "model_b": ...}
        >>> complexity = {"model_a": {"join_count": 7}, "model_b": {...}}
        >>> all_recs = generate_recommendations(bottlenecks, complexity, config)
        >>> for model, recs in all_recs.items():
        ...     print(f"{model}: {len(recs)} recommendations")
    """
    all_recommendations = {}
    
    if logger:
        logger.info(
            f"Generating recommendations for {len(bottlenecks)} bottleneck models "
            f"from {len(complexity_metrics)} total models with metrics"
        )
    
    for model_name, bottleneck_result in bottlenecks.items():
        # Get complexity metrics for this model
        model_metrics = complexity_metrics.get(model_name, {})
        
        if not model_metrics:
            if logger:
                logger.warning(f"No complexity metrics found for model: {model_name}")
            continue
        
        # Generate recommendations for this model
        model_recommendations = generate_recommendations_for_model(
            model_name,
            bottleneck_result,
            model_metrics,
            config,
            logger
        )
        
        # Only include models with recommendations
        if model_recommendations:
            all_recommendations[model_name] = model_recommendations
            
            if logger:
                logger.info(
                    f"Generated {len(model_recommendations)} recommendations for {model_name} "
                    f"(impact_score={bottleneck_result.impact_score:.2f})"
                )
    
    return all_recommendations


def rank_recommendations_by_priority(
    recommendations: Dict[str, List[Recommendation]]
) -> List[Recommendation]:
    """
    Flatten and sort all recommendations by priority score.
    
    Combines recommendations from all models and sorts globally by priority.
    This is useful for generating a unified recommendations list.
    
    Args:
        recommendations (Dict[str, List[Recommendation]]): Recommendations per model
    
    Returns:
        List[Recommendation]: All recommendations sorted by priority (highest first)
    
    Examples:
        >>> all_recs = generate_recommendations(bottlenecks, metrics, config)
        >>> ranked = rank_recommendations_by_priority(all_recs)
        >>> for rec in ranked[:10]:  # Top 10 recommendations
        ...     print(f"{rec.model_name}: {rec.priority} priority")
    """
    all_recs = []
    
    for model_recs in recommendations.values():
        all_recs.extend(model_recs)
    
    # Sort by priority score descending, then by rule_id for consistency
    all_recs.sort(key=lambda r: (r.priority_score, r.rule_id), reverse=True)
    
    return all_recs


def generate_recommendation_summary(
    recommendations: Dict[str, List[Recommendation]],
    top_n: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generate a summary of recommendations with statistics.
    
    Args:
        recommendations (Dict[str, List[Recommendation]]): Recommendations per model
        top_n (Optional[int]): If provided, include only top N recommendations
    
    Returns:
        Dict[str, Any]: Summary with counts, breakdown by priority, and top recommendations
    
    Examples:
        >>> recs = generate_recommendations(bottlenecks, metrics, config)
        >>> summary = generate_recommendation_summary(recs, top_n=10)
        >>> print(f"Total: {summary['total_recommendations']}, HIGH: {summary['high_priority_count']}")
    """
    # Count recommendations by priority
    ranked = rank_recommendations_by_priority(recommendations)
    
    high_priority = [r for r in ranked if r.priority == "HIGH"]
    medium_priority = [r for r in ranked if r.priority == "MEDIUM"]
    low_priority = [r for r in ranked if r.priority == "LOW"]
    
    # Select top N if specified
    top_recs = ranked[:top_n] if top_n else ranked
    
    summary = {
        "total_recommendations": len(ranked),
        "models_with_recommendations": len(recommendations),
        "high_priority_count": len(high_priority),
        "medium_priority_count": len(medium_priority),
        "low_priority_count": len(low_priority),
        "priority_breakdown": {
            "HIGH": len(high_priority),
            "MEDIUM": len(medium_priority),
            "LOW": len(low_priority)
        },
        "top_recommendations": [r.to_dict() for r in top_recs]
    }
    
    return summary
