#!/usr/bin/env python3
"""
Configuration Module for Benchmarking System

This module defines KPI thresholds, cost models, bottleneck detection rules, and
optimization targets. It serves as the central configuration point for the benchmarking
system with full environment variable override support.

Usage:
    from config import KPI_DEFINITIONS, BOTTLENECK_THRESHOLDS, SNOWFLAKE_PRICING
    from config import load_config, calculate_credits
    
    # Access configuration with environment overrides
    config = load_config()
    
    # Calculate Snowflake credits from bytes scanned
    credits = calculate_credits(bytes_scanned=1099511627776)  # 1 TB
    cost = credits * config['pricing']['standard']['cost_per_credit']
"""

import os
from pathlib import Path
from typing import Dict, Any, List


# ============================================================================
# SCHEMA CONFIGURATION
# ============================================================================
# Configuration for JSON Schema validation and file paths

SCHEMA_CONFIG = {
    "report": {
        "schema_file": "benchmark/schemas/report.json.schema",
        "description": "JSON Schema for report.json output (Draft 7)",
        "version": "1.0.0",
        "examples": [
            "benchmark/schemas/example-report.json"
        ]
    }
}


def get_schema_file_path(schema_type: str = "report") -> Path:
    """
    Get the absolute path to a schema file.
    
    Args:
        schema_type (str): Type of schema ("report" or other)
    
    Returns:
        Path: Absolute path to the schema file
    
    Raises:
        FileNotFoundError: If schema file does not exist
    """
    if schema_type not in SCHEMA_CONFIG:
        raise ValueError(f"Unknown schema type: {schema_type}")
    
    schema_path = Path(SCHEMA_CONFIG[schema_type]["schema_file"])
    
    # If relative path, resolve from project root
    if not schema_path.is_absolute():
        # Try to find project root
        current = Path.cwd()
        for parent in [current] + list(current.parents):
            if (parent / "dbt_project.yml").exists():
                schema_path = parent / schema_path
                break
    
    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_path}\n"
            f"Expected location: {schema_path.absolute()}"
        )
    
    return schema_path


# ============================================================================
# KPI DEFINITIONS
# ============================================================================
# Defines each of the 5 KPIs with name, description, units, acceptable ranges,
# and weight/importance in overall analysis.

KPI_DEFINITIONS = {
    "execution_time": {
        "name": "Execution Time",
        "description": "Total query execution time in seconds",
        "units": "seconds",
        "metric_key": "query_execution_time_seconds",
        "weight": 0.30,
        "baseline_expectation": "Current production baseline",
        "acceptable_range": {
            "min": 0,
            "max": None
        }
    },
    "work_metrics": {
        "name": "Work Metrics",
        "description": "Amount of work performed: rows returned and bytes scanned",
        "units": "rows (count), bytes (count)",
        "metric_keys": ["row_count", "bytes_scanned"],
        "weight": 0.25,
        "baseline_expectation": "Rows: consistent output size, Bytes: lower is better",
        "acceptable_range": {
            "row_count": {"min": 0, "max": None},
            "bytes_scanned": {"min": 0, "max": None}
        }
    },
    "data_equivalence": {
        "name": "Data Equivalence",
        "description": "Output validation via SHA256 hash for equivalence checking",
        "units": "SHA256 hash (string)",
        "metric_key": "data_hash",
        "weight": 0.20,
        "baseline_expectation": "Must match baseline hash exactly",
        "acceptable_range": {
            "match_required": True,
            "mismatch_severity": "CRITICAL"
        }
    },
    "query_complexity": {
        "name": "Query Complexity",
        "description": "SQL complexity metrics: number of JOINs, CTEs, and window functions",
        "units": "count (numeric)",
        "metric_keys": ["join_count", "cte_count", "window_function_count"],
        "weight": 0.15,
        "baseline_expectation": "Lower complexity is preferable for maintainability",
        "acceptable_range": {
            "join_count": {"min": 0, "max": None},
            "cte_count": {"min": 0, "max": None},
            "window_function_count": {"min": 0, "max": None}
        }
    },
    "cost_estimation": {
        "name": "Cost Estimation",
        "description": "Snowflake credits consumed and estimated dollar cost",
        "units": "credits (count), USD (decimal)",
        "metric_keys": ["credits_consumed", "estimated_cost_usd"],
        "weight": 0.10,
        "baseline_expectation": "Lower cost is better; cost regression >20% is significant",
        "acceptable_range": {
            "credits_consumed": {"min": 0, "max": None},
            "estimated_cost_usd": {"min": 0, "max": None}
        }
    }
}


# ============================================================================
# BOTTLENECK DETECTION THRESHOLDS
# ============================================================================
# Thresholds for detecting regressions and performance issues

BOTTLENECK_THRESHOLDS = {
    "execution_time": {
        "regression_threshold_percent": 10,
        "description": "Flag execution time increase >10%",
        "severity": "HIGH"
    },
    "cost": {
        "regression_threshold_percent": 20,
        "description": "Flag cost increase >20%",
        "severity": "MEDIUM"
    },
    "data_equivalence": {
        "mismatch_flag": True,
        "description": "Flag any data hash mismatch (critical correctness issue)",
        "severity": "CRITICAL"
    }
}


# ============================================================================
# SNOWFLAKE PRICING MODELS
# ============================================================================
# Pricing configuration for cost calculation. Formula: credits = bytes_scanned / (1024³) / 10

SNOWFLAKE_PRICING = {
    "standard": {
        "edition": "Standard Edition",
        "cost_per_credit": 2.0,
        "cost_per_credit_min": 2.0,
        "cost_per_credit_max": 3.0,
        "compute_cost_percentage": 0.75,
        "storage_cost_percentage": 0.25
    },
    "enterprise": {
        "edition": "Enterprise Edition",
        "cost_per_credit": 3.0,
        "cost_per_credit_min": 3.0,
        "cost_per_credit_max": 4.0,
        "compute_cost_percentage": 0.75,
        "storage_cost_percentage": 0.25
    },
    "credit_calculation": {
        "formula": "credits = bytes_scanned / (1024^3) / 10",
        "bytes_per_gb": 1024 ** 3,
        "gb_per_credit": 10
    }
}


# ============================================================================
# COMPARISON THRESHOLDS
# ============================================================================
# Thresholds for baseline vs candidate comparison alerts
# These determine when performance changes are significant enough to notify

COMPARISON_THRESHOLDS = {
    "execution_time": {
        "improvement_threshold_percent": 5,
        "regression_threshold_percent": 10,
        "description": "Alert if execution time improves by >=5% or regresses by >=10%",
        "alert_on_improvement": True,
        "alert_on_regression": True,
        "regression_severity": "HIGH",
        "improvement_severity": "INFO"
    },
    "bytes_scanned": {
        "improvement_threshold_percent": 10,
        "regression_threshold_percent": 15,
        "description": "Alert if bytes scanned improves by >=10% or regresses by >=15%",
        "alert_on_improvement": True,
        "alert_on_regression": True,
        "regression_severity": "MEDIUM",
        "improvement_severity": "INFO"
    },
    "cost": {
        "improvement_threshold_percent": 5,
        "regression_threshold_percent": 20,
        "description": "Alert if cost improves by >=5% or regresses by >=20%",
        "alert_on_improvement": True,
        "alert_on_regression": True,
        "regression_severity": "MEDIUM",
        "improvement_severity": "INFO"
    },
    "complexity": {
        "improvement_threshold_percent": 10,
        "regression_threshold_percent": 25,
        "description": "Alert if query complexity improves by >=10% or regresses by >=25%",
        "alert_on_improvement": False,
        "alert_on_regression": True,
        "regression_severity": "LOW",
        "improvement_severity": "INFO"
    },
    "data_equivalence": {
        "require_hash_match": True,
        "description": "CRITICAL: Baseline and candidate must produce identical output hashes",
        "alert_on_mismatch": True,
        "mismatch_severity": "CRITICAL"
    },
    "global": {
        "min_sample_size": 1,
        "description": "Minimum number of models required for statistical significance",
        "confidence_level": 0.95
    }
}


# ============================================================================
# IMPROVEMENT TARGETS
# ============================================================================
# Goals and targets for optimization efforts

IMPROVEMENT_TARGETS = {
    "execution_time": {
        "target_reduction_percent": 5,
        "description": "Reduce query execution time by at least 5%",
        "method": "Query optimization, better indexing, materialized views"
    },
    "cost": {
        "target_reduction_percent": 5,
        "description": "Reduce Snowflake credit consumption by at least 5%",
        "method": "Reduce bytes scanned, eliminate redundant computations"
    },
    "complexity": {
        "target_reduction_percent": 10,
        "description": "Simplify query structure to improve maintainability",
        "method": "Consolidate JOINs, materialize CTEs, pre-aggregate window functions"
    }
}


# ============================================================================
# OPTIMIZATION RULES
# ============================================================================
# Rule definitions for detecting optimization opportunities and generating recommendations

OPTIMIZATION_RULES = [
    {
        "rule_id": "HIGH_JOIN_COUNT",
        "name": "High JOIN Count Detection",
        "description": "Detects when a query has too many JOINs which may impact performance",
        "metric": "join_count",
        "threshold": 5,
        "comparison_operator": "greater_than",
        "severity": "MEDIUM",
        "recommendation": "Consider consolidating JOINs or breaking into multiple queries. More than 5 JOINs can impact query optimization and readability.",
        "action_items": [
            "Identify redundant JOINs",
            "Consider temporary tables or materialized views",
            "Review JOIN order and conditions",
            "Profile query execution plan"
        ],
        "optimization_technique": "JOIN Consolidation & Materialization",
        "sql_pattern_suggestion": [
            "Create materialized view for JOIN result",
            "Use temporary table to pre-compute JOIN result",
            "Consider denormalization for frequently-joined tables"
        ],
        "rationale": "Multiple JOINs increase query complexity and prevent optimizer from finding optimal execution paths. Materializing JOIN results can reduce repeated computation."
    },
    {
        "rule_id": "HIGH_CTE_COUNT",
        "name": "High CTE Count Detection",
        "description": "Detects when a query has too many CTEs which may benefit from materialization",
        "metric": "cte_count",
        "threshold": 3,
        "comparison_operator": "greater_than",
        "severity": "LOW",
        "recommendation": "Consider materializing CTEs as temporary tables or views. More than 3 CTEs can increase recomputation overhead.",
        "action_items": [
            "Identify frequently-used CTEs",
            "Create materialized views for expensive CTEs",
            "Consider dynamic materialization during pipeline",
            "Monitor CTE computation time"
        ],
        "optimization_technique": "CTE Materialization",
        "sql_pattern_suggestion": [
            "Convert expensive CTEs to temporary tables: CREATE TEMP TABLE cte_name AS SELECT ... (from CTE definition)",
            "Use materialized views for reusable CTEs",
            "Consider inlining CTEs if used only once"
        ],
        "rationale": "Multiple CTEs can be re-evaluated multiple times. Materializing them prevents recomputation and can improve performance significantly."
    },
    {
        "rule_id": "HIGH_WINDOW_FUNCTION_COUNT",
        "name": "High Window Function Count Detection",
        "description": "Detects when a query uses many window functions which may benefit from pre-aggregation",
        "metric": "window_function_count",
        "threshold": 2,
        "comparison_operator": "greater_than",
        "severity": "LOW",
        "recommendation": "Consider pre-aggregating or materializing window function results. Multiple window functions over same data can be expensive.",
        "action_items": [
            "Identify overlapping window function partitions",
            "Create pre-aggregated tables for common calculations",
            "Use staging tables for intermediate window results",
            "Consider consolidating window functions"
        ],
        "optimization_technique": "Window Function Pre-aggregation",
        "sql_pattern_suggestion": [
            "Create staging table with pre-computed window function results",
            "Use PARTITION BY optimization to reduce data scanned",
            "Consider row_number() + filtering instead of complex window logic"
        ],
        "rationale": "Multiple window functions can cause full table scans. Pre-computing results in staging tables reduces computation overhead."
    },
    {
        "rule_id": "HIGH_EXECUTION_TIME",
        "name": "High Execution Time Regression",
        "description": "Detects when execution time increases significantly compared to baseline",
        "metric": "execution_time",
        "threshold": 10,
        "comparison_operator": "percent_greater_than",
        "severity": "HIGH",
        "recommendation": "Execution time regression detected. Investigate query changes, data volume changes, or system load.",
        "action_items": [
            "Compare query plans (baseline vs current)",
            "Check for data volume changes",
            "Review recent model/SQL changes",
            "Monitor warehouse performance metrics",
            "Consider query result caching"
        ],
        "optimization_technique": "Query Rewrite & Indexing Strategy",
        "sql_pattern_suggestion": [
            "Add clustering keys to improve JOIN performance",
            "Use search optimization on frequently filtered columns",
            "Consider materialized views for expensive subqueries"
        ],
        "rationale": "Execution time regressions indicate potential query inefficiencies. Rewriting with better access patterns and indexing can recover performance."
    },
    {
        "rule_id": "HIGH_COST_REGRESSION",
        "name": "High Cost Regression",
        "description": "Detects when cost increases significantly compared to baseline",
        "metric": "cost",
        "threshold": 20,
        "comparison_operator": "percent_greater_than",
        "severity": "MEDIUM",
        "recommendation": "Cost regression detected. Likely due to increased bytes scanned or complexity.",
        "action_items": [
            "Identify increase in bytes scanned",
            "Review join conditions and filters",
            "Consider clustering and sort keys",
            "Evaluate partition pruning efficiency"
        ],
        "optimization_technique": "Materialization & Partitioning Strategy",
        "sql_pattern_suggestion": [
            "Apply partitioning strategy: ALTER TABLE ... CLUSTER BY (partition_column)",
            "Create materialized view for filtered dataset to reduce bytes scanned",
            "Add WHERE clause filters earlier in query logic"
        ],
        "rationale": "High cost indicates excessive bytes scanned. Materialization and partitioning strategies reduce the data scope and lower overall query cost."
    }
]


# ============================================================================
# ENVIRONMENT VARIABLE LOADING
# ============================================================================
# Functions to load configuration from environment variables with fallback to defaults

def load_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables with fallback to defaults.
    
    Supports overriding any numeric configuration value via environment variables
    using the pattern: BENCHMARK_<SECTION>_<KEY>
    
    Examples:
        BENCHMARK_TIME_REGRESSION_THRESHOLD=15  (overrides 10%)
        BENCHMARK_COST_REGRESSION_THRESHOLD=25  (overrides 20%)
        BENCHMARK_STANDARD_COST_PER_CREDIT=2.5  (overrides $2.0)
        BENCHMARK_ENTERPRISE_COST_PER_CREDIT=3.5 (overrides $3.0)
    
    Returns:
        Dict[str, Any]: Configuration dictionary with environment variable overrides applied
    """
    config = {
        "kpi_definitions": KPI_DEFINITIONS,
        "bottleneck_thresholds": BOTTLENECK_THRESHOLDS,
        "comparison_thresholds": COMPARISON_THRESHOLDS,
        "pricing": SNOWFLAKE_PRICING,
        "improvement_targets": IMPROVEMENT_TARGETS,
        "optimization_rules": OPTIMIZATION_RULES,
    }
    
    # Override execution time regression threshold
    time_threshold = os.getenv('BENCHMARK_TIME_REGRESSION_THRESHOLD')
    if time_threshold:
        try:
            config['bottleneck_thresholds']['execution_time']['regression_threshold_percent'] = float(time_threshold)
        except ValueError:
            pass
    
    # Override cost regression threshold
    cost_threshold = os.getenv('BENCHMARK_COST_REGRESSION_THRESHOLD')
    if cost_threshold:
        try:
            config['bottleneck_thresholds']['cost']['regression_threshold_percent'] = float(cost_threshold)
        except ValueError:
            pass
    
    # Override Standard Edition cost per credit
    standard_cost = os.getenv('BENCHMARK_STANDARD_COST_PER_CREDIT')
    if standard_cost:
        try:
            config['pricing']['standard']['cost_per_credit'] = float(standard_cost)
        except ValueError:
            pass
    
    # Override Enterprise Edition cost per credit
    enterprise_cost = os.getenv('BENCHMARK_ENTERPRISE_COST_PER_CREDIT')
    if enterprise_cost:
        try:
            config['pricing']['enterprise']['cost_per_credit'] = float(enterprise_cost)
        except ValueError:
            pass
    
    # Override execution time improvement target
    time_target = os.getenv('BENCHMARK_TIME_IMPROVEMENT_TARGET')
    if time_target:
        try:
            config['improvement_targets']['execution_time']['target_reduction_percent'] = float(time_target)
        except ValueError:
            pass
    
    # Override cost improvement target
    cost_target = os.getenv('BENCHMARK_COST_IMPROVEMENT_TARGET')
    if cost_target:
        try:
            config['improvement_targets']['cost']['target_reduction_percent'] = float(cost_target)
        except ValueError:
            pass
    
    # Override JOIN count threshold
    join_threshold = os.getenv('BENCHMARK_JOIN_THRESHOLD')
    if join_threshold:
        try:
            threshold_val = int(join_threshold)
            for rule in config['optimization_rules']:
                if rule['rule_id'] == 'HIGH_JOIN_COUNT':
                    rule['threshold'] = threshold_val
        except ValueError:
            pass
    
    # Override CTE count threshold
    cte_threshold = os.getenv('BENCHMARK_CTE_THRESHOLD')
    if cte_threshold:
        try:
            threshold_val = int(cte_threshold)
            for rule in config['optimization_rules']:
                if rule['rule_id'] == 'HIGH_CTE_COUNT':
                    rule['threshold'] = threshold_val
        except ValueError:
            pass
    
    # Override window function count threshold
    window_threshold = os.getenv('BENCHMARK_WINDOW_FUNCTION_THRESHOLD')
    if window_threshold:
        try:
            threshold_val = int(window_threshold)
            for rule in config['optimization_rules']:
                if rule['rule_id'] == 'HIGH_WINDOW_FUNCTION_COUNT':
                    rule['threshold'] = threshold_val
        except ValueError:
            pass
    
    return config


def calculate_credits(bytes_scanned: int, edition: str = "standard") -> float:
    """
    Calculate Snowflake credits consumed based on bytes scanned.
    
    Formula: credits = bytes_scanned / (1024^3) / 10
    
    This formula represents:
    - 1 credit = 10 GB of data scanned
    - More precisely: 1024^3 bytes = 1 GB
    - So: 1 credit per (10 * 1024^3) bytes scanned
    
    Args:
        bytes_scanned (int): Total bytes scanned in the query
        edition (str): Snowflake edition ("standard" or "enterprise")
    
    Returns:
        float: Number of credits consumed
    
    Examples:
        >>> calculate_credits(1099511627776)  # 1 TB
        100.0
        >>> calculate_credits(107374182400)   # 100 GB
        10.0
    """
    bytes_per_credit = SNOWFLAKE_PRICING['credit_calculation']['bytes_per_gb'] * \
                       SNOWFLAKE_PRICING['credit_calculation']['gb_per_credit']
    credits = bytes_scanned / bytes_per_credit
    return round(credits, 4)


def calculate_cost(credits: float, edition: str = "standard") -> float:
    """
    Calculate estimated cost in USD based on credits and pricing edition.
    
    Args:
        credits (float): Number of credits consumed
        edition (str): Snowflake edition ("standard" or "enterprise")
    
    Returns:
        float: Estimated cost in USD
    
    Examples:
        >>> calculate_cost(100, "standard")  # 100 credits at $2/credit
        200.0
        >>> calculate_cost(100, "enterprise")  # 100 credits at $3/credit
        300.0
    """
    if edition not in SNOWFLAKE_PRICING:
        edition = "standard"
    
    cost_per_credit = SNOWFLAKE_PRICING[edition]['cost_per_credit']
    return round(credits * cost_per_credit, 2)


def get_optimization_recommendations(
    metrics: Dict[str, Any],
    config: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """
    Generate optimization recommendations based on metrics and rules.
    
    Args:
        metrics (Dict[str, Any]): Dictionary containing metric values
        config (Dict[str, Any]): Configuration dictionary (uses default if not provided)
    
    Returns:
        List[Dict[str, Any]]: List of triggered optimization rules with recommendations
    
    Examples:
        >>> metrics = {"join_count": 7, "cte_count": 4, "window_function_count": 3}
        >>> recommendations = get_optimization_recommendations(metrics)
        >>> for rec in recommendations:
        ...     print(f"{rec['name']}: {rec['recommendation']}")
    """
    if config is None:
        config = load_config()
    
    triggered_rules = []
    
    for rule in config['optimization_rules']:
        metric_name = rule.get('metric')
        threshold = rule.get('threshold')
        operator = rule.get('comparison_operator', 'greater_than')
        
        if metric_name not in metrics:
            continue
        
        metric_value = metrics[metric_name]
        is_triggered = False
        
        if operator == 'greater_than' and metric_value > threshold:
            is_triggered = True
        elif operator == 'percent_greater_than' and metric_value > threshold:
            is_triggered = True
        
        if is_triggered:
            triggered_rules.append({
                'rule_id': rule['rule_id'],
                'name': rule['name'],
                'metric': metric_name,
                'current_value': metric_value,
                'threshold': threshold,
                'severity': rule['severity'],
                'recommendation': rule['recommendation'],
                'action_items': rule['action_items']
            })
    
    return triggered_rules


if __name__ == '__main__':
    # Test configuration loading
    config = load_config()
    print("✓ Configuration loaded successfully")
    print(f"  - KPI Definitions: {len(config['kpi_definitions'])} KPIs defined")
    print(f"  - Bottleneck Thresholds: Time >10%, Cost >20%")
    print(f"  - Pricing: Standard ${config['pricing']['standard']['cost_per_credit']}, Enterprise ${config['pricing']['enterprise']['cost_per_credit']}")
    print(f"  - Optimization Rules: {len(config['optimization_rules'])} rules")
    
    # Test credit calculation
    test_bytes = 1099511627776  # 1 TB
    test_credits = calculate_credits(test_bytes)
    test_cost = calculate_cost(test_credits, "standard")
    print(f"\n✓ Credit Calculation Test")
    print(f"  - Input: {test_bytes:,} bytes (1 TB)")
    print(f"  - Credits: {test_credits}")
    print(f"  - Cost: ${test_cost}")
    
    # Test optimization recommendations
    test_metrics = {
        "join_count": 7,
        "cte_count": 4,
        "window_function_count": 3
    }
    recommendations = get_optimization_recommendations(test_metrics, config)
    print(f"\n✓ Optimization Recommendations ({len(recommendations)} triggered)")
    for rec in recommendations:
        print(f"  - {rec['name']}: {rec['recommendation'][:70]}...")
