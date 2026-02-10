#!/usr/bin/env python3
"""
Pytest configuration and fixtures for KPI extraction tests.

Provides mock dbt artifacts (manifest.json, run_results.json) and 
helper fixtures for testing KPI extraction functions.
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import setup_logging


# ============================================================================
# FIXTURES FOR DBT ARTIFACTS
# ============================================================================

@pytest.fixture
def sample_manifest():
    """
    Fixture providing a sample dbt manifest.json structure.
    
    Returns a complete manifest with multiple models containing
    various SQL patterns (JOINs, CTEs, window functions).
    """
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.5.0",
            "generated_at": datetime.now().isoformat(),
            "invocation_id": "test-invocation-id"
        },
        "nodes": {
            "model.project.simple_model": {
                "unique_id": "model.project.simple_model",
                "name": "simple_model",
                "path": "models/simple_model.sql",
                "raw_code": "SELECT id, name FROM {{ source('raw', 'users') }}",
                "compiled_code": "SELECT id, name FROM raw.public.users",
                "materialization": "table",
                "tags": ["pipeline_a"],
                "description": "Simple select from source table"
            },
            "model.project.join_model": {
                "unique_id": "model.project.join_model",
                "name": "join_model",
                "path": "models/join_model.sql",
                "raw_code": """
                    WITH user_data AS (
                        SELECT id, name FROM {{ ref('users_base') }}
                    ),
                    order_data AS (
                        SELECT user_id, order_id FROM {{ ref('orders') }}
                    )
                    SELECT u.id, u.name, o.order_id
                    FROM user_data u
                    INNER JOIN order_data o ON u.id = o.user_id
                    LEFT JOIN {{ ref('products') }} p ON o.product_id = p.id
                """,
                "compiled_code": """
                    WITH user_data AS (
                        SELECT id, name FROM users_base
                    ),
                    order_data AS (
                        SELECT user_id, order_id FROM orders
                    )
                    SELECT u.id, u.name, o.order_id
                    FROM user_data u
                    INNER JOIN order_data o ON u.id = o.user_id
                    LEFT JOIN products p ON o.product_id = p.id
                """,
                "materialization": "table",
                "tags": ["pipeline_b"],
                "description": "Model with multiple JOINs and CTEs"
            },
            "model.project.complex_model": {
                "unique_id": "model.project.complex_model",
                "name": "complex_model",
                "path": "models/complex_model.sql",
                "raw_code": """
                    WITH cte1 AS (
                        SELECT * FROM table1
                    ),
                    cte2 AS (
                        SELECT * FROM table2
                    ),
                    cte3 AS (
                        SELECT * FROM table3
                    ),
                    cte4 AS (
                        SELECT * FROM table4
                    )
                    SELECT 
                        a.id,
                        ROW_NUMBER() OVER (PARTITION BY a.category ORDER BY a.date) as rn,
                        RANK() OVER (PARTITION BY a.category ORDER BY a.value DESC) as rank_val,
                        LAG(a.value) OVER (PARTITION BY a.category ORDER BY a.date) as prev_value
                    FROM cte1 a
                    INNER JOIN cte2 b ON a.id = b.id
                    LEFT JOIN cte3 c ON b.id = c.id
                    FULL OUTER JOIN cte4 d ON c.id = d.id
                """,
                "compiled_code": """
                    WITH cte1 AS (
                        SELECT * FROM table1
                    ),
                    cte2 AS (
                        SELECT * FROM table2
                    ),
                    cte3 AS (
                        SELECT * FROM table3
                    ),
                    cte4 AS (
                        SELECT * FROM table4
                    )
                    SELECT 
                        a.id,
                        ROW_NUMBER() OVER (PARTITION BY a.category ORDER BY a.date) as rn,
                        RANK() OVER (PARTITION BY a.category ORDER BY a.value DESC) as rank_val,
                        LAG(a.value) OVER (PARTITION BY a.category ORDER BY a.date) as prev_value
                    FROM cte1 a
                    INNER JOIN cte2 b ON a.id = b.id
                    LEFT JOIN cte3 c ON b.id = c.id
                    FULL OUTER JOIN cte4 d ON c.id = d.id
                """,
                "materialization": "table",
                "tags": ["pipeline_c"],
                "description": "Complex model with many CTEs and window functions"
            },
            "model.project.view_model": {
                "unique_id": "model.project.view_model",
                "name": "view_model",
                "path": "models/view_model.sql",
                "raw_code": "SELECT * FROM {{ ref('some_table') }}",
                "compiled_code": "SELECT * FROM some_table",
                "materialization": "view",
                "tags": ["pipeline_a"],
                "description": "Simple view model"
            },
            "model.project.no_code_model": {
                "unique_id": "model.project.no_code_model",
                "name": "no_code_model",
                "path": "models/no_code_model.sql",
                "materialization": "ephemeral",
                "tags": ["pipeline_b"],
                "description": "Model with no SQL code"
            }
        }
    }


@pytest.fixture
def sample_run_results():
    """
    Fixture providing a sample dbt run_results.json structure.
    
    Returns execution results for multiple models with various
    execution times and row counts.
    """
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
            "dbt_version": "1.5.0",
            "generated_at": datetime.now().isoformat(),
            "invocation_id": "test-invocation-id",
            "env": {},
            "select": "state:modified+",
            "state": "none",
            "args": {}
        },
        "results": [
            {
                "unique_id": "model.project.simple_model",
                "status": "success",
                "execution_time": 1.234,
                "message": None,
                "adapter_response": {
                    "type": "run_sql",
                    "code": "CREATE TABLE simple_model AS ...",
                    "rows_affected": 1000
                },
                "thread_id": "Thread-1",
                "timing": [
                    {"name": "compile", "started_at": "2024-01-01T12:00:00.000Z", "completed_at": "2024-01-01T12:00:00.500Z"},
                    {"name": "execute", "started_at": "2024-01-01T12:00:00.500Z", "completed_at": "2024-01-01T12:00:01.734Z"}
                ]
            },
            {
                "unique_id": "model.project.join_model",
                "status": "success",
                "execution_time": 5.678,
                "message": None,
                "adapter_response": {
                    "type": "run_sql",
                    "code": "CREATE TABLE join_model AS ...",
                    "rows_affected": 5000
                },
                "thread_id": "Thread-2",
                "timing": []
            },
            {
                "unique_id": "model.project.complex_model",
                "status": "success",
                "execution_time": 12.456,
                "message": None,
                "adapter_response": {
                    "type": "run_sql",
                    "code": "CREATE TABLE complex_model AS ...",
                    "rows_affected": 50000
                },
                "thread_id": "Thread-3",
                "timing": []
            },
            {
                "unique_id": "model.project.view_model",
                "status": "success",
                "execution_time": 0.567,
                "message": None,
                "adapter_response": {
                    "type": "run_sql",
                    "code": "CREATE VIEW view_model AS ...",
                    "rows_affected": 0
                },
                "thread_id": "Thread-4",
                "timing": []
            },
            {
                "unique_id": "model.project.no_code_model",
                "status": "skipped",
                "execution_time": 0.0,
                "message": "Ephemeral model",
                "adapter_response": {},
                "thread_id": "Thread-5",
                "timing": []
            }
        ],
        "generated_at": datetime.now().isoformat(),
        "elapsed_time": 20.0
    }


@pytest.fixture
def sample_model_simple():
    """Fixture for a simple model without complex SQL."""
    return {
        "unique_id": "model.project.simple_model",
        "model_name": "simple_model",
        "status": "success",
        "execution_time": 1.234,
        "materialization": "table",
        "tags": ["pipeline_a"],
        "adapter_response": {
            "rows_affected": 1000
        }
    }


@pytest.fixture
def sample_model_complex():
    """Fixture for a complex model with multiple JOINs, CTEs, and window functions."""
    return {
        "unique_id": "model.project.complex_model",
        "model_name": "complex_model",
        "status": "success",
        "execution_time": 12.456,
        "materialization": "table",
        "tags": ["pipeline_c"],
        "adapter_response": {
            "rows_affected": 50000
        }
    }


@pytest.fixture
def sample_model_with_zero_values():
    """Fixture for a model with zero execution time and rows."""
    return {
        "unique_id": "model.project.zero_model",
        "model_name": "zero_model",
        "status": "success",
        "execution_time": 0.0,
        "materialization": "table",
        "tags": ["pipeline_a"],
        "adapter_response": {
            "rows_affected": 0
        }
    }


@pytest.fixture
def sample_model_with_missing_values():
    """Fixture for a model with missing execution time and rows."""
    return {
        "unique_id": "model.project.incomplete_model",
        "model_name": "incomplete_model",
        "status": "success",
        "execution_time": None,
        "materialization": "table",
        "tags": ["pipeline_b"],
        "adapter_response": {}
    }


@pytest.fixture
def sample_model_with_negative_values():
    """Fixture for a model with negative execution time and rows."""
    return {
        "unique_id": "model.project.negative_model",
        "model_name": "negative_model",
        "status": "success",
        "execution_time": -1.5,
        "materialization": "table",
        "tags": ["pipeline_a"],
        "adapter_response": {
            "rows_affected": -100
        }
    }


@pytest.fixture
def sample_model_with_invalid_types():
    """Fixture for a model with invalid data types."""
    return {
        "unique_id": "model.project.invalid_model",
        "model_name": "invalid_model",
        "status": "success",
        "execution_time": "not_a_number",
        "materialization": "table",
        "tags": ["pipeline_a"],
        "adapter_response": {
            "rows_affected": "also_not_a_number"
        }
    }


@pytest.fixture
def sample_model_large_values():
    """Fixture for a model with very large byte counts."""
    return {
        "unique_id": "model.project.large_model",
        "model_name": "large_model",
        "status": "success",
        "execution_time": 60.0,
        "materialization": "table",
        "tags": ["pipeline_c"],
        "adapter_response": {
            "rows_affected": 1000000  # 1 million rows
        }
    }


# ============================================================================
# FIXTURES FOR LOGGING
# ============================================================================

@pytest.fixture
def mock_logger():
    """Fixture providing a mock logger for tests."""
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add a null handler for testing
    handler = logging.NullHandler()
    logger.addHandler(handler)
    
    return logger


# ============================================================================
# FIXTURES FOR PRICING CONFIGURATION
# ============================================================================

@pytest.fixture
def standard_pricing():
    """Fixture for standard edition pricing."""
    return {
        "standard": {
            "edition": "Standard Edition",
            "cost_per_credit": 2.0
        }
    }


@pytest.fixture
def enterprise_pricing():
    """Fixture for enterprise edition pricing."""
    return {
        "enterprise": {
            "edition": "Enterprise Edition",
            "cost_per_credit": 3.0
        }
    }


@pytest.fixture
def snowflake_credit_config():
    """Fixture for Snowflake credit calculation configuration."""
    return {
        "credit_calculation": {
            "bytes_per_gb": 1024 ** 3,
            "gb_per_credit": 10
        }
    }


# ============================================================================
# FIXTURES FOR EDGE CASE DATA
# ============================================================================

@pytest.fixture
def bytes_test_cases():
    """Fixture providing test cases for bytes to credits conversion."""
    return [
        # (bytes, expected_credits)
        (0, 0.0),  # Zero bytes
        (10737418240, 1.0),  # 10 GB = 1 credit
        (107374182400, 10.0),  # 100 GB = 10 credits
        (1099511627776, 100.0),  # 1 TB = 100 credits
        (10995116277760, 1000.0),  # 10 TB = 1000 credits
    ]


@pytest.fixture
def credits_test_cases():
    """Fixture providing test cases for credits to cost conversion."""
    return [
        # (credits, edition, expected_cost)
        (0, "standard", 0.0),
        (1, "standard", 2.0),
        (100, "standard", 200.0),
        (0, "enterprise", 0.0),
        (1, "enterprise", 3.0),
        (100, "enterprise", 300.0),
    ]
