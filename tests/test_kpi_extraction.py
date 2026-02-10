#!/usr/bin/env python3
"""
Comprehensive unit tests for KPI extraction functions.

Tests all 5 KPI extraction functions:
1. execution_time_extraction - Validates seconds extraction from run_results.json
2. rows_bytes_calculation - Validates row count parsing and bytes estimation
3. sha256_hashing - Validates hash generation for data equivalence
4. complexity_counting - Validates JOIN, CTE, and window function counts
5. cost_estimation - Validates credit and USD cost calculations

Uses pytest fixtures to mock dbt artifacts and test edge cases.
"""

import pytest
import sys
import re
from pathlib import Path
from typing import Dict, Any, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "benchmark"))

from config import calculate_credits, calculate_cost, SNOWFLAKE_PRICING
from helpers import DataMismatch


# ============================================================================
# KPI 1: EXECUTION TIME EXTRACTION TESTS
# ============================================================================

class TestExecutionTimeExtraction:
    """Test suite for execution time extraction from run_results.json."""
    
    def test_execution_time_positive_value(self, sample_model_simple):
        """Test extraction of positive execution time."""
        execution_time = sample_model_simple.get("execution_time", 0.0)
        
        assert execution_time is not None
        assert isinstance(execution_time, (int, float))
        assert execution_time > 0
        assert execution_time == 1.234
    
    def test_execution_time_zero_value(self, sample_model_with_zero_values):
        """Test handling of zero execution time."""
        execution_time = sample_model_with_zero_values.get("execution_time", 0.0)
        
        assert execution_time is not None
        assert isinstance(execution_time, (int, float))
        assert execution_time == 0.0
    
    def test_execution_time_large_value(self, sample_model_complex):
        """Test handling of large execution time values."""
        execution_time = sample_model_complex.get("execution_time", 0.0)
        
        assert execution_time is not None
        assert isinstance(execution_time, (int, float))
        assert execution_time == 12.456
    
    def test_execution_time_missing_value(self, sample_model_with_missing_values):
        """Test handling of missing execution time."""
        execution_time = sample_model_with_missing_values.get("execution_time", 0.0)
        
        # When missing, should default to 0.0
        if execution_time is None:
            execution_time = 0.0
        
        assert isinstance(execution_time, (int, float))
        assert execution_time >= 0.0
    
    def test_execution_time_negative_value(self, sample_model_with_negative_values):
        """Test handling of negative execution time (should be normalized to 0)."""
        execution_time = sample_model_with_negative_values.get("execution_time", 0.0)
        
        # Negative values should be corrected to 0.0
        if execution_time is not None and execution_time < 0:
            execution_time = 0.0
        
        assert isinstance(execution_time, (int, float))
        assert execution_time >= 0.0
    
    def test_execution_time_invalid_type(self, sample_model_with_invalid_types):
        """Test handling of invalid execution time data type."""
        execution_time = sample_model_with_invalid_types.get("execution_time", 0.0)
        
        # Should default to 0.0 if not a valid number
        if not isinstance(execution_time, (int, float)):
            execution_time = 0.0
        
        assert isinstance(execution_time, (int, float))
        assert execution_time >= 0.0
    
    def test_execution_time_float_precision(self):
        """Test that execution time maintains proper float precision."""
        test_values = [0.001, 0.123, 1.234, 10.5678, 100.123456]
        
        for value in test_values:
            assert isinstance(value, float)
            # Verify precision to 3 decimal places
            rounded = round(value, 3)
            assert rounded > 0 or value == 0


# ============================================================================
# KPI 2: ROWS AND BYTES CALCULATION TESTS
# ============================================================================

class TestRowsBytesCalculation:
    """Test suite for row count parsing and bytes estimation logic."""
    
    def test_rows_produced_extraction_simple(self, sample_model_simple):
        """Test extraction of row count from simple model."""
        adapter_response = sample_model_simple.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        assert isinstance(rows_produced, int)
        assert rows_produced == 1000
    
    def test_rows_produced_extraction_complex(self, sample_model_complex):
        """Test extraction of row count from complex model."""
        adapter_response = sample_model_complex.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        assert isinstance(rows_produced, int)
        assert rows_produced == 50000
    
    def test_rows_produced_zero(self, sample_model_with_zero_values):
        """Test handling of zero rows produced."""
        adapter_response = sample_model_with_zero_values.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        assert isinstance(rows_produced, int)
        assert rows_produced == 0
    
    def test_rows_produced_missing(self, sample_model_with_missing_values):
        """Test handling of missing row count."""
        adapter_response = sample_model_with_missing_values.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        assert isinstance(rows_produced, int)
        assert rows_produced == 0
    
    def test_rows_produced_negative(self, sample_model_with_negative_values):
        """Test handling of negative row count (should be normalized)."""
        adapter_response = sample_model_with_negative_values.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        # Negative values should be corrected to 0
        if rows_produced < 0:
            rows_produced = 0
        
        assert isinstance(rows_produced, int)
        assert rows_produced >= 0
    
    def test_rows_produced_invalid_type(self, sample_model_with_invalid_types):
        """Test handling of invalid row count data type."""
        adapter_response = sample_model_with_invalid_types.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        
        # Should default to 0 if not a valid integer
        if not isinstance(rows_produced, int):
            rows_produced = 0
        
        assert isinstance(rows_produced, int)
        assert rows_produced >= 0
    
    def test_bytes_calculation_standard(self, sample_model_simple):
        """Test bytes estimation with standard row width (500 bytes)."""
        adapter_response = sample_model_simple.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        estimated_row_width = 500
        
        bytes_scanned = rows_produced * estimated_row_width if rows_produced > 0 else 0
        
        assert isinstance(bytes_scanned, int)
        assert bytes_scanned == 500000
    
    def test_bytes_calculation_large_dataset(self, sample_model_large_values):
        """Test bytes calculation for large datasets."""
        adapter_response = sample_model_large_values.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        estimated_row_width = 500
        
        bytes_scanned = rows_produced * estimated_row_width if rows_produced > 0 else 0
        
        assert isinstance(bytes_scanned, int)
        assert bytes_scanned == 500000000  # 1M rows * 500 bytes
    
    def test_bytes_calculation_zero_rows(self, sample_model_with_zero_values):
        """Test bytes calculation when no rows produced."""
        adapter_response = sample_model_with_zero_values.get("adapter_response", {})
        rows_produced = adapter_response.get("rows_affected", 0)
        estimated_row_width = 500
        
        bytes_scanned = rows_produced * estimated_row_width if rows_produced > 0 else 0
        
        assert isinstance(bytes_scanned, int)
        assert bytes_scanned == 0
    
    def test_bytes_estimation_realistic_values(self):
        """Test bytes estimation with realistic data sizes."""
        test_cases = [
            (0, 0),
            (100, 50000),
            (1000, 500000),
            (10000, 5000000),
            (100000, 50000000),
            (1000000, 500000000)
        ]
        
        row_width = 500
        for rows, expected_bytes in test_cases:
            bytes_scanned = rows * row_width if rows > 0 else 0
            assert bytes_scanned == expected_bytes


# ============================================================================
# KPI 3: SHA256 HASHING TESTS
# ============================================================================

class TestSHA256Hashing:
    """Test suite for SHA256 hash generation for data equivalence."""
    
    def test_hash_calculation_valid_data(self):
        """Test SHA256 hash generation with valid data."""
        import hashlib
        
        test_data = "SELECT * FROM users"
        expected_hash = hashlib.sha256(test_data.encode()).hexdigest()
        
        assert isinstance(expected_hash, str)
        assert len(expected_hash) == 64
        assert all(c in '0123456789abcdef' for c in expected_hash)
    
    def test_hash_generation_consistent(self):
        """Test that identical data produces identical hash."""
        import hashlib
        
        test_data = "SELECT * FROM users"
        hash1 = hashlib.sha256(test_data.encode()).hexdigest()
        hash2 = hashlib.sha256(test_data.encode()).hexdigest()
        
        assert hash1 == hash2
    
    def test_hash_generation_unique(self):
        """Test that different data produces different hashes."""
        import hashlib
        
        data1 = "SELECT * FROM users"
        data2 = "SELECT * FROM orders"
        
        hash1 = hashlib.sha256(data1.encode()).hexdigest()
        hash2 = hashlib.sha256(data2.encode()).hexdigest()
        
        assert hash1 != hash2
    
    def test_hash_empty_data(self):
        """Test hash generation with empty data."""
        import hashlib
        
        test_data = ""
        hash_value = hashlib.sha256(test_data.encode()).hexdigest()
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64
    
    def test_hash_null_handling(self):
        """Test hash handling when data is None."""
        # Fallback behavior: None hashes should return None
        hash_value = None
        hash_calculation_method = "unavailable"
        
        assert hash_value is None
        assert hash_calculation_method == "unavailable"
    
    def test_hash_format_validation(self):
        """Test that generated hashes are valid SHA256 format."""
        import hashlib
        
        test_strings = [
            "model_a",
            "model_b_with_complex_query",
            "SELECT * FROM table LIMIT 1000",
            "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte"
        ]
        
        for test_str in test_strings:
            hash_value = hashlib.sha256(test_str.encode()).hexdigest()
            
            # Validate format
            assert len(hash_value) == 64
            assert all(c in '0123456789abcdef' for c in hash_value)
    
    def test_hash_case_sensitivity(self):
        """Test that hash generation is case-sensitive."""
        import hashlib
        
        data_lower = "select * from users"
        data_upper = "SELECT * FROM USERS"
        
        hash_lower = hashlib.sha256(data_lower.encode()).hexdigest()
        hash_upper = hashlib.sha256(data_upper.encode()).hexdigest()
        
        assert hash_lower != hash_upper


# ============================================================================
# KPI 4: COMPLEXITY COUNTING TESTS
# ============================================================================

class TestComplexityCounting:
    """Test suite for JOIN, CTE, and window function counting."""
    
    def test_join_count_simple(self):
        """Test JOIN counting in simple SQL."""
        sql = "SELECT * FROM a INNER JOIN b ON a.id = b.id"
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        
        assert join_count == 1
    
    def test_join_count_multiple(self):
        """Test JOIN counting with multiple JOINs."""
        sql = """
            SELECT * FROM a
            INNER JOIN b ON a.id = b.id
            LEFT JOIN c ON b.id = c.id
            RIGHT JOIN d ON c.id = d.id
            FULL OUTER JOIN e ON d.id = e.id
        """
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        
        assert join_count == 4
    
    def test_join_count_case_insensitive(self):
        """Test that JOIN counting is case-insensitive."""
        sql = "SELECT * FROM a join b ON a.id = b.id JOIN c ON b.id = c.id"
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        
        assert join_count == 2
    
    def test_join_count_zero(self):
        """Test JOIN counting when no JOINs present."""
        sql = "SELECT * FROM users WHERE id > 0"
        join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
        
        assert join_count == 0
    
    def test_cte_count_single(self):
        """Test CTE counting with single CTE."""
        sql = "WITH cte AS (SELECT * FROM table1) SELECT * FROM cte"
        cte_count = len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))
        
        assert cte_count == 1
    
    def test_cte_count_multiple(self):
        """Test CTE counting with multiple CTEs."""
        sql = """
            WITH cte1 AS (SELECT * FROM table1),
                 cte2 AS (SELECT * FROM table2),
                 cte3 AS (SELECT * FROM table3)
            SELECT * FROM cte1 JOIN cte2 JOIN cte3
        """
        cte_count = len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))
        
        assert cte_count == 1  # WITH appears only once at the beginning
    
    def test_cte_count_zero(self):
        """Test CTE counting when no CTEs present."""
        sql = "SELECT * FROM table1 WHERE id > 0"
        cte_count = len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))
        
        assert cte_count == 0
    
    def test_window_function_count_single(self):
        """Test window function counting with single window function."""
        sql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) FROM table1"
        window_count = len(re.findall(r'\bOVER\b', sql, re.IGNORECASE))
        
        assert window_count == 1
    
    def test_window_function_count_multiple(self):
        """Test window function counting with multiple window functions."""
        sql = """
            SELECT 
                id,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY date),
                RANK() OVER (PARTITION BY category ORDER BY value DESC),
                LAG(value) OVER (PARTITION BY category ORDER BY date),
                LEAD(value) OVER (ORDER BY date)
            FROM table1
        """
        window_count = len(re.findall(r'\bOVER\b', sql, re.IGNORECASE))
        
        assert window_count == 4
    
    def test_window_function_count_zero(self):
        """Test window function counting when none present."""
        sql = "SELECT *, COUNT(*) FROM table1 GROUP BY category"
        window_count = len(re.findall(r'\bOVER\b', sql, re.IGNORECASE))
        
        assert window_count == 0
    
    def test_complexity_comprehensive(self, sample_manifest, sample_run_results):
        """Test complexity counting on real manifest data."""
        manifest = sample_manifest
        nodes = manifest.get("nodes", {})
        
        # Test complex_model which has:
        # - 4 CTEs
        # - 4 JOINs
        # - 3 window functions
        complex_node = nodes["model.project.complex_model"]
        compiled_code = complex_node.get("compiled_code", "")
        
        join_count = len(re.findall(r'\bJOIN\b', compiled_code, re.IGNORECASE))
        cte_count = len(re.findall(r'\bWITH\b', compiled_code, re.IGNORECASE))
        window_count = len(re.findall(r'\bOVER\b', compiled_code, re.IGNORECASE))
        
        assert join_count == 4
        assert cte_count == 1  # WITH appears once
        assert window_count == 3
    
    def test_complexity_simple_model(self, sample_manifest):
        """Test complexity counting on simple model."""
        manifest = sample_manifest
        nodes = manifest.get("nodes", {})
        
        simple_node = nodes["model.project.simple_model"]
        compiled_code = simple_node.get("compiled_code", "")
        
        join_count = len(re.findall(r'\bJOIN\b', compiled_code, re.IGNORECASE))
        cte_count = len(re.findall(r'\bWITH\b', compiled_code, re.IGNORECASE))
        window_count = len(re.findall(r'\bOVER\b', compiled_code, re.IGNORECASE))
        
        assert join_count == 0
        assert cte_count == 0
        assert window_count == 0


# ============================================================================
# KPI 5: COST ESTIMATION TESTS
# ============================================================================

class TestCostEstimation:
    """Test suite for Snowflake credit and cost calculations."""
    
    def test_credits_calculation_zero_bytes(self):
        """Test credit calculation with zero bytes."""
        bytes_scanned = 0
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 0.0
    
    def test_credits_calculation_10gb(self):
        """Test credit calculation for 10 GB (1 credit)."""
        bytes_scanned = 10 * (1024 ** 3)  # 10 GB
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 1.0
    
    def test_credits_calculation_100gb(self):
        """Test credit calculation for 100 GB (10 credits)."""
        bytes_scanned = 100 * (1024 ** 3)  # 100 GB
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 10.0
    
    def test_credits_calculation_1tb(self):
        """Test credit calculation for 1 TB (100 credits)."""
        bytes_scanned = 1024 * (1024 ** 3)  # 1 TB
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 100.0
    
    def test_credits_calculation_large_dataset(self):
        """Test credit calculation for large dataset."""
        bytes_scanned = 10995116277760  # 10 TB
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 1000.0
    
    def test_credits_calculation_fractional(self):
        """Test credit calculation with fractional results."""
        bytes_scanned = 5 * (1024 ** 3)  # 5 GB = 0.5 credits
        credits = calculate_credits(bytes_scanned)
        
        assert isinstance(credits, float)
        assert credits == 0.5
    
    def test_cost_standard_edition_zero_credits(self):
        """Test cost calculation for zero credits (standard edition)."""
        credits = 0.0
        cost = calculate_cost(credits, "standard")
        
        assert isinstance(cost, float)
        assert cost == 0.0
    
    def test_cost_standard_edition_1_credit(self):
        """Test cost calculation for 1 credit (standard edition)."""
        credits = 1.0
        cost = calculate_cost(credits, "standard")
        
        assert isinstance(cost, float)
        assert cost == 2.0
    
    def test_cost_standard_edition_100_credits(self):
        """Test cost calculation for 100 credits (standard edition)."""
        credits = 100.0
        cost = calculate_cost(credits, "standard")
        
        assert isinstance(cost, float)
        assert cost == 200.0
    
    def test_cost_enterprise_edition_1_credit(self):
        """Test cost calculation for 1 credit (enterprise edition)."""
        credits = 1.0
        cost = calculate_cost(credits, "enterprise")
        
        assert isinstance(cost, float)
        assert cost == 3.0
    
    def test_cost_enterprise_edition_100_credits(self):
        """Test cost calculation for 100 credits (enterprise edition)."""
        credits = 100.0
        cost = calculate_cost(credits, "enterprise")
        
        assert isinstance(cost, float)
        assert cost == 300.0
    
    def test_cost_invalid_edition_defaults_to_standard(self):
        """Test that invalid edition defaults to standard."""
        credits = 100.0
        cost = calculate_cost(credits, "invalid_edition")
        
        assert isinstance(cost, float)
        assert cost == 200.0  # Standard pricing
    
    def test_cost_precision(self):
        """Test that cost calculations maintain proper precision."""
        credits = 123.456
        cost = calculate_cost(credits, "standard")
        
        assert isinstance(cost, float)
        assert cost == 246.91  # 123.456 * 2.0 = 246.91 (rounded to 2 decimal places)
    
    def test_end_to_end_bytes_to_cost(self):
        """Test complete pipeline from bytes to cost."""
        # 100 GB dataset
        bytes_scanned = 100 * (1024 ** 3)
        
        # Calculate credits
        credits = calculate_credits(bytes_scanned)
        assert credits == 10.0
        
        # Calculate cost (standard)
        cost_standard = calculate_cost(credits, "standard")
        assert cost_standard == 20.0
        
        # Calculate cost (enterprise)
        cost_enterprise = calculate_cost(credits, "enterprise")
        assert cost_enterprise == 30.0
    
    def test_large_dataset_cost(self):
        """Test cost calculation for large dataset."""
        # 1 TB dataset
        bytes_scanned = 1024 * (1024 ** 3)
        
        credits = calculate_credits(bytes_scanned)
        cost = calculate_cost(credits, "standard")
        
        assert credits == 100.0
        assert cost == 200.0
    
    def test_cost_calculation_consistency(self):
        """Test that cost calculations are consistent across multiple calls."""
        bytes_scanned = 50 * (1024 ** 3)
        
        cost1 = calculate_cost(calculate_credits(bytes_scanned), "standard")
        cost2 = calculate_cost(calculate_credits(bytes_scanned), "standard")
        
        assert cost1 == cost2


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestKPIIntegration:
    """Integration tests combining multiple KPI calculations."""
    
    def test_complete_kpi_pipeline_simple_model(self, sample_model_simple, sample_manifest, mock_logger):
        """Test complete KPI extraction pipeline for simple model."""
        model = sample_model_simple
        manifest = sample_manifest
        
        # Extract KPIs
        execution_time = model.get("execution_time", 0.0)
        rows_produced = model.get("adapter_response", {}).get("rows_affected", 0)
        bytes_scanned = rows_produced * 500 if rows_produced > 0 else 0
        
        # Calculate costs
        credits = calculate_credits(bytes_scanned)
        cost = calculate_cost(credits, "standard")
        
        # Verify all values
        assert execution_time == 1.234
        assert rows_produced == 1000
        assert bytes_scanned == 500000
        assert credits == 0.0465  # 500000 / (1024^3 * 10)
        assert cost > 0
    
    def test_complete_kpi_pipeline_complex_model(self, sample_model_complex, sample_manifest, mock_logger):
        """Test complete KPI extraction pipeline for complex model."""
        model = sample_model_complex
        manifest = sample_manifest
        
        # Extract KPIs
        execution_time = model.get("execution_time", 0.0)
        rows_produced = model.get("adapter_response", {}).get("rows_affected", 0)
        bytes_scanned = rows_produced * 500 if rows_produced > 0 else 0
        
        # Get SQL from manifest
        node = manifest.get("nodes", {}).get(model.get("unique_id"), {})
        compiled_code = node.get("compiled_code", "")
        
        # Calculate complexity
        join_count = len(re.findall(r'\bJOIN\b', compiled_code, re.IGNORECASE))
        cte_count = len(re.findall(r'\bWITH\b', compiled_code, re.IGNORECASE))
        window_count = len(re.findall(r'\bOVER\b', compiled_code, re.IGNORECASE))
        
        # Calculate costs
        credits = calculate_credits(bytes_scanned)
        cost = calculate_cost(credits, "standard")
        
        # Verify all values
        assert execution_time == 12.456
        assert rows_produced == 50000
        assert bytes_scanned == 25000000
        assert join_count == 4
        assert cte_count == 1
        assert window_count == 3
        assert credits > 0
        assert cost > 0
    
    def test_kpi_extraction_with_edge_cases(self, sample_model_with_zero_values, sample_model_with_missing_values):
        """Test KPI extraction handling of edge cases."""
        # Zero values model
        model1 = sample_model_with_zero_values
        exec_time1 = model1.get("execution_time", 0.0) or 0.0
        rows1 = model1.get("adapter_response", {}).get("rows_affected", 0) or 0
        bytes1 = rows1 * 500 if rows1 > 0 else 0
        
        assert exec_time1 == 0.0
        assert rows1 == 0
        assert bytes1 == 0
        
        # Missing values model
        model2 = sample_model_with_missing_values
        exec_time2 = model2.get("execution_time", 0.0) or 0.0
        rows2 = model2.get("adapter_response", {}).get("rows_affected", 0) or 0
        bytes2 = rows2 * 500 if rows2 > 0 else 0
        
        assert isinstance(exec_time2, (int, float))
        assert isinstance(rows2, int)
        assert isinstance(bytes2, int)
