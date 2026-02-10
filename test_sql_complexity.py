#!/usr/bin/env python3
"""
Test SQL Complexity Parsing Functions

Tests the SQL parsing functions for extracting JOINs, CTEs, and window functions.
Includes test cases for edge cases, comments, and various SQL patterns.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from helpers import (
    extract_sql_complexity,
    strip_sql_comments,
    count_joins,
    count_ctes,
    count_window_functions
)


def test_strip_comments():
    """Test SQL comment stripping."""
    print("Testing comment stripping...")
    
    # Test line comments
    sql = "SELECT * FROM table -- this is a comment\nWHERE id = 1"
    clean = strip_sql_comments(sql)
    assert "this is a comment" not in clean
    assert "SELECT" in clean
    print("  ✓ Line comments stripped correctly")
    
    # Test block comments
    sql = "SELECT * /* block comment */ FROM table"
    clean = strip_sql_comments(sql)
    assert "block comment" not in clean
    assert "SELECT" in clean
    print("  ✓ Block comments stripped correctly")
    
    # Test string literals preserved
    sql = "SELECT '-- this is not a comment' FROM table"
    clean = strip_sql_comments(sql)
    assert "-- this is not a comment" in clean
    print("  ✓ String literals preserved correctly")


def test_join_counting():
    """Test JOIN counting."""
    print("\nTesting JOIN counting...")
    
    # Single JOIN
    sql = "SELECT * FROM a INNER JOIN b ON a.id = b.id"
    assert count_joins(sql) == 1
    print("  ✓ INNER JOIN counted correctly")
    
    # Multiple JOINs
    sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id RIGHT JOIN c ON a.id = c.id"
    assert count_joins(sql) == 2
    print("  ✓ Multiple JOINs counted correctly")
    
    # FULL JOIN
    sql = "SELECT * FROM a FULL JOIN b ON a.id = b.id"
    assert count_joins(sql) == 1
    print("  ✓ FULL JOIN counted correctly")
    
    # CROSS JOIN
    sql = "SELECT * FROM a CROSS JOIN b"
    assert count_joins(sql) == 1
    print("  ✓ CROSS JOIN counted correctly")
    
    # Case insensitive
    sql = "SELECT * FROM a inner join b ON a.id = b.id"
    assert count_joins(sql) == 1
    print("  ✓ Case-insensitive JOIN counting works")
    
    # No false positives in identifiers
    sql = "SELECT JOINED FROM table WHERE column_joined = 1"
    assert count_joins(sql) == 0
    print("  ✓ No false positives in identifiers")


def test_cte_counting():
    """Test CTE counting."""
    print("\nTesting CTE counting...")
    
    # Single CTE
    sql = "WITH cte AS (SELECT * FROM table) SELECT * FROM cte"
    assert count_ctes(sql) == 1
    print("  ✓ Single CTE counted correctly")
    
    # Multiple CTEs
    sql = "WITH cte1 AS (SELECT * FROM a), cte2 AS (SELECT * FROM b) SELECT * FROM cte1"
    assert count_ctes(sql) == 2
    print("  ✓ Multiple CTEs counted correctly")
    
    # Three CTEs
    sql = "WITH cte1 AS (...), cte2 AS (...), cte3 AS (...) SELECT * FROM cte1"
    assert count_ctes(sql) == 3
    print("  ✓ Three CTEs counted correctly")
    
    # No CTE
    sql = "SELECT * FROM table"
    assert count_ctes(sql) == 0
    print("  ✓ Query without CTE returns 0")
    
    # Case insensitive
    sql = "with cte AS (SELECT * FROM table) SELECT * FROM cte"
    assert count_ctes(sql) == 1
    print("  ✓ Case-insensitive CTE counting works")


def test_window_function_counting():
    """Test window function counting."""
    print("\nTesting window function counting...")
    
    # Single window function
    sql = "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY date)"
    assert count_window_functions(sql) == 1
    print("  ✓ Single window function counted correctly")
    
    # Multiple window functions
    sql = "SELECT ROW_NUMBER() OVER (ORDER BY date), RANK() OVER (PARTITION BY id ORDER BY value)"
    assert count_window_functions(sql) == 2
    print("  ✓ Multiple window functions counted correctly")
    
    # No window functions
    sql = "SELECT * FROM table"
    assert count_window_functions(sql) == 0
    print("  ✓ Query without window function returns 0")
    
    # Case insensitive
    sql = "SELECT row_number() over (ORDER BY date)"
    assert count_window_functions(sql) == 1
    print("  ✓ Case-insensitive window function counting works")


def test_complex_query():
    """Test a complex query with all elements."""
    print("\nTesting complex query...")
    
    sql = """
    WITH cte1 AS (
        SELECT id, value FROM source1
    ),
    cte2 AS (
        SELECT id, amount FROM source2
    )
    SELECT 
        a.id,
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.date) as rn,
        RANK() OVER (ORDER BY a.value DESC) as rank
    FROM cte1 a
    INNER JOIN cte2 b ON a.id = b.id
    LEFT JOIN other_table c ON a.id = c.id
    WHERE a.value > 100
    """
    
    complexity = extract_sql_complexity(sql)
    assert complexity["join_count"] == 2  # INNER JOIN, LEFT JOIN
    assert complexity["cte_count"] == 2   # cte1, cte2
    assert complexity["window_function_count"] == 2  # ROW_NUMBER, RANK
    print("  ✓ Complex query analyzed correctly")
    print(f"    - JOINs: {complexity['join_count']}")
    print(f"    - CTEs: {complexity['cte_count']}")
    print(f"    - Window Functions: {complexity['window_function_count']}")


def test_edge_cases():
    """Test edge cases."""
    print("\nTesting edge cases...")
    
    # Empty string
    complexity = extract_sql_complexity("")
    assert complexity["join_count"] == 0
    assert complexity["cte_count"] == 0
    assert complexity["window_function_count"] == 0
    print("  ✓ Empty string handled correctly")
    
    # None (should be handled gracefully)
    complexity = extract_sql_complexity(None)
    assert complexity["join_count"] == 0
    print("  ✓ None handled gracefully")
    
    # With comments
    sql = """
    -- This query has multiple parts
    WITH cte AS (SELECT * FROM table) /* comment */
    SELECT * FROM cte
    INNER JOIN other ON cte.id = other.id
    """
    complexity = extract_sql_complexity(sql)
    assert complexity["cte_count"] == 1
    assert complexity["join_count"] == 1
    print("  ✓ Comments handled correctly")


def main():
    """Run all tests."""
    print("=" * 80)
    print("SQL Complexity Parsing Tests")
    print("=" * 80)
    
    try:
        test_strip_comments()
        test_join_counting()
        test_cte_counting()
        test_window_function_counting()
        test_complex_query()
        test_edge_cases()
        
        print("\n" + "=" * 80)
        print("✓ All tests passed!")
        print("=" * 80)
        return 0
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {str(e)}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
