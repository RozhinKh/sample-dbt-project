#!/bin/bash
# Script to run KPI extraction tests with coverage analysis

set -e  # Exit on first error

echo "=========================================="
echo "Running KPI Extraction Unit Tests"
echo "=========================================="
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "✗ pytest not found. Installing..."
    pip install pytest pytest-cov
fi

echo "[1/3] Running tests..."
pytest tests/test_kpi_extraction.py -v --tb=short

echo ""
echo "[2/3] Running tests with coverage..."
pytest tests/test_kpi_extraction.py \
    --cov=benchmark.generate_report \
    --cov=config \
    --cov-report=term-missing \
    --cov-report=html

echo ""
echo "[3/3] Test execution summary..."
pytest tests/test_kpi_extraction.py -v --tb=line -q

echo ""
echo "=========================================="
echo "✓ All tests completed successfully"
echo "=========================================="
echo ""
echo "Coverage report: htmlcov/index.html"
echo ""
