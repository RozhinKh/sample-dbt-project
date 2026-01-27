#!/bin/bash
# Artemis optimization run command
# This script runs the dbt pipeline and benchmarks optimizations

set -e

echo "=================================="
echo "Artemis Optimization Run"
echo "=================================="
echo ""

# Ensure environment variables are set
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "ERROR: Snowflake credentials not set"
    echo "Please set: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
    exit 1
fi

echo "Running dbt pipeline..."
bash run_pipeline.sh
PIPELINE_STATUS=$?

if [ $PIPELINE_STATUS -eq 0 ]; then
    echo ""
    echo "=================================="
    echo "[SUCCESS] Optimization validated"
    echo "=================================="
    exit 0
else
    echo ""
    echo "=================================="
    echo "[FAILED] Optimization rejected"
    echo "=================================="
    exit 1
fi
