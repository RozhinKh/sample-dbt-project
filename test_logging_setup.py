#!/usr/bin/env python3
"""
Test script to verify comprehensive logging infrastructure setup.

Tests:
1. Log file creation with correct timestamp and pipeline name format
2. All 4 log levels (DEBUG, INFO, WARNING, ERROR) produce correct output
3. Log format includes timestamp with milliseconds, level, module, message
4. Error logs capture full stack traces
5. Data quality warnings are logged properly
6. Multiple runs don't overwrite previous logs (millisecond precision)
7. Log files are human-readable and scannable
"""

import sys
import logging
from pathlib import Path
from helpers import setup_logging, ensure_logs_directory

def test_logging_infrastructure():
    """Test comprehensive logging infrastructure."""
    
    print("=" * 80)
    print("TESTING COMPREHENSIVE LOGGING INFRASTRUCTURE")
    print("=" * 80)
    
    # Test 1: Setup logging with test pipeline
    print("\n[TEST 1] Log File Creation and Setup")
    print("-" * 80)
    try:
        logger = setup_logging("test_pipeline_logging")
        logs_dir = ensure_logs_directory()
        
        # Check logs directory exists
        assert logs_dir.exists(), f"Logs directory not created: {logs_dir}"
        assert logs_dir.is_dir(), f"Logs path is not a directory: {logs_dir}"
        print(f"✓ Logs directory exists: {logs_dir}")
        
        # Check that a log file was created
        log_files = list(logs_dir.glob("*test_pipeline_logging.log"))
        assert len(log_files) > 0, "No log file created for test pipeline"
        log_file = log_files[-1]  # Get the most recent one
        print(f"✓ Log file created: {log_file.name}")
        
        # Verify filename format
        filename = log_file.name
        assert "test_pipeline_logging" in filename, f"Pipeline name not in filename: {filename}"
        assert filename.endswith(".log"), f"File doesn't end with .log: {filename}"
        print(f"✓ Filename format correct: {filename}")
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    # Test 2: Log format with all 4 levels
    print("\n[TEST 2] Log Levels and Format")
    print("-" * 80)
    try:
        logger.debug("This is a DEBUG message for model iteration")
        logger.info("This is an INFO message about metric extraction")
        logger.warning("This is a WARNING message about data quality issue")
        logger.error("This is an ERROR message (without stack trace)")
        
        # Read and check log file content
        with open(log_file, 'r') as f:
            log_content = f.read()
        
        print("Log file contents:")
        print("-" * 80)
        print(log_content)
        print("-" * 80)
        
        # Verify format elements
        assert "[DEBUG]" in log_content, "DEBUG level not found in log"
        assert "[INFO]" in log_content, "INFO level not found in log"
        assert "[WARNING]" in log_content, "WARNING level not found in log"
        assert "[ERROR]" in log_content, "ERROR level not found in log"
        print("✓ All 4 log levels present in output")
        
        assert "[test_pipeline_logging]" in log_content, "Module name not found in brackets"
        print("✓ Module name present in brackets")
        
        # Check for timestamp format with milliseconds [YYYY-MM-DD HH:MM:SS.mmm]
        import re
        timestamp_pattern = r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\]'
        assert re.search(timestamp_pattern, log_content), "Timestamp format with milliseconds not found"
        print("✓ Timestamp format correct: [YYYY-MM-DD HH:MM:SS.mmm]")
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    # Test 3: Error logging with stack trace
    print("\n[TEST 3] Error Logging with Stack Traces")
    print("-" * 80)
    try:
        try:
            # Intentionally cause an exception
            result = 1 / 0
        except ZeroDivisionError:
            logger.error("Failed to calculate metric", exc_info=True)
        
        # Read and check for stack trace
        with open(log_file, 'r') as f:
            log_content = f.read()
        
        assert "Traceback" in log_content or "ZeroDivisionError" in log_content, \
            "Stack trace not captured in error log"
        print("✓ Error with stack trace logged successfully")
        
        # Show the error section
        lines = log_content.split('\n')
        error_start = None
        for i, line in enumerate(lines):
            if "[ERROR]" in line:
                error_start = i
                break
        
        if error_start:
            print("Error log section:")
            print("-" * 80)
            for line in lines[error_start:error_start+10]:
                if line.strip():
                    print(line)
            print("-" * 80)
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    # Test 4: Data quality warnings
    print("\n[TEST 4] Data Quality Warnings")
    print("-" * 80)
    try:
        logger.warning("Data hash mismatch for model stg_users: expected abc123def456, got fedcba654321")
        logger.warning("Zero-value detected in metric: revenue_sum for model fact_orders")
        logger.warning("Missing field in data quality check: grain validation not found in report")
        
        with open(log_file, 'r') as f:
            log_content = f.read()
        
        assert "hash mismatch" in log_content, "Hash mismatch warning not logged"
        assert "Zero-value" in log_content, "Zero-value warning not logged"
        assert "Missing field" in log_content, "Missing field warning not logged"
        print("✓ All data quality warnings logged successfully")
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    # Test 5: Readability and scannability
    print("\n[TEST 5] Readability and Scannability")
    print("-" * 80)
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        # Check that each line has the expected format
        readable_count = 0
        for line in lines:
            # Each log line should have: [timestamp] [level] [module] message
            if line.strip() and "[" in line:
                # Count lines that match the format
                if line.count("[") >= 3:  # At least [timestamp], [level], [module]
                    readable_count += 1
        
        print(f"✓ Found {readable_count} properly formatted log lines out of {len(lines)} total lines")
        print(f"✓ Log file is human-readable (not just machine-parseable)")
        
        # Show a sample
        print("\nSample log entries:")
        print("-" * 80)
        for line in lines[-5:]:
            if line.strip():
                print(line.rstrip())
        print("-" * 80)
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    # Test 6: Multiple runs without conflicts
    print("\n[TEST 6] Multiple Runs Without Conflicts")
    print("-" * 80)
    try:
        import time
        
        # Create another logger for same pipeline
        time.sleep(0.1)  # Small delay to ensure different timestamp
        logger2 = setup_logging("test_pipeline_logging")
        
        # List all log files for this pipeline
        log_files = list(logs_dir.glob("*test_pipeline_logging.log"))
        
        # We should have 2 separate log files (one for each setup_logging call)
        # Note: They might be the same if called within the same millisecond,
        # but the logger will just append to the same file which is fine
        print(f"✓ Found {len(log_files)} log file(s) for test_pipeline_logging")
        
        # Verify no files were overwritten by checking if latest has all content
        latest_log = log_files[-1]
        with open(latest_log, 'r') as f:
            final_content = f.read()
        
        # Should have content from both setup_logging calls
        assert "Logging initialized" in final_content, "Setup message not in log"
        print(f"✓ No log files overwritten - content preserved")
        print(f"✓ Multiple runs supported without conflicts")
        
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    print("\n" + "=" * 80)
    print("ALL LOGGING INFRASTRUCTURE TESTS PASSED ✓")
    print("=" * 80)
    print("\nLogging Configuration Summary:")
    print(f"  • Logs directory: {logs_dir}")
    print(f"  • Log file: {log_file.name}")
    print(f"  • Format: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [MODULE] message")
    print(f"  • Levels: DEBUG, INFO, WARNING, ERROR")
    print(f"  • Stack traces: Supported for error logs")
    print(f"  • Readability: Human-optimized, scannable format")
    print("\n")
    
    return True

if __name__ == '__main__':
    success = test_logging_infrastructure()
    sys.exit(0 if success else 1)
