#!/usr/bin/env python3
"""
Master Benchmark Runner - Runs all pipeline benchmarks and generates reports
Runs: gen_report_a.py, gen_report_b.py, gen_report_c.py
Usage: python run_all_benchmarks.py
"""

import subprocess
import sys
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def run_pipeline_benchmark(script_name, pipeline_name):
    """Run a single pipeline benchmark script"""
    print(f"\n{'='*60}")
    print(f"Running {pipeline_name} Benchmark...")
    print(f"{'='*60}\n")

    try:
        result = subprocess.run(
            ["python", script_name],
            timeout=1200,  # 20 minutes timeout
            text=True
        )

        if result.returncode == 0:
            print(f"\n✓ {pipeline_name} benchmark completed successfully")
            return True
        else:
            print(f"\n✗ {pipeline_name} benchmark failed")
            return False

    except subprocess.TimeoutExpired:
        print(f"\n✗ {pipeline_name} benchmark timed out")
        return False
    except Exception as e:
        print(f"\n✗ Error running {pipeline_name} benchmark: {e}")
        return False

# Main execution
print("\n" + "="*60)
print("RUNNING ALL PIPELINE BENCHMARKS")
print("="*60)

results = {
    "Pipeline A": run_pipeline_benchmark("gen_report_a.py", "Pipeline A"),
    "Pipeline B": run_pipeline_benchmark("gen_report_b.py", "Pipeline B"),
    "Pipeline C": run_pipeline_benchmark("gen_report_c.py", "Pipeline C")
}

# Summary
print("\n" + "="*60)
print("BENCHMARK SUMMARY")
print("="*60)

for pipeline, success in results.items():
    status = "✓ PASSED" if success else "✗ FAILED"
    print(f"{pipeline}: {status}")

# Check if all passed
all_passed = all(results.values())

print("\n" + "="*60)
if all_passed:
    print("All benchmarks completed successfully!")
    print("\nReports saved to:")
    print("  - benchmark/pipeline_a/candidate/report.json")
    print("  - benchmark/pipeline_b/candidate/report.json")
    print("  - benchmark/pipeline_c/candidate/report.json")
else:
    print("Some benchmarks failed. Check output above for details.")
    sys.exit(1)

print("="*60)
