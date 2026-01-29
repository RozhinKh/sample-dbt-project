#!/usr/bin/env python3
"""Compare baseline vs optimized reports using KPIs

Usage:
  python compare_kpis.py --pipeline a
  python compare_kpis.py --pipeline b
  python compare_kpis.py --pipeline c
"""

import json
import os
import sys
import argparse

def load_report(path):
    """Load a report JSON file"""
    if not os.path.exists(path):
        print(f"ERROR: {path} not found")
        return None
    with open(path) as f:
        return json.load(f)

def calculate_improvement(baseline, optimized, metric_path):
    """Calculate % improvement from baseline to optimized

    metric_path: list of keys to traverse, e.g. ['kpi_1_execution', 'runtime_seconds']
    """
    try:
        baseline_val = baseline
        optimized_val = optimized
        for key in metric_path:
            baseline_val = baseline_val[key]
            optimized_val = optimized_val[key]

        if baseline_val == 0:
            return None
        improvement = ((baseline_val - optimized_val) / baseline_val) * 100
        return improvement
    except Exception as e:
        return None

def main():
    parser = argparse.ArgumentParser(
        description='Compare baseline vs optimized reports using KPIs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compare_kpis.py --pipeline a    # Compare Pipeline A reports
  python compare_kpis.py --pipeline b    # Compare Pipeline B reports
  python compare_kpis.py --pipeline c    # Compare Pipeline C reports
        """
    )
    parser.add_argument('--pipeline', required=True, choices=['a', 'b', 'c'],
                        help='Pipeline to compare (a, b, or c)')

    args = parser.parse_args()
    pipeline = args.pipeline

    baseline_path = f"pipeline_{pipeline}/baseline/report.json"
    candidate_path = f"pipeline_{pipeline}/candidate/report.json"

    print("=" * 70)
    print(f"Pipeline {pipeline.upper()} - KPI Comparison (Baseline vs Optimized)")
    print("=" * 70)
    print()

    baseline = load_report(baseline_path)
    candidate = load_report(candidate_path)

    if not baseline or not candidate:
        print("ERROR: Missing baseline or candidate report")
        return 1

    # Extract KPIs
    baseline_meta = baseline['metadata']
    candidate_meta = candidate['metadata']

    print("KPI 1: EXECUTION TIME (Runtime)")
    print("-" * 70)
    baseline_runtime = baseline['kpi_1_execution']['runtime_seconds']
    candidate_runtime = candidate['kpi_1_execution']['runtime_seconds']
    improvement = calculate_improvement(baseline, candidate,
                                       ['kpi_1_execution', 'runtime_seconds'])

    print(f"  Baseline:  {baseline_runtime:.4f}s")
    print(f"  Optimized: {candidate_runtime:.4f}s")
    if improvement is not None:
        sign = "DOWN" if improvement > 0 else "UP"
        print(f"  Change:    {sign} {abs(improvement):.1f}%")
    print()

    print("KPI 2: WORK METRICS (Output Size)")
    print("-" * 70)
    baseline_rows = baseline['kpi_2_work_metrics']['rows_returned']
    candidate_rows = candidate['kpi_2_work_metrics']['rows_returned']
    improvement = calculate_improvement(baseline, candidate,
                                       ['kpi_2_work_metrics', 'rows_returned'])

    print(f"  Baseline:  {baseline_rows} rows")
    print(f"  Optimized: {candidate_rows} rows")
    if improvement is not None:
        sign = "DOWN" if improvement > 0 else "UP"
        print(f"  Change:    {sign} {abs(improvement):.1f}%")
    print()

    print("KPI 3: OUTPUT VALIDATION (Equivalence Check)")
    print("-" * 70)
    baseline_hash = baseline['kpi_3_output_validation']['output_hash']
    candidate_hash = candidate['kpi_3_output_validation']['output_hash']
    baseline_count = baseline['kpi_3_output_validation']['row_count']
    candidate_count = candidate['kpi_3_output_validation']['row_count']

    print(f"  Baseline hash:  {baseline_hash[:16]}...")
    print(f"  Optimized hash: {candidate_hash[:16]}...")
    print(f"  Baseline rows:  {baseline_count}")
    print(f"  Optimized rows: {candidate_count}")

    output_valid = baseline_hash == candidate_hash and baseline_count == candidate_count
    if output_valid:
        print(f"  Status: [OK] IDENTICAL (output equivalence guaranteed)")
    else:
        print(f"  Status: [FAIL] DIFFERENT (output changed - optimization invalid)")
        return 1
    print()

    print("KPI 4: QUERY COMPLEXITY (Structure Analysis)")
    print("-" * 70)
    if 'kpi_4_complexity' in baseline and baseline['kpi_4_complexity']:
        baseline_complexity = baseline['kpi_4_complexity'].get('complexity_score', 'N/A')
        candidate_complexity = candidate['kpi_4_complexity'].get('complexity_score', 'N/A')
        baseline_joins = baseline['kpi_4_complexity'].get('num_joins', 'N/A')
        candidate_joins = candidate['kpi_4_complexity'].get('num_joins', 'N/A')

        print(f"  Baseline complexity:  {baseline_complexity}/10 ({baseline_joins} joins)")
        print(f"  Optimized complexity: {candidate_complexity}/10 ({candidate_joins} joins)")

        if isinstance(baseline_complexity, (int, float)) and isinstance(candidate_complexity, (int, float)):
            if candidate_complexity < baseline_complexity:
                improvement = ((baseline_complexity - candidate_complexity) / baseline_complexity) * 100
                print(f"  Change:               - {improvement:.1f}% simpler")
            elif candidate_complexity > baseline_complexity:
                increase = ((candidate_complexity - baseline_complexity) / baseline_complexity) * 100
                print(f"  Change:               + {increase:.1f}% more complex")
            else:
                print(f"  Change:               No change in complexity")
    print()

    print("KPI 5: COST ESTIMATION (Bytes Scanned -> Credits)")
    print("-" * 70)
    baseline_credits = baseline.get('kpi_5_cost_estimation', {}).get('credits_estimated', 0)
    candidate_credits = candidate.get('kpi_5_cost_estimation', {}).get('credits_estimated', 0)
    baseline_bytes = baseline.get('kpi_5_cost_estimation', {}).get('bytes_scanned', 0)
    candidate_bytes = candidate.get('kpi_5_cost_estimation', {}).get('bytes_scanned', 0)

    print(f"  Baseline:  {baseline_credits:.8f} credits ({baseline_bytes:,} bytes)")
    print(f"  Optimized: {candidate_credits:.8f} credits ({candidate_bytes:,} bytes)")

    if baseline_credits > 0:
        cost_improvement = ((baseline_credits - candidate_credits) / baseline_credits) * 100
        if cost_improvement > 0:
            print(f"  Change:    - {cost_improvement:.1f}% fewer credits")
        elif cost_improvement < 0:
            print(f"  Change:    + {abs(cost_improvement):.1f}% more credits")
        else:
            print(f"  Change:    No change in cost")
    print()

    print("=" * 70)
    print("PERFORMANCE VALIDATION")
    print("=" * 70)

    # Check for performance regressions
    runtime_increased = candidate_runtime > baseline_runtime
    cost_increased = candidate_credits > baseline_credits

    if runtime_increased or cost_increased:
        print(f"  Status: [FAIL] PERFORMANCE REGRESSED")
        print(f"  Reason: Optimization made performance worse, not better")
        if runtime_increased:
            runtime_regression = ((candidate_runtime - baseline_runtime) / baseline_runtime) * 100
            print(f"    • Runtime: {runtime_regression:.1f}% SLOWER")
        if cost_increased:
            cost_regression = ((candidate_credits - baseline_credits) / baseline_credits) * 100
            print(f"    • Cost: {cost_regression:.1f}% MORE EXPENSIVE")
        return 1
    else:
        print(f"  Status: [OK] PERFORMANCE MAINTAINED OR IMPROVED")
        if not runtime_increased and candidate_runtime < baseline_runtime:
            runtime_improvement = ((baseline_runtime - candidate_runtime) / baseline_runtime) * 100
            print(f"    • Runtime: {runtime_improvement:.1f}% faster")
        if not cost_increased and candidate_credits < baseline_credits:
            cost_improvement = ((baseline_credits - candidate_credits) / baseline_credits) * 100
            print(f"    • Cost: {cost_improvement:.1f}% cheaper")
        if candidate_runtime == baseline_runtime and candidate_credits == baseline_credits:
            print(f"    • No performance change")
    print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    # Only show improvements where applicable
    improvements = []

    runtime_improvement = calculate_improvement(baseline, candidate,
                                               ['kpi_1_execution', 'runtime_seconds'])
    if runtime_improvement is not None and runtime_improvement > 0:
        improvements.append(f"  • Runtime improved: {runtime_improvement:.1f}% faster")

    rows_improvement = calculate_improvement(baseline, candidate,
                                            ['kpi_2_work_metrics', 'rows_returned'])
    if rows_improvement is not None and rows_improvement > 0:
        improvements.append(f"  • Work reduced: {rows_improvement:.1f}% fewer rows processed")

    if baseline_hash == candidate_hash:
        improvements.append(f"  • Output validation: [OK] Guaranteed identical")

    if baseline_credits > 0 and cost_improvement is not None and cost_improvement > 0:
        improvements.append(f"  • Cost reduced: {cost_improvement:.1f}% fewer credits")

    try:
        baseline_complexity = baseline.get('kpi_4_complexity', {}).get('complexity_score')
        candidate_complexity = candidate.get('kpi_4_complexity', {}).get('complexity_score')
        if (isinstance(baseline_complexity, (int, float)) and
            isinstance(candidate_complexity, (int, float)) and
            candidate_complexity < baseline_complexity):
            complexity_improvement = ((baseline_complexity - candidate_complexity) / baseline_complexity) * 100
            improvements.append(f"  • Complexity reduced: {complexity_improvement:.1f}% simpler query")
    except:
        pass

    if improvements:
        for imp in improvements:
            print(imp)
    else:
        print("  • No improvements detected")

    print()
    print("=" * 70)
    return 0

if __name__ == '__main__':
    sys.exit(main())
