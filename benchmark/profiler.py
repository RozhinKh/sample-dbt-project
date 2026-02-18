#!/usr/bin/env python3
"""
Pipeline Profiler — Compilation + Dependency Fan-Out Analysis

Extends the baseline report with per-model profiling data that can't be
captured during a normal dbt build:

  1. Compilation time vs execution time breakdown (from QUERY_HISTORY)
  2. Dependency fan-out per model (how many downstream models reference it)
  3. View re-evaluation cost (fan_out × execution_time — the hidden cost
     of keeping a slow view instead of a table)
  4. Cumulative critical-path time (max upstream chain time + own time)
  5. Materialization candidates sorted by total pipeline cost saved

Output: benchmark/pipeline_{p}/profiling_report.json

Usage:
    python benchmark/profiler.py --pipeline c
    python benchmark/profiler.py --pipeline c --profile bain_capital
    python benchmark/profiler.py --pipeline c --no-snowflake   # skip QUERY_HISTORY lookup

CLI Arguments:
    --pipeline {a|b|c}      (required) Pipeline identifier
    --profile NAME           (optional) dbt profile name (default: bain_capital)
    --no-snowflake           (optional) Skip QUERY_HISTORY lookup, use report.json times
    --log-level LEVEL        (optional) DEBUG|INFO|WARNING|ERROR (default: INFO)

Returns:
    0 on success, 1 on critical errors
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import (
    parse_profiles_yml,
    get_query_metrics_from_history,
    load_manifest,
    setup_logging,
    ConfigError,
)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="profiler.py",
        description="Profile pipeline execution: compilation time, fan-out, view cost",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark/profiler.py --pipeline c
  python benchmark/profiler.py --pipeline c --no-snowflake
  python benchmark/profiler.py --pipeline c --log-level DEBUG
        """
    )
    parser.add_argument("--pipeline", choices=["a", "b", "c"], required=True)
    parser.add_argument("--profile", default="bain_capital",
                        help="dbt profile name for Snowflake credentials (default: bain_capital)")
    parser.add_argument("--no-snowflake", action="store_true", default=False,
                        help="Skip QUERY_HISTORY lookup, derive all times from report.json")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default="INFO")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Dependency graph
# ---------------------------------------------------------------------------

def build_dependency_graph(
    manifest: Dict[str, Any],
    pipeline_tag: str
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Build upstream and downstream adjacency maps for pipeline models.

    Args:
        manifest: Loaded manifest.json
        pipeline_tag: e.g. "pipeline_c"

    Returns:
        upstream_map:   model_name -> [upstream model names within this pipeline]
        downstream_map: model_name -> [downstream model names within this pipeline]
    """
    nodes = manifest.get("nodes", {})

    pipeline_nodes = {
        node_id: node
        for node_id, node in nodes.items()
        if node_id.startswith("model.") and pipeline_tag in node.get("tags", [])
    }

    # node_id -> name lookup for quick resolution
    id_to_name = {nid: n["name"] for nid, n in pipeline_nodes.items()}

    upstream_map: Dict[str, List[str]] = {n["name"]: [] for n in pipeline_nodes.values()}
    downstream_map: Dict[str, List[str]] = {n["name"]: [] for n in pipeline_nodes.values()}

    for node_id, node in pipeline_nodes.items():
        model_name = node["name"]
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_name = id_to_name.get(dep_id)
            if dep_name:
                upstream_map[model_name].append(dep_name)
                downstream_map[dep_name].append(model_name)

    return upstream_map, downstream_map


# ---------------------------------------------------------------------------
# Critical-path cumulative time
# ---------------------------------------------------------------------------

def compute_critical_path(
    model_name: str,
    exec_times: Dict[str, float],
    upstream_map: Dict[str, List[str]],
    cache: Optional[Dict[str, float]] = None
) -> float:
    """
    Recursive critical-path computation (longest upstream chain + own time).

    For a DAG with no cycles this correctly reflects the minimum time before
    this model's output is available, assuming unlimited parallelism.

    Args:
        model_name: Model to compute for
        exec_times: execution_time_seconds per model name
        upstream_map: model_name -> list of upstream model names
        cache: memoization dict (populated in-place)

    Returns:
        Critical-path time in seconds
    """
    if cache is None:
        cache = {}
    if model_name in cache:
        return cache[model_name]

    own = exec_times.get(model_name, 0.0)
    upstream_names = upstream_map.get(model_name, [])

    if not upstream_names:
        cache[model_name] = own
        return own

    max_upstream = max(
        compute_critical_path(u, exec_times, upstream_map, cache)
        for u in upstream_names
    )
    result = own + max_upstream
    cache[model_name] = result
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_arguments()
    logger = setup_logging(f"profiler_pipeline_{args.pipeline}")

    pipeline_tag = f"pipeline_{args.pipeline}"
    report_path = Path(f"benchmark/pipeline_{args.pipeline}/baseline/report.json")
    manifest_path = Path("target/manifest.json")
    output_path = Path(f"benchmark/pipeline_{args.pipeline}/profiling_report.json")

    logger.info("=" * 70)
    logger.info("PIPELINE PROFILER")
    logger.info("=" * 70)
    logger.info(f"Pipeline  : {pipeline_tag}")
    logger.info(f"Baseline  : {report_path}")
    logger.info(f"Snowflake : {'disabled (--no-snowflake)' if args.no_snowflake else 'enabled'}")

    # ------------------------------------------------------------------
    # Load baseline report
    # ------------------------------------------------------------------
    if not report_path.exists():
        logger.error(f"Baseline report not found: {report_path}")
        logger.error("  Run: python benchmark/generate_report.py --pipeline c --use-snowflake")
        return 1

    with open(report_path, encoding="utf-8") as f:
        baseline = json.load(f)

    models_data = baseline.get("models", [])
    logger.info(f"Loaded {len(models_data)} models from baseline report")

    # ------------------------------------------------------------------
    # Load manifest for dependency graph
    # ------------------------------------------------------------------
    if not manifest_path.exists():
        logger.error(f"manifest.json not found: {manifest_path}")
        logger.error("  Run: dbt parse  OR  dbt build --select tag:{pipeline_tag}")
        return 1

    manifest = load_manifest(str(manifest_path), logger)
    upstream_map, downstream_map = build_dependency_graph(manifest, pipeline_tag)
    logger.info(f"Dependency graph built: {len(upstream_map)} models mapped")

    # ------------------------------------------------------------------
    # Parse Snowflake credentials (only if needed)
    # ------------------------------------------------------------------
    credentials: Optional[Dict[str, str]] = None
    if not args.no_snowflake:
        try:
            credentials = parse_profiles_yml(args.profile)
            logger.info(f"Snowflake credentials loaded from profile '{args.profile}'")
        except ConfigError as e:
            logger.warning(f"Could not load Snowflake credentials: {e}")
            logger.warning("  Falling back to report.json times only")
            credentials = None

    # ------------------------------------------------------------------
    # Per-model profiling
    # ------------------------------------------------------------------
    exec_times: Dict[str, float] = {}
    profiled_models: List[Dict[str, Any]] = []
    query_history_hits = 0
    query_history_misses = 0

    for model in models_data:
        model_name = model["model_name"]
        query_id = model.get("query_id", "")
        exec_time_s = model.get("execution_time_seconds", 0.0)
        exec_times[model_name] = exec_time_s

        # ------------------------------------------------------------------
        # Query HISTORY lookup
        # ------------------------------------------------------------------
        qh: Dict[str, Any] = {}
        if credentials and query_id:
            qh = get_query_metrics_from_history(query_id, credentials, logger)
            if qh:
                query_history_hits += 1
                logger.debug(f"  {model_name}: QUERY_HISTORY hit — compile={qh.get('compilation_time', 0)}ms")
            else:
                query_history_misses += 1
                logger.debug(f"  {model_name}: QUERY_HISTORY miss (ID={query_id[:16]}...)")
        else:
            query_history_misses += 1

        # ------------------------------------------------------------------
        # Derive times
        # ------------------------------------------------------------------
        # execution_time_ms: prefer QUERY_HISTORY, fall back to dbt wall-clock
        qh_exec_ms = qh.get("execution_time_ms", 0)
        exec_time_ms = qh_exec_ms if qh_exec_ms > 0 else int(exec_time_s * 1000)

        compilation_time_ms = qh.get("compilation_time", 0)
        # Self time = elapsed minus compilation overhead (min 0)
        self_time_ms = max(0, exec_time_ms - compilation_time_ms)

        profiled_models.append({
            "model_name": model_name,
            "model_layer": model.get("model_layer", "unknown"),
            "materialization": model.get("materialization", "unknown"),
            "query_id": query_id,
            # Timing
            "execution_time_seconds": exec_time_s,
            "execution_time_ms": exec_time_ms,
            "compilation_time_ms": compilation_time_ms,
            "self_time_ms": self_time_ms,
            "query_history_source": bool(qh),
            # Work metrics
            "bytes_scanned": qh.get("bytes_scanned", model.get("bytes_scanned", 0)),
            "rows_produced": qh.get("rows_produced", model.get("rows_produced", 0)),
            "partitions_scanned": qh.get("partitions_scanned", 0),
            "warehouse_size": qh.get("warehouse_size", "unknown"),
            # SQL complexity (from baseline)
            "join_count": model.get("join_count", 0),
            "cte_count": model.get("cte_count", 0),
            "window_function_count": model.get("window_function_count", 0),
            # Dependency graph
            "upstream_models": upstream_map.get(model_name, []),
            "downstream_models": downstream_map.get(model_name, []),
            "dependency_fan_out": len(downstream_map.get(model_name, [])),
            # Placeholders — filled in after all models are processed
            "critical_path_seconds": 0.0,
            "estimated_total_view_cost_seconds": 0.0,
        })

    logger.info(f"QUERY_HISTORY: {query_history_hits} hits, {query_history_misses} misses")

    # ------------------------------------------------------------------
    # Post-pass: critical-path + view re-evaluation cost
    # ------------------------------------------------------------------
    cp_cache: Dict[str, float] = {}

    for pm in profiled_models:
        model_name = pm["model_name"]

        # Critical-path time (longest upstream chain + own time)
        pm["critical_path_seconds"] = round(
            compute_critical_path(model_name, exec_times, upstream_map, cp_cache), 4
        )

        # View re-evaluation cost:
        #   Every downstream model that queries this view re-evaluates it from
        #   scratch. Total wasted time = execution_time × fan_out.
        #   (A table materialization would pay this cost only once at build time.)
        fan_out = pm["dependency_fan_out"]
        if pm["materialization"] == "view" and fan_out > 0:
            pm["estimated_total_view_cost_seconds"] = round(
                pm["execution_time_seconds"] * fan_out, 4
            )
        else:
            pm["estimated_total_view_cost_seconds"] = 0.0

    # ------------------------------------------------------------------
    # Sort: hottest by execution_time descending
    # ------------------------------------------------------------------
    profiled_models.sort(key=lambda m: m["execution_time_seconds"], reverse=True)

    # ------------------------------------------------------------------
    # Materialization candidates
    #   Views where fan_out > 0, sorted by estimated total view cost desc
    # ------------------------------------------------------------------
    materialization_candidates = sorted(
        [m for m in profiled_models
         if m["materialization"] == "view" and m["dependency_fan_out"] > 0],
        key=lambda m: m["estimated_total_view_cost_seconds"],
        reverse=True
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    total_exec = sum(m["execution_time_seconds"] for m in profiled_models)
    total_compilation_ms = sum(m["compilation_time_ms"] for m in profiled_models)
    n = len(profiled_models)
    avg_compilation_ms = round(total_compilation_ms / n, 2) if n > 0 else 0.0
    total_view_waste = sum(m["estimated_total_view_cost_seconds"] for m in profiled_models)

    # ------------------------------------------------------------------
    # Build report
    # ------------------------------------------------------------------
    report = {
        "schema_version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "pipeline": pipeline_tag,
        "baseline_report_path": str(report_path),
        "snowflake_query_history": {
            "enabled": not args.no_snowflake and credentials is not None,
            "hits": query_history_hits,
            "misses": query_history_misses,
            "note": (
                "compilation_time_ms sourced from QUERY_HISTORY"
                if query_history_hits > 0
                else "QUERY_HISTORY unavailable — compilation_time_ms values are 0, self_time_ms equals execution_time_ms"
            )
        },
        "summary": {
            "total_models": n,
            "total_execution_time_seconds": round(total_exec, 4),
            "total_compilation_time_ms": total_compilation_ms,
            "avg_compilation_time_ms": avg_compilation_ms,
            "total_estimated_view_waste_seconds": round(total_view_waste, 4),
            "materialization_candidate_count": len(materialization_candidates),
        },
        "hottest_models": [
            {
                "rank": i + 1,
                "model_name": m["model_name"],
                "model_layer": m["model_layer"],
                "materialization": m["materialization"],
                "execution_time_seconds": m["execution_time_seconds"],
                "compilation_time_ms": m["compilation_time_ms"],
                "self_time_ms": m["self_time_ms"],
                "window_function_count": m["window_function_count"],
                "dependency_fan_out": m["dependency_fan_out"],
                "estimated_total_view_cost_seconds": m["estimated_total_view_cost_seconds"],
            }
            for i, m in enumerate(profiled_models[:10])
        ],
        "materialization_candidates": [
            {
                "model_name": m["model_name"],
                "model_layer": m["model_layer"],
                "execution_time_seconds": m["execution_time_seconds"],
                "dependency_fan_out": m["dependency_fan_out"],
                "estimated_total_view_cost_seconds": m["estimated_total_view_cost_seconds"],
                "downstream_models": m["downstream_models"],
                "current_materialization": "view",
                "recommended_materialization": "table",
                "estimated_savings_seconds": round(
                    m["estimated_total_view_cost_seconds"] - m["execution_time_seconds"], 4
                ),
            }
            for m in materialization_candidates
        ],
        "profiled_models": profiled_models,
    }

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info("=" * 70)
    logger.info("PROFILING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Models profiled          : {n}")
    logger.info(f"QUERY_HISTORY hits       : {query_history_hits}/{n}")
    logger.info(f"Avg compilation time     : {avg_compilation_ms:.0f}ms")
    logger.info(f"Total view waste         : {total_view_waste:.2f}s")
    logger.info(f"Materialization candidates: {len(materialization_candidates)}")
    logger.info(f"Output                   : {output_path}")
    logger.info("=" * 70)
    logger.info("Hottest models (by execution time):")
    for i, m in enumerate(profiled_models[:5], 1):
        logger.info(
            f"  {i}. {m['model_name']:<45} {m['execution_time_seconds']:.2f}s"
            f"  fan_out={m['dependency_fan_out']}"
            f"  view_waste={m['estimated_total_view_cost_seconds']:.2f}s"
        )
    logger.info("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
