#!/usr/bin/env python3
"""
Generate Benchmark Report from dbt Artifacts

This script orchestrates the complete benchmark report generation pipeline:
1. Parse CLI arguments (required: --pipeline, optional: --output/--config/--log-level)
2. Load configuration with environment variable overrides
3. Set up logging to benchmark/logs/{timestamp}_{pipeline}.log
4. Load dbt artifacts (manifest.json and run_results.json)
5. Filter models by pipeline tag
6. Extract KPI metrics for each model (5 KPIs)
7. Aggregate summary statistics
8. Validate against report.json schema
9. Write JSON report to output path
10. Log execution summary and exit with proper status code

Usage:
    python benchmark/generate_report.py --pipeline a
    python benchmark/generate_report.py --pipeline b --output custom/path/report.json
    python benchmark/generate_report.py --pipeline c --log-level DEBUG

CLI Arguments:
    --pipeline {a|b|c}      (required) Pipeline identifier
    --output PATH            (optional) Output file path (default: benchmark/pipeline_{pipeline}/baseline/report.json)
    --config PATH            (optional) Config file path (default: config.py)
    --log-level {DEBUG|INFO|WARNING|ERROR}  (optional) Log level (default: INFO)

Returns:
    0 on success, 1 on critical errors
"""

import sys
import json
import logging
import os
import re
import hashlib
import argparse
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import (
    load_manifest,
    load_run_results,
    extract_model_data,
    validate_artifact_fields,
    setup_logging,
    extract_execution_time,
    filter_models_by_pipeline,
    detect_execution_time_outliers,
    load_json_safe,
    schema_validator,
    MissingArtifact,
    InvalidSchema,
    DataMismatch,
    ConfigError,
    get_project_root,
    ensure_logs_directory
)

from config import load_config, calculate_credits, calculate_cost, SNOWFLAKE_PRICING


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the report generator.
    
    Returns:
        Parsed arguments with pipeline, output, config, and log_level
        
    Raises:
        SystemExit: If --pipeline is missing or invalid
    """
    parser = argparse.ArgumentParser(
        prog="generate_report.py",
        description="Generate benchmark report from dbt artifacts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark/generate_report.py --pipeline a
  python benchmark/generate_report.py --pipeline b --output custom/report.json
  python benchmark/generate_report.py --pipeline c --log-level DEBUG
        """
    )
    
    # Required arguments
    parser.add_argument(
        "--pipeline",
        choices=["a", "b", "c"],
        required=True,
        help="Pipeline identifier (a, b, or c)"
    )
    
    # Optional arguments
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path for report.json (default: benchmark/pipeline_{pipeline}/baseline/report.json)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config.py",
        help="Configuration file path (default: config.py)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


def get_default_output_path(pipeline: str) -> Path:
    """
    Get default output path for report.json based on pipeline.
    
    Args:
        pipeline: Pipeline identifier (a, b, or c)
        
    Returns:
        Path object for default report location
    """
    return Path(f"benchmark/pipeline_{pipeline}/baseline/report.json")


def extract_kpi_metrics(model: Dict[str, Any], manifest: Dict[str, Any], logger: logging.Logger) -> Tuple[Dict[str, Any], bool, List[str]]:
    """
    Extract all 5 KPI metrics for a single model with comprehensive error handling.
    
    Args:
        model: Extracted model data from extract_model_data()
        manifest: Loaded manifest.json
        logger: Logger instance
        
    Returns:
        Tuple of (kpi_metrics_dict, is_complete, warnings_list)
            - kpi_metrics_dict: Dictionary with all KPI metrics (partial data ok)
            - is_complete: True if all KPIs extracted successfully
            - warnings_list: List of warning messages for missing/incomplete KPI data
    
    Raises:
        DataMismatch: If model exists in manifest but not in run_results (critical data inconsistency)
    """
    model_id = model.get("unique_id", "")
    model_name = model.get("model_name", "unknown")
    warnings_list = []
    is_complete = True
    
    try:
        # Verify model exists in manifest (should always be true, but check for consistency)
        manifest_nodes = manifest.get("nodes", {})
        if model_id and model_id not in manifest_nodes:
            raise DataMismatch(
                f"Model mismatch: {model_name} ({model_id}) exists in parsed models but not in manifest.json\n"
                f"  This indicates a data inconsistency between artifacts.\n"
                f"  Remediation: Regenerate dbt artifacts with 'dbt parse' and 'dbt run'"
            )
        
        # KPI 1: Execution Time (in seconds)
        try:
            exec_time = model.get("execution_time", 0.0)
            if exec_time is None or not isinstance(exec_time, (int, float)):
                exec_time = 0.0
                warnings_list.append(f"Execution time missing or invalid for {model_name}, using 0.0")
                is_complete = False
            elif exec_time < 0:
                exec_time = 0.0
                warnings_list.append(f"Negative execution time for {model_name}, using 0.0")
                is_complete = False
        except Exception as e:
            logger.debug(f"Error extracting execution time for {model_name}: {str(e)}")
            exec_time = 0.0
            warnings_list.append(f"Failed to extract execution time for {model_name}")
            is_complete = False
        
        # KPI 2: Work Metrics (rows and bytes)
        try:
            adapter_response = model.get("adapter_response", {})
            rows_produced = adapter_response.get("rows_affected", 0)
            
            if rows_produced is None or not isinstance(rows_produced, int):
                rows_produced = 0
                warnings_list.append(f"Rows produced missing or invalid for {model_name}, using 0")
                is_complete = False
            elif rows_produced < 0:
                rows_produced = 0
                warnings_list.append(f"Negative rows produced for {model_name}, using 0")
                is_complete = False
            
            # Estimate bytes scanned (simple estimation: rows * avg_row_width)
            estimated_row_width = 500
            bytes_scanned = rows_produced * estimated_row_width if rows_produced > 0 else 0
        except Exception as e:
            logger.debug(f"Error extracting work metrics for {model_name}: {str(e)}")
            rows_produced = 0
            bytes_scanned = 0
            warnings_list.append(f"Failed to extract work metrics (rows/bytes) for {model_name}")
            is_complete = False
        
        # KPI 3: Output Hash (SHA256)
        # For now, we'll set to null as proper hash calculation requires Snowflake access
        output_hash = None
        hash_calculation_method = "unavailable"
        
        # KPI 4: Query Complexity (JOINs, CTEs, Window Functions)
        join_count = 0
        cte_count = 0
        window_function_count = 0
        
        try:
            node = manifest_nodes.get(model_id, {})
            sql_code = node.get("compiled_code") or node.get("raw_code") or ""
            
            if sql_code:
                try:
                    # Count JOINs (simple regex)
                    join_count = len(re.findall(r'\bJOIN\b', sql_code, re.IGNORECASE))
                    
                    # Count CTEs (WITH ... AS)
                    cte_count = len(re.findall(r'\bWITH\b', sql_code, re.IGNORECASE))
                    
                    # Count window functions (OVER)
                    window_function_count = len(re.findall(r'\bOVER\b', sql_code, re.IGNORECASE))
                except Exception as e:
                    logger.debug(f"Error analyzing SQL for {model_name}: {str(e)}")
                    warnings_list.append(f"Could not fully analyze SQL complexity for {model_name}")
                    is_complete = False
            else:
                logger.debug(f"No SQL code available for complexity analysis of {model_name}")
                warnings_list.append(f"SQL code not available for complexity analysis of {model_name}")
                is_complete = False
        except Exception as e:
            logger.debug(f"Could not extract SQL complexity for {model_name}: {str(e)}")
            warnings_list.append(f"Failed to extract SQL complexity for {model_name}")
            is_complete = False
        
        # KPI 5: Cost Estimation (Snowflake credits)
        try:
            estimated_credits = calculate_credits(bytes_scanned, "standard")
            estimated_cost_usd = calculate_cost(estimated_credits, "standard")
        except Exception as e:
            logger.debug(f"Error calculating cost for {model_name}: {str(e)}")
            estimated_credits = 0.0
            estimated_cost_usd = 0.0
            warnings_list.append(f"Failed to calculate cost estimation for {model_name}")
            is_complete = False
        
        return {
            "execution_time_seconds": exec_time,
            "rows_produced": rows_produced,
            "bytes_scanned": bytes_scanned,
            "output_hash": output_hash,
            "hash_calculation_method": hash_calculation_method,
            "join_count": join_count,
            "cte_count": cte_count,
            "window_function_count": window_function_count,
            "estimated_credits": estimated_credits,
            "estimated_cost_usd": estimated_cost_usd
        }, is_complete, warnings_list
    
    except DataMismatch as e:
        # Re-raise DataMismatch for caller to handle
        raise e
    except Exception as e:
        # Unexpected error - log and return defaults with error flag
        logger.error(f"Unexpected error extracting KPIs for {model_name}: {str(e)}", exc_info=True)
        logger.error(f"  Remediation: Check that both manifest.json and run_results.json are valid and complete")
        return {
            "execution_time_seconds": 0.0,
            "rows_produced": 0,
            "bytes_scanned": 0,
            "output_hash": None,
            "hash_calculation_method": "unavailable",
            "join_count": 0,
            "cte_count": 0,
            "window_function_count": 0,
            "estimated_credits": 0.0,
            "estimated_cost_usd": 0.0
        }, False, [f"Failed to extract all KPIs for {model_name}: {str(e)}"]


def build_report(
    pipeline: str,
    manifest: Dict[str, Any],
    run_results: Dict[str, Any],
    parsed_models: List[Dict[str, Any]],
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Build complete report.json structure matching the schema.
    
    Args:
        pipeline: Pipeline identifier (a, b, or c)
        manifest: Loaded manifest.json
        run_results: Loaded run_results.json
        parsed_models: List of extracted model data
        logger: Logger instance
        
    Returns:
        Complete report dictionary matching schema from Task 10
    """
    logger.info("Building report.json structure...")
    
    # Filter models by pipeline
    pipeline_tag = f"pipeline_{pipeline}"
    pipeline_models = filter_models_by_pipeline(parsed_models, pipeline_tag, logger)
    
    if not pipeline_models:
        logger.warning(f"No models found with tag '{pipeline_tag}'")
    
    # Extract KPIs for each model
    models_array = []
    total_exec_time = 0.0
    total_rows = 0
    total_bytes = 0
    total_credits = 0.0
    total_cost = 0.0
    models_with_errors = 0
    models_with_incomplete_data = 0
    data_quality_flags = []
    warnings_and_errors = []
    
    for model in pipeline_models:
        try:
            # Extract all KPI metrics (returns tuple: metrics, is_complete, warnings)
            try:
                kpi_metrics, is_complete, kpi_warnings = extract_kpi_metrics(model, manifest, logger)
                
                # Log any warnings from KPI extraction
                if kpi_warnings:
                    for warning in kpi_warnings:
                        logger.warning(f"  {model.get('model_name', 'unknown')}: {warning}")
                
                # Track incomplete KPI data
                if not is_complete:
                    models_with_incomplete_data += 1
                    data_quality_flags.append({
                        "model_name": model.get("model_name"),
                        "flag_type": "incomplete_kpi_data",
                        "severity": "warning",
                        "message": f"Model {model.get('model_name')} has incomplete KPI data ({len(kpi_warnings)} issues)",
                        "timestamp": datetime.now().isoformat(),
                        "details": kpi_warnings
                    })
            
            except DataMismatch as e:
                # Handle data consistency errors - log with remediation and mark as error
                logger.error(f"Data mismatch for model {model.get('model_name', 'unknown')}: {str(e)}", exc_info=True)
                logger.error(f"  This indicates a critical data inconsistency between artifacts.")
                models_with_errors += 1
                warnings_and_errors.append({
                    "level": "error",
                    "type": "DataMismatch",
                    "message": f"Data mismatch for model: {str(e)}",
                    "source": f"Model: {model.get('model_name')}",
                    "timestamp": datetime.now().isoformat()
                })
                # Continue processing other models instead of failing entirely
                continue
            
            # Determine model layer from tags
            model_layer = "staging"  # default
            tags = model.get("tags", [])
            for tag in tags:
                if tag in ["staging", "intermediate", "marts", "report"]:
                    model_layer = tag
                    break
            
            # Determine model type (materialization)
            model_type = model.get("materialization", "table")
            
            # Build model entry with incomplete data flag
            model_entry = {
                "model_name": model.get("model_name", "unknown"),
                "model_id": model.get("unique_id", ""),
                "model_type": model_type,
                "model_layer": model_layer,
                "status": model.get("status", "skipped"),
                "execution_time_seconds": kpi_metrics["execution_time_seconds"],
                "rows_produced": kpi_metrics["rows_produced"],
                "bytes_scanned": kpi_metrics["bytes_scanned"],
                "output_hash": kpi_metrics["output_hash"],
                "hash_calculation_method": kpi_metrics["hash_calculation_method"],
                "join_count": kpi_metrics["join_count"],
                "cte_count": kpi_metrics["cte_count"],
                "window_function_count": kpi_metrics["window_function_count"],
                "estimated_credits": kpi_metrics["estimated_credits"],
                "estimated_cost_usd": kpi_metrics["estimated_cost_usd"],
                "materialization": model_type,
                "tags": model.get("tags", []),
                "kpi_data_complete": is_complete
            }
            
            models_array.append(model_entry)
            
            # Accumulate summary statistics
            total_exec_time += kpi_metrics["execution_time_seconds"]
            total_rows += kpi_metrics["rows_produced"]
            total_bytes += kpi_metrics["bytes_scanned"]
            total_credits += kpi_metrics["estimated_credits"]
            total_cost += kpi_metrics["estimated_cost_usd"]
            
            # Track errors
            if model.get("status") in ["error", "partial"]:
                models_with_errors += 1
            
            # Flag data quality issues
            if kpi_metrics["output_hash"] is None and model_type != "view":
                data_quality_flags.append({
                    "model_name": model.get("model_name"),
                    "flag_type": "missing_hash",
                    "severity": "warning",
                    "message": f"Model {model.get('model_name')} could not be hashed",
                    "timestamp": datetime.now().isoformat()
                })
            
            if kpi_metrics["rows_produced"] == 0:
                data_quality_flags.append({
                    "model_name": model.get("model_name"),
                    "flag_type": "zero_rows",
                    "severity": "info",
                    "message": f"Model {model.get('model_name')} produced zero rows",
                    "timestamp": datetime.now().isoformat()
                })
            
        except Exception as e:
            logger.error(f"Error processing model {model.get('model_name', 'unknown')}: {str(e)}", exc_info=True)
            logger.error(f"  Remediation: Check that model definition and execution data are both available")
            models_with_errors += 1
            warnings_and_errors.append({
                "level": "error",
                "type": "ModelProcessingError",
                "message": f"Failed to process model: {str(e)}",
                "source": f"Model: {model.get('model_name')}",
                "timestamp": datetime.now().isoformat()
            })
    
    # Calculate summary statistics
    avg_exec_time = total_exec_time / len(pipeline_models) if pipeline_models else 0.0
    
    # Calculate average complexity
    total_complexity = sum(
        m.get("join_count", 0) + m.get("cte_count", 0) + m.get("window_function_count", 0)
        for m in models_array
    )
    avg_complexity = total_complexity / len(models_array) if models_array else 0.0
    
    # Calculate data quality score (based on successful hash calculations)
    hashes_successful = sum(1 for m in models_array if m["output_hash"] is not None)
    hash_success_rate = hashes_successful / len(models_array) if models_array else 0.0
    data_quality_score = int(hash_success_rate * 100)
    
    # Build final report
    report = {
        "schema_version": "1.0.0",
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "pipeline_name": pipeline_tag,
            "models_processed": len(pipeline_models),
            "total_duration_seconds": total_exec_time,
            "dbt_artifacts_version": run_results.get("metadata", {}).get("dbt_schema_version", "unknown"),
            "dbt_version": run_results.get("metadata", {}).get("dbt_version", "unknown")
        },
        "models": models_array,
        "summary": {
            "total_execution_time_seconds": total_exec_time,
            "total_models_processed": len(pipeline_models),
            "models_with_errors": models_with_errors,
            "models_with_incomplete_data": models_with_incomplete_data,
            "total_rows_produced": total_rows,
            "total_bytes_scanned": total_bytes,
            "total_estimated_credits": total_credits,
            "total_estimated_cost_usd": total_cost,
            "average_execution_time_seconds": avg_exec_time,
            "average_model_complexity": avg_complexity,
            "data_quality_score": data_quality_score,
            "hash_validation_success_rate": hash_success_rate
        },
        "data_quality_flags": data_quality_flags,
        "warnings_and_errors": warnings_and_errors
    }
    
    logger.info(f"Report built: {len(models_array)} models, {len(data_quality_flags)} data quality flags")
    return report


def main() -> int:
    """
    Main orchestration function for report generation.
    
    Implements the complete pipeline:
    1. Parse CLI arguments
    2. Load config
    3. Set up logging
    4. Load artifacts
    5. Extract models and KPIs
    6. Build report
    7. Validate schema
    8. Write output
    9. Log summary
    
    Returns:
        0 on success, 1 on critical errors
    """
    start_time = datetime.now()
    
    try:
        # 1. Parse CLI arguments
        args = parse_arguments()
        
        # 2. Set up logging BEFORE other operations
        logger = setup_logging(f"pipeline_{args.pipeline}")
        logger.info("=" * 80)
        logger.info("BENCHMARK REPORT GENERATION")
        logger.info("=" * 80)
        logger.info(f"Pipeline: {args.pipeline}")
        logger.info(f"Log Level: {args.log_level}")
        
        # 3. Determine output path
        if args.output:
            output_path = Path(args.output)
        else:
            output_path = get_default_output_path(args.pipeline)
        
        logger.info(f"Output path: {output_path}")
        
        # 4. Load config (with environment overrides)
        logger.info("Loading configuration...")
        try:
            config = load_config()
            logger.debug(f"Configuration loaded with {len(config)} sections")
            
            # Validate config structure
            if not isinstance(config, dict):
                raise ConfigError("Configuration must be a dictionary")
            required_config_sections = ["kpi_definitions", "bottleneck_thresholds", "pricing"]
            missing_sections = [s for s in required_config_sections if s not in config]
            if missing_sections:
                raise ConfigError(
                    f"Configuration missing required sections: {', '.join(missing_sections)}\n"
                    f"  Ensure config.py contains all required definitions.\n"
                    f"  Remediation: Check that config.py has not been corrupted or modified."
                )
            
            logger.debug(f"✓ Configuration validated with {len(config)} sections")
        except ConfigError as e:
            logger.error(f"Configuration error: {str(e)}")
            logger.error("  Remediation: Verify config.py is valid and all required sections exist")
            return 1
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}", exc_info=True)
            logger.error("  Remediation: Check config.py syntax and environment variable overrides")
            return 1
        
        # 5. Load dbt artifacts
        logger.info("Phase 1: Loading dbt artifacts...")
        try:
            # Verify artifact files exist before loading (critical failure condition)
            manifest_path = "target/manifest.json"
            run_results_path = "target/run_results.json"
            
            if not Path(manifest_path).exists():
                raise MissingArtifact(
                    f"Required artifact not found: {manifest_path}\n"
                    f"  Expected location: {Path(manifest_path).absolute()}\n"
                    f"  Remediation: Run 'dbt parse' or 'dbt run' to generate dbt artifacts"
                )
            
            if not Path(run_results_path).exists():
                raise MissingArtifact(
                    f"Required artifact not found: {run_results_path}\n"
                    f"  Expected location: {Path(run_results_path).absolute()}\n"
                    f"  Remediation: Run 'dbt run' or 'dbt build' to generate execution results"
                )
            
            manifest = load_manifest(manifest_path, logger)
            run_results = load_run_results(run_results_path, logger)
            
            # Validate artifact contents (required fields)
            manifest_required_keys = ["metadata", "nodes"]
            manifest_missing = [k for k in manifest_required_keys if k not in manifest]
            if manifest_missing:
                raise InvalidSchema(
                    f"manifest.json missing required keys: {', '.join(manifest_missing)}\n"
                    f"  Expected: {', '.join(manifest_required_keys)}\n"
                    f"  Remediation: Regenerate dbt artifacts with 'dbt parse' or 'dbt compile'"
                )
            
            run_results_required_keys = ["metadata", "results"]
            run_results_missing = [k for k in run_results_required_keys if k not in run_results]
            if run_results_missing:
                raise InvalidSchema(
                    f"run_results.json missing required keys: {', '.join(run_results_missing)}\n"
                    f"  Expected: {', '.join(run_results_required_keys)}\n"
                    f"  Remediation: Re-run 'dbt run' or 'dbt build' to regenerate execution results"
                )
            
            logger.info("✓ Artifacts loaded and validated successfully")
        except MissingArtifact as e:
            logger.error(f"Missing artifact (CRITICAL): {str(e)}")
            logger.error("  This is a critical failure - cannot proceed without dbt artifacts")
            return 1
        except InvalidSchema as e:
            logger.error(f"Invalid artifact schema (CRITICAL): {str(e)}")
            logger.error("  This is a critical failure - artifacts are malformed")
            return 1
        
        # 6. Extract all models
        logger.info("Phase 2: Extracting model data...")
        nodes = manifest.get("nodes", {})
        model_ids = [node_id for node_id in nodes if node_id.startswith("model.")]
        logger.info(f"Found {len(model_ids)} models in manifest")
        
        parsed_models = []
        for i, model_id in enumerate(model_ids, 1):
            try:
                model_data = extract_model_data(model_id, manifest, run_results, logger)
                parsed_models.append(model_data)
                logger.debug(f"[{i}/{len(model_ids)}] Extracted: {model_data.get('model_name')}")
            except Exception as e:
                logger.warning(f"Error extracting model {model_id}: {str(e)}")
        
        logger.info(f"✓ Extracted {len(parsed_models)} models")
        
        # 7. Build report
        logger.info("Phase 3: Building report structure...")
        report = build_report(args.pipeline, manifest, run_results, parsed_models, logger)
        logger.info("✓ Report structure built")
        
        # 8. Validate against schema
        logger.info("Phase 4: Validating report against schema...")
        try:
            is_valid, errors = schema_validator(report)
            if not is_valid:
                logger.warning(f"Schema validation errors: {len(errors)}")
                for error in errors[:5]:  # Show first 5 errors
                    logger.warning(f"  - {error}")
                if len(errors) > 5:
                    logger.warning(f"  ... and {len(errors) - 5} more validation errors")
            else:
                logger.info("✓ Report validates against schema")
        except ConfigError as e:
            logger.warning(f"Schema validation unavailable (jsonschema not installed): {str(e)}")
            logger.info("  Proceeding without validation")
        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}")
            # Don't fail, just warn
        
        # 9. Write report to file
        logger.info("Phase 5: Writing report to file...")
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"✓ Report written to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to write report: {str(e)}")
            return 1
        
        # 10. Log execution summary
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Pipeline: pipeline_{args.pipeline}")
        logger.info(f"Models Processed: {report['summary']['total_models_processed']}")
        logger.info(f"Models with Errors: {report['summary']['models_with_errors']}")
        logger.info(f"Models with Incomplete Data: {report['summary'].get('models_with_incomplete_data', 0)}")
        logger.info(f"Total Execution Time (all models): {report['summary']['total_execution_time_seconds']:.2f}s")
        logger.info(f"Average Execution Time (per model): {report['summary']['average_execution_time_seconds']:.2f}s")
        logger.info(f"Total Rows Produced: {report['summary']['total_rows_produced']:,}")
        logger.info(f"Total Bytes Scanned (estimated): {report['summary']['total_bytes_scanned']:,} ({report['summary']['total_bytes_scanned'] / (1024**3):.2f} GB)")
        logger.info(f"Total Estimated Credits: {report['summary']['total_estimated_credits']:.2f}")
        logger.info(f"Total Estimated Cost: ${report['summary']['total_estimated_cost_usd']:.2f}")
        logger.info(f"Data Quality Score: {report['summary']['data_quality_score']}%")
        logger.info(f"Data Quality Flags: {len(report['data_quality_flags'])}")
        logger.info(f"Script Execution Time: {elapsed:.2f}s")
        logger.info("=" * 80)
        
        # Log summary of report status (success, partial, or warning)
        if report['summary']['models_with_errors'] == 0:
            if report['summary'].get('models_with_incomplete_data', 0) == 0:
                logger.info("✓ Report generation completed successfully (all models with complete data)")
            else:
                logger.warning(f"✓ Report generation completed with warnings ({report['summary']['models_with_incomplete_data']} models with incomplete data)")
        else:
            logger.warning(f"✓ Partial report generated ({report['summary']['models_with_errors']} models with errors)")
        
        # Return 0 for both success and partial/warning cases (only return 1 for critical failures)
        return 0
        
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
