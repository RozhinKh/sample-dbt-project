#!/usr/bin/env python3
"""
Shared Utility Module for Benchmarking System

Provides foundational support functions for JSON handling, schema validation,
credential parsing, logging setup, and custom exception classes. This module
serves as a centralized location for utilities used across all benchmarking
scripts.

Usage:
    from helpers import load_json_safe, validate_report_schema, parse_profiles_yml
    from helpers import setup_logging, MissingArtifact, InvalidSchema
    
    # Load JSON safely
    data = load_json_safe("benchmark/pipeline_a/baseline/report.json")
    
    # Validate schema
    is_valid, errors = validate_report_schema(data, "report")
    
    # Parse dbt profiles
    creds = parse_profiles_yml("my_profile")
    
    # Setup logging
    logger = setup_logging("pipeline_a")
"""

import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime

try:
    import yaml
except ImportError:
    yaml = None


# ============================================================================
# CUSTOM EXCEPTION CLASSES
# ============================================================================

class MissingArtifact(Exception):
    """
    Raised when dbt artifacts (manifest.json, run_results.json) are not found.
    
    This exception indicates that required dbt output files are missing from
    the expected locations, preventing analysis or validation.
    """
    pass


class InvalidSchema(Exception):
    """
    Raised when JSON structure or data types don't match expected schema.
    
    This exception indicates that the JSON document structure, required fields,
    or data types don't conform to the schema specification. Includes details
    about which fields or types were invalid.
    """
    pass


class DataMismatch(Exception):
    """
    Raised when data inconsistencies are detected (e.g., hash mismatch).
    
    This exception indicates that data validation checks failed, such as when
    a computed hash doesn't match an expected value or output data differs
    between baseline and candidate runs.
    """
    pass


class ConfigError(Exception):
    """
    Raised when configuration is invalid or missing.
    
    This exception indicates that required configuration values are missing,
    invalid, or inaccessible. May occur during profile parsing or config loading.
    """
    pass


# ============================================================================
# SCHEMA DEFINITIONS
# ============================================================================

def get_schema_definitions() -> Dict[str, Dict[str, Any]]:
    """
    Return schema definitions for different report types.
    
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary of schema definitions indexed by type
    """
    return {
        "report": {
            "required_fields": [
                "execution_time_seconds",
                "query_hash",
                "row_count",
                "bytes_scanned"
            ],
            "field_types": {
                "execution_time_seconds": (float, int),
                "query_hash": str,
                "row_count": int,
                "bytes_scanned": int,
                "data_hash": str,
                "timestamp": str,
                "query_id": str,
                "join_count": int,
                "cte_count": int,
                "window_function_count": int
            }
        },
        "analysis": {
            "required_fields": [
                "baseline_report",
                "candidate_report",
                "comparison_results"
            ],
            "field_types": {
                "baseline_report": dict,
                "candidate_report": dict,
                "comparison_results": dict,
                "timestamp": str,
                "pipeline_name": str
            }
        }
    }


# ============================================================================
# PATH RESOLUTION UTILITIES
# ============================================================================

def resolve_dbt_profile_paths() -> List[Path]:
    """
    Resolve standard dbt profile.yml search paths.
    
    dbt looks for profiles.yml in the following order:
    1. Current working directory
    2. ~/.dbt/ directory (user home)
    3. dbt_project.yml directory
    
    Returns:
        List[Path]: List of possible profile paths in search order
    """
    paths = []
    
    # Current directory
    paths.append(Path.cwd() / "profiles.yml")
    
    # Home directory ~/.dbt/
    home = Path.home()
    paths.append(home / ".dbt" / "profiles.yml")
    
    # dbt_project directory
    dbt_project_path = Path.cwd() / "dbt_project.yml"
    if dbt_project_path.exists():
        paths.append(Path.cwd() / "profiles.yml")
    
    return paths


def get_project_root() -> Path:
    """
    Get the project root directory (contains dbt_project.yml).
    
    Returns:
        Path: Project root directory
        
    Raises:
        ConfigError: If dbt_project.yml is not found
    """
    current = Path.cwd()
    
    # Check current directory
    if (current / "dbt_project.yml").exists():
        return current
    
    # Check parent directories
    for parent in current.parents:
        if (parent / "dbt_project.yml").exists():
            return parent
    
    raise ConfigError(
        "Could not find dbt project root (dbt_project.yml). "
        "Run this script from within a dbt project directory."
    )


def ensure_logs_directory() -> Path:
    """
    Ensure benchmark/logs directory exists.
    
    Returns:
        Path: Path to benchmark/logs directory
    """
    logs_dir = Path.cwd() / "benchmark" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


# ============================================================================
# JSON HANDLING
# ============================================================================

def load_json_safe(file_path: str) -> Dict[str, Any]:
    """
    Load JSON from file path with comprehensive error handling.
    
    Attempts to load and parse JSON from the specified file path. Provides
    specific error messages for missing files and malformed JSON.
    
    Args:
        file_path (str): Path to JSON file to load
    
    Returns:
        Dict[str, Any]: Parsed JSON as dictionary
    
    Raises:
        MissingArtifact: If file does not exist
        InvalidSchema: If file contains malformed JSON or is not valid JSON
    
    Examples:
        >>> data = load_json_safe("report.json")
        >>> print(data.keys())
    """
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        raise MissingArtifact(
            f"Required artifact not found: {file_path}\n"
            f"  Expected location: {path.absolute()}\n"
            f"  Please ensure the dbt run has completed and artifacts are present."
        )
    
    # Check if file is readable
    if not path.is_file():
        raise MissingArtifact(
            f"Path is not a file: {file_path}"
        )
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise InvalidSchema(
            f"Malformed JSON in {file_path}:\n"
            f"  Error: {str(e)}\n"
            f"  Line: {e.lineno}, Column: {e.colno}\n"
            f"  Please verify the JSON file is valid."
        )
    except (IOError, OSError) as e:
        raise MissingArtifact(
            f"Error reading file {file_path}: {str(e)}"
        )


# ============================================================================
# SCHEMA VALIDATION
# ============================================================================

def validate_report_schema(
    report_dict: Dict[str, Any],
    schema_type: str = "report"
) -> Tuple[bool, List[str]]:
    """
    Validate report dictionary against expected schema.
    
    Checks that required fields are present and have correct data types.
    Supports multiple schema types (report, analysis, etc.) for extensibility.
    
    Args:
        report_dict (Dict[str, Any]): Report dictionary to validate
        schema_type (str): Type of schema to validate against ("report" or "analysis")
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
            - is_valid: True if schema is valid, False otherwise
            - error_messages: List of validation error messages (empty if valid)
    
    Raises:
        InvalidSchema: If schema_type is not recognized
    
    Examples:
        >>> report = {"execution_time_seconds": 1.5, "query_hash": "abc123", ...}
        >>> is_valid, errors = validate_report_schema(report, "report")
        >>> if not is_valid:
        ...     for error in errors:
        ...         print(f"  - {error}")
    """
    schemas = get_schema_definitions()
    
    if schema_type not in schemas:
        raise InvalidSchema(
            f"Unknown schema type: {schema_type}\n"
            f"  Supported types: {', '.join(schemas.keys())}"
        )
    
    schema = schemas[schema_type]
    errors = []
    
    # Check required fields
    required_fields = schema.get("required_fields", [])
    for field in required_fields:
        if field not in report_dict:
            errors.append(f"Missing required field: '{field}'")
    
    # Check field types
    field_types = schema.get("field_types", {})
    for field, expected_types in field_types.items():
        if field in report_dict:
            value = report_dict[field]
            
            # Handle both single type and tuple of types
            if isinstance(expected_types, tuple):
                if not isinstance(value, expected_types):
                    type_names = " or ".join(t.__name__ for t in expected_types)
                    errors.append(
                        f"Field '{field}' has wrong type: "
                        f"expected {type_names}, got {type(value).__name__}"
                    )
            else:
                if not isinstance(value, expected_types):
                    errors.append(
                        f"Field '{field}' has wrong type: "
                        f"expected {expected_types.__name__}, got {type(value).__name__}"
                    )
    
    is_valid = len(errors) == 0
    return is_valid, errors


def schema_validator(
    report_dict: Dict[str, Any],
    schema_file: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """
    Validate report.json against JSON Schema (Draft 7).
    
    Performs comprehensive validation of a report dictionary against the JSON Schema
    definition. Supports both file-based schema and inline schema for flexibility.
    This validator is more comprehensive than validate_report_schema() and checks
    type correctness, required fields, patterns, enums, and value ranges.
    
    Args:
        report_dict (Dict[str, Any]): Report dictionary to validate
        schema_file (Optional[str]): Path to JSON Schema file. If None, uses default path.
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
            - is_valid: True if report validates successfully, False otherwise
            - error_messages: List of detailed validation error messages
    
    Raises:
        MissingArtifact: If schema file not found and no default available
        InvalidSchema: If schema file contains malformed JSON
        ConfigError: If jsonschema package is not available
    
    Examples:
        >>> report = load_json_safe("report.json")
        >>> is_valid, errors = schema_validator(report)
        >>> if is_valid:
        ...     print("✓ Report is valid")
        ... else:
        ...     print("✗ Validation errors:")
        ...     for error in errors:
        ...         print(f"  - {error}")
        
        >>> # With custom schema file
        >>> is_valid, errors = schema_validator(report, "custom_schema.json")
    """
    try:
        import jsonschema
        from jsonschema import Draft7Validator, FormatChecker
    except ImportError:
        raise ConfigError(
            "JSON Schema validation requires 'jsonschema' package.\n"
            "Install with: pip install jsonschema"
        )
    
    # Determine schema file path
    if schema_file is None:
        # Use default path relative to project root
        project_root = get_project_root()
        schema_file = project_root / "benchmark" / "schemas" / "report.json.schema"
    else:
        schema_file = Path(schema_file)
    
    # Load schema
    if not schema_file.exists():
        raise MissingArtifact(
            f"Schema file not found: {schema_file}\n"
            f"  Expected location: {schema_file.absolute()}\n"
            f"  Run from project root or provide explicit schema_file path"
        )
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = json.load(f)
    except json.JSONDecodeError as e:
        raise InvalidSchema(
            f"Malformed JSON in schema file {schema_file}:\n"
            f"  Error: {str(e)}\n"
            f"  Line: {e.lineno}, Column: {e.colno}"
        )
    except (IOError, OSError) as e:
        raise MissingArtifact(
            f"Error reading schema file {schema_file}: {str(e)}"
        )
    
    # Validate report against schema
    errors = []
    validator = Draft7Validator(schema, format_checker=FormatChecker())
    
    for error in validator.iter_errors(report_dict):
        # Build detailed error message
        path = ".".join(str(p) for p in error.path) if error.path else "[root]"
        
        if error.validator == "required":
            # Missing required field
            missing_fields = error.validator_value
            found_fields = set(error.instance.keys()) if isinstance(error.instance, dict) else set()
            missing = [f for f in missing_fields if f not in found_fields]
            msg = f"Missing required field(s) at {path}: {', '.join(missing)}"
        
        elif error.validator == "type":
            # Type mismatch
            expected = error.validator_value
            actual = type(error.instance).__name__
            msg = f"Type mismatch at {path}: expected {expected}, got {actual}"
        
        elif error.validator == "enum":
            # Value not in allowed enum
            allowed = error.validator_value
            msg = f"Invalid value at {path}: {error.instance!r} not in {allowed}"
        
        elif error.validator == "pattern":
            # Value doesn't match required pattern
            pattern = error.validator_value
            msg = f"Pattern mismatch at {path}: {error.instance!r} does not match {pattern}"
        
        elif error.validator == "minimum":
            # Value below minimum
            minimum = error.validator_value
            msg = f"Value too small at {path}: {error.instance} < {minimum}"
        
        elif error.validator == "maximum":
            # Value exceeds maximum
            maximum = error.validator_value
            msg = f"Value too large at {path}: {error.instance} > {maximum}"
        
        elif error.validator == "minItems":
            # Array too short
            min_items = error.validator_value
            msg = f"Array too short at {path}: {len(error.instance)} < {min_items}"
        
        elif error.validator == "additionalProperties":
            # Extra properties not allowed in schema
            msg = f"Unexpected property(ies) at {path}: {error.message}"
        
        else:
            # Generic error message
            msg = f"Schema validation error at {path}: {error.message}"
        
        errors.append(msg)
    
    is_valid = len(errors) == 0
    return is_valid, errors


# ============================================================================
# YAML/PROFILE PARSING
# ============================================================================

ENV_VAR_PATTERN = re.compile(
    r"\{\{\s*env_var\(\s*'([^']+)'\s*(?:,\s*'([^']*)')?\s*\)\s*\}\}"
)


def _resolve_env_vars(value: Any) -> Any:
    """Resolve dbt-style env_var() Jinja expressions in profiles.yml values."""
    if isinstance(value, str):
        def _replace(match: re.Match) -> str:
            env_name = match.group(1)
            default = match.group(2)
            return os.getenv(env_name, default if default is not None else "")

        return ENV_VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    return value


def parse_profiles_yml(profile_name: str) -> Dict[str, str]:
    """
    Parse dbt's profiles.yml and extract Snowflake connection details.
    
    Locates profiles.yml in standard dbt locations, parses YAML, and extracts
    Snowflake connection configuration for the specified profile.
    
    Args:
        profile_name (str): Name of the profile to extract from profiles.yml
    
    Returns:
        Dict[str, str]: Dictionary with keys:
            - account: Snowflake account identifier
            - user: Snowflake username
            - password: Snowflake password
            - warehouse: Warehouse name
            - database: Database name
            - schema: Schema name
    
    Raises:
        ConfigError: If profiles.yml not found or profile not found in file
        ConfigError: If YAML is malformed or required fields missing
    
    Examples:
        >>> creds = parse_profiles_yml("dev")
        >>> print(f"Connecting to {creds['account']} as {creds['user']}")
    """
    if yaml is None:
        raise ConfigError(
            "YAML support requires 'pyyaml' package. "
            "Install with: pip install pyyaml"
        )
    
    # Search for profiles.yml
    profile_paths = resolve_dbt_profile_paths()
    profile_file = None
    
    for path in profile_paths:
        if path.exists():
            profile_file = path
            break
    
    if profile_file is None:
        raise ConfigError(
            f"profiles.yml not found in standard dbt locations:\n"
            f"  Searched:\n"
            f"    - {profile_paths[0]} (current directory)\n"
            f"    - {profile_paths[1]} (user home .dbt/)\n"
            f"  Please create profiles.yml or set its location explicitly."
        )
    
    # Parse YAML
    try:
        with open(profile_file, 'r', encoding='utf-8') as f:
            profiles = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(
            f"Malformed YAML in {profile_file}:\n"
            f"  Error: {str(e)}"
        )
    except (IOError, OSError) as e:
        raise ConfigError(
            f"Error reading {profile_file}: {str(e)}"
        )
    
    if not profiles or not isinstance(profiles, dict):
        raise ConfigError(
            f"profiles.yml is empty or invalid format in {profile_file}"
        )
    
    # Extract profile
    if profile_name not in profiles:
        available = list(profiles.keys())
        raise ConfigError(
            f"Profile '{profile_name}' not found in {profile_file}\n"
            f"  Available profiles: {', '.join(available)}"
        )
    
    profile = profiles[profile_name]
    
    # Handle outputs section (dbt standard)
    if "outputs" in profile and isinstance(profile["outputs"], dict):
        # Find the default output or first output
        target = profile.get("target", next(iter(profile["outputs"].keys())))
        if target in profile["outputs"]:
            config = profile["outputs"][target]
        else:
            config = next(iter(profile["outputs"].values()))
    else:
        config = profile
    
    if not isinstance(config, dict):
        raise ConfigError(
            f"Profile '{profile_name}' configuration is not a dictionary"
        )

    # Resolve env_var() expressions to concrete values
    config = _resolve_env_vars(config)

    # Extract required Snowflake connection fields
    required_fields = ["account", "user", "password", "warehouse", "database", "schema"]
    credentials = {}
    missing_fields = []
    
    for field in required_fields:
        if field in config and config[field] not in (None, ""):
            credentials[field] = config[field]
        else:
            missing_fields.append(field)
    
    if missing_fields:
        raise ConfigError(
            f"Profile '{profile_name}' missing required Snowflake fields:\n"
            f"  Missing: {', '.join(missing_fields)}\n"
            f"  Expected fields: {', '.join(required_fields)}\n"
            f"  Location: {profile_file}"
        )
    
    return credentials


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(pipeline_name: str) -> logging.Logger:
    """
    Set up and configure logger with file and console handlers.
    
    Creates a logger that writes to both console and a timestamped log file
    in benchmark/logs/. Log format includes timestamp with milliseconds, log
    level, and module name for complete debugging context. Log level is
    configurable via BENCHMARK_LOG_LEVEL environment variable (default: INFO).
    
    The log file is named with millisecond precision to support multiple runs
    per day without conflicts. Log format is optimized for human readability
    and actionable debugging.
    
    Args:
        pipeline_name (str): Name of the pipeline (used in log filename)
    
    Returns:
        logging.Logger: Configured logger instance
    
    Log Format:
        [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [module_name] message
        
        Example:
            [2024-01-15 14:30:22.123] [INFO] [generate_report] Processing model: stg_trades (1 of 42)
    
    Log Levels Usage:
        DEBUG:   Model processing details, artifact parsing steps, config values loaded
                 logger.debug("Loading manifest.json from target/")
                 logger.debug("Parsed 42 models from artifact")
        
        INFO:    Metrics extracted per model, summary statistics, script completion
                 logger.info("Extracted 156 metrics from model: stg_customers")
                 logger.info("Pipeline completed: 42 models, 3 failures")
        
        WARNING: Data quality issues (hash mismatches, zero values, missing fields), thresholds
                 logger.warning("Data hash mismatch for model stg_users: expected abc123, got def456")
                 logger.warning("Zero-value detected in metric revenue_sum for model fact_orders")
        
        ERROR:   Exceptions, failed parsing, validation failures with full stack traces
                 logger.error("Failed to parse run_results.json", exc_info=True)
                 logger.error("Schema validation failed for report artifact", exc_info=True)
    
    Examples:
        >>> logger = setup_logging("pipeline_a")
        >>> logger.info("Starting benchmark run")
        >>> logger.debug("Processing 42 models")
        >>> logger.warning("Hash mismatch detected in stg_users")
        >>> try:
        ...     parse_artifact()
        ... except Exception as e:
        ...     logger.error("Failed to parse artifact", exc_info=True)
    """
    # Ensure logs directory exists
    logs_dir = ensure_logs_directory()
    
    # Create logger
    logger = logging.getLogger(pipeline_name)
    
    # Check if logger already has handlers (avoid duplicate logs)
    if logger.handlers:
        return logger
    
    # Set log level from environment or default
    log_level_str = os.getenv("BENCHMARK_LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)
    
    # Create timestamp-based filename with millisecond precision to avoid conflicts
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Milliseconds, not microseconds
    log_file = logs_dir / f"{timestamp}_{pipeline_name}.log"
    
    # Create custom formatter that includes milliseconds and formatted brackets
    class MillisecondFormatter(logging.Formatter):
        """Custom formatter to include milliseconds in timestamp and format with brackets."""
        
        def format(self, record):
            # Format timestamp with milliseconds: [YYYY-MM-DD HH:MM:SS.mmm]
            ct = self.converter(record.created)
            timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
            ms = int((record.created - int(record.created)) * 1000)
            timestamp_with_ms = f"{timestamp}.{ms:03d}"
            
            # Format: [TIMESTAMP] [LEVEL] [MODULE] message
            formatted = (
                f"[{timestamp_with_ms}] "
                f"[{record.levelname}] "
                f"[{record.name}] "
                f"{record.getMessage()}"
            )
            
            # If there's exception info, append the stack trace
            if record.exc_info:
                formatted += "\n" + self.formatException(record.exc_info)
            
            return formatted
    
    formatter = MillisecondFormatter()
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (force UTF-8 with replacement to avoid Windows encoding errors)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Log startup message
    logger.info(f"Logging initialized for pipeline: {pipeline_name}")
    logger.debug(f"Log file: {log_file}")
    logger.debug(f"Log level: {log_level_str}")
    
    return logger


# ============================================================================
# dBT ARTIFACT PARSING
# ============================================================================

def load_manifest(manifest_path: str = "target/manifest.json", logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Load and parse manifest.json from dbt target directory.
    
    Extracts model definitions, raw SQL, compiled SQL, dependencies, tags, and
    materialization types from the manifest.json artifact.
    
    Args:
        manifest_path (str): Path to manifest.json file
        logger (Optional[logging.Logger]): Logger instance for progress tracking
    
    Returns:
        Dict[str, Any]: Parsed manifest data with nodes and metadata
    
    Raises:
        MissingArtifact: If manifest.json is not found
        InvalidSchema: If manifest.json contains malformed JSON
    
    Examples:
        >>> manifest = load_manifest()
        >>> print(f"Loaded {len(manifest['nodes'])} model nodes")
    """
    if logger:
        logger.info(f"Loading manifest from: {manifest_path}")
    
    # Load and validate JSON structure
    manifest = load_json_safe(manifest_path)
    
    # Validate root structure
    if not isinstance(manifest, dict):
        raise InvalidSchema(
            f"manifest.json root must be a dictionary, got {type(manifest).__name__}"
        )
    
    # Check for required top-level fields
    if "metadata" not in manifest:
        raise InvalidSchema(
            "manifest.json missing required field: 'metadata'\n"
            "  This field should contain dbt schema version and project information."
        )
    
    if "nodes" not in manifest:
        raise InvalidSchema(
            "manifest.json missing required field: 'nodes'\n"
            "  This field should contain all model definitions."
        )
    
    # Validate metadata structure
    metadata = manifest.get("metadata", {})
    if not isinstance(metadata, dict):
        raise InvalidSchema("manifest.json 'metadata' must be a dictionary")
    
    # Validate nodes structure
    nodes = manifest.get("nodes", {})
    if not isinstance(nodes, dict):
        raise InvalidSchema("manifest.json 'nodes' must be a dictionary")
    
    # Log parsing summary
    model_count = sum(1 for node_id in nodes if node_id.startswith("model."))
    if logger:
        logger.debug(f"Manifest parsed: {model_count} models found in {len(nodes)} total nodes")
    
    return manifest


def load_run_results(run_results_path: str = "target/run_results.json", logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Load and parse run_results.json from dbt target directory.
    
    Extracts execution timing, adapter responses, rows affected, query status,
    and relation names for all executed models.
    
    Args:
        run_results_path (str): Path to run_results.json file
        logger (Optional[logging.Logger]): Logger instance for progress tracking
    
    Returns:
        Dict[str, Any]: Parsed run_results data with metadata and execution results
    
    Raises:
        MissingArtifact: If run_results.json is not found
        InvalidSchema: If run_results.json contains malformed JSON
    
    Examples:
        >>> results = load_run_results()
        >>> print(f"Loaded {len(results['results'])} execution results")
    """
    if logger:
        logger.info(f"Loading run_results from: {run_results_path}")
    
    # Load and validate JSON structure
    run_results = load_json_safe(run_results_path)
    
    # Validate root structure
    if not isinstance(run_results, dict):
        raise InvalidSchema(
            f"run_results.json root must be a dictionary, got {type(run_results).__name__}"
        )
    
    # Check for required top-level fields
    if "metadata" not in run_results:
        raise InvalidSchema(
            "run_results.json missing required field: 'metadata'\n"
            "  This field should contain dbt schema version and invocation information."
        )
    
    if "results" not in run_results:
        raise InvalidSchema(
            "run_results.json missing required field: 'results'\n"
            "  This field should contain execution results for all models."
        )
    
    # Validate metadata structure
    metadata = run_results.get("metadata", {})
    if not isinstance(metadata, dict):
        raise InvalidSchema("run_results.json 'metadata' must be a dictionary")
    
    # Validate results structure
    results = run_results.get("results", [])
    if not isinstance(results, list):
        raise InvalidSchema("run_results.json 'results' must be a list")
    
    # Log parsing summary
    success_count = sum(1 for r in results if r.get("status") == "success")
    if logger:
        logger.debug(f"Run results parsed: {success_count}/{len(results)} models succeeded")
    
    return run_results


def extract_model_data(model_id: str, manifest: Dict[str, Any], run_results: Dict[str, Any], logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Extract model-specific data from both manifest.json and run_results.json.
    
    Combines model definition from manifest with execution telemetry from run_results
    to produce a complete picture of model metadata and performance.
    
    Args:
        model_id (str): Model unique_id from manifest (e.g., "model.project.stg_users")
        manifest (Dict[str, Any]): Parsed manifest.json
        run_results (Dict[str, Any]): Parsed run_results.json
        logger (Optional[logging.Logger]): Logger instance for progress tracking
    
    Returns:
        Dict[str, Any]: Extracted model data with metadata, SQL, dependencies, and execution metrics
    
    Raises:
        KeyError: If model_id not found in manifest
    
    Examples:
        >>> model_data = extract_model_data("model.project.stg_users", manifest, run_results)
        >>> print(f"Model: {model_data['model_name']}, Execution Time: {model_data['execution_time']}s")
    """
    # Extract model definition from manifest
    nodes = manifest.get("nodes", {})
    if model_id not in nodes:
        raise KeyError(f"Model not found in manifest: {model_id}")
    
    model_node = nodes[model_id]
    
    # Extract basic model info
    model_data = {
        "unique_id": model_id,
        "model_name": model_node.get("name", ""),
        "database": model_node.get("database", ""),
        "schema": model_node.get("schema", ""),
        "relation_name": model_node.get("relation_name", ""),
        "materialization": model_node.get("config", {}).get("materialized", ""),
        "tags": model_node.get("tags", []),
        "raw_sql": model_node.get("raw_code", ""),
        "meta": model_node.get("meta", {}),
    }
    
    # Extract compiled SQL if available
    if model_node.get("compiled"):
        model_data["compiled_sql"] = model_node.get("compiled_code", "")
    
    # Extract dependencies
    depends_on = model_node.get("depends_on", {})
    model_data["dependencies"] = {
        "macros": depends_on.get("macros", []),
        "nodes": depends_on.get("nodes", []),
        "refs": model_node.get("refs", []),
        "sources": model_node.get("sources", [])
    }
    
    # Find corresponding execution result in run_results
    results = run_results.get("results", [])
    execution_result = next((r for r in results if r.get("unique_id") == model_id), None)
    
    # Extract execution telemetry
    if execution_result:
        # Calculate total execution time from timing array
        timing_info = execution_result.get("timing", [])
        total_time = 0.0
        for timing in timing_info:
            if isinstance(timing, dict):
                start = timing.get("started_at", "")
                completed = timing.get("completed_at", "")
                if start and completed:
                    # Rough calculation - proper implementation would parse ISO timestamps
                    pass
        
        # Use execution_time field if available (more reliable)
        model_data["execution_time"] = execution_result.get("execution_time", 0.0)
        model_data["status"] = execution_result.get("status", "unknown")
        
        # Extract adapter response
        adapter_response = execution_result.get("adapter_response", {})
        model_data["adapter_response"] = {
            "code": adapter_response.get("code", ""),
            "rows_affected": adapter_response.get("rows_affected", 0),
            "query_id": adapter_response.get("query_id", ""),
            "message": adapter_response.get("_message", "")
        }
        
        # Extract compiled code from run_results if not already extracted from manifest
        if "compiled_sql" not in model_data and execution_result.get("compiled_code"):
            model_data["compiled_sql"] = execution_result.get("compiled_code", "")
    else:
        # Model not executed, set defaults
        model_data["execution_time"] = 0.0
        model_data["status"] = "not_executed"
        model_data["adapter_response"] = {
            "code": "",
            "rows_affected": 0,
            "query_id": "",
            "message": ""
        }
    
    if logger:
        logger.debug(f"Extracted data for model: {model_data['model_name']} (status: {model_data['status']})")
    
    return model_data


# ============================================================================
# EXECUTION TIME EXTRACTION AND VALIDATION
# ============================================================================

def extract_execution_time(execution_result: Dict[str, Any], model_name: str = "unknown", logger: Optional[logging.Logger] = None) -> Tuple[float, str]:
    """
    Extract and validate execution_time from run_results execution result.
    
    Extracts the execution_time field from a run_results execution result entry,
    validates that it's a valid non-negative number, and handles edge cases
    (zero time, null values, negative values). Returns the execution time in
    seconds along with a status indicator.
    
    Args:
        execution_result (Dict[str, Any]): Single result entry from run_results.json
        model_name (str): Model name for logging/error messages
        logger (Optional[logging.Logger]): Logger instance for warnings
    
    Returns:
        Tuple[float, str]: (execution_time_seconds, status)
            - execution_time_seconds: Execution time as float >= 0
            - status: One of "success", "zero_time", "null_value", "invalid"
    
    Examples:
        >>> result = {"execution_time": 2.5, "status": "success"}
        >>> exec_time, status = extract_execution_time(result, "stg_users")
        >>> print(f"{exec_time}s ({status})")
        2.5s (success)
    """
    # Get execution_time value
    exec_time = execution_result.get("execution_time")
    
    # Handle null/missing value
    if exec_time is None:
        if logger:
            logger.warning(f"Null execution_time for model '{model_name}' - treating as zero")
        return 0.0, "null_value"
    
    # Validate it's numeric
    if not isinstance(exec_time, (int, float)):
        if logger:
            logger.warning(f"Invalid execution_time type for model '{model_name}': {type(exec_time).__name__} (treating as zero)")
        return 0.0, "invalid"
    
    # Check for negative values
    if exec_time < 0:
        if logger:
            logger.warning(f"Negative execution_time for model '{model_name}': {exec_time}s (treating as zero)")
        return 0.0, "invalid"
    
    # Check for zero execution time
    if exec_time == 0:
        if logger:
            logger.debug(f"Zero execution_time for model '{model_name}' - possible timing precision issue")
        return 0.0, "zero_time"
    
    return float(exec_time), "success"


def filter_models_by_pipeline(models: List[Dict[str, Any]], target_pipeline: str, logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    """
    Filter models by pipeline tag.
    
    Filters a list of parsed models to include only those tagged with the
    target pipeline (pipeline_a, pipeline_b, or pipeline_c). Logs filtering
    summary for transparency.
    
    Args:
        models (List[Dict[str, Any]]): List of parsed model data
        target_pipeline (str): Target pipeline tag (e.g., "pipeline_a")
        logger (Optional[logging.Logger]): Logger instance for tracking
    
    Returns:
        List[Dict[str, Any]]: Filtered list of models with target pipeline tag
    
    Examples:
        >>> models = [
        ...     {"model_name": "stg_users", "tags": ["pipeline_a", "daily"]},
        ...     {"model_name": "stg_orders", "tags": ["pipeline_b"]},
        ... ]
        >>> filtered = filter_models_by_pipeline(models, "pipeline_a")
        >>> print(len(filtered))  # 1
    """
    filtered = []
    
    for model in models:
        tags = model.get("tags", [])
        if target_pipeline in tags:
            filtered.append(model)
    
    if logger:
        logger.debug(f"Pipeline filtering: {len(filtered)}/{len(models)} models match target pipeline '{target_pipeline}'")
    
    return filtered


def detect_execution_time_outliers(exec_times: List[float], logger: Optional[logging.Logger] = None) -> Tuple[List[int], Dict[str, float]]:
    """
    Detect statistical outliers in execution time data.
    
    Uses interquartile range (IQR) method to identify outliers. Values beyond
    1.5*IQR from Q1/Q3 are flagged as outliers. Also calculates statistics
    for data quality assessment.
    
    Args:
        exec_times (List[float]): List of execution times in seconds
        logger (Optional[logging.Logger]): Logger instance for warnings
    
    Returns:
        Tuple[List[int], Dict[str, float]]: (outlier_indices, statistics)
            - outlier_indices: Indices of values in original list that are outliers
            - statistics: Dict with keys: min, q1, median, q3, max, iqr, mean, stddev
    
    Examples:
        >>> times = [0.5, 1.0, 1.2, 1.1, 100.0]  # 100.0 is outlier
        >>> indices, stats = detect_execution_time_outliers(times)
        >>> print(f"Outliers at indices: {indices}")
        Outliers at indices: [4]
    """
    if len(exec_times) < 4:
        # Not enough data for meaningful outlier detection
        return [], {
            "min": min(exec_times) if exec_times else 0,
            "max": max(exec_times) if exec_times else 0,
            "mean": sum(exec_times) / len(exec_times) if exec_times else 0,
            "median": 0,
            "q1": 0,
            "q3": 0,
            "iqr": 0,
            "stddev": 0
        }
    
    # Sort to calculate percentiles
    sorted_times = sorted(exec_times)
    n = len(sorted_times)
    
    # Calculate quartiles
    q1_idx = n // 4
    median_idx = n // 2
    q3_idx = (3 * n) // 4
    
    q1 = sorted_times[q1_idx]
    median = sorted_times[median_idx]
    q3 = sorted_times[q3_idx]
    iqr = q3 - q1
    
    # Calculate mean and stddev
    mean = sum(exec_times) / len(exec_times)
    variance = sum((x - mean) ** 2 for x in exec_times) / len(exec_times)
    stddev = variance ** 0.5
    
    # Identify outliers using IQR method (1.5 * IQR)
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outlier_indices = [i for i, t in enumerate(exec_times) if t < lower_bound or t > upper_bound]
    
    stats = {
        "min": sorted_times[0],
        "q1": q1,
        "median": median,
        "q3": q3,
        "max": sorted_times[-1],
        "iqr": iqr,
        "mean": mean,
        "stddev": stddev
    }
    
    if logger and outlier_indices:
        logger.warning(f"Data quality: {len(outlier_indices)} outlier(s) detected in execution times (IQR method). Upper bound: {upper_bound:.4f}s")
    
    return outlier_indices, stats


def validate_artifact_fields(model_data: Dict[str, Any], logger: Optional[logging.Logger] = None) -> Tuple[bool, List[str]]:
    """
    Validate that extracted model data contains required fields.
    
    Checks for essential fields needed for KPI calculation and ensures data
    completeness and consistency. Returns validation errors for investigation.
    
    Args:
        model_data (Dict[str, Any]): Extracted model data from extract_model_data()
        logger (Optional[logging.Logger]): Logger instance for warnings/errors
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
|            - is_valid: True if all validations pass, False if any validation fails
|            - error_messages: List of validation errors and warnings
    
    Examples:
        >>> is_valid, errors = validate_artifact_fields(model_data)
        >>> if not is_valid:
        ...     for error in errors:
        ...         logger.warning(error)
    """
    errors = []
    
    # Check required fields for executed models
    if model_data.get("status") == "success":
        required_fields = [
            "execution_time",
            "model_name",
            "relation_name",
            "status"
        ]
        
        for field in required_fields:
            if field not in model_data or model_data[field] is None:
                errors.append(
                    f"Missing required field for executed model '{model_data.get('model_name', 'unknown')}': {field}"
                )
        
        # Validate field types
        if "execution_time" in model_data:
            exec_time = model_data["execution_time"]
            if not isinstance(exec_time, (int, float)):
                errors.append(
                    f"Field 'execution_time' must be numeric, got {type(exec_time).__name__}"
                )
            elif exec_time < 0:
                errors.append(
                    f"Field 'execution_time' cannot be negative: {exec_time}"
                )
        
        # Check rows_affected validity
        rows_affected = model_data.get("adapter_response", {}).get("rows_affected", 0)
        if not isinstance(rows_affected, int):
            errors.append(
                f"Field 'rows_affected' must be integer, got {type(rows_affected).__name__}"
            )
        
        # Warn about zero execution times (possible timing issue)
        if model_data.get("execution_time") == 0:
            warning_msg = f"Zero execution time for model '{model_data.get('model_name')}' - possible timing precision issue"
            errors.append(f"WARNING: {warning_msg}")
            if logger:
                logger.warning(warning_msg)
    
    # Always validate structure
    if not isinstance(model_data, dict):
        errors.append(f"Model data must be dictionary, got {type(model_data).__name__}")
    
    is_valid = len([e for e in errors if not e.startswith("WARNING:")]) == 0
    return is_valid, errors


# ============================================================================
# WORK METRICS EXTRACTION AND CALCULATION
# ============================================================================

def extract_rows_affected(model_data: Dict[str, Any], model_name: str = "unknown", logger: Optional[logging.Logger] = None) -> Tuple[int, str]:
    """
    Extract and validate rows_affected from run_results adapter_response.
    
    Extracts the rows_affected field from a model's adapter response in run_results.json,
    validates that it's a valid non-negative integer, and handles edge cases (zero rows,
    null values, invalid types). Returns the row count along with a status indicator.
    
    Args:
        model_data (Dict[str, Any]): Model data dict with adapter_response
        model_name (str): Model name for logging/error messages
        logger (Optional[logging.Logger]): Logger instance for warnings
    
    Returns:
        Tuple[int, str]: (rows_affected_count, status)
            - rows_affected_count: Number of rows as int >= 0
            - status: One of "success", "zero_rows", "null_value", "invalid"
    
    Examples:
        >>> model = {"adapter_response": {"rows_affected": 1500}}
        >>> rows, status = extract_rows_affected(model, "stg_users")
        >>> print(f"{rows} rows ({status})")
        1500 rows (success)
    """
    # Get adapter_response
    adapter_response = model_data.get("adapter_response", {})
    rows_affected = adapter_response.get("rows_affected")
    
    # Handle null/missing value
    if rows_affected is None:
        if logger:
            logger.warning(f"Null rows_affected for model '{model_name}' - treating as zero")
        return 0, "null_value"
    
    # Validate it's numeric
    if not isinstance(rows_affected, (int, float)):
        if logger:
            logger.warning(f"Invalid rows_affected type for model '{model_name}': {type(rows_affected).__name__} (treating as zero)")
        return 0, "invalid"
    
    # Convert to int
    rows_affected = int(rows_affected)
    
    # Check for negative values
    if rows_affected < 0:
        if logger:
            logger.warning(f"Negative rows_affected for model '{model_name}': {rows_affected} (treating as zero)")
        return 0, "invalid"
    
    # Check for zero rows
    if rows_affected == 0:
        if logger:
            logger.debug(f"Zero rows_affected for model '{model_name}' - model may be a source or produced no output")
        return 0, "zero_rows"
    
    return rows_affected, "success"


def estimate_column_byte_size(column_type: str) -> int:
    """
    Estimate average byte size for a Snowflake column based on its data type.
    
    Provides conservative estimates for common Snowflake data types. These estimates
    are used to calculate average row width for bytes_scanned estimation.
    
    Args:
        column_type (str): Snowflake data type name
    
    Returns:
        int: Estimated byte size for the column (conservative estimate)
    
    Examples:
        >>> size_varchar = estimate_column_byte_size("VARCHAR")
        >>> print(f"VARCHAR avg size: {size_varchar} bytes")
        VARCHAR avg size: 50 bytes
        >>> size_int = estimate_column_byte_size("NUMBER")
        >>> print(f"NUMBER avg size: {size_int} bytes")
        NUMBER avg size: 16 bytes
    """
    # Normalize type to uppercase
    col_type = column_type.upper() if column_type else "VARCHAR"
    
    # Extract base type (before parentheses for parameterized types)
    if '(' in col_type:
        col_type = col_type.split('(')[0].strip()
    
    # Type mappings with conservative byte estimates
    type_sizes = {
        "VARCHAR": 50, "CHAR": 1, "TEXT": 100, "STRING": 50,
        "NUMBER": 16, "DECIMAL": 16, "NUMERIC": 16,
        "INT": 4, "INTEGER": 4, "BIGINT": 8, "SMALLINT": 2, "TINYINT": 1,
        "FLOAT": 8, "DOUBLE": 8, "REAL": 4,
        "BOOLEAN": 1,
        "DATE": 8, "TIME": 8, "DATETIME": 16, "TIMESTAMP": 16,
        "TIMESTAMPNTZ": 16, "TIMESTAMPTZ": 16, "TIMESTAMP_NTZ": 16, "TIMESTAMP_TZ": 16,
        "BINARY": 20, "VARBINARY": 20,
        "VARIANT": 100, "OBJECT": 100, "ARRAY": 100,
        "GEOGRAPHY": 200, "GEOMETRY": 200,
    }
    
    return type_sizes.get(col_type, 50)


def calculate_average_row_width(columns: List[Dict[str, Any]], logger: Optional[logging.Logger] = None) -> int:
    """
    Calculate estimated average row width in bytes for a table.
    
    Sums the estimated byte sizes of all columns to determine the average
    row width. This is used to estimate bytes_scanned when multiplied by rows_affected.
    
    Args:
        columns (List[Dict[str, Any]]): List of column definitions from information_schema
            Each dict should contain at least 'DATA_TYPE' key
        logger (Optional[logging.Logger]): Logger instance for tracking
    
    Returns:
        int: Estimated average row width in bytes
    
    Examples:
        >>> columns = [
        ...     {"DATA_TYPE": "NUMBER"},
        ...     {"DATA_TYPE": "VARCHAR"},
        ...     {"DATA_TYPE": "DATE"}
        ... ]
        >>> width = calculate_average_row_width(columns)
        >>> print(f"Average row width: {width} bytes")
        Average row width: 74 bytes
    """
    if not columns:
        if logger:
            logger.warning("No column information available - using conservative default estimate (500 bytes per row)")
        return 500
    
    total_bytes = 0
    for col in columns:
        col_type = col.get("DATA_TYPE", "VARCHAR")
        col_size = estimate_column_byte_size(col_type)
        total_bytes += col_size
        if logger:
            logger.debug(f"Column {col.get('COLUMN_NAME', '?')}: {col_type} -> {col_size} bytes")
    
    if logger:
        logger.debug(f"Calculated average row width: {total_bytes} bytes")
    
    return total_bytes


def query_snowflake_schema(database: str, schema: str, table: str, credentials: Dict[str, str], logger: Optional[logging.Logger] = None) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Query Snowflake information_schema to get column information for a table.
    
    Connects to Snowflake using provided credentials and queries information_schema.columns
    to retrieve column definitions including data types. This is used to estimate average
    row width and bytes_scanned for the table.
    
    Args:
        database (str): Snowflake database name
        schema (str): Snowflake schema name
        table (str): Table name
        credentials (Dict[str, str]): Snowflake connection credentials with keys:
            account, user, password, warehouse, database (may be overridden)
        logger (Optional[logging.Logger]): Logger instance for tracking
    
    Returns:
        Tuple[List[Dict[str, Any]], bool]: (column_definitions, success_flag)
            - column_definitions: List of column dicts with DATA_TYPE, COLUMN_NAME, etc.
            - success_flag: True if query succeeded, False otherwise
    
    Examples:
        >>> creds = {"account": "xyz123", "user": "admin", "password": "...", ...}
        >>> columns, success = query_snowflake_schema("MYDB", "PUBLIC", "users", creds)
        >>> if success:
        ...     for col in columns:
        ...         print(f"{col['COLUMN_NAME']}: {col['DATA_TYPE']}")
    """
    try:
        try:
            import snowflake.connector
        except ImportError:
            if logger:
                logger.warning("snowflake-connector-python not available - cannot query schema metadata")
            return [], False
        
        required_creds = ["account", "user", "password", "warehouse"]
        missing = [c for c in required_creds if c not in credentials]
        if missing:
            if logger:
                logger.warning(f"Missing Snowflake credentials: {', '.join(missing)} - skipping schema query")
            return [], False
        
        if logger:
            logger.debug(f"Connecting to Snowflake: {credentials['account']}")
        
        conn = snowflake.connector.connect(
            account=credentials["account"],
            user=credentials["user"],
            password=credentials["password"],
            warehouse=credentials["warehouse"],
            database=database,
            schema=schema
        )
        
        cursor = conn.cursor()
        
        query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION
            FROM information_schema.columns
            WHERE TABLE_CATALOG = '{database}'
            AND TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        
        if logger:
            logger.debug(f"Querying Snowflake schema: {database}.{schema}.{table}")
        
        cursor.execute(query)
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "COLUMN_NAME": row[0],
                "DATA_TYPE": row[1],
                "ORDINAL_POSITION": row[2]
            })
        
        cursor.close()
        conn.close()
        
        if logger:
            logger.info(f"✓ Retrieved {len(columns)} columns from Snowflake schema: {database}.{schema}.{table}")
        
        return columns, True
        
    except ImportError:
        if logger:
            logger.warning("snowflake-connector-python package not installed - cannot query schema metadata")
        return [], False
    except Exception as e:
        if logger:
            logger.warning(f"Error querying Snowflake schema for {database}.{schema}.{table}: {str(e)}")
        return [], False


# ============================================================================
# OUTPUT VALIDATION AND HASH CALCULATION
# ============================================================================

def serialize_rows_consistent(rows: List[Dict[str, Any]]) -> str:
    """
    Serialize rows to a consistent JSON string for hashing.
    
    Creates a deterministic JSON representation of rows by:
    - Sorting rows by all column values for consistent ordering
    - Using sorted keys for JSON serialization
    - Removing any volatile fields (timestamps, IDs)
    - Using consistent formatting and encoding
    
    Args:
        rows (List[Dict[str, Any]]): List of row dictionaries to serialize
    
    Returns:
        str: Deterministic JSON string representation of rows
    
    Examples:
        >>> rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        >>> json_str = serialize_rows_consistent(rows)
        >>> print(json_str)
        '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
    """
    if not rows:
        return json.dumps([], separators=(',', ':'), sort_keys=True)
    
    # Sort rows by their string representation for consistent ordering
    # This ensures the same data always produces the same hash
    sorted_rows = sorted(rows, key=lambda row: json.dumps(row, separators=(',', ':'), sort_keys=True, default=str))
    
    # Serialize to JSON with consistent formatting
    return json.dumps(sorted_rows, separators=(',', ':'), sort_keys=True, default=str)


def query_model_table(database: str, schema: str, table: str, credentials: Dict[str, str], logger: Optional[logging.Logger] = None) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Query Snowflake table to retrieve all rows for output validation (hashing).
    
    Connects to Snowflake and retrieves all rows from the specified table.
    Used for calculating SHA256 hash of model output for data equivalence validation.
    Handles connection errors gracefully and logs details for debugging.
    
    Args:
        database (str): Snowflake database name
        schema (str): Snowflake schema name
        table (str): Table name to query
        credentials (Dict[str, str]): Snowflake connection credentials with keys:
            account, user, password, warehouse, database (may be overridden)
        logger (Optional[logging.Logger]): Logger instance for tracking
    
    Returns:
        Tuple[List[Dict[str, Any]], bool]: (row_data, success_flag)
            - row_data: List of row dictionaries from the table
            - success_flag: True if query succeeded, False otherwise
    
    Examples:
        >>> creds = {"account": "xyz123", "user": "admin", "password": "...", ...}
        >>> rows, success = query_model_table("MYDB", "PUBLIC", "fact_trades", creds)
        >>> if success:
        ...     print(f"Retrieved {len(rows)} rows")
    """
    try:
        try:
            import snowflake.connector
        except ImportError:
            if logger:
                logger.warning("snowflake-connector-python not available - cannot query model table for hashing")
            return [], False
        
        required_creds = ["account", "user", "password", "warehouse"]
        missing = [c for c in required_creds if c not in credentials]
        if missing:
            if logger:
                logger.warning(f"Missing Snowflake credentials for table query: {', '.join(missing)}")
            return [], False
        
        if logger:
            logger.debug(f"Connecting to Snowflake to query table: {database}.{schema}.{table}")
        
        conn = snowflake.connector.connect(
            account=credentials["account"],
            user=credentials["user"],
            password=credentials["password"],
            warehouse=credentials["warehouse"],
            database=database,
            schema=schema
        )
        
        cursor = conn.cursor()
        
        # Query all rows from the table
        query = f"SELECT * FROM {database}.{schema}.{table}"
        
        if logger:
            logger.debug(f"Executing query: {query}")
        
        cursor.execute(query)
        
        # Get column names from cursor description
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows and convert to list of dictionaries
        rows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            rows.append(row_dict)
        
        cursor.close()
        conn.close()
        
        if logger:
            logger.info(f"✓ Retrieved {len(rows)} rows from {database}.{schema}.{table}")
        
        return rows, True
        
    except ImportError:
        if logger:
            logger.warning("snowflake-connector-python package not installed - cannot query model table")
        return [], False
    except Exception as e:
        if logger:
            logger.warning(f"Error querying model table {database}.{schema}.{table}: {str(e)}")
        return [], False


def calculate_output_hash(model_data: Dict[str, Any], database: str, schema: str, table: str, 
                         credentials: Dict[str, str], model_name: str = "unknown", 
                         logger: Optional[logging.Logger] = None) -> Tuple[Optional[str], str, str]:
    """
    Calculate SHA256 hash of model output for data equivalence validation.
    
    Attempts to hash model output using two approaches:
    1. Primary: Extract row data from run_results.json if available
    2. Fallback: Query Snowflake table directly to get rows
    
    Uses deterministic serialization (sorted JSON) to ensure the same data always
    produces the same hash. Handles edge cases gracefully:
    - Empty result sets → SHA256 of empty list
    - Sources/ephemeral models → null hash with explanation
    - Query failures → null hash with warning
    - Models with no output → null hash with explanation
    
    Args:
        model_data (Dict[str, Any]): Model data dictionary from run_results
        database (str): Snowflake database name
        schema (str): Snowflake schema name
        table (str): Table name for fallback query
        credentials (Dict[str, str]): Snowflake connection credentials
        model_name (str): Model name for logging
        logger (Optional[logging.Logger]): Logger instance
    
    Returns:
        Tuple[str, str, str]: (output_hash, data_source, hash_algorithm)
            - output_hash: SHA256 hex string, or None if unable to hash
            - data_source: "run_results_json", "snowflake_query", or "unavailable"
            - hash_algorithm: "sha256" (always)
    
    Examples:
        >>> hash_val, source, algo = calculate_output_hash(
        ...     model_data, "MYDB", "PUBLIC", "fact_trades", creds, "fact_trades"
        ... )
        >>> print(f"Hash: {hash_val} (from {source})")
    """
    import hashlib
    
    rows = []
    data_source = "unavailable"
    
    # Try primary approach: Check if run_results has batch_results (row data)
    # Note: Most dbt adapters don't store row data in run_results, so this is a fallback
    batch_results = model_data.get("batch_results")
    if batch_results and isinstance(batch_results, list):
        if logger:
            logger.debug(f"Found batch_results in run_results for {model_name}")
        rows = batch_results
        data_source = "run_results_json"
    else:
        # Fallback approach: Query Snowflake table directly
        if credentials and database and schema and table:
            if logger:
                logger.debug(f"Querying Snowflake table for {model_name} hash calculation")
            rows, query_success = query_model_table(database, schema, table, credentials, logger)
            if query_success:
                data_source = "snowflake_query"
            else:
                if logger:
                    logger.warning(f"Could not retrieve data from Snowflake for {model_name} - hash will be unavailable")
                return None, "unavailable", "sha256"
        else:
            if logger:
                logger.warning(f"Missing connection info for {model_name} - cannot query table for hash")
            return None, "unavailable", "sha256"
    
    # If still no rows, return null hash
    if rows is None:
        if logger:
            logger.warning(f"No row data available for {model_name} - hash will be unavailable")
        return None, "unavailable", "sha256"
    
    # Handle empty result set
    if not rows:
        # Empty result set is valid - hash the empty array
        if logger:
            logger.debug(f"Model {model_name} produced empty result set")
        empty_json = json.dumps([], separators=(',', ':'), sort_keys=True)
        empty_hash = hashlib.sha256(empty_json.encode('utf-8')).hexdigest()
        return empty_hash, data_source, "sha256"
    
    # Serialize and hash the rows
    try:
        json_str = serialize_rows_consistent(rows)
        hash_value = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
        
        if logger:
            logger.debug(f"✓ Calculated hash for {model_name}: {hash_value[:16]}... ({len(rows)} rows from {data_source})")
        
        return hash_value, data_source, "sha256"
        
    except Exception as e:
        if logger:
            logger.warning(f"Error calculating hash for {model_name}: {str(e)}")
        return None, data_source, "sha256"


def get_query_metrics_from_history(
    query_id: str,
    credentials: Dict[str, str],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Fetch actual execution metrics from Snowflake QUERY_HISTORY.

    Queries the Snowflake ACCOUNT_USAGE.QUERY_HISTORY view to retrieve
    actual performance metrics for a specific query ID. This provides
    more accurate data than estimations for bytes scanned, credits used,
    and other execution details.

    Args:
        query_id (str): Snowflake query ID from adapter response
        credentials (Dict[str, str]): Snowflake connection credentials with keys:
            - account: Snowflake account identifier
            - user: Username for authentication
            - password: Password for authentication
            - warehouse: Warehouse name (optional for system queries)
        logger (Optional[logging.Logger]): Logger instance for debug/error messages

    Returns:
        Dict[str, Any]: Dictionary containing query metrics:
            - bytes_scanned (int): Actual bytes scanned by the query
            - rows_produced (int): Actual number of rows produced
            - credits_used (float): Actual Snowflake credits consumed
            - execution_time_ms (int): Total elapsed time in milliseconds
            - warehouse_size (str): Warehouse size used (XS, S, M, L, etc.)
            - compilation_time (int): Query compilation time in milliseconds
            - execution_status (str): Query execution status (SUCCESS, FAILED, etc.)
            - partitions_scanned (int): Number of micro-partitions scanned

        Returns empty dict {} if query not found or error occurs.

    Raises:
        None: All exceptions are caught and logged. Empty dict returned on error.

    Examples:
        >>> from helpers import get_query_metrics_from_history, parse_profiles_yml
        >>> creds = parse_profiles_yml('bain_capital')
        >>> metrics = get_query_metrics_from_history(
        ...     query_id='01c253ce-0307-b61a-001d-93430006a1e2',
        ...     credentials=creds
        ... )
        >>> print(f"Bytes scanned: {metrics['bytes_scanned']:,}")
        Bytes scanned: 1,234,567

    Notes:
        - Requires IMPORTED PRIVILEGES on SNOWFLAKE database
        - QUERY_HISTORY data may have a few minutes delay
        - Only queries from the last 365 days are available in ACCOUNT_USAGE
    """
    try:
        import snowflake.connector
    except ImportError:
        if logger:
            logger.warning("snowflake-connector-python not installed, query history unavailable")
        return {}

    try:
        # Connect to Snowflake system database
        conn = snowflake.connector.connect(
            account=credentials.get("account"),
            user=credentials.get("user"),
            password=credentials.get("password"),
            warehouse=credentials.get("warehouse"),  # Optional for system queries
            database="SNOWFLAKE",  # System database
            schema="ACCOUNT_USAGE"
        )

        # Query ACCOUNT_USAGE.QUERY_HISTORY for metrics
        query = """
        SELECT
            BYTES_SCANNED,
            ROWS_PRODUCED,
            CREDITS_USED_CLOUD_SERVICES as CREDITS_USED,
            TOTAL_ELAPSED_TIME as EXECUTION_TIME_MS,
            WAREHOUSE_SIZE,
            COMPILATION_TIME,
            EXECUTION_STATUS,
            PARTITIONS_SCANNED
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_ID = %s
        LIMIT 1
        """

        cursor = conn.cursor()
        cursor.execute(query, (query_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            if logger:
                logger.debug(f"Query {query_id} not found in QUERY_HISTORY (may not be available yet)")
            return {}

        # Parse results into dictionary
        return {
            'bytes_scanned': int(result[0]) if result[0] is not None else 0,
            'rows_produced': int(result[1]) if result[1] is not None else 0,
            'credits_used': float(result[2]) if result[2] is not None else 0.0,
            'execution_time_ms': int(result[3]) if result[3] is not None else 0,
            'warehouse_size': result[4],
            'compilation_time': int(result[5]) if result[5] is not None else 0,
            'execution_status': result[6],
            'partitions_scanned': int(result[7]) if result[7] is not None else 0
        }

    except Exception as e:
        if logger:
            logger.error(f"Error fetching query history for {query_id}: {str(e)}")
        return {}


# ============================================================================
# SQL COMPLEXITY ANALYSIS
# ============================================================================

def strip_sql_comments(sql: str) -> str:
    """
    Remove SQL comments from query while preserving string literals.
    
    Handles both line comments (--) and block comments (/* */).
    Preserves string literals to avoid removing SQL keywords inside strings.
    
    Args:
        sql (str): Raw SQL query
    
    Returns:
        str: SQL query with comments removed
    
    Examples:
        >>> sql = "SELECT * FROM table -- this is a comment"
        >>> clean = strip_sql_comments(sql)
        >>> print(clean)
        "SELECT * FROM table "
    """
    if not sql:
        return ""
    
    result = []
    i = 0
    in_string = False
    string_char = None
    
    while i < len(sql):
        # Handle string literals
        if sql[i] in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
            if not in_string:
                in_string = True
                string_char = sql[i]
                result.append(sql[i])
            elif sql[i] == string_char:
                in_string = False
                string_char = None
                result.append(sql[i])
            else:
                result.append(sql[i])
            i += 1
            continue
        
        # If inside a string, keep everything
        if in_string:
            result.append(sql[i])
            i += 1
            continue
        
        # Handle block comments /* */
        if i < len(sql) - 1 and sql[i:i+2] == '/*':
            # Find the end of the block comment
            end = sql.find('*/', i + 2)
            if end != -1:
                i = end + 2
                result.append(' ')  # Replace comment with space to avoid concatenation
            else:
                # Unclosed block comment, skip to end
                i = len(sql)
            continue
        
        # Handle line comments --
        if i < len(sql) - 1 and sql[i:i+2] == '--':
            # Skip until end of line
            end = sql.find('\n', i)
            if end != -1:
                i = end
                result.append('\n')  # Preserve newlines
            else:
                i = len(sql)
            continue
        
        result.append(sql[i])
        i += 1
    
    return ''.join(result)


def count_joins(sql: str) -> int:
    """
    Count JOIN clauses in SQL query (case-insensitive).
    
    Matches INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, and CROSS JOIN.
    Uses word boundaries to avoid false positives in identifiers.
    
    Args:
        sql (str): SQL query (should have comments stripped)
    
    Returns:
        int: Number of JOIN clauses found
    
    Examples:
        >>> sql = "SELECT * FROM a INNER JOIN b ON a.id = b.id"
        >>> count_joins(sql)
        1
    """
    if not sql:
        return 0
    
    # Pattern matches: INNER/LEFT/RIGHT/FULL/CROSS JOIN
    # Using word boundaries and case-insensitive matching
    pattern = r'\b(INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\b'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    
    return len(matches)


def count_ctes(sql: str) -> int:
    """
    Count WITH clauses (CTEs) in SQL query (case-insensitive).
    
    Counts WITH statements at the beginning and nested CTEs separated by commas.
    Does not count WITH inside string literals.
    
    Args:
        sql (str): SQL query (should have comments stripped)
    
    Returns:
        int: Number of CTEs found
    
    Examples:
        >>> sql = "WITH cte1 AS (...), cte2 AS (...) SELECT * FROM cte1"
        >>> count_ctes(sql)
        2
    """
    if not sql:
        return 0
    
    # Find the WITH keyword at the start (may have leading whitespace)
    # Then count the number of CTEs by counting CTE definitions
    # Each CTE is: name AS (...)
    # Multiple CTEs are separated by commas
    
    # First check if there's a WITH clause
    if not re.search(r'\bWITH\b', sql, re.IGNORECASE):
        return 0
    
    # Remove the SELECT/INSERT/UPDATE/DELETE part to focus on WITH clause
    # Find where WITH starts
    with_match = re.search(r'\bWITH\b', sql, re.IGNORECASE)
    if not with_match:
        return 0
    
    with_start = with_match.start()
    
    # Find where the main query starts (SELECT, INSERT, UPDATE, DELETE, or MERGE)
    remaining_sql = sql[with_start:]
    main_query_match = re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|MERGE)\b', remaining_sql[5:], re.IGNORECASE)
    
    if main_query_match:
        with_clause = remaining_sql[:main_query_match.start() + 5]
    else:
        with_clause = remaining_sql
    
    # Count CTE definitions: look for pattern "name AS ("
    # Each CTE is separated by a comma at the same nesting level
    # Count the number of commas that are at the top level (not inside parentheses)
    
    cte_count = 0
    paren_depth = 0
    in_string = False
    string_char = None
    
    for i, char in enumerate(with_clause):
        # Track string literals
        if char in ("'", '"') and (i == 0 or with_clause[i-1] != '\\'):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
        
        if not in_string:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                cte_count += 1
    
    # Add 1 for the first CTE if WITH clause exists
    if with_match:
        cte_count += 1
    
    return cte_count


def count_window_functions(sql: str) -> int:
    """
    Count window functions in SQL query (case-insensitive).
    
    Identifies window functions by counting OVER clauses. Each OVER keyword
    indicates a window function call.
    
    Args:
        sql (str): SQL query (should have comments stripped)
    
    Returns:
        int: Number of window functions found
    
    Examples:
        >>> sql = "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY date)"
        >>> count_window_functions(sql)
        1
    """
    if not sql:
        return 0
    
    # Pattern matches: OVER keyword with optional parentheses and content
    pattern = r'\bOVER\s*\('
    matches = re.findall(pattern, sql, re.IGNORECASE)
    
    return len(matches)


def extract_sql_complexity(sql: str, logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    """
    Extract SQL complexity metrics from query.
    
    Analyzes SQL query to count JOINs, CTEs, and window functions.
    Handles comments and edge cases gracefully.
    
    Args:
        sql (str): Raw SQL query from manifest
        logger (Optional[logging.Logger]): Logger for debugging
    
    Returns:
        Dict[str, int]: Dictionary with keys:
            - join_count: Number of JOINs
            - cte_count: Number of CTEs (WITH clauses)
            - window_function_count: Number of window functions (OVER clauses)
    
    Examples:
        >>> sql = "WITH cte AS (SELECT * FROM t) SELECT * FROM cte JOIN other ON cte.id = other.id OVER (PARTITION BY id)"
        >>> metrics = extract_sql_complexity(sql)
        >>> print(metrics['cte_count'], metrics['join_count'], metrics['window_function_count'])
        1 1 1
    """
    try:
        # Return zeros for empty SQL
        if not sql or not isinstance(sql, str):
            return {
                "join_count": 0,
                "cte_count": 0,
                "window_function_count": 0
            }
        
        # Strip comments first
        clean_sql = strip_sql_comments(sql)
        
        # Extract metrics
        join_count = count_joins(clean_sql)
        cte_count = count_ctes(clean_sql)
        window_count = count_window_functions(clean_sql)
        
        if logger:
            logger.debug(f"SQL Complexity: {join_count} JOINs, {cte_count} CTEs, {window_count} window functions")
        
        return {
            "join_count": join_count,
            "cte_count": cte_count,
            "window_function_count": window_count
        }
        
    except Exception as e:
        if logger:
            logger.warning(f"Error extracting SQL complexity: {str(e)}")
        return {
            "join_count": 0,
            "cte_count": 0,
            "window_function_count": 0
        }


# ============================================================================
# REPORT VALIDATION
# ============================================================================

def is_valid_iso_timestamp(timestamp_str: str) -> bool:
    """
    Validate ISO 8601 timestamp format.
    
    Args:
        timestamp_str (str): Timestamp string to validate
    
    Returns:
        bool: True if valid ISO 8601 format, False otherwise
    
    Examples:
        >>> is_valid_iso_timestamp("2024-01-15T10:30:45.123456")
        True
        >>> is_valid_iso_timestamp("invalid")
        False
    """
    if not isinstance(timestamp_str, str):
        return False
    
    # ISO 8601 format: YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM or Z]
    iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$'
    return bool(re.match(iso_pattern, timestamp_str))


def is_valid_hex_hash(hash_str: str) -> bool:
    """
    Validate hex string format for hash values.
    
    Args:
        hash_str (str): Hash string to validate
    
    Returns:
        bool: True if valid hex format, False otherwise
    
    Examples:
        >>> is_valid_hex_hash("abc123def456")
        True
        >>> is_valid_hex_hash("xyz123")
        False
    """
    if not isinstance(hash_str, str):
        return False
    
    # Hex string must contain only 0-9, a-f, A-F
    return bool(re.match(r'^[a-fA-F0-9]+$', hash_str))


def validate_report_schema(report: Dict[str, Any], logger: Optional[logging.Logger] = None) -> Tuple[bool, List[str]]:
    """
    Validate report structure against expected schema.
    
    Checks for:
    - Required top-level keys (metadata, models, summary)
    - Required metadata keys
    - Required per-model KPI keys
    - Data type validation for numeric, string, and timestamp fields
    
    Args:
        report (Dict[str, Any]): Report dictionary to validate
        logger (Optional[logging.Logger]): Logger for detailed validation messages
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, list_of_error_messages)
            - is_valid: True if report passes all validation checks
            - list_of_error_messages: List of validation error messages
    
    Examples:
        >>> report = {
        ...     "metadata": {"timestamp": "2024-01-15T10:30:45", "pipeline_name": "pipeline_a"},
        ...     "models": [{
        ...         "model_name": "my_model",
        ...         "execution_time_seconds": 1.5,
        ...         "rows_produced": 1000,
        ...         "bytes_scanned": 500000,
        ...         "output_hash": "abc123",
        ...         "join_count": 2,
        ...         "cte_count": 1,
        ...         "window_function_count": 0,
        ...         "estimated_credits": 0.05,
        ...         "estimated_cost_usd": 0.10
        ...     }],
        ...     "summary": {"total_models_processed": 1}
        ... }
        >>> is_valid, errors = validate_report_schema(report)
        >>> print(f"Valid: {is_valid}")
        Valid: True
    """
    errors = []
    
    # Check if report is a dictionary
    if not isinstance(report, dict):
        errors.append("Report must be a dictionary")
        return False, errors
    
    # 1. Validate top-level structure
    required_top_level = ["metadata", "models", "summary"]
    for key in required_top_level:
        if key not in report:
            errors.append(f"Missing required top-level key: '{key}'")
    
    # If missing critical keys, return early
    if errors:
        if logger:
            for error in errors:
                logger.error(f"Schema validation failed: {error}")
        return False, errors
    
    # 2. Validate metadata structure
    metadata = report.get("metadata", {})
    if not isinstance(metadata, dict):
        errors.append("'metadata' must be a dictionary")
        return False, errors
    
    required_metadata = ["timestamp", "pipeline_name", "models_processed"]
    for key in required_metadata:
        if key not in metadata:
            errors.append(f"Missing required metadata key: '{key}'")
        elif key == "timestamp" and not is_valid_iso_timestamp(metadata[key]):
            errors.append(f"Invalid timestamp format in metadata: {metadata[key]}")
        elif key == "models_processed" and not isinstance(metadata[key], int):
            errors.append(f"'metadata.models_processed' must be an integer, got {type(metadata[key]).__name__}")
    
    if errors:
        if logger:
            for error in errors:
                logger.error(f"Schema validation failed: {error}")
        return False, errors
    
    # 3. Validate models array
    models = report.get("models", [])
    if not isinstance(models, list):
        errors.append("'models' must be a list")
        return False, errors
    
    if not models:
        if logger:
            logger.warning("Report contains no models")
    
    # Required KPI fields for each model
    required_kpi_fields = [
        "execution_time_seconds",
        "rows_produced",
        "bytes_scanned",
        "output_hash",
        "join_count",
        "cte_count",
        "window_function_count",
        "estimated_credits",
        "estimated_cost_usd"
    ]
    
    # Expected data types for validation
    field_types = {
        "execution_time_seconds": (int, float),
        "rows_produced": int,
        "bytes_scanned": int,
        "output_hash": (str, type(None)),
        "join_count": int,
        "cte_count": int,
        "window_function_count": int,
        "estimated_credits": (int, float),
        "estimated_cost_usd": (int, float)
    }
    
    # Validate each model
    for idx, model in enumerate(models):
        if not isinstance(model, dict):
            errors.append(f"Model at index {idx} is not a dictionary")
            continue
        
        model_name = model.get("model_name", f"model_{idx}")
        
        # Check required model fields
        if "model_name" not in model:
            errors.append(f"Model at index {idx} is missing 'model_name'")
        
        # Check all required KPI fields
        for field in required_kpi_fields:
            if field not in model:
                errors.append(f"Model '{model_name}': missing required KPI field '{field}'")
                continue
            
            field_value = model[field]
            expected_type = field_types.get(field)
            
            # Allow None for output_hash
            if field == "output_hash" and field_value is None:
                continue
            
            # Validate type
            if expected_type and not isinstance(field_value, expected_type):
                actual_type = type(field_value).__name__
                if isinstance(expected_type, tuple):
                    expected_str = " or ".join(t.__name__ for t in expected_type)
                else:
                    expected_str = expected_type.__name__
                errors.append(f"Model '{model_name}': field '{field}' has type {actual_type}, expected {expected_str}")
                continue
            
            # Validate specific field formats
            if field == "output_hash" and isinstance(field_value, str):
                if not is_valid_hex_hash(field_value):
                    errors.append(f"Model '{model_name}': field '{field}' is not a valid hex string: {field_value}")
            
            # Validate numeric fields are non-negative
            elif field in ["execution_time_seconds", "rows_produced", "bytes_scanned", "estimated_credits", "estimated_cost_usd"]:
                if isinstance(field_value, (int, float)) and field_value < 0:
                    errors.append(f"Model '{model_name}': field '{field}' must be non-negative, got {field_value}")
            
            elif field in ["join_count", "cte_count", "window_function_count"]:
                if isinstance(field_value, int) and field_value < 0:
                    errors.append(f"Model '{model_name}': field '{field}' must be non-negative, got {field_value}")
    
    # 4. Validate summary structure
    summary = report.get("summary", {})
    if not isinstance(summary, dict):
        errors.append("'summary' must be a dictionary")
    else:
        # Check key summary fields
        summary_keys = ["total_models_processed", "total_execution_time_seconds"]
        for key in summary_keys:
            if key in summary:
                if key == "total_models_processed" and not isinstance(summary[key], int):
                    errors.append(f"'summary.{key}' must be an integer")
                elif key == "total_execution_time_seconds" and not isinstance(summary[key], (int, float)):
                    errors.append(f"'summary.{key}' must be numeric")
    
    # Log results
    if logger:
        if errors:
            logger.error(f"Schema validation found {len(errors)} error(s):")
            for error in errors:
                logger.error(f"  - {error}")
        else:
            logger.info(f"✓ Schema validation passed for report with {len(models)} models")
    
    return len(errors) == 0, errors


# ============================================================================
# MAIN / TEST SECTION
# ============================================================================

if __name__ == '__main__':
    print("=" * 80)
    print("Testing Helpers Module")
    print("=" * 80)
    
    # Test 1: Setup logging
    print("\n✓ Test 1: Setup Logging")
    try:
        logger = setup_logging("test_pipeline")
        logger.info("Logging system initialized successfully")
        print("  - Logger created and configured")
        print("  - File handler enabled")
        print("  - Console handler enabled")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 2: Schema definitions
    print("\n✓ Test 2: Schema Definitions")
    try:
        schemas = get_schema_definitions()
        print(f"  - Available schemas: {', '.join(schemas.keys())}")
        print(f"  - Report schema fields: {len(schemas['report']['field_types'])} fields")
        print(f"  - Analysis schema fields: {len(schemas['analysis']['field_types'])} fields")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 3: Path resolution
    print("\n✓ Test 3: Path Resolution")
    try:
        profile_paths = resolve_dbt_profile_paths()
        print(f"  - Resolved {len(profile_paths)} profile search paths")
        for i, path in enumerate(profile_paths, 1):
            status = "✓" if path.exists() else "✗"
            print(f"    {i}. [{status}] {path}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 4: Project root
    print("\n✓ Test 4: Project Root Detection")
    try:
        root = get_project_root()
        print(f"  - Project root: {root}")
        print(f"  - dbt_project.yml present: {(root / 'dbt_project.yml').exists()}")
    except ConfigError as e:
        print(f"  ℹ Note: {e}")
    
    # Test 5: Logs directory
    print("\n✓ Test 5: Logs Directory Setup")
    try:
        logs_dir = ensure_logs_directory()
        print(f"  - Logs directory: {logs_dir}")
        print(f"  - Directory exists: {logs_dir.exists()}")
        print(f"  - Is writable: {os.access(logs_dir, os.W_OK)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 6: Schema validation
    print("\n✓ Test 6: Schema Validation")
    try:
        # Valid report
        valid_report = {
            "execution_time_seconds": 1.5,
            "query_hash": "abc123def456",
            "row_count": 1000,
            "bytes_scanned": 1000000
        }
        is_valid, errors = validate_report_schema(valid_report, "report")
        print(f"  - Valid report: {is_valid}")
        
        # Invalid report
        invalid_report = {
            "execution_time_seconds": "not_a_number",
            "query_hash": "abc123"
        }
        is_valid, errors = validate_report_schema(invalid_report, "report")
        print(f"  - Invalid report detected: {not is_valid}")
        if errors:
            print(f"    Errors found: {len(errors)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 7: JSON loading
    print("\n✓ Test 7: JSON Loading")
    try:
        # Test missing file
        try:
            load_json_safe("nonexistent_file.json")
        except MissingArtifact as e:
            print(f"  - MissingArtifact exception caught correctly")
        
        # Create test JSON file
        test_data = {"test": "data", "value": 123}
        test_file = Path("test_report.json")
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        loaded = load_json_safe("test_report.json")
        print(f"  - Successfully loaded test JSON: {loaded}")
        
        # Cleanup
        test_file.unlink()
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Test 8: Custom exceptions
    print("\n✓ Test 8: Custom Exceptions")
    try:
        print(f"  - MissingArtifact defined: {MissingArtifact.__name__}")
        print(f"  - InvalidSchema defined: {InvalidSchema.__name__}")
        print(f"  - DataMismatch defined: {DataMismatch.__name__}")
        print(f"  - ConfigError defined: {ConfigError.__name__}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    print("\n" + "=" * 80)
    print("Testing Complete")
    print("=" * 80)
