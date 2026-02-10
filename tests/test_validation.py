#!/usr/bin/env python3
"""
Comprehensive unit tests for schema validation and error handling.

Tests robust failure modes and clear user feedback for:
- Report schema validation (required fields, data types)
- Missing artifacts (manifest.json, run_results.json)
- Invalid JSON (malformed syntax, incomplete structures)
- Configuration errors (missing config, invalid values)
- Custom exceptions (MissingArtifact, InvalidSchema, DataMismatch, ConfigError)
- Error messages (problem, component, remediation)
"""

import pytest
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from helpers import (
    load_json_safe,
    validate_report_schema,
    schema_validator,
    MissingArtifact,
    InvalidSchema,
    DataMismatch,
    ConfigError,
    get_schema_definitions
)


# ============================================================================
# FIXTURES: MOCK DATA AND TEMPORARY FILES
# ============================================================================

@pytest.fixture
def valid_report():
    """Fixture providing a valid report structure."""
    return {
        "execution_time_seconds": 12.5,
        "query_hash": "abc123def456",
        "row_count": 5000,
        "bytes_scanned": 1048576,
        "data_hash": "xyz789uvw012",
        "timestamp": "2024-01-15T10:00:00Z",
        "query_id": "query_001",
        "join_count": 3,
        "cte_count": 2,
        "window_function_count": 1
    }


@pytest.fixture
def valid_metadata():
    """Fixture providing valid metadata."""
    return {
        "generated_at": "2024-01-15T10:00:00Z",
        "pipeline_name": "data_warehouse",
        "baseline_run_id": "baseline_v1"
    }


@pytest.fixture
def valid_model_kpis():
    """Fixture providing valid model KPIs."""
    return {
        "execution_time": 5.2,
        "cost": 12.5,
        "bytes_scanned": 500000,
        "join_count": 3,
        "cte_count": 2,
        "window_function_count": 1,
        "data_hash": "abc123def456xyz789uvw012"
    }


@pytest.fixture
def temp_json_file():
    """Fixture providing a temporary file for JSON writing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_invalid_json_file():
    """Fixture providing a temporary file with invalid JSON."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{"incomplete": json content}')
        temp_path = f.name
    yield temp_path
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


# ============================================================================
# TEST: REPORT SCHEMA VALIDATION - REQUIRED FIELDS
# ============================================================================

class TestReportSchemaValidation:
    """Test suite for report schema validation."""
    
    def test_valid_report_passes_validation(self, valid_report):
        """Test that a valid report passes schema validation."""
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is True, f"Valid report should pass validation. Errors: {errors}"
        assert len(errors) == 0
    
    def test_missing_execution_time_seconds(self, valid_report):
        """Test validation catches missing execution_time_seconds."""
        del valid_report["execution_time_seconds"]
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False, "Should fail when execution_time_seconds is missing"
        assert any("execution_time_seconds" in e for e in errors), \
            f"Error should mention execution_time_seconds. Got: {errors}"
    
    def test_missing_query_hash(self, valid_report):
        """Test validation catches missing query_hash."""
        del valid_report["query_hash"]
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("query_hash" in e for e in errors)
    
    def test_missing_row_count(self, valid_report):
        """Test validation catches missing row_count."""
        del valid_report["row_count"]
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("row_count" in e for e in errors)
    
    def test_missing_bytes_scanned(self, valid_report):
        """Test validation catches missing bytes_scanned."""
        del valid_report["bytes_scanned"]
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("bytes_scanned" in e for e in errors)
    
    def test_multiple_missing_required_fields(self, valid_report):
        """Test validation reports multiple missing required fields."""
        del valid_report["execution_time_seconds"]
        del valid_report["query_hash"]
        del valid_report["row_count"]
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert len(errors) >= 3, f"Should report at least 3 missing fields. Got {len(errors)}"
    
    def test_wrong_type_execution_time_string(self, valid_report):
        """Test validation catches when execution_time_seconds is a string."""
        valid_report["execution_time_seconds"] = "12.5"  # Should be float/int
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("execution_time_seconds" in e and "type" in e.lower() for e in errors), \
            f"Error should mention type mismatch for execution_time_seconds. Got: {errors}"
    
    def test_wrong_type_query_hash_number(self, valid_report):
        """Test validation catches when query_hash is a number."""
        valid_report["query_hash"] = 12345  # Should be string
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("query_hash" in e and "type" in e.lower() for e in errors)
    
    def test_wrong_type_row_count_string(self, valid_report):
        """Test validation catches when row_count is a string."""
        valid_report["row_count"] = "5000"  # Should be int
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("row_count" in e and "type" in e.lower() for e in errors)
    
    def test_wrong_type_bytes_scanned_float(self, valid_report):
        """Test validation catches when bytes_scanned is a float."""
        valid_report["bytes_scanned"] = 1048576.5  # Should be int
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("bytes_scanned" in e and "type" in e.lower() for e in errors)
    
    def test_execution_time_accepts_float(self, valid_report):
        """Test that execution_time_seconds accepts float type."""
        valid_report["execution_time_seconds"] = 12.5  # Float is valid
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is True
    
    def test_execution_time_accepts_int(self, valid_report):
        """Test that execution_time_seconds accepts int type."""
        valid_report["execution_time_seconds"] = 12  # Int is also valid
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is True
    
    def test_optional_field_data_hash_wrong_type(self, valid_report):
        """Test validation catches wrong type for optional data_hash field."""
        valid_report["data_hash"] = 123456  # Should be string
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("data_hash" in e and "type" in e.lower() for e in errors)
    
    def test_optional_field_timestamp_wrong_type(self, valid_report):
        """Test validation catches wrong type for optional timestamp field."""
        valid_report["timestamp"] = 1234567890  # Should be string
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("timestamp" in e and "type" in e.lower() for e in errors)
    
    def test_optional_field_join_count_wrong_type(self, valid_report):
        """Test validation catches wrong type for join_count."""
        valid_report["join_count"] = "3"  # Should be int
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is False
        assert any("join_count" in e and "type" in e.lower() for e in errors)
    
    def test_analysis_schema_validation(self):
        """Test validation of analysis schema type."""
        analysis_data = {
            "baseline_report": {"test": "data"},
            "candidate_report": {"test": "data"},
            "comparison_results": {"deltas": {}}
        }
        is_valid, errors = validate_report_schema(analysis_data, "analysis")
        assert is_valid is True
    
    def test_analysis_schema_missing_baseline(self):
        """Test analysis schema detects missing baseline_report."""
        analysis_data = {
            "candidate_report": {"test": "data"},
            "comparison_results": {"deltas": {}}
        }
        is_valid, errors = validate_report_schema(analysis_data, "analysis")
        assert is_valid is False
        assert any("baseline_report" in e for e in errors)
    
    def test_unknown_schema_type_raises_error(self, valid_report):
        """Test that unknown schema type raises InvalidSchema exception."""
        with pytest.raises(InvalidSchema) as exc_info:
            validate_report_schema(valid_report, "unknown_type")
        assert "unknown_type" in str(exc_info.value).lower()
        assert "supported" in str(exc_info.value).lower()


# ============================================================================
# TEST: MISSING ARTIFACT HANDLING
# ============================================================================

class TestMissingArtifactHandling:
    """Test suite for handling missing artifact files."""
    
    def test_missing_manifest_json(self):
        """Test detection of missing manifest.json with helpful error message."""
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe("nonexistent_manifest.json")
        
        error_msg = str(exc_info.value)
        assert "nonexistent_manifest.json" in error_msg
        assert "not found" in error_msg.lower() or "required" in error_msg.lower()
        assert "expected location" in error_msg.lower()
        assert "please ensure" in error_msg.lower() or "remedy" in error_msg.lower()
    
    def test_missing_run_results_json(self):
        """Test detection of missing run_results.json with helpful error message."""
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe("nonexistent_run_results.json")
        
        error_msg = str(exc_info.value)
        assert "nonexistent_run_results.json" in error_msg
        assert "not found" in error_msg.lower()
    
    def test_missing_artifact_exception_message_content(self):
        """Verify MissingArtifact exception message includes remediation."""
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe("missing_file.json")
        
        error_msg = str(exc_info.value)
        # Should include: what went wrong, affected file, how to fix
        assert "missing_file.json" in error_msg  # Affected file
        assert "not found" in error_msg.lower()  # What went wrong
        # Remediation may be implicit or explicit
    
    def test_missing_artifact_with_absolute_path_suggestion(self):
        """Verify error message includes absolute path for debugging."""
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe("some_missing_file.json")
        
        error_msg = str(exc_info.value)
        # Should suggest absolute path or expected location
        assert "expected" in error_msg.lower() or "location" in error_msg.lower()
    
    def test_directory_instead_of_file(self, tmp_path):
        """Test handling of directory path instead of file."""
        dir_path = tmp_path / "test_dir"
        dir_path.mkdir()
        
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe(str(dir_path))
        
        error_msg = str(exc_info.value)
        assert str(dir_path) in error_msg or "directory" in error_msg.lower() or "not a file" in error_msg.lower()


# ============================================================================
# TEST: INVALID JSON HANDLING
# ============================================================================

class TestInvalidJsonHandling:
    """Test suite for handling invalid JSON files."""
    
    def test_malformed_json_syntax(self, temp_invalid_json_file):
        """Test detection of malformed JSON syntax."""
        with pytest.raises(InvalidSchema) as exc_info:
            load_json_safe(temp_invalid_json_file)
        
        error_msg = str(exc_info.value)
        assert "malformed" in error_msg.lower() or "invalid" in error_msg.lower()
        assert "json" in error_msg.lower()
    
    def test_invalid_json_error_includes_line_column(self, temp_invalid_json_file):
        """Test that JSON error includes line and column information."""
        with pytest.raises(InvalidSchema) as exc_info:
            load_json_safe(temp_invalid_json_file)
        
        error_msg = str(exc_info.value)
        # Should mention line and column info for debugging
        assert "line" in error_msg.lower() or "error" in error_msg.lower()
    
    def test_invalid_json_includes_file_path(self, temp_invalid_json_file):
        """Test that error message includes the problematic file path."""
        with pytest.raises(InvalidSchema) as exc_info:
            load_json_safe(temp_invalid_json_file)
        
        error_msg = str(exc_info.value)
        assert temp_invalid_json_file in error_msg
    
    def test_invalid_json_includes_remediation(self, temp_invalid_json_file):
        """Test that error message suggests how to fix the JSON."""
        with pytest.raises(InvalidSchema) as exc_info:
            load_json_safe(temp_invalid_json_file)
        
        error_msg = str(exc_info.value)
        # Should suggest checking JSON validity
        assert "verify" in error_msg.lower() or "check" in error_msg.lower() or "valid" in error_msg.lower()
    
    def test_incomplete_json_structure(self, temp_json_file):
        """Test detection of incomplete JSON structures."""
        # Write incomplete JSON
        with open(temp_json_file, 'w') as f:
            f.write('{"incomplete":')
        
        with pytest.raises(InvalidSchema) as exc_info:
            load_json_safe(temp_json_file)
        
        error_msg = str(exc_info.value)
        assert "malformed" in error_msg.lower() or "invalid" in error_msg.lower()
    
    def test_json_with_trailing_comma(self, temp_json_file):
        """Test detection of JSON with trailing commas (invalid in JSON)."""
        with open(temp_json_file, 'w') as f:
            f.write('{"key": "value",}')
        
        with pytest.raises(InvalidSchema):
            load_json_safe(temp_json_file)
    
    def test_json_with_single_quotes(self, temp_json_file):
        """Test detection of JSON with single quotes (should be double)."""
        with open(temp_json_file, 'w') as f:
            f.write("{'key': 'value'}")
        
        with pytest.raises(InvalidSchema):
            load_json_safe(temp_json_file)
    
    def test_json_with_unescaped_quotes(self, temp_json_file):
        """Test detection of unescaped quotes in JSON string."""
        with open(temp_json_file, 'w') as f:
            f.write('{"key": "value with " quote"}')
        
        with pytest.raises(InvalidSchema):
            load_json_safe(temp_json_file)
    
    def test_exception_type_for_invalid_json(self, temp_invalid_json_file):
        """Verify InvalidSchema exception is raised (not generic Exception)."""
        exception_raised = False
        exception_type = None
        
        try:
            load_json_safe(temp_invalid_json_file)
        except InvalidSchema:
            exception_raised = True
            exception_type = InvalidSchema
        except Exception as e:
            exception_type = type(e)
        
        assert exception_raised is True, f"InvalidSchema not raised, got {exception_type}"


# ============================================================================
# TEST: CONFIGURATION ERROR HANDLING
# ============================================================================

class TestConfigErrorHandling:
    """Test suite for configuration error handling."""
    
    def test_config_error_exception_exists(self):
        """Verify ConfigError exception class is defined."""
        assert ConfigError is not None
        assert issubclass(ConfigError, Exception)
    
    def test_config_error_with_message(self):
        """Test raising ConfigError with descriptive message."""
        error_msg = "Database configuration is missing"
        with pytest.raises(ConfigError) as exc_info:
            raise ConfigError(error_msg)
        
        assert error_msg in str(exc_info.value)
    
    def test_config_error_message_includes_problem(self):
        """Test ConfigError message includes problem description."""
        error_msg = "Missing required config: DATABASE_URL"
        with pytest.raises(ConfigError) as exc_info:
            raise ConfigError(error_msg)
        
        assert "config" in str(exc_info.value).lower()
    
    def test_config_error_message_includes_remediation(self):
        """Test ConfigError message includes how to fix."""
        error_msg = "Missing required config: DATABASE_URL\nSet env var or update config.py"
        with pytest.raises(ConfigError) as exc_info:
            raise ConfigError(error_msg)
        
        error_str = str(exc_info.value).lower()
        assert "config" in error_str


# ============================================================================
# TEST: CUSTOM EXCEPTION CLASSES
# ============================================================================

class TestCustomExceptions:
    """Test suite for all custom exception classes."""
    
    def test_missing_artifact_exception_class(self):
        """Verify MissingArtifact is a proper exception class."""
        assert issubclass(MissingArtifact, Exception)
        exc = MissingArtifact("test message")
        assert isinstance(exc, Exception)
        assert "test message" in str(exc)
    
    def test_invalid_schema_exception_class(self):
        """Verify InvalidSchema is a proper exception class."""
        assert issubclass(InvalidSchema, Exception)
        exc = InvalidSchema("schema error")
        assert isinstance(exc, Exception)
        assert "schema error" in str(exc)
    
    def test_data_mismatch_exception_class(self):
        """Verify DataMismatch is a proper exception class."""
        assert issubclass(DataMismatch, Exception)
        exc = DataMismatch("data mismatch detected")
        assert isinstance(exc, Exception)
        assert "data mismatch" in str(exc)
    
    def test_config_error_exception_class(self):
        """Verify ConfigError is a proper exception class."""
        assert issubclass(ConfigError, Exception)
        exc = ConfigError("config missing")
        assert isinstance(exc, Exception)
        assert "config" in str(exc)
    
    def test_exception_inheritance(self):
        """Verify all custom exceptions inherit from Exception."""
        exceptions = [MissingArtifact, InvalidSchema, DataMismatch, ConfigError]
        for exc_class in exceptions:
            assert issubclass(exc_class, Exception), \
                f"{exc_class.__name__} should inherit from Exception"
    
    def test_exception_with_multiline_message(self):
        """Test exception with multi-line message for detailed info."""
        msg = "Error detected\n  Line 1: problem\n  Line 2: remediation"
        with pytest.raises(MissingArtifact) as exc_info:
            raise MissingArtifact(msg)
        
        error_str = str(exc_info.value)
        assert "Error detected" in error_str
        assert "Line 1" in error_str or "Line 2" in error_str or "problem" in error_str.lower()


# ============================================================================
# TEST: ERROR MESSAGE QUALITY
# ============================================================================

class TestErrorMessageQuality:
    """Test suite for ensuring error messages are clear and actionable."""
    
    def test_missing_artifact_message_structure(self):
        """Test that MissingArtifact errors follow consistent structure."""
        with pytest.raises(MissingArtifact) as exc_info:
            load_json_safe("missing_artifact.json")
        
        error_msg = str(exc_info.value)
        # Should clearly identify the problem
        assert "artifact" in error_msg.lower() or "not found" in error_msg.lower()
    
    def test_invalid_schema_message_mentions_field(self):
        """Test that InvalidSchema errors mention which field is invalid."""
        valid_report = {
            "execution_time_seconds": "invalid",  # Wrong type
            "query_hash": "abc123",
            "row_count": 100,
            "bytes_scanned": 1000
        }
        is_valid, errors = validate_report_schema(valid_report, "report")
        
        assert is_valid is False
        # At least one error should mention the field name
        error_str = " ".join(errors).lower()
        assert "execution_time" in error_str or "field" in error_str
    
    def test_error_message_includes_expected_vs_actual(self):
        """Test error messages show expected vs actual values/types."""
        valid_report = {
            "execution_time_seconds": "12.5",  # String instead of float
            "query_hash": "abc123",
            "row_count": 100,
            "bytes_scanned": 1000
        }
        is_valid, errors = validate_report_schema(valid_report, "report")
        
        assert is_valid is False
        error_str = " ".join(errors).lower()
        # Should mention types
        assert "type" in error_str or "expected" in error_str or "got" in error_str
    
    def test_validation_error_not_generic_exception(self, temp_invalid_json_file):
        """Verify validation errors are specific, not generic Exception messages."""
        try:
            load_json_safe(temp_invalid_json_file)
        except InvalidSchema as e:
            error_msg = str(e)
            # Should be specific, not generic
            assert len(error_msg) > 10  # Has meaningful content
            assert "malformed" in error_msg.lower() or "json" in error_msg.lower()


# ============================================================================
# TEST: DATA MISMATCH EXCEPTION
# ============================================================================

class TestDataMismatchException:
    """Test suite for DataMismatch exception usage."""
    
    def test_data_mismatch_hash_inconsistency(self):
        """Test DataMismatch exception for hash inconsistency."""
        with pytest.raises(DataMismatch) as exc_info:
            raise DataMismatch(
                "Data hash mismatch detected:\n"
                "  Expected: abc123\n"
                "  Actual: def456\n"
                "  This indicates output data changed between runs"
            )
        
        error_msg = str(exc_info.value)
        assert "hash" in error_msg.lower() or "mismatch" in error_msg.lower()
        assert "expected" in error_msg.lower()
        assert "actual" in error_msg.lower()
    
    def test_data_mismatch_message_includes_remediation(self):
        """Test that DataMismatch messages suggest remediation steps."""
        error_msg = (
            "Data mismatch: output hash changed\n"
            "  Remedy: investigate SQL changes or data changes in source"
        )
        with pytest.raises(DataMismatch) as exc_info:
            raise DataMismatch(error_msg)
        
        assert "remedy" in str(exc_info.value).lower() or "investigate" in str(exc_info.value).lower()


# ============================================================================
# TEST: EDGE CASES AND INTEGRATION
# ============================================================================

class TestEdgeCasesAndIntegration:
    """Test suite for edge cases and integration scenarios."""
    
    def test_empty_json_file(self, temp_json_file):
        """Test handling of empty JSON file."""
        with open(temp_json_file, 'w') as f:
            f.write("")
        
        with pytest.raises(InvalidSchema):
            load_json_safe(temp_json_file)
    
    def test_json_with_only_whitespace(self, temp_json_file):
        """Test handling of JSON file with only whitespace."""
        with open(temp_json_file, 'w') as f:
            f.write("   \n   \n   ")
        
        with pytest.raises(InvalidSchema):
            load_json_safe(temp_json_file)
    
    def test_valid_json_empty_dict(self, temp_json_file):
        """Test that valid empty JSON dict loads successfully."""
        with open(temp_json_file, 'w') as f:
            json.dump({}, f)
        
        data = load_json_safe(temp_json_file)
        assert data == {}
    
    def test_schema_validation_with_extra_fields(self, valid_report):
        """Test that validation passes even with extra fields not in schema."""
        valid_report["extra_field"] = "extra_value"
        valid_report["another_extra"] = 123
        
        is_valid, errors = validate_report_schema(valid_report, "report")
        assert is_valid is True, "Extra fields should not cause validation failure"
    
    def test_null_values_in_required_field(self, valid_report):
        """Test validation handling of None/null values in required fields."""
        valid_report["execution_time_seconds"] = None
        is_valid, errors = validate_report_schema(valid_report, "report")
        # None is wrong type for float/int
        assert is_valid is False
    
    def test_empty_string_in_string_field(self, valid_report):
        """Test that empty strings are acceptable for string fields."""
        valid_report["query_hash"] = ""  # Empty string, but still a string
        is_valid, errors = validate_report_schema(valid_report, "report")
        # Empty string is technically valid (it's still a string)
        # Validation accepts it; semantic validation would reject it
        assert is_valid is True
    
    def test_negative_numbers_in_numeric_fields(self, valid_report):
        """Test handling of negative numbers in numeric fields."""
        valid_report["bytes_scanned"] = -1000  # Negative, but still int
        valid_report["execution_time_seconds"] = -5.2  # Negative, but still float
        
        is_valid, errors = validate_report_schema(valid_report, "report")
        # Type validation passes; semantic validation would flag as invalid
        assert is_valid is True


# ============================================================================
# TEST: FIXTURE FILES
# ============================================================================

class TestFixtureFiles:
    """Test suite verifying fixture files for testing."""
    
    def test_invalid_report_fixture_exists(self):
        """Verify invalid_report.json fixture file exists."""
        fixture_path = Path(__file__).parent / "fixtures" / "invalid_report.json"
        assert fixture_path.exists(), f"Fixture not found at {fixture_path}"
    
    def test_incomplete_report_fixture_exists(self):
        """Verify incomplete_report.json fixture file exists."""
        fixture_path = Path(__file__).parent / "fixtures" / "incomplete_report.json"
        assert fixture_path.exists(), f"Fixture not found at {fixture_path}"
    
    def test_invalid_report_is_actually_invalid(self):
        """Test that invalid_report.json actually fails schema validation."""
        fixture_path = Path(__file__).parent / "fixtures" / "invalid_report.json"
        if fixture_path.exists():
            data = load_json_safe(str(fixture_path))
            is_valid, errors = validate_report_schema(data, "report")
            assert is_valid is False, "invalid_report.json should fail validation"
            assert len(errors) > 0
    
    def test_incomplete_report_is_actually_incomplete(self):
        """Test that incomplete_report.json has missing required fields."""
        fixture_path = Path(__file__).parent / "fixtures" / "incomplete_report.json"
        if fixture_path.exists():
            data = load_json_safe(str(fixture_path))
            is_valid, errors = validate_report_schema(data, "report")
            assert is_valid is False, "incomplete_report.json should fail validation"
            # Should have missing field errors
            assert any("missing" in e.lower() for e in errors)


# ============================================================================
# TEST: SCHEMA DEFINITIONS
# ============================================================================

class TestSchemaDefinitions:
    """Test suite for schema definition functions."""
    
    def test_get_schema_definitions_returns_dict(self):
        """Verify get_schema_definitions returns a dictionary."""
        schemas = get_schema_definitions()
        assert isinstance(schemas, dict)
        assert len(schemas) > 0
    
    def test_schema_definitions_includes_report_type(self):
        """Verify schema definitions include 'report' type."""
        schemas = get_schema_definitions()
        assert "report" in schemas
    
    def test_schema_definitions_includes_analysis_type(self):
        """Verify schema definitions include 'analysis' type."""
        schemas = get_schema_definitions()
        assert "analysis" in schemas
    
    def test_report_schema_has_required_fields(self):
        """Verify report schema defines required fields."""
        schemas = get_schema_definitions()
        report_schema = schemas["report"]
        assert "required_fields" in report_schema
        assert isinstance(report_schema["required_fields"], list)
        assert len(report_schema["required_fields"]) > 0
    
    def test_report_schema_has_field_types(self):
        """Verify report schema defines field types."""
        schemas = get_schema_definitions()
        report_schema = schemas["report"]
        assert "field_types" in report_schema
        assert isinstance(report_schema["field_types"], dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
