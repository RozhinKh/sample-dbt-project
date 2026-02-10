# Test Validation and Error Handling Documentation

## Overview

`tests/test_validation.py` contains comprehensive unit tests for schema validation and error handling in the benchmarking system. The test suite ensures robust failure modes and clear, actionable user feedback through 69+ tests across 10 test classes.

## Test Organization

### 1. TestReportSchemaValidation (21 tests)
Tests for JSON schema validation against defined structures.

**Required Field Tests:**
- `test_valid_report_passes_validation()` - Verify valid reports pass
- `test_missing_execution_time_seconds()` - Catch missing execution_time_seconds
- `test_missing_query_hash()` - Catch missing query_hash
- `test_missing_row_count()` - Catch missing row_count
- `test_missing_bytes_scanned()` - Catch missing bytes_scanned
- `test_multiple_missing_required_fields()` - Detect multiple errors

**Data Type Validation Tests:**
- `test_wrong_type_execution_time_string()` - Reject string for numeric field
- `test_wrong_type_query_hash_number()` - Reject number for string field
- `test_wrong_type_row_count_string()` - Type validation for row_count
- `test_wrong_type_bytes_scanned_float()` - Type validation for bytes_scanned
- `test_execution_time_accepts_float()` - Accept float type
- `test_execution_time_accepts_int()` - Accept int type

**Optional Field Tests:**
- `test_optional_field_data_hash_wrong_type()` - Validate optional fields
- `test_optional_field_timestamp_wrong_type()` - Timestamp type checking
- `test_optional_field_join_count_wrong_type()` - Complexity metric types

**Schema Type Tests:**
- `test_analysis_schema_validation()` - Validate analysis schema
- `test_analysis_schema_missing_baseline()` - Analysis schema requirements
- `test_unknown_schema_type_raises_error()` - Invalid schema type detection

### 2. TestMissingArtifactHandling (5 tests)
Tests for graceful handling of missing dbt artifacts.

- `test_missing_manifest_json()` - Detection with helpful error message
- `test_missing_run_results_json()` - Missing run_results detection
- `test_missing_artifact_exception_message_content()` - Message structure
- `test_missing_artifact_with_absolute_path_suggestion()` - Path guidance
- `test_directory_instead_of_file()` - Directory path handling

**MissingArtifact Exception Ensures:**
- Clear identification of missing file
- Expected location path
- Remediation steps (ensure dbt run completed)

### 3. TestInvalidJsonHandling (9 tests)
Tests for malformed JSON detection and reporting.

- `test_malformed_json_syntax()` - Basic malformed JSON detection
- `test_invalid_json_error_includes_line_column()` - Line/column reporting
- `test_invalid_json_includes_file_path()` - Problem file identification
- `test_invalid_json_includes_remediation()` - Fix suggestions
- `test_incomplete_json_structure()` - Incomplete JSON detection
- `test_json_with_trailing_comma()` - Invalid syntax patterns
- `test_json_with_single_quotes()` - Quote validation
- `test_json_with_unescaped_quotes()` - Escape sequence validation
- `test_exception_type_for_invalid_json()` - Proper exception class

**InvalidSchema Exception Ensures:**
- Malformed JSON identification
- Line and column information
- Specific error messages (not generic)
- File path in error message
- Remediation steps

### 4. TestConfigErrorHandling (4 tests)
Tests for configuration error detection.

- `test_config_error_exception_exists()` - ConfigError class verification
- `test_config_error_with_message()` - Message handling
- `test_config_error_message_includes_problem()` - Problem description
- `test_config_error_message_includes_remediation()` - Remediation steps

### 5. TestCustomExceptions (6 tests)
Tests for all custom exception classes.

- `test_missing_artifact_exception_class()` - MissingArtifact class
- `test_invalid_schema_exception_class()` - InvalidSchema class
- `test_data_mismatch_exception_class()` - DataMismatch class
- `test_config_error_exception_class()` - ConfigError class
- `test_exception_inheritance()` - Proper Exception inheritance
- `test_exception_with_multiline_message()` - Detailed error messages

### 6. TestErrorMessageQuality (4 tests)
Tests ensuring error messages are clear and actionable.

- `test_missing_artifact_message_structure()` - Consistent structure
- `test_invalid_schema_message_mentions_field()` - Field identification
- `test_error_message_includes_expected_vs_actual()` - Expected/actual values
- `test_validation_error_not_generic_exception()` - Specific messages

### 7. TestDataMismatchException (2 tests)
Tests for data consistency validation.

- `test_data_mismatch_hash_inconsistency()` - Hash mismatch detection
- `test_data_mismatch_message_includes_remediation()` - Fix guidance

### 8. TestEdgeCasesAndIntegration (9 tests)
Tests for edge cases and special conditions.

- `test_empty_json_file()` - Empty file handling
- `test_json_with_only_whitespace()` - Whitespace-only files
- `test_valid_json_empty_dict()` - Empty valid JSON
- `test_schema_validation_with_extra_fields()` - Extra field tolerance
- `test_null_values_in_required_field()` - Null value handling
- `test_empty_string_in_string_field()` - Empty string acceptance
- `test_negative_numbers_in_numeric_fields()` - Negative number handling

### 9. TestFixtureFiles (4 tests)
Tests for fixture file integrity and validity.

- `test_invalid_report_fixture_exists()` - Fixture file presence
- `test_incomplete_report_fixture_exists()` - Fixture file presence
- `test_invalid_report_is_actually_invalid()` - Fixture validity
- `test_incomplete_report_is_actually_incomplete()` - Fixture validity

### 10. TestSchemaDefinitions (5 tests)
Tests for schema definition structure.

- `test_get_schema_definitions_returns_dict()` - Dictionary return type
- `test_schema_definitions_includes_report_type()` - Report schema present
- `test_schema_definitions_includes_analysis_type()` - Analysis schema present
- `test_report_schema_has_required_fields()` - Required fields list
- `test_report_schema_has_field_types()` - Field types dictionary

## Fixture Files

### invalid_report.json
Contains a report with intentional type mismatches:
- `execution_time_seconds`: string (should be float/int)
- `query_hash`: number (should be string)
- `row_count`: string (should be int)
- `bytes_scanned`: float (should be int)
- `data_hash`: array (should be string)
- `join_count`: string (should be int)
- etc.

Used by: `test_invalid_report_is_actually_invalid()`

### incomplete_report.json
Contains a report missing required fields:
- Missing: `query_hash`, `bytes_scanned`
- Present: `execution_time_seconds`, `row_count`, `timestamp`

Used by: `test_incomplete_report_is_actually_incomplete()`

## Running Tests

### Run all validation tests:
```bash
pytest tests/test_validation.py -v
```

### Run specific test class:
```bash
pytest tests/test_validation.py::TestReportSchemaValidation -v
pytest tests/test_validation.py::TestMissingArtifactHandling -v
pytest tests/test_validation.py::TestInvalidJsonHandling -v
```

### Run specific test:
```bash
pytest tests/test_validation.py::TestReportSchemaValidation::test_valid_report_passes_validation -v
```

### Run with coverage:
```bash
pytest tests/test_validation.py \
  --cov=helpers \
  --cov-report=html \
  --cov-report=term-missing \
  -v
```

### Run all tests including this suite:
```bash
pytest tests/ -v
```

## Exception Types and Messages

### MissingArtifact
**When:** Required file not found or not accessible
**Message Format:**
```
Required artifact not found: {file_path}
  Expected location: {absolute_path}
  Please ensure the dbt run has completed and artifacts are present.
```
**Components:** Problem (not found), Component (file path), Remediation (ensure dbt run completed)

### InvalidSchema
**When:** Malformed JSON or schema validation failure
**Message Format:**
```
Malformed JSON in {file_path}:
  Error: {error_details}
  Line: {lineno}, Column: {colno}
  Please verify the JSON file is valid.
```
**Components:** Problem (malformed), Component (file/line/column), Remediation (verify JSON)

### DataMismatch
**When:** Data hash inconsistency detected
**Message Format:**
```
Data hash mismatch detected:
  Expected: {hash}
  Actual: {hash}
  This indicates output data changed between runs
```
**Components:** Problem (mismatch), Component (hash values), Remediation (investigate changes)

### ConfigError
**When:** Configuration missing or invalid
**Message Format:**
```
Could not find dbt project root (dbt_project.yml).
Run this script from within a dbt project directory.
```
**Components:** Problem (missing config), Component (config name), Remediation (fix instruction)

## Success Criteria Met

✅ **Schema Validation**
- Tests pass for well-formed reports
- Tests fail correctly for invalid reports
- Type validation for all required fields

✅ **Missing Artifact Detection**
- Correct identification of missing files
- Appropriate exception raised (MissingArtifact)
- Clear error messages with remediation

✅ **Invalid JSON Parsing**
- Malformed JSON caught and reported
- Helpful error messages (line/column info)
- Specific exception type (InvalidSchema)

✅ **Configuration Errors**
- Early detection with clear guidance
- ConfigError exception properly raised

✅ **Custom Exceptions**
- All four types tested (MissingArtifact, InvalidSchema, DataMismatch, ConfigError)
- Proper inheritance from Exception
- Multi-line messages with details

✅ **Error Message Quality**
- What went wrong: Clearly stated
- Affected component: File/field identification
- How to fix: Remediation steps included

✅ **No External Dependencies**
- All data mocked using fixtures
- No file system access except temporary test files
- No external services required

✅ **Not Masking Issues**
- Specific exception types (not generic Exception)
- Detailed error messages (not one-liners)
- Proper error propagation

## Dependencies

- `pytest`: Unit testing framework
- `helpers.py`: Validation functions and custom exceptions
- `config.py`: Configuration definitions
- Temporary file handling via `tempfile` module

## Notes

- All tests use mocked data or temporary files
- No production data access required
- Tests are isolated and can run in any order
- Fixture files are small JSON documents for quick validation
- Test suite follows pytest conventions
