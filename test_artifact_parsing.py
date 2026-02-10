#!/usr/bin/env python3
"""
Test script to validate artifact parsing functionality.

Tests the load_manifest(), load_run_results(), extract_model_data(), 
and validate_artifact_fields() functions with actual artifacts.
"""

import sys
from pathlib import Path

# Add helpers to path
sys.path.insert(0, str(Path(__file__).parent))

from helpers import (
    load_manifest,
    load_run_results,
    extract_model_data,
    validate_artifact_fields,
    setup_logging,
    MissingArtifact,
    InvalidSchema
)


def test_artifact_parsing():
    """Test parsing of dbt artifacts."""
    logger = setup_logging("test_parser")
    
    print("\n" + "=" * 80)
    print("ARTIFACT PARSING TEST")
    print("=" * 80)
    
    # Test 1: Load manifest
    print("\n[TEST 1] Loading manifest.json...")
    try:
        manifest = load_manifest("target/manifest.json", logger)
        print("✓ Manifest loaded successfully")
        
        # Check structure
        assert "metadata" in manifest, "Missing 'metadata' in manifest"
        assert "nodes" in manifest, "Missing 'nodes' in manifest"
        
        nodes = manifest.get("nodes", {})
        model_count = sum(1 for node_id in nodes if node_id.startswith("model."))
        print(f"  - Total nodes: {len(nodes)}")
        print(f"  - Model nodes: {model_count}")
        
    except MissingArtifact as e:
        print(f"✗ MissingArtifact: {e}")
        return False
    except InvalidSchema as e:
        print(f"✗ InvalidSchema: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 2: Load run_results
    print("\n[TEST 2] Loading run_results.json...")
    try:
        run_results = load_run_results("target/run_results.json", logger)
        print("✓ Run results loaded successfully")
        
        # Check structure
        assert "metadata" in run_results, "Missing 'metadata' in run_results"
        assert "results" in run_results, "Missing 'results' in run_results"
        
        results = run_results.get("results", [])
        success_count = sum(1 for r in results if r.get("status") == "success")
        print(f"  - Total execution results: {len(results)}")
        print(f"  - Successful executions: {success_count}")
        
    except MissingArtifact as e:
        print(f"✗ MissingArtifact: {e}")
        return False
    except InvalidSchema as e:
        print(f"✗ InvalidSchema: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Extract model data
    print("\n[TEST 3] Extracting model data...")
    try:
        nodes = manifest.get("nodes", {})
        model_ids = [node_id for node_id in nodes if node_id.startswith("model.")]
        
        if not model_ids:
            print("✗ No models found in manifest")
            return False
        
        test_model_id = model_ids[0]
        print(f"  - Testing with model: {test_model_id}")
        
        model_data = extract_model_data(test_model_id, manifest, run_results, logger)
        print("✓ Model data extracted successfully")
        
        # Check extracted fields
        required_fields = ["unique_id", "model_name", "status", "execution_time"]
        missing_fields = [f for f in required_fields if f not in model_data]
        if missing_fields:
            print(f"✗ Missing fields: {missing_fields}")
            return False
        
        print(f"  - Model name: {model_data.get('model_name')}")
        print(f"  - Status: {model_data.get('status')}")
        print(f"  - Execution time: {model_data.get('execution_time')}s")
        print(f"  - Rows affected: {model_data.get('adapter_response', {}).get('rows_affected')}")
        
    except KeyError as e:
        print(f"✗ KeyError: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 4: Validate model data
    print("\n[TEST 4] Validating model data...")
    try:
        is_valid, errors = validate_artifact_fields(model_data, logger)
        print(f"✓ Validation completed (valid: {is_valid})")
        
        if errors:
            for error in errors:
                prefix = "  [WARNING]" if error.startswith("WARNING:") else "  [ERROR]"
                print(f"{prefix} {error}")
        else:
            print("  - No validation errors")
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 5: Parse all models
    print("\n[TEST 5] Parsing all models...")
    try:
        model_ids = [node_id for node_id in nodes if node_id.startswith("model.")]
        parsed_count = 0
        error_count = 0
        
        for model_id in model_ids:
            try:
                model_data = extract_model_data(model_id, manifest, run_results, logger)
                is_valid, errors = validate_artifact_fields(model_data, logger)
                parsed_count += 1
            except Exception as e:
                error_count += 1
        
        print(f"✓ All models processed")
        print(f"  - Successfully parsed: {parsed_count}/{len(model_ids)}")
        print(f"  - Errors: {error_count}")
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 80)
    print("✓ ALL TESTS PASSED")
    print("=" * 80 + "\n")
    return True


if __name__ == "__main__":
    success = test_artifact_parsing()
    sys.exit(0 if success else 1)
