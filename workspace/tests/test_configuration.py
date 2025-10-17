"""
Test suite for configuration management operations

This test file validates configuration functionality including:
- Reading configuration from database
- Updating configuration status
- Loading and validating Excel configuration files

All tests run in the 'test' schema to avoid affecting production data.

Run this test:
    python workspace/tests/test_configuration.py
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add workspace to path
workspace_path = Path(__file__).parent.parent
sys.path.insert(0, str(workspace_path))

# Import required modules
from helpers.database import (
    execute_query,
    execute_insert,
    execute_command,
    test_connection,
    init_database,
    DatabaseError
)
from helpers.configuration import (
    read_configuration,
    update_configuration_status,
    load_configuration_file,
    ConfigurationError,
    ConfigurationTransformer
)
from helpers.constants import ConfigurationStatus


TEST_SCHEMA = Path(__file__).stem

TEST_EXCEL_PATH = str(Path(__file__).parent / 'test_configuration.xlsx')

def setup_test_schema():
    """Initialize test schema with database tables"""
    print("\n" + "="*80)
    print("SETUP: Initializing Test Schema")
    print("="*80)

    try:
        # Initialize test schema
        print(f"Creating and initializing schema '{TEST_SCHEMA}'...")
        success = init_database(schema=TEST_SCHEMA)

        if not success:
            print("Failed to initialize test schema")
            return False

        print(f"Test schema '{TEST_SCHEMA}' initialized successfully")
        return True

    except Exception as e:
        print(f"Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_test_schema():
    """Drop the test schema"""
    print("\n" + "="*80)
    print("CLEANUP: Dropping Test Schema")
    print("="*80)

    try:
        from helpers.database import get_engine
        from sqlalchemy import text

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"))
            conn.commit()

        print(f"✓ Test schema '{TEST_SCHEMA}' dropped successfully")
        return True

    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")
        return False


def create_test_cycle(cycle_name):
    """Helper function to create a test cycle and return its ID"""
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
        schema=TEST_SCHEMA
    )
    return cycle_id


def test_read_configuration():
    """Test 1: Read configuration from database"""
    print("\n" + "="*80)
    print("TEST 1: Read Configuration")
    print("="*80)

    try:
        # Setup: Create cycle and configuration
        print("Setting up test data...")
        cycle_id = create_test_cycle('test-1')
        print(f"  Created test cycle ID: {cycle_id}")

        config_data = {
            'TAB-A': [
                {'A-1': 'val1', 'A-2': 'data1', 'A-3': 'info1'},
                {'A-1': 'val2', 'A-2': 'data2', 'A-3': 'info2'}
            ],
            'TAB-B': [
                {'B-1': 100, 'B-2': 10.5}
            ]
        }

        import json
        config_id = execute_insert(
            """INSERT INTO irp_configuration
               (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
               VALUES (%s, %s, %s, %s, %s)""",
            (cycle_id, '/test/config.xlsx', json.dumps(config_data),
             ConfigurationStatus.NEW, datetime.now()),
            schema=TEST_SCHEMA
        )
        print(f"  Created configuration ID: {config_id}")

        # Test reading configuration
        print("\nReading configuration...")
        result = read_configuration(config_id, schema=TEST_SCHEMA)

        print(f"✓ Successfully read configuration")
        print(f"  Configuration ID: {result['id']}")
        print(f"  Cycle ID: {result['cycle_id']}")
        print(f"  File Name: {result['configuration_file_name']}")
        print(f"  Status: {result['status']}")
        print(f"  Data Keys: {list(result['configuration_data'].keys())}")

        # Verify data structure
        assert result['id'] == config_id
        assert result['cycle_id'] == cycle_id
        assert result['status'] == ConfigurationStatus.NEW
        assert 'TAB-A' in result['configuration_data']
        assert 'TAB-B' in result['configuration_data']

        print("✓ All assertions passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_update_configuration_status():
    """Test 2: Update configuration status"""
    print("\n" + "="*80)
    print("TEST 2: Update Configuration Status")
    print("="*80)

    try:
        # Setup: Create cycle and configuration
        print("Setting up test data...")
        cycle_id = create_test_cycle('test-2')

        import json
        config_id = execute_insert(
            """INSERT INTO irp_configuration
               (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
               VALUES (%s, %s, %s, %s, %s)""",
            (cycle_id, '/test/config.xlsx', json.dumps({'test': 'data'}),
             ConfigurationStatus.NEW, datetime.now()),
            schema=TEST_SCHEMA
        )
        print(f"  Created configuration ID: {config_id}")

        # Test updating status
        print("\nUpdating status from NEW to VALID...")
        result = update_configuration_status(config_id, ConfigurationStatus.VALID, schema=TEST_SCHEMA)
        assert result == True, "Update should return True"
        print("✓ Status updated successfully")

        # Verify the update
        config = read_configuration(config_id, schema=TEST_SCHEMA)
        assert config['status'] == ConfigurationStatus.VALID
        print(f"✓ Verified status is now: {config['status']}")

        # Test updating to same status (should return False)
        print("\nUpdating to same status (should return False)...")
        result = update_configuration_status(config_id, ConfigurationStatus.VALID, schema=TEST_SCHEMA)
        assert result == False, "Update to same status should return False"
        print("✓ Correctly returned False for same status")

        # Test updating to ERROR status
        print("\nUpdating status to ERROR...")
        result = update_configuration_status(config_id, ConfigurationStatus.ERROR, schema=TEST_SCHEMA)
        assert result == True
        config = read_configuration(config_id, schema=TEST_SCHEMA)
        assert config['status'] == ConfigurationStatus.ERROR
        print(f"✓ Status updated to: {config['status']}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_configuration_file_success():
    """Test 3: Load valid configuration file"""
    print("\n" + "="*80)
    print("TEST 3: Load Valid Configuration File")
    print("="*80)

    try:
        # Setup: Create cycle
        print("Setting up test data...")
        cycle_id = create_test_cycle('test-3')
        print(f"  Created test cycle ID: {cycle_id}")

        # Verify test Excel file exists
        test_file = Path(TEST_EXCEL_PATH)
        if not test_file.exists():
            print(f"✗ Test Excel file not found: {TEST_EXCEL_PATH}")
            return False
        print(f"  Test file found: {test_file.name}")

        # Test loading configuration
        print(f"\nLoading configuration from Excel file...")
        print(f"  File: {TEST_EXCEL_PATH}")
        print(f"  Cycle ID: {cycle_id}")
        print(f"  Register: True")

        config_id = load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=TEST_EXCEL_PATH,
            register=True,
            schema=TEST_SCHEMA
        )

        print(f"✓ Configuration loaded successfully")
        print(f"  Configuration ID: {config_id}")

        # Verify the configuration was created
        config = read_configuration(config_id, schema=TEST_SCHEMA)
        print(f"\n✓ Configuration details:")
        print(f"  ID: {config['id']}")
        print(f"  Cycle ID: {config['cycle_id']}")
        print(f"  Status: {config['status']}")
        print(f"  File: {config['configuration_file_name']}")

        # Verify configuration data
        config_data = config['configuration_data']
        print(f"\n✓ Configuration data tabs: {list(config_data.keys())}")

        # Check TAB-A
        assert 'TAB-A' in config_data
        tab_a_data = config_data['TAB-A']
        print(f"  TAB-A: {len(tab_a_data)} rows")
        assert len(tab_a_data) == 3
        assert all(key in tab_a_data[0] for key in ['A-1', 'A-2', 'A-3'])

        # Check TAB-B
        assert 'TAB-B' in config_data
        tab_b_data = config_data['TAB-B']
        print(f"  TAB-B: {len(tab_b_data)} rows")
        assert len(tab_b_data) == 3
        assert all(key in tab_b_data[0] for key in ['B-1', 'B-2'])

        # Check validation status
        assert '_validation' in config_data
        validation = config_data['_validation']
        print(f"\n✓ Validation status:")
        for tab, status_info in validation.items():
            print(f"  {tab}: {status_info['status']}")
            assert status_info['status'] == 'SUCCESS'

        # Verify final status is VALID
        assert config['status'] == ConfigurationStatus.VALID
        print(f"\n✓ Configuration status is VALID")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_configuration_file_validation_errors():
    """Test 4: Load configuration with validation errors"""
    print("\n" + "="*80)
    print("TEST 4: Load Configuration with Validation Errors")
    print("="*80)

    try:
        # Setup: Create cycle and a bad Excel file
        print("Setting up test data...")
        cycle_id = create_test_cycle('test-4')

        # Create a test Excel file missing TAB-B
        import pandas as pd
        bad_file_path = Path(__file__).parent / 'test_config_bad.xlsx'

        with pd.ExcelWriter(bad_file_path, engine='openpyxl') as writer:
            df = pd.DataFrame({'A-1': ['val'], 'A-2': ['data']})  # Missing A-3
            df.to_excel(writer, sheet_name='TAB-A', index=False)

        print(f"  Created bad test file: {bad_file_path.name}")

        # Test loading - should fail
        print("\nAttempting to load configuration with missing columns...")
        try:
            config_id = load_configuration_file(
                cycle_id=cycle_id,
                excel_config_path=str(bad_file_path),
                register=True,
                schema=TEST_SCHEMA
            )
            print(f"✗ Expected error but got success: {config_id}")
            bad_file_path.unlink()  # Clean up
            return False

        except ConfigurationError as e:
            print(f"✓ Correctly caught validation error: {e}")
            bad_file_path.unlink()  # Clean up
            return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_configuration_active_cycle_check():
    """Test 5: Verify active cycle validation"""
    print("\n" + "="*80)
    print("TEST 5: Active Cycle Validation")
    print("="*80)

    try:
        # Setup: Create two cycles - only one active
        print("Setting up test data...")
        active_cycle_id = create_test_cycle('test-5')
        print(f"  Created active cycle ID: {active_cycle_id}")

        # Create another cycle (archived)
        archived_cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('archived_cycle', 'ARCHIVED'),
            schema=TEST_SCHEMA
        )
        print(f"  Created archived cycle ID: {archived_cycle_id}")

        # Test 1: Loading config for active cycle - should succeed
        print("\nTest 5a: Loading config for active cycle...")
        try:
            config_id = load_configuration_file(
                cycle_id=active_cycle_id,
                excel_config_path=TEST_EXCEL_PATH,
                register=True,
                schema=TEST_SCHEMA
            )
            print(f"✓ Successfully loaded config for active cycle: {config_id}")
        except ConfigurationError as e:
            print(f"✗ Unexpected error: {e}")
            return False

        # Test 2: Loading config for archived cycle - should fail
        print("\nTest 5b: Loading config for archived cycle (should fail)...")
        try:
            config_id = load_configuration_file(
                cycle_id=archived_cycle_id,
                excel_config_path=TEST_EXCEL_PATH,
                register=True,
                schema=TEST_SCHEMA
            )
            print(f"✗ Expected error but got success: {config_id}")
            return False
        except ConfigurationError as e:
            print(f"✓ Correctly rejected archived cycle: {e}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_configuration_duplicate_active():
    """Test 6: Prevent multiple ACTIVE configurations for same cycle"""
    print("\n" + "="*80)
    print("TEST 6: Prevent Duplicate ACTIVE Configurations")
    print("="*80)

    try:
        # Setup: Create cycle
        print("Setting up test data...")
        cycle_id = create_test_cycle('test-6')

        # Load first configuration
        print("\nLoading first configuration...")
        config_id_1 = load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=TEST_EXCEL_PATH,
            register=True,
            schema=TEST_SCHEMA
        )
        print(f"✓ First configuration loaded: {config_id_1}")

        # Update to ACTIVE status
        print("\nUpdating first configuration to ACTIVE...")
        update_configuration_status(config_id_1, ConfigurationStatus.ACTIVE, schema=TEST_SCHEMA)
        config = read_configuration(config_id_1, schema=TEST_SCHEMA)
        print(f"✓ Status is now: {config['status']}")

        # Try to load second configuration - should fail
        print("\nAttempting to load second configuration (should fail)...")
        try:
            config_id_2 = load_configuration_file(
                cycle_id=cycle_id,
                excel_config_path=TEST_EXCEL_PATH,
                register=True,
                schema=TEST_SCHEMA
            )
            print(f"✗ Expected error but got success: {config_id_2}")
            return False
        except ConfigurationError as e:
            print(f"✓ Correctly prevented duplicate ACTIVE config: {e}")
            return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_default():
    """Test 7: ConfigurationTransformer - default type"""
    print("\n" + "="*80)
    print("TEST 7: ConfigurationTransformer - Default Type")
    print("="*80)

    try:
        config = {
            'param1': 'value1',
            'param2': 100,
            'nested': {'key': 'value'}
        }

        print("Testing default transformer...")
        result = ConfigurationTransformer.get_job_configurations('default', config)

        print(f"✓ Transformation successful")
        print(f"  Input config: {config}")
        print(f"  Output jobs: {len(result)}")
        print(f"  Job config: {result[0]}")

        # Verify results
        assert len(result) == 1, "Should return single job config"
        assert result[0] == config, "Should copy config as-is"
        assert result[0] is not config, "Should be a copy, not the same object"

        print("✓ All assertions passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_passthrough():
    """Test 8: ConfigurationTransformer - passthrough type"""
    print("\n" + "="*80)
    print("TEST 8: ConfigurationTransformer - Passthrough Type")
    print("="*80)

    try:
        config = {'data': 'test', 'count': 5}

        print("Testing passthrough transformer...")
        result = ConfigurationTransformer.get_job_configurations('passthrough', config)

        print(f"✓ Transformation successful")
        print(f"  Input config: {config}")
        print(f"  Output jobs: {len(result)}")

        # Verify results
        assert len(result) == 1, "Should return single job config"
        assert result[0] == config, "Should return same config"
        assert result[0] is config, "Should be the same object (not a copy)"

        print("✓ All assertions passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_multi_job():
    """Test 9: ConfigurationTransformer - multi_job type"""
    print("\n" + "="*80)
    print("TEST 9: ConfigurationTransformer - Multi Job Type")
    print("="*80)

    try:
        # Test with jobs list
        config_with_jobs = {
            'batch_type': 'test_batch',
            'jobs': [
                {'job_id': 1, 'param': 'A'},
                {'job_id': 2, 'param': 'B'},
                {'job_id': 3, 'param': 'C'}
            ]
        }

        print("Testing multi_job transformer with jobs list...")
        result = ConfigurationTransformer.get_job_configurations('multi_job', config_with_jobs)

        print(f"✓ Transformation successful")
        print(f"  Input config with {len(config_with_jobs['jobs'])} jobs")
        print(f"  Output jobs: {len(result)}")

        assert len(result) == 3, "Should return 3 job configs"
        assert result[0] == {'job_id': 1, 'param': 'A'}
        assert result[1] == {'job_id': 2, 'param': 'B'}
        assert result[2] == {'job_id': 3, 'param': 'C'}

        print("✓ Jobs list test passed")

        # Test without jobs list (fallback)
        config_no_jobs = {'single_job': 'data'}

        print("\nTesting multi_job transformer without jobs list (fallback)...")
        result = ConfigurationTransformer.get_job_configurations('multi_job', config_no_jobs)

        print(f"  Output jobs: {len(result)}")

        assert len(result) == 1, "Should return single job config"
        assert result[0] == config_no_jobs

        print("✓ Fallback test passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_unknown_type():
    """Test 10: ConfigurationTransformer - unknown type error"""
    print("\n" + "="*80)
    print("TEST 10: ConfigurationTransformer - Unknown Type Error")
    print("="*80)

    try:
        config = {'data': 'test'}

        print("Testing transformer with unknown type...")
        try:
            result = ConfigurationTransformer.get_job_configurations('nonexistent_type', config)
            print(f"✗ Expected error but got success: {result}")
            return False
        except ConfigurationError as e:
            print(f"✓ Correctly caught error: {e}")
            assert 'nonexistent_type' in str(e)
            assert 'Available types' in str(e)

        print("✓ Error handling test passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_list_types():
    """Test 11: ConfigurationTransformer - list registered types"""
    print("\n" + "="*80)
    print("TEST 11: ConfigurationTransformer - List Registered Types")
    print("="*80)

    try:
        print("Getting list of registered transformer types...")
        types = ConfigurationTransformer.list_types()

        print(f"✓ Found {len(types)} registered types:")
        for t in types:
            print(f"  - {t}")

        # Verify expected types are registered
        assert 'default' in types, "Should have 'default' type"
        assert 'passthrough' in types, "Should have 'passthrough' type"
        assert 'multi_job' in types, "Should have 'multi_job' type"

        print("✓ All expected types are registered")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_transformer_custom_registration():
    """Test 12: ConfigurationTransformer - custom registration"""
    print("\n" + "="*80)
    print("TEST 12: ConfigurationTransformer - Custom Registration")
    print("="*80)

    try:
        # Register a custom transformer for testing
        @ConfigurationTransformer.register('test_custom')
        def transform_custom(config):
            """Custom transformer that doubles values"""
            return [
                {'value': config.get('value', 0) * 2},
                {'value': config.get('value', 0) * 3}
            ]

        print("Registered custom transformer 'test_custom'")

        # Test the custom transformer
        config = {'value': 10}
        result = ConfigurationTransformer.get_job_configurations('test_custom', config)

        print(f"✓ Custom transformation successful")
        print(f"  Input: {config}")
        print(f"  Output: {result}")

        assert len(result) == 2, "Should return 2 job configs"
        assert result[0] == {'value': 20}, "First job should have doubled value"
        assert result[1] == {'value': 30}, "Second job should have tripled value"

        print("✓ Custom transformer works correctly")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests(preserve=False):
    """Run all tests and report results

    Args:
        preserve: If True, keep schema after tests for debugging
    """
    print("\n" + "="*80)
    print("CONFIGURATION MANAGEMENT TEST SUITE")
    print("="*80)

    # Test database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("Database connection failed. Please check your configuration.")
        return
    print("✓ Database connection successful")

    # Cleanup any preserved schema from last run
    cleanup_test_schema()

    # Setup test schema
    if not setup_test_schema():
        print("\nFailed to setup test schema. Aborting tests.")
        return

    # Run tests
    tests = [
        ("Read Configuration", test_read_configuration),
        ("Update Configuration Status", test_update_configuration_status),
        ("Load Valid Configuration File", test_load_configuration_file_success),
        ("Validation Errors", test_load_configuration_file_validation_errors),
        ("Active Cycle Validation", test_load_configuration_active_cycle_check),
        ("Prevent Duplicate ACTIVE", test_load_configuration_duplicate_active),
        ("Transformer - Default Type", test_configuration_transformer_default),
        ("Transformer - Passthrough Type", test_configuration_transformer_passthrough),
        ("Transformer - Multi Job Type", test_configuration_transformer_multi_job),
        ("Transformer - Unknown Type Error", test_configuration_transformer_unknown_type),
        ("Transformer - List Types", test_configuration_transformer_list_types),
        ("Transformer - Custom Registration", test_configuration_transformer_custom_registration),
    ]


    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n{test_name} crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))


    df = execute_query(
        "SELECT * FROM irp_configuration",
        schema=TEST_SCHEMA
    )
    print(df)



    # Clean up test schema (unless preserve flag set)
    if not preserve:
        cleanup_test_schema()
    else:
        print(f"\nSchema '{TEST_SCHEMA}' preserved for debugging")


    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\nAll tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run database tests')
    parser.add_argument('--preserve', action='store_true',
                       help='Preserve test schema after tests for debugging')
    args = parser.parse_args()

    run_all_tests(preserve=args.preserve)
