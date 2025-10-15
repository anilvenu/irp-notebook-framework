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
    ConfigurationError
)
from helpers.constants import ConfigurationStatus

TEST_SCHEMA = 'test'
TEST_EXCEL_PATH = str(Path(__file__).parent / 'test_configuration.xlsx')

TEST_SCHEMA = 'test'


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
        "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
        (cycle_name, 'ACTIVE', 'test_user'),
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
            "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
            ('archived_cycle', 'ARCHIVED', 'test_user'),
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


def run_all_tests():
    """Run all tests and report results"""
    print("\n" + "="*80)
    print("CONFIGURATION MANAGEMENT TEST SUITE")
    print("="*80)

    # Test database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("Database connection failed. Please check your configuration.")
        return
    print("✓ Database connection successful")

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



    # Clean up test schema
    cleanup_test_schema()

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
    run_all_tests()
