"""
Test suite for database operations

This test file demonstrates and validates database functionality including:
- bulk_insert with JSONB support
- Error handling and transaction rollback
- Performance comparisons

All tests run in the 'test' schema to avoid affecting production data.

Run this test:
    python workspace/tests/test_database.py
"""

import argparse
from datetime import datetime
import sys
import time
from pathlib import Path

# Add workspace to path - go up two levels from tests/ to project root, then into workspace
workspace_path = Path(__file__).parent.parent
sys.path.insert(0, str(workspace_path))

# Import database functions
from helpers.database import (
    bulk_insert,
    execute_query,
    execute_command,
    execute_insert,
    test_connection,
    init_database,
    DatabaseError
)


TEST_SCHEMA = Path(__file__).stem


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
            print("✗ Failed to initialize test schema")
            return False

        print(f"✓ Test schema '{TEST_SCHEMA}' initialized successfully")
        return True

    except Exception as e:
        print(f"✗ Setup failed: {e}")
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


def test_basic_bulk_insert():
    """Test 1: Basic bulk insert without JSONB fields"""
    print("\n" + "="*80)
    print("TEST 1: Basic Bulk Insert (Multiple Cycles)")
    print("="*80)

    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    params_list = [
        ('test_cycle_1', 'ACTIVE'),
        ('test_cycle_2', 'ACTIVE'),
        ('test_cycle_3', 'ARCHIVED'),
        ('test_cycle_4', 'ACTIVE'),
        ('test_cycle_5', 'ACTIVE'),
    ]

    try:
        print(f"Inserting {len(params_list)} cycles...")
        start_time = time.time()
        ids = bulk_insert(query, params_list, schema=TEST_SCHEMA)
        elapsed = time.time() - start_time

        print(f"✓ Successfully inserted {len(ids)} records")
        print(f"  Returned IDs: {ids}")
        print(f"  Time elapsed: {elapsed:.4f} seconds")

        # Verify inserts
        df = execute_query(
            "SELECT * FROM irp_cycle WHERE cycle_name LIKE 'test_cycle_%' ORDER BY id",
            schema=TEST_SCHEMA
        )
        print(f"✓ Verified {len(df)} records in database")
        print("\nInserted records:")
        print(df[['id', 'cycle_name', 'status']].to_string(index=False))

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_error_handling():
    """Test 2: Error handling and transaction rollback"""
    print("\n" + "="*80)
    print("TEST 2: Error Handling and Transaction Rollback")
    print("="*80)

    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    # Create a duplicate entry scenario
    params_list = [
        ('rollback_1', 'ACTIVE'),
        ('rollback_2', 'ACTIVE'),
        ('rollback_1', 'ACTIVE'),  # Duplicate - should fail
    ]

    try:
        print("Testing rollback with duplicate key violation...")
        print(f"Attempting to insert {len(params_list)} records (with 1 duplicate)...")

        # Count before insert
        before_count = execute_query(
            "SELECT COUNT(*) as count FROM irp_cycle WHERE cycle_name LIKE 'rollback_%'",
            schema=TEST_SCHEMA
        ).iloc[0]['count']
        print(f"  Records before insert: {before_count}")

        try:
            ids = bulk_insert(query, params_list, schema=TEST_SCHEMA)
            print(f"✗ Expected failure but got success: {ids}")
            return False
        except DatabaseError as e:
            print(f"✓ Correctly caught error: {e}")

            # Verify rollback - no records should be inserted
            after_count = execute_query(
                "SELECT COUNT(*) as count FROM irp_cycle WHERE cycle_name LIKE 'rollback_%'",
                schema=TEST_SCHEMA
            ).iloc[0]['count']
            print(f"  Records after failed insert: {after_count}")

            if after_count == before_count:
                print("✓ Transaction correctly rolled back - no partial inserts")
                return True
            else:
                print(f"✗ Transaction not rolled back - {after_count - before_count} records inserted")
                return False

    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_batch_configuration_jsonb():
    """Test 3: Bulk insert into irp_configuration table with JSONB"""
    print("\n" + "="*80)
    print("TEST 3: Bulk Insert Configurations with JSONB config_data")
    print("="*80)

    try:
        # Setup: Create cycle, stage, step, and batch
        print("Setting up test hierarchy...")
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('config_cycle', 'ACTIVE'),
            schema=TEST_SCHEMA
        )

        stage_id = execute_insert(
            "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
            (cycle_id, 1, 'config_stage'),
            schema=TEST_SCHEMA
        )

        step_id = execute_insert(
            "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
            (stage_id, 1, 'config_step'),
            schema=TEST_SCHEMA
        )

        # Bulk insert configurations
        query = """
            INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, file_last_updated_ts)
            VALUES (%s, %s, %s, %s)
        """

        params_list = [
            (cycle_id, 'file-A.xlsx', {
                'portfolio': 'Portfolio_A',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'high', 'threshold': 0.95}
            },  datetime.now()),
            (cycle_id, 'file-B.xlsx', {
                'portfolio': 'Portfolio_B',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'medium', 'threshold': 0.85}
            }, datetime.now()),
            (cycle_id, 'file-C.xlsx', {
                'portfolio': 'Portfolio_C',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'low', 'threshold': 0.75}
            }, datetime.now()),  
        ]

        print(f"\nInserting {len(params_list)} configurations with JSONB config_data...")
        start_time = time.time()
        ids = bulk_insert(query, params_list, jsonb_columns=[2], schema=TEST_SCHEMA)
        elapsed = time.time() - start_time

        print(f"✓ Successfully inserted {len(ids)} configurations")
        print(f"  Returned IDs: {ids}")
        print(f"  Time elapsed: {elapsed:.4f} seconds")

        # Verify inserts
        df = execute_query(
            "SELECT * FROM irp_configuration WHERE cycle_id = %s ORDER BY id",
            (cycle_id,),
            schema=TEST_SCHEMA
        )
        print(f"✓ Verified {len(df)} configurations in database")
        print("\nInserted configurations:")
        for _, row in df.iterrows():
            print(f"  ID {row['id']}: {row['configuration_file_name']}")
            print(f"    Config Data: {row['configuration_data']}")

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
    print("BULK INSERT TEST SUITE")
    print("="*80)

    # Test database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("✗ Database connection failed. Please check your configuration.")
        return
    print("✓ Database connection successful")

    # Cleanup any preserved schema from last run
    cleanup_test_schema()

    # Setup test schema
    if not setup_test_schema():
        print("\n✗ Failed to setup test schema. Aborting tests.")
        return

    # Run tests
    tests = [
        ("Basic Bulk Insert", test_basic_bulk_insert),
        ("Error Handling", test_error_handling),
        ("Configuration JSONB", test_batch_configuration_jsonb),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ {test_name} crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

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
