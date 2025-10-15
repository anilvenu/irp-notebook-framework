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
        INSERT INTO irp_cycle (cycle_name, status, created_by)
        VALUES (%s, %s, %s)
    """

    params_list = [
        ('test_cycle_1', 'ACTIVE', 'test_user'),
        ('test_cycle_2', 'ACTIVE', 'test_user'),
        ('test_cycle_3', 'ARCHIVED', 'test_user'),
        ('test_cycle_4', 'ACTIVE', 'admin_user'),
        ('test_cycle_5', 'ACTIVE', 'admin_user'),
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
        print(df[['id', 'cycle_name', 'status', 'created_by']].to_string(index=False))

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_jsonb_bulk_insert():
    """Test 2: Bulk insert with JSONB fields"""
    print("\n" + "="*80)
    print("TEST 2: Bulk Insert with JSONB Fields (Cycles with Metadata)")
    print("="*80)

    query = """
        INSERT INTO irp_cycle (cycle_name, status, created_by, metadata)
        VALUES (%s, %s, %s, %s)
    """

    params_list = [
        ('json_cycle_1', 'ACTIVE', 'test_user', {
            'environment': 'production',
            'priority': 'high',
            'tags': ['urgent', 'client-facing']
        }),
        ('json_cycle_2', 'ACTIVE', 'test_user', {
            'environment': 'staging',
            'priority': 'medium',
            'settings': {'auto_archive': True}
        }),
        ('json_cycle_3', 'ACTIVE', 'admin_user', {
            'environment': 'development',
            'priority': 'low',
            'notes': 'Test cycle for development'
        }),
    ]

    try:
        print(f"Inserting {len(params_list)} cycles with JSONB metadata...")
        start_time = time.time()
        ids = bulk_insert(query, params_list, jsonb_columns=[3], schema=TEST_SCHEMA)
        elapsed = time.time() - start_time

        print(f"✓ Successfully inserted {len(ids)} records with JSONB")
        print(f"  Returned IDs: {ids}")
        print(f"  Time elapsed: {elapsed:.4f} seconds")

        # Verify inserts and JSONB content
        df = execute_query(
            "SELECT * FROM irp_cycle WHERE cycle_name LIKE 'json_cycle_%' ORDER BY id",
            schema=TEST_SCHEMA
        )
        print(f"✓ Verified {len(df)} records in database")
        print("\nInserted records with JSONB metadata:")
        for _, row in df.iterrows():
            print(f"  ID {row['id']}: {row['cycle_name']}")
            print(f"    Metadata: {row['metadata']}")

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_complex_jsonb_inserts():
    """Test 3: Bulk insert with complex JSONB across different tables"""
    print("\n" + "="*80)
    print("TEST 3: Complex JSONB Inserts (Step Runs with Output Data)")
    print("="*80)

    try:
        # Setup: Create cycle, stage, and step
        print("Setting up test hierarchy...")
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
            ('hierarchy_cycle', 'ACTIVE', 'test_user'),
            schema=TEST_SCHEMA
        )
        print(f"  Created cycle ID: {cycle_id}")

        stage_id = execute_insert(
            "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
            (cycle_id, 1, 'test_stage'),
            schema=TEST_SCHEMA
        )
        print(f"  Created stage ID: {stage_id}")

        step_id = execute_insert(
            "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
            (stage_id, 1, 'test_step'),
            schema=TEST_SCHEMA
        )
        print(f"  Created step ID: {step_id}")

        # Now bulk insert step runs with complex output_data
        query = """
            INSERT INTO irp_step_run (step_id, run_number, status, started_by, output_data)
            VALUES (%s, %s, %s, %s, %s)
        """

        params_list = [
            (step_id, 1, 'COMPLETED', 'test_user', {
                'records_processed': 1000,
                'execution_time': 45.2,
                'results': {
                    'success': 950,
                    'failed': 50,
                    'warnings': ['Duplicate key in row 123']
                }
            }),
            (step_id, 2, 'COMPLETED', 'test_user', {
                'records_processed': 2000,
                'execution_time': 89.5,
                'results': {
                    'success': 1980,
                    'failed': 20,
                    'warnings': []
                }
            }),
            (step_id, 3, 'FAILED', 'test_user', {
                'records_processed': 500,
                'execution_time': 12.3,
                'results': {
                    'success': 0,
                    'failed': 500,
                    'error': 'Database connection lost'
                }
            }),
        ]

        print(f"\nInserting {len(params_list)} step runs with complex JSONB output_data...")
        start_time = time.time()
        ids = bulk_insert(query, params_list, jsonb_columns=[4], schema=TEST_SCHEMA)
        elapsed = time.time() - start_time

        print(f"✓ Successfully inserted {len(ids)} step runs")
        print(f"  Returned IDs: {ids}")
        print(f"  Time elapsed: {elapsed:.4f} seconds")

        # Verify inserts
        df = execute_query(
            "SELECT * FROM irp_step_run WHERE step_id = %s ORDER BY run_number",
            (step_id,),
            schema=TEST_SCHEMA
        )
        print(f"✓ Verified {len(df)} step runs in database")
        print("\nStep runs with output data:")
        for _, row in df.iterrows():
            print(f"  Run {row['run_number']} ({row['status']}):")
            print(f"    Output Data: {row['output_data']}")

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_error_handling():
    """Test 4: Error handling and transaction rollback"""
    print("\n" + "="*80)
    print("TEST 4: Error Handling and Transaction Rollback")
    print("="*80)

    query = """
        INSERT INTO irp_cycle (cycle_name, status, created_by)
        VALUES (%s, %s, %s)
    """

    # Create a duplicate entry scenario
    params_list = [
        ('rollback_1', 'ACTIVE', 'test_user'),
        ('rollback_2', 'ACTIVE', 'test_user'),
        ('rollback_1', 'ACTIVE', 'test_user'),  # Duplicate - should fail
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


def test_performance_comparison():
    """Test 5: Performance comparison - bulk vs individual inserts"""
    print("\n" + "="*80)
    print("TEST 5: Performance Comparison - Bulk vs Individual Inserts")
    print("="*80)

    num_records = 50

    query = """
        INSERT INTO irp_cycle (cycle_name, status, created_by, metadata)
        VALUES (%s, %s, %s, %s)
    """

    # Prepare test data
    bulk_params = [
        (f'perf_bulk_{i}', 'ACTIVE', 'test_user', {'iteration': i, 'batch': 'bulk'})
        for i in range(num_records)
    ]

    individual_params = [
        (f'perf_individual_{i}', 'ACTIVE', 'test_user', {'iteration': i, 'batch': 'individual'})
        for i in range(num_records)
    ]

    try:
        # Test bulk insert
        print(f"\nBulk insert of {num_records} records...")
        start_time = time.time()
        bulk_ids = bulk_insert(query, bulk_params, jsonb_columns=[3], schema=TEST_SCHEMA)
        bulk_time = time.time() - start_time
        print(f"  ✓ Bulk insert completed in {bulk_time:.4f} seconds")
        print(f"    Average: {bulk_time/num_records*1000:.2f} ms per record")

        # Test individual inserts
        print(f"\nIndividual insert of {num_records} records...")
        import json
        start_time = time.time()
        individual_ids = []
        for params in individual_params:
            # Convert dict to JSON for individual insert
            params_converted = list(params)
            params_converted[3] = json.dumps(params_converted[3])
            id = execute_insert(query, tuple(params_converted), schema=TEST_SCHEMA)
            individual_ids.append(id)
        individual_time = time.time() - start_time
        print(f"  ✓ Individual inserts completed in {individual_time:.4f} seconds")
        print(f"    Average: {individual_time/num_records*1000:.2f} ms per record")

        # Compare
        print(f"\n{'='*60}")
        print(f"PERFORMANCE COMPARISON")
        print(f"{'='*60}")
        print(f"Bulk insert:       {bulk_time:.4f} seconds ({bulk_time/num_records*1000:.2f} ms/record)")
        print(f"Individual insert: {individual_time:.4f} seconds ({individual_time/num_records*1000:.2f} ms/record)")
        speedup = individual_time / bulk_time
        print(f"Speedup:           {speedup:.2f}x faster")
        print(f"Time saved:        {individual_time - bulk_time:.4f} seconds ({(1-1/speedup)*100:.1f}%)")

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_batch_configuration_jsonb():
    """Test 6: Bulk insert into irp_configuration table with JSONB"""
    print("\n" + "="*80)
    print("TEST 6: Bulk Insert Configurations with JSONB config_data")
    print("="*80)

    try:
        # Setup: Create cycle, stage, step, and batch
        print("Setting up test hierarchy...")
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
            ('config_cycle', 'ACTIVE', 'test_user'),
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

        batch_id = execute_insert(
            "INSERT INTO irp_batch (step_id, batch_name, status) VALUES (%s, %s, %s)",
            (step_id, 'config_batch', 'PENDING'),
            schema=TEST_SCHEMA
        )
        print(f"  Created batch ID: {batch_id}")

        # Bulk insert configurations
        query = """
            INSERT INTO irp_configuration (batch_id, config_name, config_data, skip)
            VALUES (%s, %s, %s, %s)
        """

        params_list = [
            (batch_id, 'config_portfolio_A', {
                'portfolio': 'Portfolio_A',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'high', 'threshold': 0.95}
            }, False),
            (batch_id, 'config_portfolio_B', {
                'portfolio': 'Portfolio_B',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'medium', 'threshold': 0.85}
            }, False),
            (batch_id, 'config_portfolio_C', {
                'portfolio': 'Portfolio_C',
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'parameters': {'risk_level': 'low', 'threshold': 0.75}
            }, True),  # This one is skipped
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
            "SELECT * FROM irp_configuration WHERE batch_id = %s ORDER BY id",
            (batch_id,),
            schema=TEST_SCHEMA
        )
        print(f"✓ Verified {len(df)} configurations in database")
        print("\nInserted configurations:")
        for _, row in df.iterrows():
            skip_status = "SKIPPED" if row['skip'] else "ACTIVE"
            print(f"  ID {row['id']}: {row['config_name']} ({skip_status})")
            print(f"    Config Data: {row['config_data']}")

        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests and report results"""
    print("\n" + "="*80)
    print("BULK INSERT TEST SUITE")
    print("="*80)

    # Test database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("✗ Database connection failed. Please check your configuration.")
        return
    print("✓ Database connection successful")

    # Setup test schema
    if not setup_test_schema():
        print("\n✗ Failed to setup test schema. Aborting tests.")
        return

    # Run tests
    tests = [
        ("Basic Bulk Insert", test_basic_bulk_insert),
        ("JSONB Bulk Insert", test_jsonb_bulk_insert),
        ("Complex JSONB Inserts", test_complex_jsonb_inserts),
        ("Error Handling", test_error_handling),
        ("Performance Comparison", test_performance_comparison),
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
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    run_all_tests()
