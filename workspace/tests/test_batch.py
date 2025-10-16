"""
Test suite for batch management operations

This test file validates batch functionality including:
- Batch CRUD operations
- Batch creation with transformer integration
- Batch submission and job orchestration
- Batch reconciliation with various job states
- Error handling and validation

All tests run in the 'test' schema to avoid affecting production data.

Run this test:
    python workspace/tests/test_batch.py
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
from helpers.batch import (
    read_batch,
    update_batch_status,
    create_batch,
    submit_batch,
    get_batch_jobs,
    get_batch_job_configurations,
    recon_batch,
    BatchError
)
from helpers.job import create_job, update_job_status
from helpers.constants import BatchStatus, JobStatus, ConfigurationStatus

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

        print(f"✓ Test schema '{TEST_SCHEMA}' initialized successfully")
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


def create_test_hierarchy(cycle_name='test_cycle'):
    """Helper to create cycle, stage, step, and configuration"""
    import json

    # Create cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
        (cycle_name, 'ACTIVE', 'test_user'),
        schema=TEST_SCHEMA
    )

    # Create stage
    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'test_stage'),
        schema=TEST_SCHEMA
    )

    # Create step
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'test_step'),
        schema=TEST_SCHEMA
    )

    # Create configuration
    config_data = {
        'param1': 'value1',
        'param2': 100,
        'nested': {'key': 'value'}
    }

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=TEST_SCHEMA
    )

    return cycle_id, stage_id, step_id, config_id


# ============================================================================
# TESTS
# ============================================================================

def test_read_batch():
    """Test 1: Read batch by ID"""
    print("\n" + "="*80)
    print("TEST 1: Read Batch")
    print("="*80)

    try:
        # Setup: Create hierarchy and batch
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_read_batch')

        # Create batch manually
        batch_id = execute_insert(
            "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
            (step_id, config_id, 'default', BatchStatus.INITIATED),
            schema=TEST_SCHEMA
        )
        print(f"  Created batch ID: {batch_id}")

        # Test reading batch
        print("\nReading batch...")
        result = read_batch(batch_id, schema=TEST_SCHEMA)

        print(f"✓ Successfully read batch")
        print(f"  Batch ID: {result['id']}")
        print(f"  Step ID: {result['step_id']}")
        print(f"  Configuration ID: {result['configuration_id']}")
        print(f"  Batch Type: {result['batch_type']}")
        print(f"  Status: {result['status']}")

        # Verify data
        assert result['id'] == batch_id
        assert result['step_id'] == step_id
        assert result['configuration_id'] == config_id
        assert result['batch_type'] == 'default'
        assert result['status'] == BatchStatus.INITIATED

        print("✓ All assertions passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_update_batch_status():
    """Test 2: Update batch status"""
    print("\n" + "="*80)
    print("TEST 2: Update Batch Status")
    print("="*80)

    try:
        # Setup
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_update_status')

        batch_id = execute_insert(
            "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
            (step_id, config_id, 'default', BatchStatus.INITIATED),
            schema=TEST_SCHEMA
        )

        # Test updating status
        print("\nUpdating status from INITIATED to ACTIVE...")
        result = update_batch_status(batch_id, BatchStatus.ACTIVE, schema=TEST_SCHEMA)
        assert result == True, "Update should return True"
        print("✓ Status updated successfully")

        # Verify the update
        batch = read_batch(batch_id, schema=TEST_SCHEMA)
        assert batch['status'] == BatchStatus.ACTIVE
        print(f"✓ Verified status is now: {batch['status']}")

        # Test updating to same status (should return False)
        print("\nUpdating to same status (should return False)...")
        result = update_batch_status(batch_id, BatchStatus.ACTIVE, schema=TEST_SCHEMA)
        assert result == False, "Update to same status should return False"
        print("✓ Correctly returned False for same status")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_create_batch_default():
    """Test 3: Create batch with default transformer"""
    print("\n" + "="*80)
    print("TEST 3: Create Batch with Default Transformer")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_create_batch')

        # Create batch
        print(f"\nCreating batch with batch_type='default'...")
        batch_id = create_batch(
            batch_type='default',
            configuration_id=config_id,
            step_id=step_id,
            schema=TEST_SCHEMA
        )
        print(f"✓ Batch created successfully: {batch_id}")

        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        
        print(f"✓ Batch submiited successfully: {batch_id}")

        # Verify batch
        batch = read_batch(batch_id, schema=TEST_SCHEMA)
        print(f"\n✓ Batch details:")
        print(f"  ID: {batch['id']}")
        print(f"  Type: {batch['batch_type']}")
        print(f"  Status: {batch['status']}")

        # Verify jobs were created
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        print(f"\n✓ Jobs created: {len(jobs)}")
        assert len(jobs) == 1, "Default transformer should create 1 job"

        # Verify job configurations
        job_configs = get_batch_job_configurations(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Job configurations: {len(job_configs)}")
        assert len(job_configs) == 1

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_create_batch_multi_job():
    """Test 4: Create batch with multi_job transformer"""
    print("\n" + "="*80)
    print("TEST 4: Create Batch with Multi-Job Transformer")
    print("="*80)

    try:
        print("Setting up test data...")
        # Create hierarchy with multi-job config
        import json
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
            ('test_multi_job', 'ACTIVE', 'test_user'),
            schema=TEST_SCHEMA
        )
        stage_id = execute_insert(
            "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
            (cycle_id, 1, 'test_stage'),
            schema=TEST_SCHEMA
        )
        step_id = execute_insert(
            "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
            (stage_id, 1, 'test_step'),
            schema=TEST_SCHEMA
        )

        config_data = {
            'batch_type': 'multi_test',
            'jobs': [
                {'job_id': 1, 'param': 'A'},
                {'job_id': 2, 'param': 'B'},
                {'job_id': 3, 'param': 'C'}
            ]
        }

        config_id = execute_insert(
            """INSERT INTO irp_configuration
               (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
               VALUES (%s, %s, %s, %s, %s)""",
            (cycle_id, '/test/multi_config.xlsx', json.dumps(config_data),
             ConfigurationStatus.VALID, datetime.now()),
            schema=TEST_SCHEMA
        )

        # Create batch
        print(f"\nCreating batch with batch_type='multi_job'...")
        batch_id = create_batch(
            batch_type='multi_job',
            configuration_id=config_id,
            step_id=step_id,
            schema=TEST_SCHEMA
        )
        print(f"✓ Batch created successfully: {batch_id}")

        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        
        print(f"✓ Batch submiited successfully: {batch_id}")

        # Verify jobs were created
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        print(f"\n✓ Jobs created: {len(jobs)}")
        assert len(jobs) == 3, "Multi-job transformer should create 3 jobs"

        # Verify all jobs are SUBMITTED (from submit_batch)
        for job in jobs:
            print(f"  Job {job['id']}: {job['status']}")
            assert job['status'] == JobStatus.SUBMITTED

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_get_batch_jobs_with_filters():
    """Test 5: Get batch jobs with filters"""
    print("\n" + "="*80)
    print("TEST 5: Get Batch Jobs with Filters")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_get_jobs')

        # Create batch
        batch_id = execute_insert(
            "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
            (step_id, config_id, 'default', BatchStatus.ACTIVE),
            schema=TEST_SCHEMA
        )

        # Create multiple jobs with different states
        from helpers import job as job_module

        job_config_data = {'param': 'value'}

        job1_id = job_module.create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=TEST_SCHEMA)
        job2_id = job_module.create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=TEST_SCHEMA)
        job3_id = job_module.create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=TEST_SCHEMA)

        # Update jobs to different states
        update_job_status(job1_id, JobStatus.SUBMITTED, schema=TEST_SCHEMA)
        update_job_status(job2_id, JobStatus.FINISHED, schema=TEST_SCHEMA)
        job_module.skip_job(job3_id, schema=TEST_SCHEMA)

        # Test: Get all jobs
        print("\nGetting all jobs...")
        all_jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Total jobs: {len(all_jobs)}")
        assert len(all_jobs) == 3

        # Test: Get non-skipped jobs
        print("\nGetting non-skipped jobs...")
        non_skipped = get_batch_jobs(batch_id, skipped=False, schema=TEST_SCHEMA)
        print(f"✓ Non-skipped jobs: {len(non_skipped)}")
        assert len(non_skipped) == 2

        # Test: Get skipped jobs
        print("\nGetting skipped jobs...")
        skipped = get_batch_jobs(batch_id, skipped=True, schema=TEST_SCHEMA)
        print(f"✓ Skipped jobs: {len(skipped)}")
        assert len(skipped) == 1

        # Test: Get jobs by status
        print("\nGetting COMPLETED jobs...")
        completed = get_batch_jobs(batch_id, status=JobStatus.FINISHED, schema=TEST_SCHEMA)
        print(f"✓ COMPLETED jobs: {len(completed)}")
        assert len(completed) == 1

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_recon_batch_all_completed():
    """Test 6: Recon batch with all jobs completed"""
    print("\n" + "="*80)
    print("TEST 6: Recon Batch - All Completed")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_recon_completed')

        # Create batch with jobs
        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        # Get jobs and mark them as FINISHED
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        for job in jobs:
            update_job_status(job['id'], JobStatus.FINISHED, schema=TEST_SCHEMA)

        # Recon batch
        print("\nReconciling batch...")
        result_status = recon_batch(batch_id, schema=TEST_SCHEMA)

        print(f"✓ Recon result: {result_status}")
        assert result_status == BatchStatus.COMPLETED, f"Expected COMPLETED, got {result_status}"

        # Verify batch status updated
        batch = read_batch(batch_id, schema=TEST_SCHEMA)
        assert batch['status'] == BatchStatus.COMPLETED

        print("✓ Batch status updated to COMPLETED")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_recon_batch_with_failures():
    """Test 7: Recon batch with failed jobs"""
    print("\n" + "="*80)
    print("TEST 7: Recon Batch - With Failures")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_recon_failed')

        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        # Get jobs and mark some as FAILED
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        update_job_status(jobs[0]['id'], JobStatus.FAILED, schema=TEST_SCHEMA)

        # Recon batch
        print("\nReconciling batch...")
        result_status = recon_batch(batch_id, schema=TEST_SCHEMA)

        print(f"✓ Recon result: {result_status}")
        assert result_status == BatchStatus.FAILED, f"Expected FAILED, got {result_status}"

        print("✓ Batch correctly marked as FAILED")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_recon_batch_all_cancelled():
    """Test 8: Recon batch with all jobs cancelled"""
    print("\n" + "="*80)
    print("TEST 8: Recon Batch - All Cancelled")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_recon_cancelled')

        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        # Get jobs and mark them as CANCELLED
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        for job in jobs:
            update_job_status(job['id'], JobStatus.CANCELLED, schema=TEST_SCHEMA)

        # Recon batch
        print("\nReconciling batch...")
        result_status = recon_batch(batch_id, schema=TEST_SCHEMA)

        print(f"✓ Recon result: {result_status}")
        assert result_status == BatchStatus.CANCELLED, f"Expected CANCELLED, got {result_status}"

        print("✓ Batch correctly marked as CANCELLED")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_batch_error_handling():
    """Test 9: Batch error handling"""
    print("\n" + "="*80)
    print("TEST 9: Batch Error Handling")
    print("="*80)

    try:
        # Test invalid batch_id
        print("\nTest 9a: Invalid batch_id...")
        try:
            read_batch(-1, schema=TEST_SCHEMA)
            print("✗ Should have raised BatchError")
            return False
        except BatchError as e:
            print(f"✓ Correctly raised BatchError: {e}")

        # Test batch not found
        print("\nTest 9b: Batch not found...")
        try:
            read_batch(999999, schema=TEST_SCHEMA)
            print("✗ Should have raised BatchError")
            return False
        except BatchError as e:
            print(f"✓ Correctly raised BatchError: {e}")

        # Test invalid status
        print("\nTest 9c: Invalid status...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_error')
        batch_id = execute_insert(
            "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
            (step_id, config_id, 'default', BatchStatus.INITIATED),
            schema=TEST_SCHEMA
        )
        try:
            update_batch_status(batch_id, 'INVALID_STATUS', schema=TEST_SCHEMA)
            print("✗ Should have raised BatchError")
            return False
        except BatchError as e:
            print(f"✓ Correctly raised BatchError: {e}")

        # Test unknown batch type
        print("\nTest 9d: Unknown batch type...")
        try:
            create_batch('nonexistent_type', config_id, step_id, schema=TEST_SCHEMA)
            print("✗ Should have raised BatchError")
            return False
        except BatchError as e:
            print(f"✓ Correctly raised BatchError: {e}")

        print("\n✓ All error handling tests passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests and report results"""
    print("\n" + "="*80)
    print("BATCH MANAGEMENT TEST SUITE")
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
        ("Read Batch", test_read_batch),
        ("Update Batch Status", test_update_batch_status),
        ("Create Batch - Default", test_create_batch_default),
        ("Create Batch - Multi-Job", test_create_batch_multi_job),
        ("Get Batch Jobs with Filters", test_get_batch_jobs_with_filters),
        ("Recon Batch - All Completed", test_recon_batch_all_completed),
        ("Recon Batch - With Failures", test_recon_batch_with_failures),
        ("Recon Batch - All Cancelled", test_recon_batch_all_cancelled),
        ("Batch Error Handling", test_batch_error_handling),
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
