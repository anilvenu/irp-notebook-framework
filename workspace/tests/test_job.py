"""
Test suite for job management operations

This test file validates job functionality including:
- Job CRUD operations
- Job creation with dual-mode configuration
- Job submission and status tracking
- Job resubmission with and without overrides
- Parent-child job relationships
- Error handling and validation

All tests run in the 'test' schema to avoid affecting production data.

Run this test:
    python workspace/tests/test_job.py
"""

import sys
from pathlib import Path
from datetime import datetime
import json

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
from helpers.job import (
    read_job,
    update_job_status,
    get_job_config,
    create_job,
    skip_job,
    submit_job,
    track_job_status,
    resubmit_job,
    JobError
)
from helpers.constants import JobStatus, ConfigurationStatus

TEST_SCHEMA = 'test'


def setup_test_schema():
    """Initialize test schema with database tables"""
    print("\n" + "="*80)
    print("SETUP: Initializing Test Schema")
    print("="*80)

    try:
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
    """Helper to create cycle, stage, step, configuration, and batch"""
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
    config_data = {'param1': 'value1', 'param2': 100}
    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=TEST_SCHEMA
    )

    # Create batch
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', 'INITIATED'),
        schema=TEST_SCHEMA
    )

    return cycle_id, stage_id, step_id, config_id, batch_id


# ============================================================================
# TESTS
# ============================================================================

def test_read_job():
    """Test 1: Read job by ID"""
    print("\n" + "="*80)
    print("TEST 1: Read Job")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_read_job')

        # Create job manually
        job_config_data = {'test': 'data'}
        job_config_id = execute_insert(
            """INSERT INTO irp_job_configuration
               (batch_id, configuration_id, job_configuration_data)
               VALUES (%s, %s, %s)""",
            (batch_id, config_id, json.dumps(job_config_data)),
            schema=TEST_SCHEMA
        )

        job_id = execute_insert(
            "INSERT INTO irp_job (batch_id, job_configuration_id, status) VALUES (%s, %s, %s)",
            (batch_id, job_config_id, JobStatus.INITIATED),
            schema=TEST_SCHEMA
        )
        print(f"  Created job ID: {job_id}")

        # Test reading job
        print("\nReading job...")
        result = read_job(job_id, schema=TEST_SCHEMA)

        print(f"✓ Successfully read job")
        print(f"  Job ID: {result['id']}")
        print(f"  Batch ID: {result['batch_id']}")
        print(f"  Job Config ID: {result['job_configuration_id']}")
        print(f"  Status: {result['status']}")

        # Verify data
        assert result['id'] == job_id
        assert result['batch_id'] == batch_id
        assert result['job_configuration_id'] == job_config_id
        assert result['status'] == JobStatus.INITIATED

        print("✓ All assertions passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_update_job_status():
    """Test 2: Update job status"""
    print("\n" + "="*80)
    print("TEST 2: Update Job Status")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_update_job_status')

        job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'test': 'data'},
            schema=TEST_SCHEMA
        )

        # Test updating status
        print("\nUpdating status from INITIATED to SUBMITTED...")
        result = update_job_status(job_id, JobStatus.SUBMITTED, schema=TEST_SCHEMA)
        assert result == True, "Update should return True"
        print("✓ Status updated successfully")

        # Verify the update
        job = read_job(job_id, schema=TEST_SCHEMA)
        assert job['status'] == JobStatus.SUBMITTED
        print(f"✓ Verified status is now: {job['status']}")

        # Test updating to same status (should return False)
        print("\nUpdating to same status (should return False)...")
        result = update_job_status(job_id, JobStatus.SUBMITTED, schema=TEST_SCHEMA)
        assert result == False, "Update to same status should return False"
        print("✓ Correctly returned False for same status")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_create_job_with_new_config():
    """Test 3: Create job with new configuration"""
    print("\n" + "="*80)
    print("TEST 3: Create Job with New Configuration")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_create_new')

        # Create job with new configuration
        job_config_data = {'param_a': 'value_a', 'param_b': 123}

        print("\nCreating job with new configuration...")
        job_id = create_job(
            batch_id=batch_id,
            configuration_id=config_id,
            job_configuration_data=job_config_data,
            schema=TEST_SCHEMA
        )

        print(f"✓ Job created successfully: {job_id}")

        # Verify job
        job = read_job(job_id, schema=TEST_SCHEMA)
        assert job['status'] == JobStatus.INITIATED
        print(f"✓ Job status: {job['status']}")

        # Verify job configuration
        job_config = get_job_config(job_id, schema=TEST_SCHEMA)
        assert job_config['job_configuration_data'] == job_config_data
        print(f"✓ Job configuration created with correct data")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_create_job_with_existing_config():
    """Test 4: Create job with existing configuration"""
    print("\n" + "="*80)
    print("TEST 4: Create Job with Existing Configuration")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_create_existing')

        # Create first job with new config
        job_config_data = {'param': 'value'}
        job1_id = create_job(
            batch_id=batch_id,
            configuration_id=config_id,
            job_configuration_data=job_config_data,
            schema=TEST_SCHEMA
        )

        # Get the job config ID
        job1 = read_job(job1_id, schema=TEST_SCHEMA)
        job_config_id = job1['job_configuration_id']

        # Create second job reusing same config
        print("\nCreating job with existing configuration...")
        job2_id = create_job(
            batch_id=batch_id,
            configuration_id=config_id,
            job_configuration_id=job_config_id,
            schema=TEST_SCHEMA
        )

        print(f"✓ Second job created successfully: {job2_id}")

        # Verify both jobs use same config
        job2 = read_job(job2_id, schema=TEST_SCHEMA)
        assert job2['job_configuration_id'] == job_config_id
        print(f"✓ Both jobs use same configuration ID: {job_config_id}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_create_job_parameter_validation():
    """Test 5: Create job parameter validation"""
    print("\n" + "="*80)
    print("TEST 5: Create Job Parameter Validation")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_validation')

        # Test: Neither parameter provided
        print("\nTest 5a: Neither parameter provided...")
        try:
            create_job(batch_id, config_id, schema=TEST_SCHEMA)
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:80]}...")

        # Test: Both parameters provided
        print("\nTest 5b: Both parameters provided...")
        try:
            create_job(
                batch_id, config_id,
                job_configuration_id=1,
                job_configuration_data={'test': 'data'},
                schema=TEST_SCHEMA
            )
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:80]}...")

        print("\n✓ All parameter validation tests passed")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_skip_job():
    """Test 6: Skip job"""
    print("\n" + "="*80)
    print("TEST 6: Skip Job")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_skip')

        job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'test': 'data'},
            schema=TEST_SCHEMA
        )

        # Verify job is not skipped initially
        job = read_job(job_id, schema=TEST_SCHEMA)
        assert job['skipped'] == False
        print(f"✓ Job initially not skipped")

        # Skip job
        print("\nSkipping job...")
        skip_job(job_id, schema=TEST_SCHEMA)

        # Verify job is now skipped
        job = read_job(job_id, schema=TEST_SCHEMA)
        assert job['skipped'] == True
        print(f"✓ Job successfully skipped")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_submit_job():
    """Test 7: Submit job"""
    print("\n" + "="*80)
    print("TEST 7: Submit Job")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_submit')

        job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'test': 'data'},
            schema=TEST_SCHEMA
        )

        # Submit job
        print("\nSubmitting job...")
        result_id = submit_job(job_id, schema=TEST_SCHEMA)
        assert result_id == job_id
        print(f"✓ Job submitted successfully")

        # Verify job is SUBMITTED
        job = read_job(job_id, schema=TEST_SCHEMA)
        assert job['status'] == JobStatus.SUBMITTED
        assert job['moodys_workflow_id'] is not None
        assert job['submitted_ts'] is not None
        print(f"✓ Job status: {job['status']}")
        print(f"✓ Workflow ID: {job['moodys_workflow_id']}")

        # Verify submission request/response stored
        assert job['submission_request'] is not None
        assert job['submission_response'] is not None
        print(f"✓ Submission request/response stored")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_submit_job_force():
    """Test 8: Force resubmit job"""
    print("\n" + "="*80)
    print("TEST 8: Force Resubmit Job")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_force_submit')

        job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'test': 'data'},
            schema=TEST_SCHEMA
        )

        # Submit job first time
        print("\nSubmitting job (first time)...")
        submit_job(job_id, schema=TEST_SCHEMA)
        job1 = read_job(job_id, schema=TEST_SCHEMA)
        workflow_id_1 = job1['moodys_workflow_id']
        print(f"✓ First workflow ID: {workflow_id_1}")

        # Force resubmit
        print("\nForce resubmitting job...")
        submit_job(job_id, force=True, schema=TEST_SCHEMA)
        job2 = read_job(job_id, schema=TEST_SCHEMA)
        workflow_id_2 = job2['moodys_workflow_id']
        print(f"✓ Second workflow ID: {workflow_id_2}")

        # Workflow IDs should be different
        assert workflow_id_1 != workflow_id_2
        print(f"✓ Workflow IDs are different (resubmitted)")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_track_job_status():
    """Test 9: Track job status"""
    print("\n" + "="*80)
    print("TEST 9: Track Job Status")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_track')

        job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'test': 'data'},
            schema=TEST_SCHEMA
        )

        # Submit job
        submit_job(job_id, schema=TEST_SCHEMA)

        # Track job status multiple times
        print("\nTracking job status (attempt 1)...")
        status1 = track_job_status(job_id, schema=TEST_SCHEMA)
        print(f"  Status: {status1}")

        print("\nTracking job status (attempt 2)...")
        status2 = track_job_status(job_id, schema=TEST_SCHEMA)
        print(f"  Status: {status2}")

        print("\nTracking job status (attempt 3)...")
        status3 = track_job_status(job_id, schema=TEST_SCHEMA)
        print(f"  Status: {status3}")

        # Verify tracking logs created
        df = execute_query(
            "SELECT COUNT(*) as count FROM irp_job_tracking_log WHERE job_id = %s",
            (job_id,),
            schema=TEST_SCHEMA
        )
        tracking_count = df.iloc[0]['count']
        assert tracking_count == 3
        print(f"\n✓ Created {tracking_count} tracking log entries")

        # Status should progress (or stay same) based on stub transitions
        print(f"✓ Status transitions: {status1} -> {status2} -> {status3}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_resubmit_job_without_override():
    """Test 10: Resubmit job without override"""
    print("\n" + "="*80)
    print("TEST 10: Resubmit Job Without Override")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_resubmit_no_override')

        # Create and submit original job
        original_job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'original': 'config'},
            schema=TEST_SCHEMA
        )
        submit_job(original_job_id, schema=TEST_SCHEMA)

        # Get original job config ID
        original_job = read_job(original_job_id, schema=TEST_SCHEMA)
        original_config_id = original_job['job_configuration_id']

        # Resubmit without override
        print("\nResubmitting job without override...")
        new_job_id = resubmit_job(original_job_id, schema=TEST_SCHEMA)
        print(f"✓ New job created: {new_job_id}")

        # Verify original job is skipped
        original_job = read_job(original_job_id, schema=TEST_SCHEMA)
        assert original_job['skipped'] == True
        print(f"✓ Original job marked as skipped")

        # Verify new job uses same config
        new_job = read_job(new_job_id, schema=TEST_SCHEMA)
        assert new_job['job_configuration_id'] == original_config_id
        print(f"✓ New job uses same configuration")

        # Verify parent-child relationship
        assert new_job['parent_job_id'] == original_job_id
        print(f"✓ Parent-child relationship established")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_resubmit_job_with_override():
    """Test 11: Resubmit job with configuration override"""
    print("\n" + "="*80)
    print("TEST 11: Resubmit Job With Configuration Override")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_resubmit_override')

        # Create original job
        original_job_id = create_job(
            batch_id, config_id,
            job_configuration_data={'original': 'config'},
            schema=TEST_SCHEMA
        )

        # Get original config ID
        original_job = read_job(original_job_id, schema=TEST_SCHEMA)
        original_config_id = original_job['job_configuration_id']

        # Resubmit with override
        override_config = {'overridden': 'config', 'new_param': 999}
        override_reason = "User requested parameter change"

        print("\nResubmitting job with override...")
        new_job_id = resubmit_job(
            original_job_id,
            job_configuration_data=override_config,
            override_reason=override_reason,
            schema=TEST_SCHEMA
        )
        print(f"✓ New job created: {new_job_id}")

        # Verify original job is skipped
        original_job = read_job(original_job_id, schema=TEST_SCHEMA)
        assert original_job['skipped'] == True
        print(f"✓ Original job marked as skipped")

        # Verify new job uses DIFFERENT config
        new_job = read_job(new_job_id, schema=TEST_SCHEMA)
        assert new_job['job_configuration_id'] != original_config_id
        print(f"✓ New job uses different configuration")

        # Verify new config has override data
        new_config = get_job_config(new_job_id, schema=TEST_SCHEMA)
        assert new_config['job_configuration_data'] == override_config
        assert new_config['overridden'] == True
        assert new_config['override_reason_txt'] == override_reason
        print(f"✓ New configuration has override data")
        print(f"  Override reason: {new_config['override_reason_txt']}")

        # Verify parent-child relationship
        assert new_job['parent_job_id'] == original_job_id
        print(f"✓ Parent-child relationship established")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_job_error_handling():
    """Test 12: Job error handling"""
    print("\n" + "="*80)
    print("TEST 12: Job Error Handling")
    print("="*80)

    try:
        # Test invalid job_id
        print("\nTest 12a: Invalid job_id...")
        try:
            read_job(-1, schema=TEST_SCHEMA)
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:60]}...")

        # Test job not found
        print("\nTest 12b: Job not found...")
        try:
            read_job(999999, schema=TEST_SCHEMA)
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:60]}...")

        # Test invalid status
        print("\nTest 12c: Invalid status...")
        cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy('test_error')
        job_id = create_job(batch_id, config_id, job_configuration_data={'test': 'data'}, schema=TEST_SCHEMA)

        try:
            update_job_status(job_id, 'INVALID_STATUS', schema=TEST_SCHEMA)
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:60]}...")

        # Test tracking without submission
        print("\nTest 12d: Track job without submission...")
        job_id2 = create_job(batch_id, config_id, job_configuration_data={'test': 'data'}, schema=TEST_SCHEMA)
        try:
            track_job_status(job_id2, schema=TEST_SCHEMA)
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:60]}...")

        # Test resubmit with override but no reason
        print("\nTest 12e: Resubmit with override but no reason...")
        try:
            resubmit_job(
                job_id,
                job_configuration_data={'new': 'config'},
                schema=TEST_SCHEMA
            )
            print("✗ Should have raised JobError")
            return False
        except JobError as e:
            print(f"✓ Correctly raised JobError: {str(e)[:60]}...")

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
    print("JOB MANAGEMENT TEST SUITE")
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
        ("Read Job", test_read_job),
        ("Update Job Status", test_update_job_status),
        ("Create Job with New Config", test_create_job_with_new_config),
        ("Create Job with Existing Config", test_create_job_with_existing_config),
        ("Create Job Parameter Validation", test_create_job_parameter_validation),
        ("Skip Job", test_skip_job),
        ("Submit Job", test_submit_job),
        ("Force Resubmit Job", test_submit_job_force),
        ("Track Job Status", test_track_job_status),
        ("Resubmit Job Without Override", test_resubmit_job_without_override),
        ("Resubmit Job With Override", test_resubmit_job_with_override),
        ("Job Error Handling", test_job_error_handling),
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
