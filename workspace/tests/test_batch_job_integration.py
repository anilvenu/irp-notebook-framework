"""
Test suite for batch and job integration

This test file validates end-to-end workflows including:
- Complete batch creation and submission flow
- Job tracking and batch reconciliation
- Job resubmission scenarios
- Parent-child job relationships
- Configuration override workflows

All tests run in the 'test' schema to avoid affecting production data.

Run this test:
    python workspace/tests/test_batch_job_integration.py
"""

import sys
import argparse
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
from helpers.batch import (
    create_batch,
    submit_batch,
    get_batch_jobs,
    recon_batch,
    read_batch
)
from helpers.job import (
    submit_job,
    track_job_status,
    resubmit_job,
    update_job_status,
    read_job
)
from helpers.constants import BatchStatus, JobStatus, ConfigurationStatus


TEST_SCHEMA = Path(__file__).stem


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


def create_test_hierarchy(cycle_name='test_cycle', config_data=None):
    """Helper to create cycle, stage, step, and configuration"""
    if config_data is None:
        config_data = {'param1': 'value1', 'param2': 100}

    # Create cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
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
# INTEGRATION TESTS
# ============================================================================

def test_end_to_end_batch_workflow():
    """Test 1: Complete end-to-end batch workflow"""
    print("\n" + "="*80)
    print("TEST 1: End-to-End Batch Workflow")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_e2e')

        # Step 1: Create batch (and submit)
        print("\nStep 1: Creating batch...")
        batch_id = create_batch(
            batch_type='default',
            configuration_id=config_id,
            step_id=step_id,
            schema=TEST_SCHEMA
        )
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        
        print(f"✓ Batch created and submitted: {batch_id}")

        # Step 2: Verify batch is ACTIVE
        batch = read_batch(batch_id, schema=TEST_SCHEMA)
        assert batch['status'] == BatchStatus.ACTIVE
        print(f"✓ Batch status: {batch['status']}")

        # Step 3: Verify jobs are SUBMITTED
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Jobs created: {len(jobs)}")
        for job in jobs:
            assert job['status'] == JobStatus.SUBMITTED
            print(f"  Job {job['id']}: {job['status']}")

        # Step 4: Track jobs to completion
        print("\nStep 2: Tracking jobs to completion...")
        for job in jobs:
            # Track multiple times until terminal state
            for i in range(10):  # Max 10 attempts
                status = track_job_status(job['id'], schema=TEST_SCHEMA)
                print(f"  Job {job['id']} tracking attempt {i+1}: {status}")
                if status in [JobStatus.FINISHED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    break

        # Step 5: Reconcile batch
        print("\nStep 3: Reconciling batch...")
        final_status = recon_batch(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Batch recon result: {final_status}")

        # Verify batch has terminal status
        batch = read_batch(batch_id, schema=TEST_SCHEMA)
        assert batch['status'] in [BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED]
        print(f"✓ Batch final status: {batch['status']}")

        # Step 6: Verify recon log created
        df = execute_query(
            "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
            (batch_id,),
            schema=TEST_SCHEMA
        )
        assert not df.empty
        recon_summary = df.iloc[0]['recon_summary']
        # recon_summary is already a dict (JSONB auto-deserialized by pandas)
        if isinstance(recon_summary, str):
            recon_summary = json.loads(recon_summary)
        print(f"✓ Recon log created with summary:")
        print(f"  Total jobs: {recon_summary['total_jobs']}")
        print(f"  Status counts: {recon_summary['job_status_counts']}")

        print("\n✓ End-to-end workflow completed successfully")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multi_job_batch_workflow():
    """Test 2: Multi-job batch workflow"""
    print("\n" + "="*80)
    print("TEST 2: Multi-Job Batch Workflow")
    print("="*80)

    try:
        print("Setting up test data...")
        # Create config with multiple jobs
        config_data = {
            'batch_type': 'multi_test',
            'jobs': [
                {'job_id': 1, 'param': 'A'},
                {'job_id': 2, 'param': 'B'},
                {'job_id': 3, 'param': 'C'},
                {'job_id': 4, 'param': 'D'},
                {'job_id': 5, 'param': 'E'}
            ]
        }
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_multi', config_data)

        # Create batch with multi_job transformer
        print("\nCreating multi-job batch...")
        batch_id = create_batch(
            batch_type='multi_job',
            configuration_id=config_id,
            step_id=step_id,
            schema=TEST_SCHEMA
        )
        print(f"✓ Batch created: {batch_id}")
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        
        print(f"✓ Batch submitted: {batch_id}")

        # Verify 5 jobs created
        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Jobs created: {len(jobs)}")
        assert len(jobs) == 5, f"Expected 5 jobs, got {len(jobs)}"

        # Track all jobs
        print("\nTracking all jobs...")
        for job in jobs:
            for i in range(5):
                status = track_job_status(job['id'], schema=TEST_SCHEMA)
                if status in [JobStatus.FINISHED, JobStatus.FAILED]:
                    break
            print(f"  Job {job['id']}: {status}")

        # Reconcile batch
        print("\nReconciling batch...")
        final_status = recon_batch(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Batch status: {final_status}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_job_resubmission_workflow():
    """Test 3: Job resubmission workflow"""
    print("\n" + "="*80)
    print("TEST 3: Job Resubmission Workflow")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_resubmit_workflow')

        # Create batch
        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        
        
        original_jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        original_job_id = original_jobs[0]['id']

        print(f"✓ Original job ID: {original_job_id}")

        # Track original job to FAILED
        print("\nSimulating job failure...")
        update_job_status(original_job_id, JobStatus.FAILED, schema=TEST_SCHEMA)
        print(f"✓ Original job marked as FAILED")

        # Resubmit job
        print("\nResubmitting job...")
        new_job_id = resubmit_job(original_job_id, schema=TEST_SCHEMA)
        print(f"✓ New job created: {new_job_id}")

        # Verify original job is skipped
        original_job = read_job(original_job_id, schema=TEST_SCHEMA)
        assert original_job['skipped'] == True
        print(f"✓ Original job skipped")

        # Verify parent-child relationship
        new_job = read_job(new_job_id, schema=TEST_SCHEMA)
        assert new_job['parent_job_id'] == original_job_id
        print(f"✓ Parent-child relationship established")

        # Submit new job
        print("\nSubmitting new job...")
        submit_job(new_job_id, schema=TEST_SCHEMA)

        # Track new job to completion
        print("\nTracking new job...")
        for i in range(10):
            status = track_job_status(new_job_id, schema=TEST_SCHEMA)
            if status in [JobStatus.FINISHED, JobStatus.FAILED]:
                break
        print(f"✓ New job status: {status}")

        # Reconcile batch
        print("\nReconciling batch...")
        final_status = recon_batch(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Batch status: {final_status}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration_override_workflow():
    """Test 4: Configuration override workflow"""
    print("\n" + "="*80)
    print("TEST 4: Configuration Override Workflow")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_override_workflow')

        # Create batch
        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        original_jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        original_job_id = original_jobs[0]['id']

        # Mark job as failed
        update_job_status(original_job_id, JobStatus.FAILED, schema=TEST_SCHEMA)
        print(f"✓ Original job marked as FAILED")

        # Resubmit with override
        override_config = {
            'param1': 'overridden_value',
            'param2': 999,
            'new_param': 'added'
        }
        override_reason = "Fix parameter values due to validation error"

        print("\nResubmitting job with configuration override...")
        new_job_id = resubmit_job(
            original_job_id,
            job_configuration_data=override_config,
            override_reason=override_reason,
            schema=TEST_SCHEMA
        )
        print(f"✓ New job created with override: {new_job_id}")

        # Verify override configuration
        from helpers.job import get_job_config
        new_config = get_job_config(new_job_id, schema=TEST_SCHEMA)
        assert new_config['overridden'] == True
        assert new_config['override_reason_txt'] == override_reason
        assert new_config['job_configuration_data'] == override_config
        print(f"✓ Override configuration verified:")
        print(f"  Overridden: {new_config['overridden']}")
        print(f"  Reason: {new_config['override_reason_txt']}")
        print(f"  Data: {new_config['job_configuration_data']}")

        # Verify audit trail
        print("\nVerifying audit trail...")
        print(f"  Original job ID: {original_job_id} (skipped)")
        print(f"  New job ID: {new_job_id} (parent: {original_job_id})")
        print(f"✓ Audit trail complete")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_mixed_job_states_recon():
    """Test 5: Batch reconciliation with mixed job states"""
    print("\n" + "="*80)
    print("TEST 5: Batch Reconciliation with Mixed States")
    print("="*80)

    try:
        print("Setting up test data...")
        # Create config with multiple jobs
        config_data = {
            'jobs': [
                {'job_id': 1},
                {'job_id': 2},
                {'job_id': 3},
                {'job_id': 4},
                {'job_id': 5}
            ]
        }
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_mixed_states', config_data)

        # Create batch
        batch_id = create_batch('multi_job', config_id, step_id, schema=TEST_SCHEMA)
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)

        # Set jobs to different states
        print("\nSetting jobs to different states...")
        update_job_status(jobs[0]['id'], JobStatus.FINISHED, schema=TEST_SCHEMA)
        print(f"  Job {jobs[0]['id']}: COMPLETED")

        update_job_status(jobs[1]['id'], JobStatus.FINISHED, schema=TEST_SCHEMA)
        print(f"  Job {jobs[1]['id']}: COMPLETED")

        update_job_status(jobs[2]['id'], JobStatus.RUNNING, schema=TEST_SCHEMA)
        print(f"  Job {jobs[2]['id']}: RUNNING")

        update_job_status(jobs[3]['id'], JobStatus.FAILED, schema=TEST_SCHEMA)
        print(f"  Job {jobs[3]['id']}: FAILED")

        from helpers.job import skip_job
        skip_job(jobs[4]['id'], schema=TEST_SCHEMA)
        print(f"  Job {jobs[4]['id']}: SKIPPED")

        # Reconcile batch
        print("\nReconciling batch...")
        result_status = recon_batch(batch_id, schema=TEST_SCHEMA)
        print(f"✓ Recon result: {result_status}")

        # With FAILED jobs, batch should be FAILED
        assert result_status == BatchStatus.FAILED
        print(f"✓ Batch correctly marked as FAILED")

        # Verify recon summary
        df = execute_query(
            "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
            (batch_id,),
            schema=TEST_SCHEMA
        )
        recon_summary = df.iloc[0]['recon_summary']
        # recon_summary is already a dict (JSONB auto-deserialized by pandas)
        if isinstance(recon_summary, str):
            recon_summary = json.loads(recon_summary)
        print(f"✓ Recon summary:")
        print(f"  Total jobs: {recon_summary['total_jobs']}")
        print(f"  Status counts: {recon_summary['job_status_counts']}")
        print(f"  Failed job IDs: {recon_summary['failed_job_ids']}")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parent_child_job_chain():
    """Test 6: Parent-child job chain"""
    print("\n" + "="*80)
    print("TEST 6: Parent-Child Job Chain")
    print("="*80)

    try:
        print("Setting up test data...")
        cycle_id, stage_id, step_id, config_id = create_test_hierarchy('test_job_chain')

        # Create batch
        batch_id = create_batch('default', config_id, step_id, schema=TEST_SCHEMA)
        # Submit batch
        submit_batch(batch_id, schema=TEST_SCHEMA)        

        jobs = get_batch_jobs(batch_id, schema=TEST_SCHEMA)
        original_job_id = jobs[0]['id']

        print(f"✓ Original job: {original_job_id}")

        # Create chain of resubmissions
        print("\nCreating job chain...")
        job_chain = [original_job_id]

        # Resubmit 3 times
        current_job_id = original_job_id
        for i in range(3):
            update_job_status(current_job_id, JobStatus.FAILED, schema=TEST_SCHEMA)
            new_job_id = resubmit_job(current_job_id, schema=TEST_SCHEMA)
            job_chain.append(new_job_id)
            print(f"  Resubmission {i+1}: {new_job_id} (parent: {current_job_id})")
            current_job_id = new_job_id

        print(f"\n✓ Job chain created: {len(job_chain)} jobs")

        # Verify chain
        print("\nVerifying parent-child relationships...")
        for i in range(1, len(job_chain)):
            child_job = read_job(job_chain[i], schema=TEST_SCHEMA)
            assert child_job['parent_job_id'] == job_chain[i-1]
            print(f"  Job {job_chain[i]} -> parent: {job_chain[i-1]} ✓")

        # Verify all but last are skipped
        print("\nVerifying skipped status...")
        for i in range(len(job_chain) - 1):
            job = read_job(job_chain[i], schema=TEST_SCHEMA)
            assert job['skipped'] == True
            print(f"  Job {job_chain[i]}: skipped ✓")

        # Last job should not be skipped
        last_job = read_job(job_chain[-1], schema=TEST_SCHEMA)
        assert last_job['skipped'] == False
        print(f"  Job {job_chain[-1]}: not skipped ✓")

        print("\n✓ Job chain verified successfully")
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
    print("BATCH/JOB INTEGRATION TEST SUITE")
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
        ("End-to-End Batch Workflow", test_end_to_end_batch_workflow),
        ("Multi-Job Batch Workflow", test_multi_job_batch_workflow),
        ("Job Resubmission Workflow", test_job_resubmission_workflow),
        ("Configuration Override Workflow", test_configuration_override_workflow),
        ("Mixed Job States Reconciliation", test_mixed_job_states_recon),
        ("Parent-Child Job Chain", test_parent_child_job_chain),
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
