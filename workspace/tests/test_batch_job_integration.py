"""
Test suite for batch and job integration (pytest version)

This test file validates end-to-end workflows including:
- Complete batch creation and submission flow
- Job tracking and batch reconciliation
- Job resubmission scenarios
- Parent-child job relationships
- Configuration override workflows

All tests run in the 'test_batch_job_integration' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_batch_job_integration.py
    pytest workspace/tests/test_batch_job_integration.py -v
    pytest workspace/tests/test_batch_job_integration.py --preserve-schema
"""

import pytest
import json
from datetime import datetime

from helpers.database import execute_query, execute_insert
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
    read_job,
    skip_job,
    get_job_config
)
from helpers.constants import BatchStatus, JobStatus, ConfigurationStatus


# ============================================================================
# Helper Functions
# ============================================================================

def create_test_hierarchy(test_schema, cycle_name='test_cycle', config_data=None):
    """Helper to create cycle, stage, step, and configuration"""
    if config_data is None:
        config_data = {'param1': 'value1', 'param2': 100}

    # Create cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
        schema=test_schema
    )

    # Create stage
    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'test_stage'),
        schema=test_schema
    )

    # Create step
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'test_step'),
        schema=test_schema
    )

    # Create configuration
    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    return cycle_id, stage_id, step_id, config_id


# ============================================================================
# Tests - End-to-End Workflows
# ============================================================================

@pytest.mark.database
@pytest.mark.e2e
def test_end_to_end_batch_workflow(test_schema):
    """Test complete end-to-end batch workflow"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_e2e')

    # Step 1: Create and submit batch
    batch_id = create_batch(
        batch_type='default',
        configuration_id=config_id,
        step_id=step_id,
        schema=test_schema
    )
    submit_batch(batch_id, schema=test_schema)

    # Step 2: Verify batch is ACTIVE
    batch = read_batch(batch_id, schema=test_schema)
    assert batch['status'] == BatchStatus.ACTIVE, f"Expected {BatchStatus.ACTIVE} for batch and received {batch['status']}"

    # Step 3: Verify jobs are SUBMITTED
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) > 0, f"No job retieved by get_batch_jobs"
    for job in jobs:
        assert job['status'] == JobStatus.SUBMITTED, f"Expected {JobStatus.ACTIVE} for jobs and received {batch['status']}"

    # Step 4: Track jobs to completion
    for job in jobs:
        for i in range(10):  # Max 10 attempts
            status = track_job_status(job['id'], schema=test_schema)
            if status in [JobStatus.FINISHED, JobStatus.FAILED, JobStatus.CANCELLED]:
                break

    # Step 5: Reconcile batch
    final_status = recon_batch(batch_id, schema=test_schema)
    assert final_status in [BatchStatus.ACTIVE, BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED]

    # Step 6: Verify recon log created
    df = execute_query(
        "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
        (batch_id,),
        schema=test_schema
    )
    assert not df.empty

    recon_summary = df.iloc[0]['recon_summary']
    if isinstance(recon_summary, str):
        recon_summary = json.loads(recon_summary)

    assert 'total_jobs' in recon_summary
    assert 'job_status_counts' in recon_summary


@pytest.mark.database
@pytest.mark.e2e
def test_multi_job_batch_workflow(test_schema):
    """Test multi-job batch workflow"""
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
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_multi', config_data)

    # Create batch with multi_job transformer
    batch_id = create_batch(
        batch_type='multi_job',
        configuration_id=config_id,
        step_id=step_id,
        schema=test_schema
    )
    submit_batch(batch_id, schema=test_schema)

    # Verify 5 jobs created
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) == 5, f"Expected 5 jobs, got {len(jobs)}"
    # TODO : Assert the configurations for each job

@pytest.mark.database
@pytest.mark.e2e
def test_job_resubmission_workflow(test_schema):
    """Test job resubmission workflow"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_resubmit_workflow')

    # Create and submit batch
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    original_jobs = get_batch_jobs(batch_id, schema=test_schema)
    original_job_id = original_jobs[0]['id']

    # Simulate job failure
    update_job_status(original_job_id, JobStatus.FAILED, schema=test_schema)

    # Resubmit job
    new_job_id = resubmit_job(original_job_id, schema=test_schema)

    # Verify original job is skipped
    original_job = read_job(original_job_id, schema=test_schema)
    assert original_job['skipped'] == True

    # Verify parent-child relationship
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['parent_job_id'] == original_job_id

    # Submit and track new job
    submit_job(new_job_id, schema=test_schema)
    for i in range(10):
        status = track_job_status(new_job_id, schema=test_schema)
        if status in [JobStatus.FINISHED, JobStatus.FAILED]:
            break

    # Reconcile batch
    final_status = recon_batch(batch_id, schema=test_schema)
    assert final_status in [BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED]


@pytest.mark.database
@pytest.mark.e2e
def test_configuration_override_workflow(test_schema):
    """Test configuration override workflow"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_override_workflow')

    # Create and submit batch
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    original_jobs = get_batch_jobs(batch_id, schema=test_schema)
    original_job_id = original_jobs[0]['id']

    # Mark job as failed
    update_job_status(original_job_id, JobStatus.FAILED, schema=test_schema)

    # Resubmit with override
    override_config = {
        'param1': 'overridden_value',
        'param2': 999,
        'new_param': 'added'
    }
    override_reason = "Fix parameter values due to validation error"

    new_job_id = resubmit_job(
        original_job_id,
        job_configuration_data=override_config,
        override_reason=override_reason,
        schema=test_schema
    )

    # Verify override configuration
    new_config = get_job_config(new_job_id, schema=test_schema)
    assert new_config['overridden'] == True
    assert new_config['override_reason_txt'] == override_reason
    assert new_config['job_configuration_data'] == override_config

    # Verify audit trail
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['parent_job_id'] == original_job_id


@pytest.mark.database
@pytest.mark.e2e
def test_mixed_job_states_recon(test_schema):
    """Test batch reconciliation with mixed job states"""
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
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_mixed_states', config_data)

    # Create and submit batch
    batch_id = create_batch('multi_job', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    jobs = get_batch_jobs(batch_id, schema=test_schema)

    # Set jobs to different states
    update_job_status(jobs[0]['id'], JobStatus.FINISHED, schema=test_schema)
    update_job_status(jobs[1]['id'], JobStatus.FINISHED, schema=test_schema)
    update_job_status(jobs[2]['id'], JobStatus.RUNNING, schema=test_schema)
    update_job_status(jobs[3]['id'], JobStatus.FAILED, schema=test_schema)
    skip_job(jobs[4]['id'], schema=test_schema)

    # Reconcile batch
    result_status = recon_batch(batch_id, schema=test_schema)

    # With FAILED jobs, batch should be FAILED
    assert result_status == BatchStatus.FAILED

    # Verify recon summary
    df = execute_query(
        "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
        (batch_id,),
        schema=test_schema
    )
    recon_summary = df.iloc[0]['recon_summary']
    if isinstance(recon_summary, str):
        recon_summary = json.loads(recon_summary)

    assert 'total_jobs' in recon_summary
    assert 'job_status_counts' in recon_summary
    assert 'failed_job_ids' in recon_summary


@pytest.mark.database
@pytest.mark.e2e
def test_parent_child_job_chain(test_schema):
    """Test parent-child job chain"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_job_chain')

    # Create and submit batch
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    jobs = get_batch_jobs(batch_id, schema=test_schema)
    original_job_id = jobs[0]['id']

    # Create chain of resubmissions
    job_chain = [original_job_id]

    # Resubmit 3 times
    current_job_id = original_job_id
    for i in range(3):
        update_job_status(current_job_id, JobStatus.FAILED, schema=test_schema)
        new_job_id = resubmit_job(current_job_id, schema=test_schema)
        job_chain.append(new_job_id)
        current_job_id = new_job_id

    assert len(job_chain) == 4, "Should have 4 jobs in chain"

    # Verify parent-child relationships
    for i in range(1, len(job_chain)):
        child_job = read_job(job_chain[i], schema=test_schema)
        assert child_job['parent_job_id'] == job_chain[i-1]

    # Verify all but last are skipped
    for i in range(len(job_chain) - 1):
        job = read_job(job_chain[i], schema=test_schema)
        assert job['skipped'] == True

    # Last job should not be skipped
    last_job = read_job(job_chain[-1], schema=test_schema)
    assert last_job['skipped'] == False
