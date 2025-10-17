"""
Test suite for batch management operations (pytest version)

This test file validates batch functionality including:
- Batch CRUD operations
- Batch creation with transformer integration
- Batch submission and job orchestration
- Batch reconciliation with various job states
- Error handling and validation

All tests run in the 'test_batch' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_batch.py
    pytest workspace/tests/test_batch.py -v
    pytest workspace/tests/test_batch.py --preserve-schema
"""

import pytest
import json
from datetime import datetime

from helpers.database import execute_query, execute_insert
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
from helpers.job import create_job, update_job_status, skip_job
from helpers.constants import BatchStatus, JobStatus, ConfigurationStatus


# ============================================================================
# Helper Functions
# ============================================================================

def create_test_hierarchy(test_schema, cycle_name='test_cycle'):
    """Helper to create cycle, stage, step, and configuration"""
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
        schema=test_schema
    )

    return cycle_id, stage_id, step_id, config_id


# ============================================================================
# Tests - CRUD Operations
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_read_batch(test_schema):
    """Test reading batch by ID"""
    # Setup: Create hierarchy and batch
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_read_batch')

    # Create batch manually
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.INITIATED),
        schema=test_schema
    )

    # Test reading batch
    result = read_batch(batch_id, schema=test_schema)

    # Assertions
    assert result['id'] == batch_id
    assert result['step_id'] == step_id
    assert result['configuration_id'] == config_id
    assert result['batch_type'] == 'default'
    assert result['status'] == BatchStatus.INITIATED


@pytest.mark.database
@pytest.mark.unit
def test_update_batch_status(test_schema):
    """Test updating batch status"""
    # Setup
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_update_status')

    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.INITIATED),
        schema=test_schema
    )

    # Test updating status
    result = update_batch_status(batch_id, BatchStatus.ACTIVE, schema=test_schema)
    assert result == True, "Update should return True"

    # Verify the update
    batch = read_batch(batch_id, schema=test_schema)
    assert batch['status'] == BatchStatus.ACTIVE

    # Test updating to same status (should return False)
    result = update_batch_status(batch_id, BatchStatus.ACTIVE, schema=test_schema)
    assert result == False, "Update to same status should return False"


# ============================================================================
# Tests - Batch Creation with Transformers
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_create_batch_default(test_schema):
    """Test creating batch with default transformer"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_create_batch')

    # Create batch
    batch_id = create_batch(
        batch_type='default',
        configuration_id=config_id,
        step_id=step_id,
        schema=test_schema
    )
    assert isinstance(batch_id, int)

    # Submit batch
    submit_batch(batch_id, schema=test_schema)

    # Verify batch
    batch = read_batch(batch_id, schema=test_schema)
    assert batch['id'] == batch_id
    assert batch['batch_type'] == 'default'

    # Verify jobs were created
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) == 1, "Default transformer should create 1 job"

    # Verify job configurations
    job_configs = get_batch_job_configurations(batch_id, schema=test_schema)
    assert len(job_configs) == 1


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_multi_job(test_schema):
    """Test creating batch with multi_job transformer"""
    # Create hierarchy with multi-job config
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('test_multi_job', 'ACTIVE'),
        schema=test_schema
    )
    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'test_stage'),
        schema=test_schema
    )
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'test_step'),
        schema=test_schema
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
        schema=test_schema
    )

    # Create batch
    batch_id = create_batch(
        batch_type='multi_job',
        configuration_id=config_id,
        step_id=step_id,
        schema=test_schema
    )

    # Submit batch
    submit_batch(batch_id, schema=test_schema)

    # Verify jobs were created
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) == 3, "Multi-job transformer should create 3 jobs"

    # Verify all jobs are SUBMITTED
    for job in jobs:
        assert job['status'] == JobStatus.SUBMITTED


# ============================================================================
# Tests - Job Filtering
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_get_batch_jobs_with_filters(test_schema):
    """Test getting batch jobs with various filters"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_get_jobs')

    # Create batch
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.ACTIVE),
        schema=test_schema
    )

    # Create multiple jobs with different states
    job_config_data = {'param': 'value'}

    job1_id = create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=test_schema)
    job2_id = create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=test_schema)
    job3_id = create_job(batch_id, config_id, job_configuration_data=job_config_data, schema=test_schema)

    # Update jobs to different states
    update_job_status(job1_id, JobStatus.SUBMITTED, schema=test_schema)
    update_job_status(job2_id, JobStatus.FINISHED, schema=test_schema)
    skip_job(job3_id, schema=test_schema)

    # Test: Get all jobs
    all_jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(all_jobs) == 3

    # Test: Get non-skipped jobs
    non_skipped = get_batch_jobs(batch_id, skipped=False, schema=test_schema)
    assert len(non_skipped) == 2

    # Test: Get skipped jobs
    skipped = get_batch_jobs(batch_id, skipped=True, schema=test_schema)
    assert len(skipped) == 1

    # Test: Get jobs by status
    completed = get_batch_jobs(batch_id, status=JobStatus.FINISHED, schema=test_schema)
    assert len(completed) == 1


# ============================================================================
# Tests - Batch Reconciliation
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_all_completed(test_schema):
    """Test reconciling batch with all jobs completed"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_recon_completed')

    # Create batch with jobs
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Get jobs and mark them as FINISHED
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    for job in jobs:
        update_job_status(job['id'], JobStatus.FINISHED, schema=test_schema)

    # Recon batch
    result_status = recon_batch(batch_id, schema=test_schema)

    assert result_status == BatchStatus.COMPLETED, f"Expected COMPLETED, got {result_status}"

    # Verify batch status updated
    batch = read_batch(batch_id, schema=test_schema)
    assert batch['status'] == BatchStatus.COMPLETED


@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_with_failures(test_schema):
    """Test reconciling batch with failed jobs"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_recon_failed')

    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Get jobs and mark some as FAILED
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    update_job_status(jobs[0]['id'], JobStatus.FAILED, schema=test_schema)

    # Recon batch
    result_status = recon_batch(batch_id, schema=test_schema)

    assert result_status == BatchStatus.FAILED, f"Expected FAILED, got {result_status}"


@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_all_cancelled(test_schema):
    """Test reconciling batch with all jobs cancelled"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_recon_cancelled')

    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Get jobs and mark them as CANCELLED
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    for job in jobs:
        update_job_status(job['id'], JobStatus.CANCELLED, schema=test_schema)

    # Recon batch
    result_status = recon_batch(batch_id, schema=test_schema)

    assert result_status == BatchStatus.CANCELLED, f"Expected CANCELLED, got {result_status}"


# ============================================================================
# Tests - Error Handling
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_batch_error_invalid_id(test_schema):
    """Test error handling for invalid batch ID"""
    with pytest.raises(BatchError):
        read_batch(-1, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_batch_error_not_found(test_schema):
    """Test error handling for batch not found"""
    with pytest.raises(BatchError):
        read_batch(999999, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_batch_error_invalid_status(test_schema):
    """Test error handling for invalid status"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_error')
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.INITIATED),
        schema=test_schema
    )

    with pytest.raises(BatchError):
        update_batch_status(batch_id, 'INVALID_STATUS', schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_batch_error_unknown_type(test_schema):
    """Test error handling for unknown batch type"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_error_type')

    with pytest.raises(BatchError):
        create_batch('nonexistent_type', config_id, step_id, schema=test_schema)
