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


# ============================================================================
# Tests - Quick Wins: Invalid Inputs and Error Paths
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_create_batch_invalid_configuration_id(test_schema):
    """Test create_batch with invalid configuration_id"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_invalid_config')

    # Test with 0
    with pytest.raises(BatchError, match="Invalid configuration_id"):
        create_batch('default', 0, step_id, schema=test_schema)

    # Test with negative
    with pytest.raises(BatchError, match="Invalid configuration_id"):
        create_batch('default', -1, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_create_batch_invalid_batch_type(test_schema):
    """Test create_batch with invalid batch_type"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_invalid_type')

    # Empty string
    with pytest.raises(BatchError, match="Invalid batch_type"):
        create_batch('', config_id, step_id, schema=test_schema)

    # Whitespace only
    with pytest.raises(BatchError, match="Invalid batch_type"):
        create_batch('   ', config_id, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_configuration_wrong_status(test_schema):
    """Test create_batch when configuration has wrong status"""
    from helpers.database import execute_command

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_wrong_status')

    # Set config to ERROR status
    execute_command(
        "UPDATE irp_configuration SET status = %s WHERE id = %s",
        (ConfigurationStatus.ERROR, config_id),
        schema=test_schema
    )

    with pytest.raises(BatchError, match="has invalid status"):
        create_batch('default', config_id, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_configuration_new_status(test_schema):
    """Test create_batch when configuration has NEW status (not VALID/ACTIVE)"""
    from helpers.database import execute_command

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_new_status')

    # Set config to NEW status
    execute_command(
        "UPDATE irp_configuration SET status = %s WHERE id = %s",
        (ConfigurationStatus.NEW, config_id),
        schema=test_schema
    )

    with pytest.raises(BatchError, match="has invalid status"):
        create_batch('default', config_id, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_no_step_id_lookup(test_schema):
    """Test create_batch with step_id=None (triggers lookup)"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_no_step')

    # Since _lookup_step_id returns None, this should fail
    with pytest.raises(BatchError, match="step_id is required"):
        create_batch('default', config_id, step_id=None, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_invalid_step_id(test_schema):
    """Test create_batch with invalid step_id values"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_invalid_step')

    # Test with 0
    with pytest.raises(BatchError, match="Invalid step_id"):
        create_batch('default', config_id, step_id=0, schema=test_schema)

    # Test with negative
    with pytest.raises(BatchError, match="Invalid step_id"):
        create_batch('default', config_id, step_id=-5, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_transformer_exception(test_schema):
    """Test create_batch when transformer raises exception"""
    from helpers.configuration import ConfigurationTransformer

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_transformer_exc')

    # Register a transformer that raises exception
    @ConfigurationTransformer.register('test_error_transformer')
    def transform_error(config):
        raise ValueError("Transformer failed!")

    with pytest.raises(BatchError, match="Transformer failed"):
        create_batch('test_error_transformer', config_id, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_create_batch_transformer_returns_empty(test_schema):
    """Test create_batch when transformer returns empty list"""
    from helpers.configuration import ConfigurationTransformer

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_empty_transformer')

    # Register a transformer that returns empty list
    @ConfigurationTransformer.register('test_empty_transformer')
    def transform_empty(config):
        return []

    with pytest.raises(BatchError, match="returned no job configurations"):
        create_batch('test_empty_transformer', config_id, step_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_submit_batch_configuration_error_status(test_schema):
    """Test submit_batch when configuration has ERROR status"""
    from helpers.database import execute_command

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_submit_error')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)

    # Set config to ERROR status
    execute_command(
        "UPDATE irp_configuration SET status = %s WHERE id = %s",
        (ConfigurationStatus.ERROR, config_id),
        schema=test_schema
    )

    with pytest.raises(BatchError, match="Must be VALID or ACTIVE to submit"):
        submit_batch(batch_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_submit_batch_cycle_not_active(test_schema):
    """Test submit_batch when cycle is not ACTIVE"""
    from helpers.database import execute_command

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_cycle_archived')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)

    # Archive the cycle
    execute_command(
        "UPDATE irp_cycle SET status = %s WHERE id = %s",
        ('ARCHIVED', cycle_id),
        schema=test_schema
    )

    with pytest.raises(BatchError, match="Must be ACTIVE to submit"):
        submit_batch(batch_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_get_batch_jobs_with_json_parsing(test_schema):
    """Test get_batch_jobs parses JSON fields correctly"""
    from helpers.database import execute_command

    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_json_parse')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)

    # Get jobs
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) > 0

    # Manually update job to have JSON strings
    execute_command(
        "UPDATE irp_job SET submission_request = %s, submission_response = %s WHERE id = %s",
        (json.dumps({'test': 'request'}), json.dumps({'test': 'response'}), jobs[0]['id']),
        schema=test_schema
    )

    # Fetch and verify parsing
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert jobs[0]['submission_request'] == {'test': 'request'}
    assert jobs[0]['submission_response'] == {'test': 'response'}


@pytest.mark.database
@pytest.mark.integration
def test_get_batch_job_configurations_with_skipped_filter(test_schema):
    """Test get_batch_job_configurations with skipped filter"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_config_skip')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)

    # Get all configs
    all_configs = get_batch_job_configurations(batch_id, schema=test_schema)
    assert len(all_configs) == 1

    # Get non-skipped configs
    non_skipped = get_batch_job_configurations(batch_id, skipped=False, schema=test_schema)
    assert len(non_skipped) == 1

    # Get skipped configs (should be empty)
    skipped_configs = get_batch_job_configurations(batch_id, skipped=True, schema=test_schema)
    assert len(skipped_configs) == 0


@pytest.mark.database
@pytest.mark.integration
def test_get_batch_job_configurations_json_parsing(test_schema):
    """Test get_batch_job_configurations parses JSON correctly"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_config_json')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)

    # Get job configurations
    job_configs = get_batch_job_configurations(batch_id, schema=test_schema)

    # Verify that job_configuration_data is parsed as dict
    assert len(job_configs) > 0
    assert isinstance(job_configs[0]['job_configuration_data'], dict)


# ============================================================================
# Tests - Must-Dos: Critical Business Logic
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_with_error_jobs(test_schema):
    """Test recon_batch with ERROR status jobs"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_recon_error')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Mark job as ERROR
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    update_job_status(jobs[0]['id'], JobStatus.ERROR, schema=test_schema)

    result = recon_batch(batch_id, schema=test_schema)
    assert result == BatchStatus.ERROR


@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_all_skipped(test_schema):
    """Test recon_batch with all jobs skipped"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_recon_skipped')
    batch_id = create_batch('default', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Skip all jobs
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    for job in jobs:
        skip_job(job['id'], schema=test_schema)

    # Recon should handle this gracefully
    result = recon_batch(batch_id, schema=test_schema)


    # Assert irp_batch_recon_log record created
    log_records = execute_query(
        "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
        (batch_id,),
        schema=test_schema
    )
    # Print the recon_result field from the log record without truncation
    if not log_records.empty:
        log_record = log_records.iloc[0]

    # With all jobs skipped and one config not skipped, should be ACTIVE
    assert result == BatchStatus.ACTIVE
    assert not log_record.empty
    assert log_record['recon_result'] == BatchStatus.ACTIVE
    assert log_record['recon_summary']['total_configs'] == 1
    assert log_record['recon_summary']['non_skipped_configs'] == 1
    assert log_record['recon_summary']['unfulfilled_configs'] == 1


@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_empty_batch(test_schema):
    """Test recon_batch with batch that has no jobs"""
    cycle_id, stage_id, step_id, config_id = create_test_hierarchy(test_schema, 'test_empty_batch')

    # Create batch manually without jobs
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.INITIATED),
        schema=test_schema
    )

    # Recon should handle empty batch
    result = recon_batch(batch_id, schema=test_schema)

    # Assert irp_batch_recon_log record created
    log_records = execute_query(
        "SELECT * FROM irp_batch_recon_log WHERE batch_id = %s ORDER BY recon_ts DESC LIMIT 1",
        (batch_id,),
        schema=test_schema
    )
    if not log_records.empty:
        log_record = log_records.iloc[0]

    # With no non-skipped configs and no non-skipped jobs, should be ACTIVE
    assert result == BatchStatus.ACTIVE
    assert not log_record.empty
    assert log_record['recon_result'] == BatchStatus.ACTIVE


@pytest.mark.database
@pytest.mark.integration
def test_recon_batch_mixed_job_states(test_schema):
    """Test recon_batch with mixed job states (some finished, some in progress)"""
    # Create multi-job batch
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('test_mixed_states', 'ACTIVE'),
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
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    batch_id = create_batch('multi_job', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)

    # Set different job states
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    update_job_status(jobs[0]['id'], JobStatus.FINISHED, schema=test_schema)
    update_job_status(jobs[1]['id'], JobStatus.RUNNING, schema=test_schema)
    # jobs[2] stays SUBMITTED

    # Recon - should be ACTIVE (not all finished)
    result = recon_batch(batch_id, schema=test_schema)
    assert result == BatchStatus.ACTIVE
