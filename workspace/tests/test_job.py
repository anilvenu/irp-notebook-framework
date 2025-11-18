"""
Test suite for job management operations (pytest version)

This test file validates job functionality including:
- Job CRUD operations
- Job creation with dual-mode configuration
- Job submission and status tracking
- Job resubmission with and without overrides
- Parent-child job relationships
- Error handling and validation

All tests run in the 'test_job' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_job.py
    pytest workspace/tests/test_job.py -v
    pytest workspace/tests/test_job.py --preserve-schema
"""

import pytest
import json
from datetime import datetime

from helpers.database import execute_query, execute_insert
from helpers.job import (
    read_job,
    update_job_status,
    get_job_config,
    create_job,  # CRUD function - takes job_configuration_id
    create_job_with_config,  # Atomic wrapper - takes job_configuration_data
    skip_job,
    submit_job,
    track_job_status,
    resubmit_job as resubmit_job,
    JobError
)
from helpers.constants import JobStatus, ConfigurationStatus


# ============================================================================
# Helper Functions
# ============================================================================

def create_test_hierarchy(test_schema, cycle_name='test_cycle'):
    """Helper to create cycle, stage, step, configuration, and batch

    Creates a configuration suitable for EDM Creation batch type by default,
    which is needed for tests that submit jobs.
    """
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

    # Create configuration with EDM Creation-compatible structure
    config_data = {
        'Metadata': {
            'cycle': cycle_name,
            'date': '2024-01-01'
        },
        'Databases': [
            {'Database': 'TestDB', 'Server': 'databridge-1'}
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

    # Create batch with EDM Creation type
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'EDM Creation', 'INITIATED'),
        schema=test_schema
    )

    return cycle_id, stage_id, step_id, config_id, batch_id


# ============================================================================
# Tests - CRUD Operations
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_read_job(test_schema):
    """Test reading job by ID"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_read_job')

    # Create job manually
    job_config_data = {'test': 'data'}
    job_config_id = execute_insert(
        """INSERT INTO irp_job_configuration
           (batch_id, configuration_id, job_configuration_data)
           VALUES (%s, %s, %s)""",
        (batch_id, config_id, json.dumps(job_config_data)),
        schema=test_schema
    )

    job_id = execute_insert(
        "INSERT INTO irp_job (batch_id, job_configuration_id, status) VALUES (%s, %s, %s)",
        (batch_id, job_config_id, JobStatus.INITIATED),
        schema=test_schema
    )

    # Test reading job
    result = read_job(job_id, schema=test_schema)

    # Assertions
    assert result['id'] == job_id
    assert result['batch_id'] == batch_id
    assert result['job_configuration_id'] == job_config_id
    assert result['status'] == JobStatus.INITIATED


@pytest.mark.database
@pytest.mark.unit
def test_update_job_status(test_schema):
    """Test updating job status"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_update_status')

    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'test': 'data'},
        schema=test_schema
    )

    # Test updating status
    result = update_job_status(job_id, JobStatus.SUBMITTED, schema=test_schema)
    assert result == True, "Update should return True"

    # Verify the update
    job = read_job(job_id, schema=test_schema)
    assert job['status'] == JobStatus.SUBMITTED

    # Test updating to same status (should return False)
    result = update_job_status(job_id, JobStatus.SUBMITTED, schema=test_schema)
    assert result == False, "Update to same status should return False"


# ============================================================================
# Tests - Job Creation
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_create_job_with_new_config(test_schema):
    """Test creating job with new configuration"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_create_new')

    # Create job with new configuration
    job_config_data = {'param_a': 'value_a', 'param_b': 123}

    job_id = create_job_with_config(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data=job_config_data,
        schema=test_schema
    )

    # Verify job
    job = read_job(job_id, schema=test_schema)
    assert job['status'] == JobStatus.INITIATED

    # Verify job configuration
    job_config = get_job_config(job_id, schema=test_schema)
    assert job_config['job_configuration_data'] == job_config_data


@pytest.mark.database
@pytest.mark.integration
def test_create_job_with_existing_config(test_schema):
    """Test creating job with existing configuration"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_create_existing')

    # Create first job with new config
    job_config_data = {'param': 'value'}
    job1_id = create_job_with_config(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data=job_config_data,
        schema=test_schema
    )

    # Get the job config ID
    job1 = read_job(job1_id, schema=test_schema)
    job_config_id = job1['job_configuration_id']

    # Create second job reusing same config
    job2_id = create_job(
        batch_id=batch_id,
        job_configuration_id=job_config_id,
        schema=test_schema
    )

    # Verify both jobs use same config
    job2 = read_job(job2_id, schema=test_schema)
    assert job2['job_configuration_id'] == job_config_id


@pytest.mark.database
@pytest.mark.unit
def test_create_job_validation_neither_param(test_schema):
    """Test create job with CRUD function - requires job_configuration_id"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_validation_neither')

    # CRUD create_job() requires job_configuration_id parameter
    # Calling it without will cause TypeError (missing required argument)
    with pytest.raises(TypeError):
        create_job(batch_id=batch_id, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_create_job_validation_both_params(test_schema):
    """Test create_job_with_config - requires job_configuration_data"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_validation_both')

    # create_job_with_config() requires job_configuration_data parameter
    # Calling it without will cause TypeError (missing required argument)
    with pytest.raises(TypeError):
        create_job_with_config(
            batch_id=batch_id,
            configuration_id=config_id,
            schema=test_schema
        )


# ============================================================================
# Tests - Job Skip
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_skip_job(test_schema):
    """Test skipping a job"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_skip')

    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'test': 'data'},
        schema=test_schema
    )

    # Verify job is not skipped initially
    job = read_job(job_id, schema=test_schema)
    assert job['skipped'] == False

    # Skip job
    skip_job(job_id, schema=test_schema)

    # Verify job is now skipped
    job = read_job(job_id, schema=test_schema)
    assert job['skipped'] == True


# ============================================================================
# Tests - Job Submission
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_submit_job(test_schema, mock_irp_client):
    """Test submitting a job"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_submit')

    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'TestDB'},
        schema=test_schema
    )

    # Submit job
    result_id = submit_job(job_id, 'EDM Creation', mock_irp_client, schema=test_schema)
    assert result_id == job_id

    # Verify job is SUBMITTED
    job = read_job(job_id, schema=test_schema)
    assert job['status'] == JobStatus.SUBMITTED
    assert job['moodys_workflow_id'] is not None
    assert job['submitted_ts'] is not None

    # Verify submission request/response stored
    assert job['submission_request'] is not None
    assert job['submission_response'] is not None


@pytest.mark.database
@pytest.mark.integration
def test_submit_job_force_resubmit(test_schema, mock_irp_client):
    """Test force resubmitting a job"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_force_submit')

    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'TestDB'},
        schema=test_schema
    )

    # Submit job first time
    submit_job(job_id, 'EDM Creation', mock_irp_client, schema=test_schema)
    job1 = read_job(job_id, schema=test_schema)
    workflow_id_1 = job1['moodys_workflow_id']

    # Force resubmit
    submit_job(job_id, 'EDM Creation', mock_irp_client, force=True, schema=test_schema)
    job2 = read_job(job_id, schema=test_schema)
    workflow_id_2 = job2['moodys_workflow_id']

    # Workflow IDs should be different
    assert workflow_id_1 != workflow_id_2


# ============================================================================
# Tests - Job Status Tracking
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_track_job_status(test_schema, mock_irp_client):
    """Test tracking job status"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_track')

    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'TestDB'},
        schema=test_schema
    )

    # Submit job
    submit_job(job_id, 'EDM Creation', mock_irp_client, schema=test_schema)

    # Track job status multiple times
    status1 = track_job_status(job_id, mock_irp_client, schema=test_schema)
    status2 = track_job_status(job_id, mock_irp_client, schema=test_schema)
    status3 = track_job_status(job_id, mock_irp_client, schema=test_schema)

    # Verify tracking logs created
    df = execute_query(
        "SELECT COUNT(*) as count FROM irp_job_tracking_log WHERE job_id = %s",
        (job_id,),
        schema=test_schema
    )
    tracking_count = df.iloc[0]['count']
    assert tracking_count == 3

    # Status should be valid (any valid job status after submission)
    expected_statuses = [JobStatus.SUBMITTED, JobStatus.QUEUED, JobStatus.PENDING,
                        JobStatus.RUNNING, JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ERROR]
    assert status3 in expected_statuses, f"Expected status to be one of {expected_statuses}, but got: '{status3}'"


# ============================================================================
# Tests - Job Resubmission
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_resubmit_job_without_override(test_schema, mock_irp_client):
    """Test resubmitting job without configuration override"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_resubmit_no_override')

    # Create and submit original job
    original_job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'OriginalDB'},
        schema=test_schema
    )
    submit_job(original_job_id, 'EDM Creation', mock_irp_client, schema=test_schema)

    # Get original job config ID
    original_job = read_job(original_job_id, schema=test_schema)
    original_config_id = original_job['job_configuration_id']

    # Resubmit without override
    new_job_id = resubmit_job(original_job_id, mock_irp_client, 'EDM Creation', schema=test_schema)

    # Verify original job is skipped
    original_job = read_job(original_job_id, schema=test_schema)
    assert original_job['skipped'] == True

    # Verify new job uses same config
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['job_configuration_id'] == original_config_id

    # Verify parent-child relationship
    assert new_job['parent_job_id'] == original_job_id


@pytest.mark.database
@pytest.mark.integration
def test_resubmit_job_with_override(test_schema, mock_irp_client):
    """Test resubmitting job with configuration override"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_resubmit_override')

    # Create original job
    original_job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'OriginalDB'},
        schema=test_schema
    )

    # Get original config ID
    original_job = read_job(original_job_id, schema=test_schema)
    original_config_id = original_job['job_configuration_id']

    # Resubmit with override
    override_config = {'Database': 'OverriddenDB', 'new_param': 999}
    override_reason = "User requested parameter change"

    new_job_id = resubmit_job(
        original_job_id,
        mock_irp_client,
        'EDM Creation',
        job_configuration_data=override_config,
        override_reason=override_reason,
        schema=test_schema
    )

    # Verify original job is skipped
    original_job = read_job(original_job_id, schema=test_schema)
    assert original_job['skipped'] == True

    # Verify new job uses DIFFERENT config
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['job_configuration_id'] != original_config_id

    # Verify new config has override data
    new_config = get_job_config(new_job_id, schema=test_schema)
    assert new_config['job_configuration_data'] == override_config
    assert new_config['overridden'] == True
    assert new_config['override_reason_txt'] == override_reason

    # Verify parent-child relationship
    assert new_job['parent_job_id'] == original_job_id


# ============================================================================
# Tests - Error Handling
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_job_error_invalid_id(test_schema):
    """Test error handling for invalid job ID"""
    with pytest.raises(JobError):
        read_job(-1, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_job_error_not_found(test_schema):
    """Test error handling for job not found"""
    with pytest.raises(JobError):
        read_job(999999, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_job_error_invalid_status(test_schema):
    """Test error handling for invalid status"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_error')
    job_id = create_job_with_config(batch_id, config_id, job_configuration_data={'test': 'data'}, schema=test_schema)

    with pytest.raises(JobError):
        update_job_status(job_id, 'INVALID_STATUS', schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_job_error_track_without_submission(test_schema, mock_irp_client):
    """Test error handling for tracking unsubmitted job"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_track_error')
    job_id = create_job_with_config(batch_id, config_id, job_configuration_data={'test': 'data'}, schema=test_schema)

    with pytest.raises(JobError):
        track_job_status(job_id, mock_irp_client, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_job_error_resubmit_override_no_reason(test_schema, mock_irp_client):
    """Test error handling for resubmit with override but no reason"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_resubmit_error')
    job_id = create_job_with_config(batch_id, config_id, job_configuration_data={'test': 'data'}, schema=test_schema)

    with pytest.raises(JobError):
        resubmit_job(
            job_id,
            mock_irp_client,
            'default',
            job_configuration_data={'new': 'config'},
            schema=test_schema
        )


# ==============================================================================
# PHASE 3: VALIDATION ERROR PATH TESTS - Private Functions
# ==============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_create_job_configuration_invalid_batch_id(test_schema):
    """Test create_job_configuration() with invalid batch_id"""
    from helpers.job import create_job_configuration

    # Test with batch_id = 0
    with pytest.raises(JobError) as exc_info:
        create_job_configuration(
            batch_id=0,
            configuration_id=1,
            job_configuration_data={'test': 'data'},
            schema=test_schema
        )
    assert 'invalid batch_id' in str(exc_info.value).lower()

    # Test with negative batch_id
    with pytest.raises(JobError):
        create_job_configuration(
            batch_id=-1,
            configuration_id=1,
            job_configuration_data={'test': 'data'},
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_create_job_configuration_invalid_configuration_id(test_schema):
    """Test create_job_configuration() with invalid configuration_id"""
    from helpers.job import create_job_configuration

    with pytest.raises(JobError) as exc_info:
        create_job_configuration(
            batch_id=1,
            configuration_id=0,
            job_configuration_data={'test': 'data'},
            schema=test_schema
        )
    assert 'invalid configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_create_job_configuration_invalid_data_type(test_schema):
    """Test create_job_configuration() with invalid data type"""
    from helpers.job import create_job_configuration

    with pytest.raises(JobError) as exc_info:
        create_job_configuration(
            batch_id=1,
            configuration_id=1,
            job_configuration_data="not a dict",  # Should be dict
            schema=test_schema
        )
    assert 'must be a dictionary' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_create_job_invalid_batch_id_private(test_schema):
    """Test create_job() with invalid batch_id"""
    from helpers.job import create_job

    # Test with batch_id = 0
    with pytest.raises(JobError):
        create_job(batch_id=0, job_configuration_id=1, schema=test_schema)

    # Test with negative batch_id
    with pytest.raises(JobError):
        create_job(batch_id=-1, job_configuration_id=1, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_create_job_invalid_job_config_id_private(test_schema):
    """Test create_job() with invalid job_configuration_id"""
    from helpers.job import create_job

    with pytest.raises(JobError):
        create_job(batch_id=1, job_configuration_id=0, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_create_job_invalid_parent_job_id(test_schema):
    """Test create_job() with invalid parent_job_id"""
    from helpers.job import create_job

    with pytest.raises(JobError):
        create_job(
            batch_id=1,
            job_configuration_id=1,
            parent_job_id=0,
            schema=test_schema
        )


# ==============================================================================
# PHASE 4: SUBMISSION AND TRACKING ERROR PATH TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_register_job_submission_invalid_job_id(test_schema):
    """Test _register_job_submission() with invalid job_id"""
    from helpers.job import _register_job_submission
    from datetime import datetime

    with pytest.raises(JobError) as exc_info:
        _register_job_submission(
            job_id=-1,
            workflow_id='WF-123',
            request={'test': 'request'},
            response={'test': 'response'},
            submitted_ts=datetime.now(),
            schema=test_schema
        )
    assert 'invalid job_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_register_job_submission_invalid_workflow_id(test_schema):
    """Test _register_job_submission() with invalid workflow_id"""
    from helpers.job import _register_job_submission
    from datetime import datetime

    # Test with None workflow_id
    with pytest.raises(JobError) as exc_info:
        _register_job_submission(
            job_id=1,
            workflow_id=None,
            request={'test': 'request'},
            response={'test': 'response'},
            submitted_ts=datetime.now(),
            schema=test_schema
        )
    assert 'invalid workflow_id' in str(exc_info.value).lower()

    # Test with empty string workflow_id
    with pytest.raises(JobError):
        _register_job_submission(
            job_id=1,
            workflow_id='',
            request={'test': 'request'},
            response={'test': 'response'},
            submitted_ts=datetime.now(),
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_insert_tracking_log_invalid_job_id(test_schema):
    """Test _insert_tracking_log() with invalid job_id"""
    from helpers.job import _insert_tracking_log

    with pytest.raises(JobError) as exc_info:
        _insert_tracking_log(
            job_id=-1,
            workflow_id='WF-123',
            job_status=JobStatus.RUNNING,
            tracking_data={'status': 'running'},
            schema=test_schema
        )
    assert 'invalid job_id' in str(exc_info.value).lower()


# ==============================================================================
# PHASE 5: CRUD AND BUSINESS LOGIC ERROR PATH TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_read_job_json_parsing_submission_request(test_schema, mock_irp_client):
    """Test read_job() parses JSON submission_request field"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_json_parse')

    # Create and submit job
    job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'TestDB'},
        schema=test_schema
    )
    submit_job(job_id, 'EDM Creation', mock_irp_client, schema=test_schema)

    # Read job - submission_request should be parsed from JSON to dict
    job = read_job(job_id, schema=test_schema)

    # Verify JSON was parsed
    assert isinstance(job['submission_request'], dict)
    assert 'job_id' in job['submission_request']
    assert isinstance(job['submission_response'], dict)


@pytest.mark.database
@pytest.mark.unit
def test_get_job_config_invalid_job_id(test_schema):
    """Test get_job_config() with invalid job_id"""
    with pytest.raises(JobError) as exc_info:
        get_job_config(job_id=-1, schema=test_schema)
    assert 'invalid job_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.integration
def test_create_job_invalid_batch_id_validation(test_schema):
    """Test create_job_with_config() validates batch_id"""
    # Test with batch_id = 0
    with pytest.raises(JobError) as exc_info:
        create_job_with_config(
            batch_id=0,
            configuration_id=1,
            job_configuration_data={'test': 'data'},
            schema=test_schema
        )
    assert 'invalid batch_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.integration
def test_create_job_invalid_configuration_id(test_schema):
    """Test create_job_with_config() validates configuration_id"""
    with pytest.raises(JobError) as exc_info:
        create_job_with_config(
            batch_id=1,
            configuration_id=-1,
            job_configuration_data={'test': 'data'},
            schema=test_schema
        )
    assert 'invalid configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.integration
def test_create_job_invalid_existing_config_id(test_schema):
    """Test create_job() CRUD function validates job_configuration_id"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_invalid_config_id')

    with pytest.raises(JobError) as exc_info:
        create_job(
            batch_id=batch_id,
            job_configuration_id=-1,  # Invalid
            schema=test_schema
        )
    assert 'invalid job_configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_invalid_id(test_schema):
    """Test skip_job() with invalid job_id"""
    with pytest.raises(JobError) as exc_info:
        skip_job(job_id=-1, schema=test_schema)
    assert 'invalid job_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_not_found(test_schema):
    """Test skip_job() with non-existent job"""
    with pytest.raises(JobError):
        skip_job(job_id=999999, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_submit_job_invalid_id(test_schema, mock_irp_client):
    """Test submit_job() with invalid job_id"""
    with pytest.raises(JobError) as exc_info:
        submit_job(job_id=-1, batch_type='EDM Creation', irp_client=mock_irp_client, schema=test_schema)
    assert 'invalid job_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_track_job_status_invalid_id(test_schema, mock_irp_client):
    """Test track_job_status() with invalid job_id"""
    with pytest.raises(JobError) as exc_info:
        track_job_status(job_id=-1, irp_client=mock_irp_client, schema=test_schema)
    assert 'invalid job_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_resubmit_job_invalid_id(test_schema, mock_irp_client):
    """Test resubmit_job() with invalid job_id"""
    with pytest.raises(JobError) as exc_info:
        resubmit_job(job_id=-1, irp_client=mock_irp_client, batch_type='EDM Creation', schema=test_schema)
    assert 'invalid job_id' in str(exc_info.value).lower()


# ==============================================================================
# JOB CONFIGURATION SKIP TESTS (NEW FEATURE)
# ==============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_standalone(test_schema):
    """Test skip_job_configuration() standalone (manual skip)"""
    from helpers.job import skip_job_configuration, create_job_configuration

    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_skip_config_standalone')

    # Create job configuration manually
    job_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'test': 'data'},
        schema=test_schema
    )

    # Skip the configuration
    skip_reason = "Manual skip for testing purposes"
    skip_job_configuration(
        job_configuration_id=job_config_id,
        skipped_reason_txt=skip_reason,
        schema=test_schema
    )

    # Verify configuration is marked as skipped
    df = execute_query(
        """SELECT skipped, skipped_reason_txt, override_job_configuration_id
           FROM irp_job_configuration WHERE id = %s""",
        (job_config_id,),
        schema=test_schema
    )

    assert df.iloc[0]['skipped'] == True
    assert df.iloc[0]['skipped_reason_txt'] == skip_reason
    assert df.iloc[0]['override_job_configuration_id'] is None


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_with_override_id(test_schema):
    """Test skip_job_configuration() with override_job_configuration_id"""
    from helpers.job import skip_job_configuration, create_job_configuration

    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_skip_with_override')

    # Create original configuration
    original_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'original': 'data'},
        schema=test_schema
    )

    # Create override configuration
    override_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'override': 'data'},
        schema=test_schema
    )

    # Skip original configuration pointing to override
    skip_reason = "Configuration overridden by user"
    skip_job_configuration(
        job_configuration_id=original_config_id,
        skipped_reason_txt=skip_reason,
        override_job_configuration_id=override_config_id,
        schema=test_schema
    )

    # Verify
    df = execute_query(
        """SELECT skipped, skipped_reason_txt, override_job_configuration_id
           FROM irp_job_configuration WHERE id = %s""",
        (original_config_id,),
        schema=test_schema
    )

    assert df.iloc[0]['skipped'] == True
    assert df.iloc[0]['skipped_reason_txt'] == skip_reason
    assert df.iloc[0]['override_job_configuration_id'] == override_config_id


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_invalid_id(test_schema):
    """Test skip_job_configuration() with invalid job_configuration_id"""
    from helpers.job import skip_job_configuration

    with pytest.raises(JobError) as exc_info:
        skip_job_configuration(
            job_configuration_id=-1,
            skipped_reason_txt="Test reason",
            schema=test_schema
        )
    assert 'invalid job_configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_empty_reason(test_schema):
    """Test skip_job_configuration() with empty skipped_reason_txt"""
    from helpers.job import skip_job_configuration

    # Test with empty string
    with pytest.raises(JobError) as exc_info:
        skip_job_configuration(
            job_configuration_id=1,
            skipped_reason_txt="",
            schema=test_schema
        )
    assert 'skipped_reason_txt' in str(exc_info.value).lower()

    # Test with whitespace only
    with pytest.raises(JobError):
        skip_job_configuration(
            job_configuration_id=1,
            skipped_reason_txt="   ",
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_invalid_override_id(test_schema):
    """Test skip_job_configuration() with invalid override_job_configuration_id"""
    from helpers.job import skip_job_configuration

    with pytest.raises(JobError) as exc_info:
        skip_job_configuration(
            job_configuration_id=1,
            skipped_reason_txt="Test reason",
            override_job_configuration_id=0,  # Invalid
            schema=test_schema
        )
    assert 'invalid override_job_configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.unit
def test_skip_job_configuration_not_found(test_schema):
    """Test skip_job_configuration() with non-existent configuration"""
    from helpers.job import skip_job_configuration

    with pytest.raises(JobError) as exc_info:
        skip_job_configuration(
            job_configuration_id=999999,
            skipped_reason_txt="Test reason",
            schema=test_schema
        )
    assert 'not found' in str(exc_info.value).lower()


# ==============================================================================
# JOB CONFIGURATION PARENT-CHILD RELATIONSHIP TESTS (NEW FEATURE)
# ==============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_resubmit_job_with_override_config_relationships(test_schema, mock_irp_client):
    """Test resubmit_job with override creates proper parent-child relationships"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_config_relationships')

    # Create original job
    original_job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'OriginalDB'},
        schema=test_schema
    )

    # Get original config ID
    original_job = read_job(original_job_id, schema=test_schema)
    original_config_id = original_job['job_configuration_id']

    # Resubmit with override
    override_config = {'Database': 'OverriddenDB'}
    override_reason = "Testing parent-child relationship"

    new_job_id = resubmit_job(
        original_job_id,
        mock_irp_client,
        'EDM Creation',
        job_configuration_data=override_config,
        override_reason=override_reason,
        schema=test_schema
    )

    # Get new config ID
    new_job = read_job(new_job_id, schema=test_schema)
    new_config_id = new_job['job_configuration_id']

    # Verify NEW configuration has parent_job_configuration_id set
    new_config = get_job_config(new_job_id, schema=test_schema)
    assert new_config['parent_job_configuration_id'] == original_config_id
    assert new_config['overridden'] == True
    assert new_config['override_reason_txt'] == override_reason

    # Verify ORIGINAL configuration is marked as skipped
    df = execute_query(
        """SELECT skipped, skipped_reason_txt, override_job_configuration_id
           FROM irp_job_configuration WHERE id = %s""",
        (original_config_id,),
        schema=test_schema
    )

    assert df.iloc[0]['skipped'] == True
    assert df.iloc[0]['skipped_reason_txt'] == override_reason
    assert df.iloc[0]['override_job_configuration_id'] == new_config_id

    # Verify bidirectional linkage
    # Parent -> Child: original_config.override_job_configuration_id points to new_config
    # Child -> Parent: new_config.parent_job_configuration_id points to original_config
    assert df.iloc[0]['override_job_configuration_id'] == new_config_id
    assert new_config['parent_job_configuration_id'] == original_config_id


@pytest.mark.database
@pytest.mark.integration
def test_resubmit_job_without_override_no_config_skip(test_schema, mock_irp_client):
    """Test resubmit_job without override does NOT skip original configuration"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_no_override_no_skip')

    # Create original job
    original_job_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'OriginalDB'},
        schema=test_schema
    )

    # Get original config ID
    original_job = read_job(original_job_id, schema=test_schema)
    original_config_id = original_job['job_configuration_id']

    # Resubmit WITHOUT override
    new_job_id = resubmit_job(original_job_id, mock_irp_client, 'EDM Creation', schema=test_schema)

    # Verify ORIGINAL configuration is NOT marked as skipped
    # (because we're reusing the same configuration)
    df = execute_query(
        """SELECT skipped, skipped_reason_txt, override_job_configuration_id
           FROM irp_job_configuration WHERE id = %s""",
        (original_config_id,),
        schema=test_schema
    )

    assert df.iloc[0]['skipped'] == False
    assert df.iloc[0]['skipped_reason_txt'] is None
    assert df.iloc[0]['override_job_configuration_id'] is None

    # Verify new job uses SAME configuration
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['job_configuration_id'] == original_config_id


@pytest.mark.database
@pytest.mark.integration
def test_create_job_configuration_with_parent(test_schema):
    """Test create_job_configuration() with parent_job_configuration_id"""
    from helpers.job import create_job_configuration

    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_create_with_parent')

    # Create parent configuration
    parent_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'parent': 'config'},
        schema=test_schema
    )

    # Create child configuration with parent reference
    child_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'child': 'config'},
        parent_job_configuration_id=parent_config_id,
        schema=test_schema
    )

    # Verify parent-child relationship
    df = execute_query(
        """SELECT parent_job_configuration_id FROM irp_job_configuration WHERE id = %s""",
        (child_config_id,),
        schema=test_schema
    )

    assert df.iloc[0]['parent_job_configuration_id'] == parent_config_id


@pytest.mark.database
@pytest.mark.unit
def test_create_job_configuration_invalid_parent_id(test_schema):
    """Test create_job_configuration() with invalid parent_job_configuration_id"""
    from helpers.job import create_job_configuration

    with pytest.raises(JobError) as exc_info:
        create_job_configuration(
            batch_id=1,
            configuration_id=1,
            job_configuration_data={'test': 'data'},
            parent_job_configuration_id=0,  # Invalid
            schema=test_schema
        )
    assert 'invalid parent_job_configuration_id' in str(exc_info.value).lower()


@pytest.mark.database
@pytest.mark.integration
def test_get_job_config_returns_new_fields(test_schema):
    """Test get_job_config() returns new fields (parent_job_configuration_id, etc.)"""
    from helpers.job import create_job_configuration

    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_get_config_fields')

    # Create configuration with parent
    parent_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'parent': 'config'},
        schema=test_schema
    )

    child_config_id = create_job_configuration(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'child': 'config'},
        parent_job_configuration_id=parent_config_id,
        overridden=True,
        override_reason_txt="Test override",
        schema=test_schema
    )

    # Create job with child configuration
    job_id = create_job(
        batch_id=batch_id,
        job_configuration_id=child_config_id,
        schema=test_schema
    )

    # Get job config
    job_config = get_job_config(job_id, schema=test_schema)

    # Verify all new fields are present
    assert 'parent_job_configuration_id' in job_config
    assert 'skipped_reason_txt' in job_config
    assert 'override_job_configuration_id' in job_config
    assert job_config['parent_job_configuration_id'] == parent_config_id


@pytest.mark.database
@pytest.mark.integration
def test_chained_resubmissions_with_override(test_schema, mock_irp_client):
    """Test multiple chained resubmissions create proper parent-child chain"""
    cycle_id, stage_id, step_id, config_id, batch_id = create_test_hierarchy(test_schema, 'test_chained_resubmit')

    # Create original job
    job1_id = create_job_with_config(
        batch_id, config_id,
        job_configuration_data={'Database': 'DB_v1'},
        schema=test_schema
    )
    config1_id = read_job(job1_id, schema=test_schema)['job_configuration_id']

    # First resubmission
    job2_id = resubmit_job(
        job1_id,
        mock_irp_client,
        'EDM Creation',
        job_configuration_data={'Database': 'DB_v2'},
        override_reason="First override",
        schema=test_schema
    )
    config2_id = read_job(job2_id, schema=test_schema)['job_configuration_id']

    # Second resubmission
    job3_id = resubmit_job(
        job2_id,
        mock_irp_client,
        'EDM Creation',
        job_configuration_data={'Database': 'DB_v3'},
        override_reason="Second override",
        schema=test_schema
    )
    config3_id = read_job(job3_id, schema=test_schema)['job_configuration_id']

    # Verify chain: config1 -> config2 -> config3
    config2 = get_job_config(job2_id, schema=test_schema)
    config3 = get_job_config(job3_id, schema=test_schema)

    assert config2['parent_job_configuration_id'] == config1_id
    assert config3['parent_job_configuration_id'] == config2_id

    # Verify config1 and config2 are both marked as skipped
    df = execute_query(
        """SELECT id, skipped, override_job_configuration_id FROM irp_job_configuration WHERE id IN (%s, %s)""",
        (config1_id, config2_id),
        schema=test_schema
    )

    for row in df.itertuples():
        assert row.skipped == True
        assert row.override_job_configuration_id is not None
