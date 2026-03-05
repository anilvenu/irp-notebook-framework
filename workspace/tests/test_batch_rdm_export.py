"""
Test suite for RDM export batch submission idempotency.

Tests _submit_rdm_export_batch_with_seed behavior across:
- First-time submission (all INITIATED, RDM does not exist)
- Re-run with RDM existing + FAILED jobs (resubmit failed only)
- Re-run with RDM missing after deletion (full recreate)

All tests run in the 'test_batch_rdm_export' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_batch_rdm_export.py -v
"""

import pytest
import json
from unittest.mock import MagicMock
from datetime import datetime

from helpers.database import execute_query, execute_insert, execute_command
from helpers.batch import (
    _submit_rdm_export_batch_with_seed,
    get_batch_jobs,
    read_batch,
    BatchError
)
from helpers.constants import (
    BatchStatus, JobStatus, ConfigurationStatus, BatchType, DEFAULT_DATABASE_SERVER
)
from helpers.irp_integration.exceptions import IRPAPIError


# ============================================================================
# Helper Functions
# ============================================================================

def create_rdm_export_hierarchy(test_schema, cycle_name, num_remaining=3):
    """
    Create a complete RDM export batch with seed + remaining jobs.

    Returns:
        dict with keys: cycle_id, stage_id, step_id, config_id, batch_id,
                        seed_job_id, seed_jc_id, remaining_ids, remaining_jc_ids
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
        (cycle_id, 6, 'Data_Export'),
        schema=test_schema
    )

    # Create step
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'Export_to_RDM'),
        schema=test_schema
    )

    # Create configuration
    config_data = {'Metadata': {'cycle': cycle_name}}
    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    # Create batch
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, BatchType.EXPORT_TO_RDM, BatchStatus.INITIATED),
        schema=test_schema
    )

    # Create seed job config + job
    seed_config_data = {
        'is_seed_job': True,
        'rdm_name': 'TestRDM',
        'server_name': 'databridge-1',
        'analysis_names': ['Analysis-Seed'],
        'database_id': None,
        'is_group': False,
        'edm_name': 'TestEDM'
    }
    seed_jc_id = execute_insert(
        """INSERT INTO irp_job_configuration
           (batch_id, configuration_id, job_configuration_data)
           VALUES (%s, %s, %s)""",
        (batch_id, config_id, json.dumps(seed_config_data)),
        schema=test_schema
    )
    seed_job_id = execute_insert(
        "INSERT INTO irp_job (batch_id, job_configuration_id, status) VALUES (%s, %s, %s)",
        (batch_id, seed_jc_id, JobStatus.INITIATED),
        schema=test_schema
    )

    # Create remaining job configs + jobs
    remaining_ids = []
    remaining_jc_ids = []
    for i in range(num_remaining):
        remaining_config_data = {
            'is_seed_job': False,
            'rdm_name': 'TestRDM',
            'server_name': 'databridge-1',
            'analysis_names': [f'Analysis-{i + 1}'],
            'database_id': None,
            'is_group': False,
            'edm_name': 'TestEDM'
        }
        jc_id = execute_insert(
            """INSERT INTO irp_job_configuration
               (batch_id, configuration_id, job_configuration_data)
               VALUES (%s, %s, %s)""",
            (batch_id, config_id, json.dumps(remaining_config_data)),
            schema=test_schema
        )
        job_id = execute_insert(
            "INSERT INTO irp_job (batch_id, job_configuration_id, status) VALUES (%s, %s, %s)",
            (batch_id, jc_id, JobStatus.INITIATED),
            schema=test_schema
        )
        remaining_ids.append(job_id)
        remaining_jc_ids.append(jc_id)

    return {
        'cycle_id': cycle_id,
        'stage_id': stage_id,
        'step_id': step_id,
        'config_id': config_id,
        'batch_id': batch_id,
        'seed_job_id': seed_job_id,
        'seed_jc_id': seed_jc_id,
        'remaining_ids': remaining_ids,
        'remaining_jc_ids': remaining_jc_ids
    }


def set_job_status(job_id, status, moodys_workflow_id, schema):
    """Set job status and moodys_workflow_id in DB."""
    execute_command(
        "UPDATE irp_job SET status = %s, moodys_workflow_id = %s WHERE id = %s",
        (status, moodys_workflow_id, job_id),
        schema=schema
    )


def set_job_config_database_id(jc_id, database_id, schema):
    """Update database_id in a job configuration's JSON data."""
    from helpers.job import update_job_configuration_data
    update_job_configuration_data(jc_id, {'database_id': database_id}, schema=schema)


# ============================================================================
# Tests
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_first_time_submission(test_schema, mocker):
    """First-time submission: all jobs INITIATED, RDM does not exist.

    Verifies:
    - Seed job is submitted (not resubmitted)
    - Seed job is polled to completion
    - database_id is retrieved from newly created RDM
    - Remaining jobs are submitted with database_id
    """
    h = create_rdm_export_hierarchy(test_schema, 'test_first_time', num_remaining=2)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    # Mock IRPClient
    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.side_effect = [
        IRPAPIError("Not found"),  # Initial check: RDM doesn't exist
        42                          # After seed creates it
    ]
    mock_client.rdm.poll_rdm_export_job_to_completion.return_value = {'status': 'FINISHED'}

    # Mock job module functions that make API calls
    import helpers.job as job_module

    def mock_submit_job(job_id, batch_type, irp_client, force=False, track_immediately=False, schema='public'):
        """Mock submit: sets moodys_workflow_id in DB."""
        execute_command(
            "UPDATE irp_job SET status = %s, moodys_workflow_id = %s WHERE id = %s",
            (JobStatus.SUBMITTED, str(1000 + job_id), job_id),
            schema=schema
        )
        return job_id

    mocker.patch.object(job_module, 'submit_job', side_effect=mock_submit_job)

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    # Verify result
    assert result['batch_status'] == BatchStatus.ACTIVE
    assert result['database_id'] == 42

    # Seed should be submitted (not resubmitted)
    seed_entries = [j for j in result['jobs'] if j.get('is_seed')]
    assert len(seed_entries) == 1
    assert seed_entries[0]['status'] == 'SUBMITTED'
    assert seed_entries[0]['job_id'] == h['seed_job_id']

    # All remaining should be submitted
    remaining_entries = [j for j in result['jobs'] if not j.get('is_seed')]
    assert len(remaining_entries) == 2
    assert all(j['status'] == 'SUBMITTED' for j in remaining_entries)

    # submit_job should have been called 3 times (1 seed + 2 remaining)
    assert job_module.submit_job.call_count == 3

    # Verify remaining jobs had database_id updated in their configs
    for jc_id in h['remaining_jc_ids']:
        config = execute_query(
            "SELECT job_configuration_data FROM irp_job_configuration WHERE id = %s",
            (jc_id,), schema=test_schema
        )
        config_data = config.iloc[0]['job_configuration_data']
        assert config_data['database_id'] == 42

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_rerun_rdm_exists_resubmit_failed(test_schema, mocker):
    """Re-run: RDM exists, some remaining jobs FAILED.

    Should skip seed job, resubmit only FAILED jobs, leave FINISHED jobs alone.
    """
    h = create_rdm_export_hierarchy(test_schema, 'test_rerun_fail', num_remaining=3)

    # Simulate previous run: seed FINISHED, remaining[0] FINISHED, remaining[1] FAILED, remaining[2] FINISHED
    set_job_status(h['seed_job_id'], JobStatus.FINISHED, '1001', test_schema)
    set_job_status(h['remaining_ids'][0], JobStatus.FINISHED, '1002', test_schema)
    set_job_status(h['remaining_ids'][1], JobStatus.FAILED, '1003', test_schema)
    set_job_status(h['remaining_ids'][2], JobStatus.FINISHED, '1004', test_schema)

    # Update remaining job configs with database_id from first run
    for jc_id in h['remaining_jc_ids']:
        set_job_config_database_id(jc_id, 42, test_schema)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    # Mock IRPClient - RDM exists
    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.return_value = 42

    # Mock resubmit_job
    import helpers.job as job_module
    mocker.patch.object(job_module, 'resubmit_job', return_value=999)

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    # Verify result
    assert result['database_id'] == 42
    assert result['batch_status'] == BatchStatus.ACTIVE

    # Seed should be skipped (RDM already exists)
    seed_entries = [j for j in result['jobs'] if j.get('is_seed')]
    assert len(seed_entries) == 1
    assert seed_entries[0]['status'] == 'SKIPPED_RDM_EXISTS'

    # Only the FAILED job (remaining[1]) should be resubmitted
    resubmitted = [j for j in result['jobs'] if j.get('status') == 'RESUBMITTED']
    assert len(resubmitted) == 1
    assert resubmitted[0]['original_job_id'] == h['remaining_ids'][1]
    assert resubmitted[0]['job_id'] == 999

    # resubmit_job should be called once, without config override (reuses existing config)
    job_module.resubmit_job.assert_called_once_with(
        h['remaining_ids'][1],
        mock_client,
        BatchType.EXPORT_TO_RDM,
        schema=test_schema
    )

    # FINISHED jobs should NOT appear as resubmitted
    assert len(result['jobs']) == 2  # 1 seed (skipped) + 1 resubmitted

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_rerun_rdm_exists_no_failures(test_schema, mocker):
    """Re-run: RDM exists, all jobs FINISHED. Nothing to do except update batch status."""
    h = create_rdm_export_hierarchy(test_schema, 'test_rerun_ok', num_remaining=2)

    # Simulate previous run: all FINISHED
    set_job_status(h['seed_job_id'], JobStatus.FINISHED, '1001', test_schema)
    set_job_status(h['remaining_ids'][0], JobStatus.FINISHED, '1002', test_schema)
    set_job_status(h['remaining_ids'][1], JobStatus.FINISHED, '1003', test_schema)

    for jc_id in h['remaining_jc_ids']:
        set_job_config_database_id(jc_id, 42, test_schema)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    # Mock IRPClient - RDM exists
    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.return_value = 42

    import helpers.job as job_module
    mocker.patch.object(job_module, 'resubmit_job')
    mocker.patch.object(job_module, 'submit_job')

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    # Seed skipped, no resubmissions
    assert result['database_id'] == 42
    seed_entries = [j for j in result['jobs'] if j.get('is_seed')]
    assert seed_entries[0]['status'] == 'SKIPPED_RDM_EXISTS'

    # No jobs should be resubmitted or submitted
    job_module.resubmit_job.assert_not_called()
    job_module.submit_job.assert_not_called()

    # Only the seed entry (skipped) should be in results
    assert len(result['jobs']) == 1

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_rerun_rdm_missing_full_recreate(test_schema, mocker):
    """Re-run: RDM was deleted. Seed and all remaining must be resubmitted.

    Verifies:
    - Seed job is resubmitted (not submitted directly, since it was previously FINISHED)
    - Seed is polled to completion
    - New database_id is retrieved
    - ALL remaining jobs are resubmitted with new database_id in config
    """
    h = create_rdm_export_hierarchy(test_schema, 'test_recreate', num_remaining=2)

    # Simulate previous run: all FINISHED
    set_job_status(h['seed_job_id'], JobStatus.FINISHED, '1001', test_schema)
    set_job_status(h['remaining_ids'][0], JobStatus.FINISHED, '1002', test_schema)
    set_job_status(h['remaining_ids'][1], JobStatus.FINISHED, '1003', test_schema)

    # Set old database_id in remaining job configs
    for jc_id in h['remaining_jc_ids']:
        set_job_config_database_id(jc_id, 42, test_schema)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    # Mock IRPClient - RDM does NOT exist
    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.side_effect = [
        IRPAPIError("Not found"),  # Initial check
        99                          # After seed recreates it
    ]
    mock_client.rdm.poll_rdm_export_job_to_completion.return_value = {'status': 'FINISHED'}

    import helpers.job as job_module

    # Track resubmit calls and return incrementing IDs
    resubmit_counter = [0]

    def mock_resubmit(job_id, irp_client, batch_type, job_configuration_data=None,
                      override_reason=None, schema='public'):
        resubmit_counter[0] += 1
        return 900 + resubmit_counter[0]

    mocker.patch.object(job_module, 'resubmit_job', side_effect=mock_resubmit)

    # Mock read_job for the new seed job (returned by resubmit_job)
    real_read_job = job_module.read_job

    def mock_read_job(job_id, schema='public'):
        if job_id == 901:  # New seed job from resubmit
            return {
                'id': 901,
                'batch_id': h['batch_id'],
                'job_configuration_id': h['seed_jc_id'],
                'status': JobStatus.SUBMITTED,
                'moodys_workflow_id': '5001',
                'skipped': False
            }
        return real_read_job(job_id, schema=schema)

    mocker.patch.object(job_module, 'read_job', side_effect=mock_read_job)
    mocker.patch.object(job_module, 'update_job_status')

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    # Verify result
    assert result['database_id'] == 99
    assert result['batch_status'] == BatchStatus.ACTIVE

    # Seed should be resubmitted (was FINISHED, not INITIATED)
    seed_entries = [j for j in result['jobs'] if j.get('is_seed')]
    assert len(seed_entries) == 1
    assert seed_entries[0]['status'] == 'RESUBMITTED'
    assert seed_entries[0]['original_job_id'] == h['seed_job_id']
    assert seed_entries[0]['job_id'] == 901

    # ALL remaining should be resubmitted (RDM was recreated)
    remaining_entries = [j for j in result['jobs'] if not j.get('is_seed')]
    assert len(remaining_entries) == 2
    assert all(j['status'] == 'RESUBMITTED' for j in remaining_entries)

    # resubmit_job should be called 3 times (1 seed + 2 remaining)
    assert job_module.resubmit_job.call_count == 3

    # Verify remaining jobs were resubmitted with new database_id (99) in config
    remaining_resubmit_calls = job_module.resubmit_job.call_args_list[1:]  # Skip seed call
    for call in remaining_resubmit_calls:
        kwargs = call.kwargs if call.kwargs else {}
        # Check that job_configuration_data was passed with new database_id
        config_data = kwargs.get('job_configuration_data')
        assert config_data is not None, "Remaining jobs should be resubmitted with config override"
        assert config_data['database_id'] == 99, "database_id should be the new RDM's ID"
        assert kwargs.get('override_reason') is not None

    # Seed resubmit should NOT have config override (uses existing config with database_id=None)
    seed_call = job_module.resubmit_job.call_args_list[0]
    seed_kwargs = seed_call.kwargs if seed_call.kwargs else {}
    assert seed_kwargs.get('job_configuration_data') is None

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_rerun_rdm_missing_mixed_statuses(test_schema, mocker):
    """Re-run: RDM was deleted, mix of FINISHED and FAILED remaining jobs.

    All non-skipped remaining jobs should be resubmitted regardless of status.
    """
    h = create_rdm_export_hierarchy(test_schema, 'test_mixed', num_remaining=3)

    # Simulate previous run: seed FINISHED, remaining mixed
    set_job_status(h['seed_job_id'], JobStatus.FINISHED, '1001', test_schema)
    set_job_status(h['remaining_ids'][0], JobStatus.FINISHED, '1002', test_schema)
    set_job_status(h['remaining_ids'][1], JobStatus.FAILED, '1003', test_schema)
    set_job_status(h['remaining_ids'][2], JobStatus.FINISHED, '1004', test_schema)

    for jc_id in h['remaining_jc_ids']:
        set_job_config_database_id(jc_id, 42, test_schema)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    # Mock IRPClient - RDM does NOT exist
    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.side_effect = [
        IRPAPIError("Not found"),
        99
    ]
    mock_client.rdm.poll_rdm_export_job_to_completion.return_value = {'status': 'FINISHED'}

    import helpers.job as job_module

    resubmit_counter = [0]

    def mock_resubmit(job_id, irp_client, batch_type, job_configuration_data=None,
                      override_reason=None, schema='public'):
        resubmit_counter[0] += 1
        return 900 + resubmit_counter[0]

    mocker.patch.object(job_module, 'resubmit_job', side_effect=mock_resubmit)

    # Mock read_job only for fake resubmitted IDs; fall back to real DB for real jobs
    real_read_job = job_module.read_job

    def mock_read_job(job_id, schema='public'):
        if job_id == 901:  # New seed job from resubmit
            return {
                'id': 901,
                'batch_id': h['batch_id'],
                'job_configuration_id': h['seed_jc_id'],
                'status': JobStatus.SUBMITTED,
                'moodys_workflow_id': '5001',
                'skipped': False
            }
        return real_read_job(job_id, schema=schema)

    mocker.patch.object(job_module, 'read_job', side_effect=mock_read_job)
    mocker.patch.object(job_module, 'update_job_status')

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    assert result['database_id'] == 99

    # Seed resubmitted
    seed_entries = [j for j in result['jobs'] if j.get('is_seed')]
    assert seed_entries[0]['status'] == 'RESUBMITTED'

    # ALL 3 remaining should be resubmitted (RDM recreated = everything needs re-export)
    remaining_entries = [j for j in result['jobs'] if not j.get('is_seed')]
    assert len(remaining_entries) == 3
    assert all(j['status'] == 'RESUBMITTED' for j in remaining_entries)

    # Total resubmit calls: 1 seed + 3 remaining = 4
    assert job_module.resubmit_job.call_count == 4

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_no_seed_job_raises_error(test_schema):
    """Batch with no seed job should raise BatchError."""
    h = create_rdm_export_hierarchy(test_schema, 'test_no_seed', num_remaining=2)

    # Remove the is_seed_job flag from the seed job's config
    execute_command(
        """UPDATE irp_job_configuration
           SET job_configuration_data = job_configuration_data::jsonb || '{"is_seed_job": false}'::jsonb
           WHERE id = %s""",
        (h['seed_jc_id'],),
        schema=test_schema
    )

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    import helpers.job as job_module
    mock_client = MagicMock()

    with pytest.raises(BatchError, match="must have a seed job"):
        _submit_rdm_export_batch_with_seed(
            batch_id=h['batch_id'],
            batch=batch,
            jobs=jobs,
            irp_client=mock_client,
            job_module=job_module,
            schema=test_schema
        )

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )


@pytest.mark.database
@pytest.mark.integration
def test_rerun_rdm_exists_with_error_jobs(test_schema, mocker):
    """Re-run: RDM exists, some jobs in ERROR status (submission failures).

    ERROR jobs should also be resubmitted (they are in ready_for_submit).
    """
    h = create_rdm_export_hierarchy(test_schema, 'test_error_jobs', num_remaining=2)

    # Simulate: seed FINISHED, remaining[0] FINISHED, remaining[1] ERROR
    set_job_status(h['seed_job_id'], JobStatus.FINISHED, '1001', test_schema)
    set_job_status(h['remaining_ids'][0], JobStatus.FINISHED, '1002', test_schema)
    set_job_status(h['remaining_ids'][1], JobStatus.ERROR, None, test_schema)

    for jc_id in h['remaining_jc_ids']:
        set_job_config_database_id(jc_id, 42, test_schema)

    batch = read_batch(h['batch_id'], schema=test_schema)
    jobs = get_batch_jobs(h['batch_id'], schema=test_schema)

    mock_client = MagicMock()
    mock_client.rdm.get_rdm_database_id.return_value = 42

    import helpers.job as job_module
    mocker.patch.object(job_module, 'resubmit_job', return_value=888)

    result = _submit_rdm_export_batch_with_seed(
        batch_id=h['batch_id'],
        batch=batch,
        jobs=jobs,
        irp_client=mock_client,
        job_module=job_module,
        schema=test_schema
    )

    # Only the ERROR job should be resubmitted
    resubmitted = [j for j in result['jobs'] if j.get('status') == 'RESUBMITTED']
    assert len(resubmitted) == 1
    assert resubmitted[0]['original_job_id'] == h['remaining_ids'][1]

    # Cleanup
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE id = %s",
        (h['cycle_id'],), schema=test_schema
    )
