"""
IRP Notebook Framework - Batch Management

This module provides functions for managing batches of Moody's workflow jobs.
A batch represents a collection of jobs generated from a master configuration
for a specific batch type (e.g., portfolio analysis, risk calculation).

ARCHITECTURE:
-------------
Layer 2 (CRUD): _create_batch (private helper)
Layer 3 (Workflow): create_batch (uses transaction for batch + jobs atomically),
                    submit_batch, recon_batch

TRANSACTION BEHAVIOR:
--------------------
- create_batch(): Uses transaction_context() to create batch + all jobs atomically
  - Calls CRUD functions directly (job.create_job_configuration, job.create_job)
  - All operations are atomic (all-or-nothing)
- submit_batch(): No transaction needed (read-only + external API calls)
- recon_batch(): No transaction needed (reconciliation logic)

Key Features:
- Create batches with automatic job configuration generation via transformers
- Submit batches to Moody's workflow system
- Track batch status and reconcile job completion
- Support for batch resubmission and error handling

Workflow:
1. create_batch() - Creates batch with job configurations from master config
2. submit_batch() - Submits all eligible jobs in batch to Moody's
3. track jobs via job.py module
4. recon_batch() - Reconciles batch status based on job states
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from helpers.irp_integration import IRPClient
from helpers.database import (
    execute_query, execute_command, execute_insert, bulk_insert, DatabaseError
)
from helpers.constants import BatchStatus, ConfigurationStatus, CycleStatus, JobStatus, BatchType
from helpers.configuration import (
    read_configuration, update_configuration_status,
    create_job_configurations, BATCH_TYPE_TRANSFORMERS,
    validate_reference_data_with_api
)
from helpers.cycle import get_active_cycle_id


class BatchError(Exception):
    """Custom exception for batch operation errors"""
    pass


# ============================================================================
# NOTIFICATION HELPERS
# ============================================================================

def _get_batch_context(batch_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Get cycle/stage/step context for a batch (used for notifications).

    Args:
        batch_id: Batch ID
        schema: Database schema

    Returns:
        Dictionary with context info (cycle_name, stage_num, step_name, notebook_path, batch_type)
    """
    query = """
        SELECT c.cycle_name, st.stage_num, s.step_num, s.step_name, s.notebook_path, b.batch_type
        FROM irp_batch b
        JOIN irp_configuration cfg ON b.configuration_id = cfg.id
        JOIN irp_cycle c ON cfg.cycle_id = c.id
        LEFT JOIN irp_step s ON b.step_id = s.id
        LEFT JOIN irp_stage st ON s.stage_id = st.id
        WHERE b.id = %s
    """
    result = execute_query(query, (batch_id,), schema=schema)

    if not result.empty:
        return {
            'cycle_name': result.iloc[0]['cycle_name'],
            'stage_num': result.iloc[0]['stage_num'],
            'step_name': result.iloc[0]['step_name'] or result.iloc[0]['batch_type'],
            'notebook_path': str(result.iloc[0]['notebook_path'] or ''),
            'batch_type': result.iloc[0]['batch_type']
        }
    else:
        return {
            'cycle_name': "Unknown",
            'stage_num': None,
            'step_name': "Unknown",
            'notebook_path': '',
            'batch_type': "Unknown"
        }


def _send_batch_failure_notification(
    batch_id: int,
    batch_status: str,
    recon_summary: Dict[str, Any],
    schema: str = 'public'
) -> None:
    """
    Send Teams notification when batch reconciliation results in failure status.

    Args:
        batch_id: Batch ID
        batch_status: Final batch status (FAILED, ERROR, or CANCELLED)
        recon_summary: Reconciliation summary data
        schema: Database schema
    """
    try:
        from helpers.teams_notification import TeamsNotificationClient, build_notification_actions

        teams = TeamsNotificationClient()
        ctx = _get_batch_context(batch_id, schema=schema)
        actions = build_notification_actions(ctx['notebook_path'], ctx['cycle_name'], schema)

        stage_str = f"Stage {ctx['stage_num']:02d}" if ctx['stage_num'] else "Unknown"

        # Build summary based on status
        status_counts = recon_summary.get('job_status_counts', {})
        total_jobs = recon_summary.get('non_skipped_jobs', 0)
        finished = status_counts.get(JobStatus.FINISHED, 0)
        failed = status_counts.get(JobStatus.FAILED, 0)
        error = status_counts.get(JobStatus.ERROR, 0)
        cancelled = status_counts.get(JobStatus.CANCELLED, 0)

        summary_parts = [f"**Total Jobs:** {total_jobs}"]
        if finished > 0:
            summary_parts.append(f"**Finished:** {finished}")
        if failed > 0:
            summary_parts.append(f"**Failed:** {failed}")
        if error > 0:
            summary_parts.append(f"**Error:** {error}")
        if cancelled > 0:
            summary_parts.append(f"**Cancelled:** {cancelled}")

        # Choose notification style based on status
        if batch_status == BatchStatus.CANCELLED:
            title = f"[{ctx['cycle_name']}] Batch Cancelled: {ctx['batch_type']}"
            send_method = teams.send_warning
        else:
            title = f"[{ctx['cycle_name']}] Batch Failed: {ctx['batch_type']}"
            send_method = teams.send_error

        send_method(
            title=title,
            message=f"**Cycle:** {ctx['cycle_name']}\n"
                    f"**Stage:** {stage_str}\n"
                    f"**Batch Type:** {ctx['batch_type']}\n"
                    f"**Batch ID:** {batch_id}\n"
                    f"**Status:** {batch_status}\n\n" +
                    "\n".join(summary_parts),
            actions=actions if actions else None
        )

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to send batch failure notification: {e}")


# ============================================================================
# PRIVATE FUNCTIONS
# ============================================================================

def _create_batch(
    batch_type: str,
    configuration_id: int,
    step_id: int,
    schema: str = 'public'
) -> int:
    """
    Create a batch record in database (used by create_batch).

    LAYER: 2 (CRUD - private helper)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        batch_type: Type of batch (must be in BATCH_TYPE_TRANSFORMERS)
        configuration_id: Configuration to use for this batch
        step_id: Step this batch belongs to
        schema: Database schema

    Returns:
        Batch ID

    Raises:
        BatchError: If creation fails
    """
    try:
        query = """
            INSERT INTO irp_batch (step_id, configuration_id, batch_type, status)
            VALUES (%s, %s, %s, %s)
        """
        batch_id = execute_insert(
            query,
            (step_id, configuration_id, batch_type, BatchStatus.INITIATED),
            schema=schema
        )
        return batch_id
    except DatabaseError as e:
        raise BatchError(f"Failed to create batch: {str(e)}") # pragma: no cover


def _insert_recon_log(
    batch_id: int,
    recon_result: str,
    recon_summary: Dict[str, Any],
    schema: str = 'public'
) -> int:
    """
    Insert batch reconciliation log entry.

    Args:
        batch_id: Batch ID
        recon_result: Reconciliation result status
        recon_summary: Summary data for reconciliation
        schema: Database schema

    Returns:
        Recon log ID
    """
    try:
        query = """
            INSERT INTO irp_batch_recon_log (batch_id, recon_result, recon_summary)
            VALUES (%s, %s, %s)
        """
        return execute_insert(
            query,
            (batch_id, recon_result, json.dumps(recon_summary)),
            schema=schema
        )
    except DatabaseError as e:
        raise BatchError(f"Failed to insert recon log: {str(e)}") # pragma: no cover


def _lookup_step_id(configuration_id: int, schema: str = 'public') -> Optional[int]:
    """

    TODO

    Lookup step_id for a configuration (placeholder - returns None for now).

    In future, this could derive step from configuration or context.
    For now, requires explicit step_id parameter.

    Args:
        configuration_id: Configuration ID
        schema: Database schema

    Returns:
        Step ID or None
    """
    # TODO: Implement step lookup logic based on configuration context
    # For now, return None to require explicit step_id
    return None


# ============================================================================
# CORE CRUD OPERATIONS
# ============================================================================

def read_batch(batch_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Read batch by ID.

    Args:
        batch_id: Batch ID to read
        schema: Database schema

    Returns:
        Dictionary with batch details

    Raises:
        BatchError: If batch not found
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}. Must be a positive integer.")

    query = """
        SELECT id, step_id, configuration_id, batch_type, status,
               created_ts, submitted_ts, completed_ts
        FROM irp_batch
        WHERE id = %s
    """

    df = execute_query(query, (batch_id,), schema=schema)

    if df.empty:
        raise BatchError(f"Batch with id {batch_id} not found")

    batch = df.iloc[0].to_dict()

    return batch


def update_batch_status(
    batch_id: int,
    status: str,
    schema: str = 'public'
) -> bool:
    """
    Update batch status with validation.

    Args:
        batch_id: Batch ID
        status: New status (INITIATED, ACTIVE, COMPLETED, FAILED, CANCELLED)
        schema: Database schema

    Returns:
        True if status was updated, False if status unchanged

    Raises:
        BatchError: If batch not found or invalid status
    """
    # Validate batch_id
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}. Must be a positive integer.") # pragma: no cover

    # Validate status
    if status not in BatchStatus.all():
        raise BatchError(
            f"Invalid status: {status}. Must be one of {BatchStatus.all()}"
        )

    # Read current batch
    current_batch = read_batch(batch_id, schema=schema)

    # If status is the same, no update needed
    if current_batch['status'] == status:
        return False

    # Update status and timestamp
    query = """
        UPDATE irp_batch
        SET status = %s,
            completed_ts = CASE WHEN %s IN (%s, %s, %s) THEN NOW() ELSE completed_ts END,
            updated_ts = NOW()
        WHERE id = %s
    """

    rows = execute_command(
        query,
        (status, status, BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED, batch_id),
        schema=schema
    )

    return rows > 0


# ============================================================================
# BATCH CREATION AND SUBMISSION
# ============================================================================

def create_batch(
    batch_type: str,
    configuration_id: int,
    step_id: Optional[int] = None,
    schema: str = 'public'
) -> int:
    """
    Create a new batch with job configurations atomically.

    LAYER: 3 (Workflow)

    TRANSACTION BEHAVIOR:
        - Uses transaction_context() to ensure atomicity
        - Creates batch + all job configurations + all jobs in single transaction
        - If any operation fails, entire batch creation is rolled back

    Process:
    1. Validate configuration is VALID or ACTIVE
    2. Validate batch_type is in BATCH_TYPE_TRANSFORMERS
    3. Lookup step_id if not provided (from configuration context)
    4. Apply transformer to generate job configurations
    5. In transaction:
       - Create batch record in INITIATED status
       - Create job configuration for each transformed config
       - Create job for each configuration
       - All operations committed atomically

    Args:
        batch_type: Type of batch processing
        configuration_id: Master configuration ID
        step_id: Optional step ID (looked up if None)
        schema: Database schema

    Returns:
        Batch ID

    Raises:
        BatchError: If validation fails or creation fails
    """
    # Import here to avoid circular dependency
    from helpers import job

    # Validate inputs
    if not isinstance(configuration_id, int) or configuration_id <= 0:
        raise BatchError(f"Invalid configuration_id: {configuration_id}")

    if not isinstance(batch_type, str) or not batch_type.strip():
        raise BatchError(f"Invalid batch_type: {batch_type}")

    # Validate batch_type is registered
    if batch_type not in BATCH_TYPE_TRANSFORMERS:
        available_types = list(BATCH_TYPE_TRANSFORMERS.keys())
        raise BatchError(
            f"Unknown batch_type '{batch_type}'. "
            f"Available types: {available_types}"
        )

    # Read and validate configuration
    config = read_configuration(configuration_id, schema=schema)

    if config['status'] not in [ConfigurationStatus.VALID, ConfigurationStatus.ACTIVE]:
        raise BatchError(
            f"Configuration {configuration_id} has invalid status '{config['status']}'. "
            f"Must be VALID or ACTIVE."
        )

    # Lookup step_id if not provided
    if step_id is None:
        step_id = _lookup_step_id(configuration_id, schema=schema)
        if step_id is None:
            raise BatchError(
                "step_id is required but not provided and could not be looked up. "
                "Please provide step_id explicitly."
            )

    if not isinstance(step_id, int) or step_id <= 0:
        raise BatchError(f"Invalid step_id: {step_id}")

    # Apply transformer to generate job configurations
    try:
        job_configs = create_job_configurations(
            batch_type,
            config['configuration_data']
        )
    except Exception as e:
        raise BatchError(f"Transformer failed for batch_type '{batch_type}': {str(e)}")

    # Note: Empty job_configs is allowed - batch will be created with 0 jobs
    # This is useful for optional batch types (e.g., Create Reinsurance Treaties when no treaties defined)
    # Empty batches will be immediately marked as COMPLETED during reconciliation

    # Create batch and jobs in transaction
    # Use transaction_context to ensure atomicity: if any job creation fails,
    # the entire batch creation is rolled back (no orphaned batch records)
    try:
        from helpers.database import transaction_context

        with transaction_context(schema=schema):
            # Create batch record
            batch_id = _create_batch(batch_type, configuration_id, step_id, schema=schema)

            # Create job configuration and job for each transformed config
            # NOTE: We call CRUD functions directly (not the atomic wrapper) since we're already in a transaction
            for job_config_data in job_configs:
                # Create job configuration
                config_id = job.create_job_configuration(
                    batch_id=batch_id,
                    configuration_id=configuration_id,
                    job_configuration_data=job_config_data,
                    skipped=False,
                    overridden=False,
                    override_reason_txt=None,
                    schema=schema
                )

                # Create job
                job_id = job.create_job(
                    batch_id=batch_id,
                    job_configuration_id=config_id,
                    parent_job_id=None,
                    schema=schema
                )

            # All operations committed together at end of context
            return batch_id

    except Exception as e:
        # Transaction automatically rolled back on exception
        raise BatchError(f"Failed to create batch: {str(e)}")


def _validate_batch_submission(
    batch: Dict[str, Any],
    batch_id: int,
    schema: str = 'public'
) -> List[str]:
    """
    Validate batch submission prerequisites and entity existence.

    Validates:
    - Pre-requisites exist (e.g., server for EDM, EDMs for Portfolio)
    - Entities to be created don't already exist

    Args:
        batch: Batch record dictionary
        batch_id: Batch ID
        schema: Database schema

    Returns:
        List of validation error messages (empty if valid)
    """
    from helpers.entity_validator import EntityValidator
    from helpers.constants import DEFAULT_DATABASE_SERVER

    batch_type = batch['batch_type']
    validator = EntityValidator()

    # Get job configurations for this batch
    job_configs = get_batch_job_configurations(batch_id, skipped=False, schema=schema)

    if batch_type == BatchType.EDM_CREATION:
        edm_names = [jc['job_configuration_data'].get('Database') for jc in job_configs]
        edm_names = [name for name in edm_names if name]
        return validator.validate_edm_batch(
            edm_names=edm_names,
            server_name=DEFAULT_DATABASE_SERVER
        )

    elif batch_type == BatchType.PORTFOLIO_CREATION:
        # Get ALL portfolios from configuration (base + sub-portfolios)
        # Job configs only contain base portfolios, but we want to validate all
        from helpers.configuration import read_configuration
        config = read_configuration(batch['configuration_id'], schema=schema)
        config_data = config.get('configuration_data', {})
        portfolios = config_data.get('Portfolios', [])
        return validator.validate_portfolio_batch(portfolios=portfolios)

    # Add validation for other batch types here as needed

    return []


def submit_batch(
    batch_id: int,
    irp_client: IRPClient,
    step_id: Optional[int] = None,
    schema: str = 'public'
) -> Dict[str, Any]:
    """
    Submit all eligible jobs in batch to Moody's.

    Process:
    1. Validate configuration is VALID or ACTIVE
    2. Validate cycle is ACTIVE
    3. Update batch step_id if provided (for batches submitted in different step than created)
    4. Get all jobs in batch
    5. For each job in INITIATED status, call submit_job
    6. Update batch status to ACTIVE
    7. Update batch submitted_ts
    8. Update configuration status to ACTIVE

    Args:
        batch_id: Batch ID
        irp_client: IRP client for Moody's API
        step_id: Optional step_run ID to associate batch with (useful when submitting
                 a batch created in a different step)
        schema: Database schema

    Returns:
        Dictionary with submission summary:
        {
            'batch_id': int,
            'batch_status': str,
            'submitted_jobs': int,
            'jobs': [{'job_id': int, 'status': str}, ...]
        }

    Raises:
        BatchError: If validation fails
    """
    # Import here to avoid circular dependency
    from helpers import job

    # Validate batch_id
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}") # pragma: no cover

    # Read batch
    batch = read_batch(batch_id, schema=schema)

    # Validate batch status
    if batch['batch_type'] not in BatchType.all():
        raise BatchError(f"Invalid batch_type: {batch['batch_type']}") # pragma: no cover

    # Read and validate configuration
    config = read_configuration(batch['configuration_id'], schema=schema)

    if config['status'] not in [ConfigurationStatus.VALID, ConfigurationStatus.ACTIVE]:
        raise BatchError(
            f"Configuration {config['id']} has status '{config['status']}'. "
            f"Must be VALID or ACTIVE to submit batch."
        )

    # Validate cycle is ACTIVE
    cycle_id = config['cycle_id']
    from helpers.database import execute_scalar
    cycle_status = execute_scalar(
        "SELECT status FROM irp_cycle WHERE id = %s",
        (cycle_id,),
        schema=schema
    )

    if cycle_status != CycleStatus.ACTIVE:
        raise BatchError(
            f"Cycle {cycle_id} has status '{cycle_status}'. "
            f"Must be ACTIVE to submit batch."
        )

    # Validate reference data for Analysis batches before submission
    if batch['batch_type'] == BatchType.ANALYSIS:
        job_configs = get_batch_job_configurations(batch_id, skipped=False, schema=schema)
        analysis_job_configs = [jc['job_configuration_data'] for jc in job_configs]
        ref_data_errors = validate_reference_data_with_api(analysis_job_configs, irp_client)
        if ref_data_errors:
            raise BatchError(
                f"Reference data validation failed. Cannot submit batch.\n"
                + "\n".join(ref_data_errors[:10])
            )

    # Validate batch prerequisites and entity existence before first submission
    # Skip validation on retry (batch already ACTIVE) to allow resubmitting failed jobs
    if batch['status'] == BatchStatus.INITIATED:
        validation_errors = _validate_batch_submission(batch, batch_id, schema)
        if validation_errors:
            raise BatchError(
                f"Batch validation failed. Cannot submit batch.\n"
                + "\n".join(validation_errors)
            )

    # Update batch step_id if provided (allows re-associating batch with submission step)
    if step_id is not None:
        query = """
            UPDATE irp_batch
            SET step_id = %s
            WHERE id = %s
        """
        execute_command(query, (step_id, batch_id), schema=schema)

    # Get all jobs for this batch
    jobs = get_batch_jobs(batch_id, schema=schema)

    # Special handling for RDM export with multiple jobs (seed job pattern)
    # When >100 analyses need to be exported, we create a seed job (1 analysis)
    # that creates the RDM, then submit remaining jobs with the databaseId
    if batch['batch_type'] == BatchType.EXPORT_TO_RDM and len(jobs) > 1:
        return _submit_rdm_export_batch_with_seed(
            batch_id=batch_id,
            batch=batch,
            jobs=jobs,
            irp_client=irp_client,
            job_module=job,
            schema=schema
        )

    # Submit eligible jobs
    submitted_jobs = []
    for job_record in jobs:
        if job_record['status'] in JobStatus.ready_for_submit() and not job_record['skipped']:
            try:
                job.submit_job(job_record['id'], batch['batch_type'], irp_client, schema=schema)
                submitted_jobs.append({
                    'job_id': job_record['id'],
                    'status': 'SUBMITTED'
                })
            except Exception as e:
                # Log error but continue with other jobs
                submitted_jobs.append({
                    'job_id': job_record['id'],
                    'status': 'FAILED',
                    'error': str(e)
                })

    # Update batch status to ACTIVE and set submitted_ts
    query = """
        UPDATE irp_batch
        SET status = %s, submitted_ts = NOW()
        WHERE id = %s
    """
    execute_command(query, (BatchStatus.ACTIVE, batch_id), schema=schema)

    # Update configuration status to ACTIVE
    update_configuration_status(batch['configuration_id'], ConfigurationStatus.ACTIVE, schema=schema)

    return {
        'batch_id': batch_id,
        'batch_status': BatchStatus.ACTIVE,
        'submitted_jobs': len(submitted_jobs),
        'jobs': submitted_jobs
    }


def _submit_rdm_export_batch_with_seed(
    batch_id: int,
    batch: Dict[str, Any],
    jobs: List[Dict[str, Any]],
    irp_client: IRPClient,
    job_module,
    schema: str = 'public'
) -> Dict[str, Any]:
    """
    Submit RDM export batch using seed job pattern.

    When exporting more than 100 analyses to RDM, the Moody's API requires
    multiple requests. To ensure all exports go to the same RDM, we:
    1. Submit seed job (first job with is_seed_job=True, contains 1 analysis)
    2. Wait for seed job to complete (creates the RDM)
    3. Get databaseId from created RDM
    4. Update remaining jobs with databaseId
    5. Submit remaining jobs (they append to the existing RDM)

    This introduces a blocking wait for the seed job (~1-5 minutes) but
    allows the remaining jobs to be tracked asynchronously.

    Args:
        batch_id: Batch ID
        batch: Batch record dict
        jobs: List of job records from get_batch_jobs()
        irp_client: IRP client for Moody's API
        job_module: The helpers.job module (passed to avoid re-importing)
        schema: Database schema

    Returns:
        Dictionary with submission summary

    Raises:
        BatchError: If seed job not found or submission fails
    """
    # Separate seed job from remaining jobs
    seed_job = None
    remaining_jobs = []

    for job_record in jobs:
        if job_record['skipped']:
            continue

        # Read job configuration to check is_seed_job flag
        job_config = job_module.get_job_config(job_record['id'], schema=schema)
        config_data = job_config.get('job_configuration_data', {})

        if config_data.get('is_seed_job'):
            seed_job = job_record
        else:
            remaining_jobs.append(job_record)

    if not seed_job:
        raise BatchError("RDM export batch with multiple jobs must have a seed job")

    submitted_jobs = []

    # 1. Submit seed job
    try:
        job_module.submit_job(seed_job['id'], BatchType.EXPORT_TO_RDM, irp_client, schema=schema)
        submitted_jobs.append({
            'job_id': seed_job['id'],
            'status': 'SUBMITTED',
            'is_seed': True
        })
    except Exception as e:
        raise BatchError(f"Failed to submit seed job {seed_job['id']}: {str(e)}")

    # 2. Wait for seed job to complete
    # Re-read job to get moodys_workflow_id after submission
    seed_job_record = job_module.read_job(seed_job['id'], schema=schema)
    moodys_job_id = seed_job_record['moodys_workflow_id']

    if not moodys_job_id:
        raise BatchError(f"Seed job {seed_job['id']} has no moodys_workflow_id after submission")

    # Poll until complete (blocking wait ~1-5 minutes for single analysis)
    job_result = irp_client.rdm.poll_rdm_export_job_to_completion(int(moodys_job_id))

    # Update seed job status based on result
    final_status = job_result.get('status', 'FINISHED')
    if final_status == 'FINISHED':
        job_module.update_job_status(seed_job['id'], JobStatus.FINISHED, schema=schema)
        submitted_jobs[0]['status'] = 'FINISHED'
    else:
        job_module.update_job_status(seed_job['id'], JobStatus.FAILED, schema=schema)
        raise BatchError(f"Seed job failed with status: {final_status}")

    # 3. Get databaseId from created RDM
    seed_config = job_module.get_job_config(seed_job['id'], schema=schema)
    rdm_name = seed_config['job_configuration_data'].get('rdm_name')
    server_name = seed_config['job_configuration_data'].get('server_name', 'databridge-1')

    database_id = irp_client.rdm.get_rdm_database_id(rdm_name, server_name)

    # 4. Update remaining jobs with databaseId and submit them
    for job_record in remaining_jobs:
        if job_record['status'] not in JobStatus.ready_for_submit():
            continue

        try:
            # Update job configuration with database_id
            job_module.update_job_configuration_data(
                job_record['job_configuration_id'],
                {'database_id': database_id},
                schema=schema
            )

            # Submit job
            job_module.submit_job(job_record['id'], BatchType.EXPORT_TO_RDM, irp_client, schema=schema)
            submitted_jobs.append({
                'job_id': job_record['id'],
                'status': 'SUBMITTED',
                'is_seed': False
            })
        except Exception as e:
            submitted_jobs.append({
                'job_id': job_record['id'],
                'status': 'FAILED',
                'error': str(e),
                'is_seed': False
            })

    # 5. Update batch status to ACTIVE
    query = """
        UPDATE irp_batch
        SET status = %s, submitted_ts = NOW()
        WHERE id = %s
    """
    execute_command(query, (BatchStatus.ACTIVE, batch_id), schema=schema)

    # Update configuration status to ACTIVE
    update_configuration_status(batch['configuration_id'], ConfigurationStatus.ACTIVE, schema=schema)

    return {
        'batch_id': batch_id,
        'batch_status': BatchStatus.ACTIVE,
        'submitted_jobs': len(submitted_jobs),
        'jobs': submitted_jobs,
        'database_id': database_id
    }


# ============================================================================
# BATCH QUERIES
# ============================================================================

def get_batch_jobs(
    batch_id: int,
    skipped: Optional[bool] = None,
    status: Optional[str] = None,
    schema: str = 'public'
) -> List[Dict[str, Any]]:
    """
    Get all jobs for a batch with optional filters.

    Args:
        batch_id: Batch ID
        skipped: Filter by skipped status (None = all, True = skipped only, False = not skipped)
        status: Filter by job status (None = all, specific status string to filter)
        schema: Database schema

    Returns:
        List of job dictionaries

    Raises:
        BatchError: If batch not found
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}")

    # Build query with optional filters
    query = """
        SELECT id, batch_id, job_configuration_id, moodys_workflow_id,
               status, skipped, last_error, parent_job_id,
               submitted_ts, completed_ts, last_tracked_ts,
               created_ts, updated_ts,
               submission_request, submission_response
        FROM irp_job
        WHERE batch_id = %s
    """

    params = [batch_id]

    if skipped is not None:
        query += " AND skipped = %s"
        params.append(skipped)

    if status is not None:
        query += " AND status = %s"
        params.append(status)

    query += " ORDER BY id"

    df = execute_query(query, tuple(params), schema=schema)

    if df.empty:
        return []

    jobs = df.to_dict(orient='records')

    # Parse JSON fields if they're strings
    for job in jobs:
        if isinstance(job.get('submission_request'), str):
            job['submission_request'] = json.loads(job['submission_request'])
        if isinstance(job.get('submission_response'), str):
            job['submission_response'] = json.loads(job['submission_response'])

    return jobs


def get_batch_job_configurations(
    batch_id: int,
    skipped: Optional[bool] = None,
    schema: str = 'public'
) -> List[Dict[str, Any]]:
    """
    Get all job configurations for a batch with optional filters.

    Args:
        batch_id: Batch ID
        skipped: Filter by skipped status (None = all, True = skipped only, False = not skipped)
        schema: Database schema

    Returns:
        List of job configuration dictionaries

    Raises:
        BatchError: If batch not found
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}") # pragma: no cover

    query = """
        SELECT id, batch_id, configuration_id, job_configuration_data,
               skipped, overridden, override_reason_txt,
               parent_job_configuration_id, skipped_reason_txt, override_job_configuration_id,
               created_ts, updated_ts
        FROM irp_job_configuration
        WHERE batch_id = %s
    """

    params = [batch_id]

    if skipped is not None:
        query += " AND skipped = %s"
        params.append(skipped)

    query += " ORDER BY id"

    df = execute_query(query, tuple(params), schema=schema)

    if df.empty:
        return []

    job_configs = df.to_dict(orient='records')

    # Parse JSON configuration data if it's a string
    for config in job_configs:
        if isinstance(config.get('job_configuration_data'), str):
            config['job_configuration_data'] = json.loads(config['job_configuration_data'])

    return job_configs


# ============================================================================
# BATCH RECONCILIATION
# ============================================================================

def recon_batch(batch_id: int, schema: str = 'public') -> str:
    """
    Reconcile batch status based on job and configuration states.

    Logic:
    1. Get all non-skipped job configurations
    2. Get all non-skipped jobs
    3. Check if all jobs are in terminal states (FINISHED, FAILED, CANCELLED, ERROR)
    4. Determine batch status:
       - If jobs still in progress: ACTIVE (continue polling)
       - If all jobs terminal:
         - CANCELLED: All jobs are CANCELLED
         - ERROR: At least one job is ERROR
         - FAILED: At least one job is FAILED (but not all cancelled)
         - COMPLETED: All configs have at least one FINISHED job
    5. Create recon log entry with detailed summary
    6. Update batch status

    Args:
        batch_id: Batch ID
        schema: Database schema

    Returns:
        New batch status (CANCELLED, FAILED, ERROR, COMPLETED, or ACTIVE)

    Raises:
        BatchError: If batch not found
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}")

    # Terminal job statuses - jobs that won't change status anymore
    TERMINAL_JOB_STATUSES = JobStatus.terminal()

    # Get all job configurations (including skipped for counting)
    all_configs = get_batch_job_configurations(batch_id, schema=schema)
    non_skipped_configs = [c for c in all_configs if not c['skipped']]

    # Get all jobs (including skipped for counting)
    all_jobs = get_batch_jobs(batch_id, schema=schema)
    non_skipped_jobs = [j for j in all_jobs if not j['skipped']]

    # Count jobs by status
    status_counts = {}
    for job in all_jobs:
        status = job['status']
        # Treat skipped jobs as a pseudo-status for recon summary
        if job['skipped']:
            status = 'SKIPPED'  # This is a pseudo-status that we will use to track SKIPPED jobs
        status_counts[status] = status_counts.get(status, 0) + 1

    # Determine batch status
    recon_result = None
    failed_job_ids = []
    cancelled_job_ids = []
    fulfilled_configs = 0
    unfulfilled_configs = 0
    unfulfilled_config_ids = []

    # Group jobs by configuration (needed for config fulfillment check)
    jobs_by_config = {}
    for job in all_jobs:
        config_id = job['job_configuration_id']
        if config_id not in jobs_by_config:
            jobs_by_config[config_id] = []
        jobs_by_config[config_id].append(job)

    # Check each non-skipped config has at least one successful job
    for config in non_skipped_configs:
        config_jobs = jobs_by_config.get(config['id'], [])
        has_success = config_jobs and any(
            j['status'] in [JobStatus.FINISHED]
            for j in config_jobs
        )
        if has_success:
            fulfilled_configs += 1
        else:
            unfulfilled_configs += 1
            unfulfilled_config_ids.append(config['id'])

    # First, check if all non-skipped jobs are in terminal states
    # If any job is still in progress, batch remains ACTIVE
    all_jobs_terminal = non_skipped_jobs and all(
        j['status'] in TERMINAL_JOB_STATUSES for j in non_skipped_jobs
    )

    # Handle empty batch (no non-skipped jobs/configs) - immediately COMPLETED
    # This supports optional batch types where transformer returned 0 job configurations
    if len(non_skipped_jobs) == 0 and len(non_skipped_configs) == 0:
        recon_result = BatchStatus.COMPLETED
    elif not all_jobs_terminal:
        # Jobs still in progress - batch remains ACTIVE
        recon_result = BatchStatus.ACTIVE
    else:
        # All jobs are in terminal states - determine final batch status

        # Check for all CANCELLED
        if all(j['status'] == JobStatus.CANCELLED for j in non_skipped_jobs):
            recon_result = BatchStatus.CANCELLED

        # Check for any ERROR
        elif any(j['status'] == JobStatus.ERROR for j in non_skipped_jobs):
            recon_result = BatchStatus.ERROR

        # Check for any FAILED
        elif any(j['status'] == JobStatus.FAILED for j in non_skipped_jobs):
            recon_result = BatchStatus.FAILED

        # All jobs finished successfully - check config fulfillment
        elif unfulfilled_configs == 0 and len(non_skipped_configs) > 0:
            recon_result = BatchStatus.COMPLETED
        else:
            # This shouldn't happen if all jobs are terminal and none failed
            # But keep as safety fallback
            recon_result = BatchStatus.ACTIVE

    # Build recon summary
    cancelled_job_ids = [j['id'] for j in non_skipped_jobs if j['status'] == JobStatus.CANCELLED]
    error_job_ids = [j['id'] for j in non_skipped_jobs if j['status'] == JobStatus.ERROR]
    failed_job_ids = [j['id'] for j in non_skipped_jobs if j['status'] == JobStatus.FAILED]

    recon_summary = {
        'total_configs': len(all_configs),
        'non_skipped_configs': len(non_skipped_configs),
        'fulfilled_configs': fulfilled_configs,
        'unfulfilled_configs': unfulfilled_configs,
        'unfulfilled_config_ids': unfulfilled_config_ids,
        'total_jobs': len(all_jobs),
        'non_skipped_jobs': len(non_skipped_jobs),
        'job_status_counts': status_counts,
        'failed_job_ids': failed_job_ids,
        'cancelled_job_ids': cancelled_job_ids,
        'error_job_ids': error_job_ids
    }

    # Insert recon log
    _insert_recon_log(batch_id, recon_result, recon_summary, schema=schema)

    # Update batch status
    update_batch_status(batch_id, recon_result, schema=schema)

    # Send notification for failure statuses
    if recon_result in [BatchStatus.FAILED, BatchStatus.ERROR, BatchStatus.CANCELLED]:
        _send_batch_failure_notification(batch_id, recon_result, recon_summary, schema=schema)

    return recon_result


def get_batches_for_configuration(
    configuration_id: int,
    batch_type: Optional[str] = None,
    exclude_statuses: Optional[List[str]] = None,
    schema: str = 'public'
) -> List[Dict[str, Any]]:
    """
    Get all batches for a configuration, optionally filtered by batch type and status.

    Args:
        configuration_id: Configuration ID
        batch_type: Optional batch type filter
        exclude_statuses: Optional list of statuses to exclude (e.g., ['COMPLETED', 'CANCELLED'])
        schema: Database schema

    Returns:
        List of batch dictionaries with keys: id, batch_type, status, created_ts, job_count
    """
    query = """
        SELECT
            b.id,
            b.batch_type,
            b.status,
            b.created_ts,
            COUNT(j.id) as job_count
        FROM irp_batch b
        LEFT JOIN irp_job j ON b.id = j.batch_id
        WHERE b.configuration_id = %s
    """
    params: List[Any] = [configuration_id]

    if batch_type:
        query += " AND b.batch_type = %s"
        params.append(batch_type)

    if exclude_statuses:
        placeholders = ', '.join(['%s'] * len(exclude_statuses))
        query += f" AND b.status NOT IN ({placeholders})"
        params.extend(exclude_statuses)

    query += " GROUP BY b.id, b.batch_type, b.status, b.created_ts ORDER BY b.created_ts DESC"

    result = execute_query(query, tuple(params), schema=schema)

    if result.empty:
        return []

    return result.to_dict('records')  # type: ignore


def delete_batch(batch_id: int, schema: str = 'public') -> bool:
    """
    Delete a batch and all its associated jobs and job configurations.

    This operation is irreversible. Use with caution.

    Args:
        batch_id: Batch ID to delete
        schema: Database schema

    Returns:
        True if deleted successfully

    Raises:
        BatchError: If batch not found or deletion fails
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}")

    # Verify batch exists
    batch = read_batch(batch_id, schema=schema)
    if not batch:
        raise BatchError(f"Batch {batch_id} not found")

    # Delete in order: jobs → job_configs → batch
    # Note: job_config deletion is cascaded by FK constraint, but explicit for clarity
    try:
        # Delete all jobs for this batch
        execute_command(
            "DELETE FROM irp_job WHERE batch_id = %s",
            (batch_id,),
            schema=schema
        )

        # Delete all job configurations for this batch
        execute_command(
            "DELETE FROM irp_job_configuration WHERE batch_id = %s",
            (batch_id,),
            schema=schema
        )

        # Delete the batch itself
        execute_command(
            "DELETE FROM irp_batch WHERE id = %s",
            (batch_id,),
            schema=schema
        )

        return True

    except Exception as e:
        raise BatchError(f"Failed to delete batch {batch_id}: {str(e)}")


# ============================================================================
# ANALYSIS BATCH RECONCILIATION
# ============================================================================

def reconcile_analysis_batch(
    batch_id: int,
    irp_client: IRPClient,
    schema: str = 'public'
) -> Dict[str, Any]:
    """
    Reconcile an analysis batch by comparing job states with Moody's analyses.

    Categorizes jobs based on both Moody's state (source of truth) and job status:
    - jobs_successful: Analysis exists in Moody's AND job is FINISHED
    - jobs_failed: No analysis AND job is FAILED/ERROR/CANCELLED
    - jobs_missing_analysis: No analysis AND job is FINISHED (mismatch)
    - jobs_pending: Has workflow ID but not terminal (still processing)
    - jobs_fresh: INITIATED, no workflow ID, no analysis (ready for submission)
    - jobs_blocked: INITIATED but analysis already exists (need to delete first)

    Args:
        batch_id: Batch ID to reconcile
        irp_client: IRPClient instance for Moody's API calls
        schema: Database schema

    Returns:
        Dictionary with reconciliation summary:
        - total_jobs: Total number of non-skipped jobs
        - jobs_by_status: Dict of status -> count
        - analyses_in_moodys: Number of analyses found in Moody's
        - jobs_successful: Jobs with analysis in Moody's and FINISHED status
        - jobs_failed: Jobs with no analysis and FAILED/ERROR/CANCELLED status
        - jobs_missing_analysis: Jobs with no analysis but FINISHED status
        - jobs_pending: Jobs still processing (have workflow ID, not terminal)
        - jobs_fresh: Jobs ready for submission (INITIATED, no workflow ID, no analysis)
        - jobs_blocked: Jobs blocked by existing analysis (INITIATED but analysis exists)
        - existing_analyses: List of existing analysis dicts from Moody's

    Raises:
        BatchError: If batch not found or not an analysis batch
    """
    # Validate and read batch
    batch = read_batch(batch_id, schema=schema)

    if batch['batch_type'] != BatchType.ANALYSIS:
        raise BatchError(
            f"Batch {batch_id} is type '{batch['batch_type']}', not '{BatchType.ANALYSIS}'"
        )

    # Get terminal statuses
    terminal_statuses = JobStatus.terminal()

    # Get all non-skipped jobs and their configurations
    jobs = get_batch_jobs(batch_id, skipped=False, schema=schema)
    job_configs = get_batch_job_configurations(batch_id, skipped=False, schema=schema)

    # Build a lookup from job_configuration_id -> job_configuration_data
    config_lookup = {
        jc['id']: jc['job_configuration_data']
        for jc in job_configs
    }

    # Count jobs by status
    jobs_by_status: Dict[str, int] = {}
    for job in jobs:
        status = job['status']
        jobs_by_status[status] = jobs_by_status.get(status, 0) + 1

    # Get job configs for Moody's lookup (need Analysis Name + Database)
    job_config_data_list = [jc['job_configuration_data'] for jc in job_configs]

    # Check which analyses exist in Moody's
    existing_analyses = irp_client.analysis.find_existing_analyses_from_job_configs(
        job_config_data_list
    )

    # Build set of (analysis_name, edm_name) that exist in Moody's
    existing_keys = set()
    for item in existing_analyses:
        analysis_name = item['job_config'].get('Analysis Name')
        edm_name = item['job_config'].get('Database')
        if analysis_name and edm_name:
            existing_keys.add((analysis_name, edm_name))

    # Categorize jobs based on Moody's state (source of truth) AND job status:
    # 1. jobs_successful: Analysis exists in Moody's AND job is FINISHED
    # 2. jobs_failed: No analysis AND job is in failed terminal state (FAILED, ERROR, CANCELLED)
    # 3. jobs_missing_analysis: No analysis AND job is FINISHED (mismatch - analysis deleted?)
    # 4. jobs_pending: Has workflow ID but not terminal (still processing)
    # 5. jobs_fresh: INITIATED status, no workflow ID, no analysis (ready for first submission)
    # 6. jobs_blocked: INITIATED but analysis already exists (need to delete before submitting)
    jobs_successful = []
    jobs_failed = []
    jobs_missing_analysis = []
    jobs_pending = []
    jobs_fresh = []
    jobs_blocked = []

    failed_statuses = JobStatus.failed()

    for job in jobs:
        job_config_data = config_lookup.get(job['job_configuration_id'], {})
        analysis_name = job_config_data.get('Analysis Name')
        edm_name = job_config_data.get('Database')

        workflow_id = job.get('moodys_workflow_id')
        has_workflow_id = workflow_id is not None and workflow_id != ''
        is_terminal = job['status'] in terminal_statuses
        has_analysis = (analysis_name, edm_name) in existing_keys
        is_initiated = job['status'] == JobStatus.INITIATED
        is_finished = job['status'] == JobStatus.FINISHED
        is_failed_status = job['status'] in failed_statuses

        job_info = {
            'job_id': job['id'],
            'status': job['status'],
            'analysis_name': analysis_name,
            'edm_name': edm_name,
            'job_configuration_id': job['job_configuration_id'],
            'moodys_workflow_id': workflow_id,
            'has_analysis': has_analysis
        }

        if has_analysis and is_finished:
            # Analysis exists and job FINISHED - truly successful
            jobs_successful.append(job_info)
        elif has_analysis and is_initiated:
            # INITIATED but analysis already exists - blocked, need to delete first
            jobs_blocked.append(job_info)
        elif not has_analysis and is_failed_status:
            # No analysis and job failed/errored/cancelled
            jobs_failed.append(job_info)
        elif not has_analysis and is_finished:
            # Job says FINISHED but analysis is missing (was deleted or something went wrong)
            jobs_missing_analysis.append(job_info)
        elif has_workflow_id and not is_terminal:
            # Has workflow ID but not terminal - still processing
            jobs_pending.append(job_info)
        elif is_initiated and not has_workflow_id and not has_analysis:
            # INITIATED with no workflow ID and no analysis - ready for first submission
            jobs_fresh.append(job_info)
        else:
            # Edge case: treat as pending
            jobs_pending.append(job_info)

    return {
        'batch_id': batch_id,
        'batch_type': batch['batch_type'],
        'batch_status': batch['status'],
        'total_jobs': len(jobs),
        'jobs_by_status': jobs_by_status,
        'analyses_in_moodys': len(existing_analyses),
        'jobs_successful': jobs_successful,
        'jobs_failed': jobs_failed,
        'jobs_missing_analysis': jobs_missing_analysis,
        'jobs_pending': jobs_pending,
        'jobs_fresh': jobs_fresh,
        'jobs_blocked': jobs_blocked,
        'existing_analyses': existing_analyses
    }
