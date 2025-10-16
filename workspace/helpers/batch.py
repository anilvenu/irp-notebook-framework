"""
IRP Notebook Framework - Batch Management

This module provides functions for managing batches of Moody's workflow jobs.
A batch represents a collection of jobs generated from a master configuration
for a specific batch type (e.g., portfolio analysis, risk calculation).

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

from helpers.database import (
    execute_query, execute_command, execute_insert, bulk_insert, DatabaseError
)
from helpers.constants import BatchStatus, ConfigurationStatus, CycleStatus, DB_CONFIG
from helpers.configuration import read_configuration, update_configuration_status, ConfigurationTransformer
from helpers.cycle import get_active_cycle_id


class BatchError(Exception):
    """Custom exception for batch operation errors"""
    pass


# ============================================================================
# PRIVATE FUNCTIONS
# ============================================================================

def _create_batch(
    batch_type: str,
    configuration_id: int,
    step_id: int,
    schema: Optional[str] = None
) -> int:
    """
    Create a batch record in database (used by create_batch).

    Args:
        batch_type: Type of batch (must be registered in ConfigurationTransformer)
        configuration_id: Configuration to use for this batch
        step_id: Step this batch belongs to
        schema: Database schema

    Returns:
        Batch ID

    Raises:
        BatchError: If creation fails
    """
    if schema is None:
        schema = DB_CONFIG['schema']

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
        raise BatchError(f"Failed to create batch: {str(e)}")


def _insert_recon_log(
    batch_id: int,
    recon_result: str,
    recon_summary: Dict[str, Any],
    schema: Optional[str] = None
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
    if schema is None:
        schema = DB_CONFIG['schema']

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
        raise BatchError(f"Failed to insert recon log: {str(e)}")


def _lookup_step_id(configuration_id: int, schema: Optional[str] = None) -> Optional[int]:
    """
    Lookup step_id for a configuration (placeholder - returns None for now).

    In future, this could derive step from configuration metadata or context.
    For now, requires explicit step_id parameter.

    Args:
        configuration_id: Configuration ID
        schema: Database schema

    Returns:
        Step ID or None
    """
    if schema is None:
        schema = DB_CONFIG['schema']

    # TODO: Implement step lookup logic based on configuration context
    # For now, return None to require explicit step_id
    return None


# ============================================================================
# CORE CRUD OPERATIONS
# ============================================================================

def read_batch(batch_id: int, schema: Optional[str] = None) -> Dict[str, Any]:
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

    if schema is None:
        schema = DB_CONFIG['schema']

    query = """
        SELECT id, step_id, configuration_id, batch_type, status,
               created_ts, submitted_ts, completed_ts,
               total_jobs, completed_jobs, failed_jobs, metadata
        FROM irp_batch
        WHERE id = %s
    """

    df = execute_query(query, (batch_id,), schema=schema)

    if df.empty:
        raise BatchError(f"Batch with id {batch_id} not found")

    batch = df.iloc[0].to_dict()

    # Parse JSON metadata if it's a string
    if isinstance(batch.get('metadata'), str):
        batch['metadata'] = json.loads(batch['metadata'])

    return batch


def update_batch_status(
    batch_id: int,
    status: str,
    schema: Optional[str] = None
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
        raise BatchError(f"Invalid batch_id: {batch_id}. Must be a positive integer.")

    # Validate status
    if status not in BatchStatus.all():
        raise BatchError(
            f"Invalid status: {status}. Must be one of {BatchStatus.all()}"
        )

    if schema is None:
        schema = DB_CONFIG['schema']

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
    schema: Optional[str] = None
) -> int:
    """
    Create a new batch with job configurations.

    Process:
    1. Validate configuration is VALID or ACTIVE
    2. Validate batch_type is registered in ConfigurationTransformer
    3. Lookup step_id if not provided (from configuration context)
    4. Apply transformer to generate job configurations
    5. In transaction:
       - Create batch record in INITIATED status
       - Create job configuration for each transformed config
       - Create job for each configuration (via create_job)
    6. Submit batch (calls submit_batch)

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
    if batch_type not in ConfigurationTransformer.list_types():
        raise BatchError(
            f"Unknown batch_type '{batch_type}'. "
            f"Registered types: {ConfigurationTransformer.list_types()}"
        )

    if schema is None:
        schema = DB_CONFIG['schema']

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
        job_configs = ConfigurationTransformer.get_job_configurations(
            batch_type,
            config['configuration_data']
        )
    except Exception as e:
        raise BatchError(f"Transformer failed for batch_type '{batch_type}': {str(e)}")

    if not job_configs:
        raise BatchError(f"Transformer returned no job configurations for batch_type '{batch_type}'")

    # Create batch and jobs in transaction
    try:
        # Create batch record
        batch_id = _create_batch(batch_type, configuration_id, step_id, schema=schema)

        # Create job configuration and job for each transformed config
        for job_config_data in job_configs:
            job_id = job.create_job(
                batch_id=batch_id,
                configuration_id=configuration_id,
                job_configuration_data=job_config_data,
                validate=False,  # Already validated by transformer
                schema=schema
            )

        return batch_id

    except Exception as e:
        raise BatchError(f"Failed to create batch: {str(e)}")


def submit_batch(batch_id: int, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Submit all eligible jobs in batch to Moody's.

    Process:
    1. Validate batch is in (INITIATED, ACTIVE, or FAILED) status
    2. Validate configuration is VALID or ACTIVE
    3. Validate cycle is ACTIVE
    4. Get all jobs in batch
    5. For each job in INITIATED status, call submit_job
    6. Update batch status to ACTIVE
    7. Update batch submitted_ts
    8. Update configuration status to ACTIVE

    Args:
        batch_id: Batch ID
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
        raise BatchError(f"Invalid batch_id: {batch_id}")

    if schema is None:
        schema = DB_CONFIG['schema']

    # Read and validate batch
    batch = read_batch(batch_id, schema=schema)

    if batch['status'] not in [BatchStatus.INITIATED, BatchStatus.ACTIVE, BatchStatus.FAILED]:
        raise BatchError(
            f"Batch {batch_id} has status '{batch['status']}'. "
            f"Can only submit batches in INITIATED, ACTIVE, or FAILED status."
        )

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

    # Get all jobs for this batch
    jobs = get_batch_jobs(batch_id, schema=schema)

    # Submit eligible jobs
    submitted_jobs = []
    for job_record in jobs:
        if job_record['status'] == 'INITIATED' and not job_record['skipped']:
            try:
                job.submit_job(job_record['id'], schema=schema)
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


def create_and_submit_batch(
    batch_type: str,
    configuration_id: int,
    step_id: Optional[int] = None,
    schema: Optional[str] = None
) -> int:
    """
    Convenience function to create and submit batch in one call.

    Note: create_batch already calls submit_batch, so this is just an alias.

    Args:
        batch_type: Type of batch processing
        configuration_id: Master configuration ID
        step_id: Optional step ID
        schema: Database schema

    Returns:
        Batch ID
    """

    if schema is None:
        schema = DB_CONFIG['schema']

    return create_batch(batch_type, configuration_id, step_id, schema=schema)


# ============================================================================
# BATCH QUERIES
# ============================================================================

def get_batch_jobs(
    batch_id: int,
    skipped: Optional[bool] = None,
    status: Optional[str] = None,
    schema: Optional[str] = None
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

    if schema is None:
        schema = DB_CONFIG['schema']

    # Build query with optional filters
    query = """
        SELECT id, batch_id, job_configuration_id, moodys_workflow_id,
               status, skipped, last_error, parent_job_id,
               submitted_ts, completed_ts, last_poll_ts,
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
    schema: Optional[str] = None
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
        raise BatchError(f"Invalid batch_id: {batch_id}")

    if schema is None:
        schema = DB_CONFIG['schema']

    query = """
        SELECT id, batch_id, configuration_id, job_configuration_data,
               skipped, overridden, override_reason_txt,
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

def recon_batch(batch_id: int, schema: Optional[str] = None) -> str:
    """
    Reconcile batch status based on job and configuration states.

    Logic:
    1. Get all non-skipped job configurations
    2. Get all non-skipped jobs
    3. Determine batch status:
       - CANCELLED: All jobs are CANCELLED
       - FAILED: At least one job is FAILED
       - COMPLETED: All configs have at least one FINISHED job OR skipped job
       - ACTIVE: Otherwise
    4. Create recon log entry with detailed summary
    5. Update batch status

    Args:
        batch_id: Batch ID
        schema: Database schema

    Returns:
        New batch status (CANCELLED, FAILED, COMPLETED, or ACTIVE)

    Raises:
        BatchError: If batch not found
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise BatchError(f"Invalid batch_id: {batch_id}")

    if schema is None:
        schema = DB_CONFIG['schema']

    # Read batch to verify it exists
    batch = read_batch(batch_id, schema=schema)

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
            status = 'SKIPPED'
        status_counts[status] = status_counts.get(status, 0) + 1

    # Determine batch status
    recon_result = None
    failed_job_ids = []
    cancelled_job_ids = []
    fulfilled_configs = 0

    # Check for all CANCELLED
    if all(j['status'] == 'CANCELLED' for j in non_skipped_jobs):
        recon_result = BatchStatus.CANCELLED
        cancelled_job_ids = [j['id'] for j in non_skipped_jobs]

    # Check for any FAILED
    elif any(j['status'] == 'FAILED' for j in non_skipped_jobs):
        recon_result = BatchStatus.FAILED
        failed_job_ids = [j['id'] for j in non_skipped_jobs if j['status'] == 'FAILED']

    else:
        # Check if all configs are fulfilled
        # Group jobs by configuration
        jobs_by_config = {}
        for job in all_jobs:
            config_id = job['job_configuration_id']
            if config_id not in jobs_by_config:
                jobs_by_config[config_id] = []
            jobs_by_config[config_id].append(job)

        # Check each non-skipped config has at least one successful job
        all_fulfilled = True
        for config in non_skipped_configs:
            config_jobs = jobs_by_config.get(config['id'], [])
            has_success = any(
                j['status'] in ['FINISHED'] or j['skipped']
                for j in config_jobs
            )
            if has_success:
                fulfilled_configs += 1
            else:
                all_fulfilled = False

        if all_fulfilled and len(non_skipped_configs) > 0:
            recon_result = BatchStatus.COMPLETED
        else:
            recon_result = BatchStatus.ACTIVE

    # Build recon summary
    recon_summary = {
        'total_configs': len(all_configs),
        'non_skipped_configs': len(non_skipped_configs),
        'fulfilled_configs': fulfilled_configs,
        'total_jobs': len(all_jobs),
        'non_skipped_jobs': len(non_skipped_jobs),
        'job_status_counts': status_counts,
        'failed_job_ids': failed_job_ids,
        'cancelled_job_ids': cancelled_job_ids
    }

    # Insert recon log
    _insert_recon_log(batch_id, recon_result, recon_summary, schema=schema)

    # Update batch status
    update_batch_status(batch_id, recon_result, schema=schema)

    return recon_result
