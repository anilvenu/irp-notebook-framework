"""
IRP Notebook Framework - Job Management

This module provides functions for managing individual Moody's workflow jobs.
Jobs are the atomic unit of work, each executing with a specific job configuration
derived from a master configuration.

Key Features:
- Create jobs with new or existing job configurations
- Submit jobs to Moody's workflow system
- Track job status through polling
- Support for job resubmission with optional configuration override
- Parent-child job relationships for audit trail

Workflow:
1. create_job() - Creates job with configuration
2. submit_job() - Submits job to Moody's (stubbed for now)
3. track_job_status() - Polls Moody's for job status (stubbed for now)
4. resubmit_job() - Creates new job, optionally with override, and skips original
"""

import os
import json
import random
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from helpers.database import (
    execute_query, execute_command, execute_insert, DatabaseError
)
from helpers.constants import JobStatus


class JobError(Exception):
    """Custom exception for job operation errors"""
    pass


# ============================================================================
# PRIVATE FUNCTIONS
# ============================================================================

def _create_job_configuration(
    batch_id: int,
    configuration_id: int,
    job_configuration_data: Dict[str, Any],
    skipped: bool = False,
    overridden: bool = False,
    override_reason_txt: Optional[str] = None,
    schema: str = 'public'
) -> int:
    """
    Create job configuration record.

    Args:
        batch_id: Batch ID
        configuration_id: Master configuration ID
        job_configuration_data: Configuration data for this specific job
        skipped: Whether this configuration is skipped
        overridden: Whether this is an override configuration
        override_reason_txt: Reason for override (if overridden=True)
        schema: Database schema

    Returns:
        Job configuration ID

    Raises:
        JobError: If creation fails
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise JobError(f"Invalid batch_id: {batch_id}")

    if not isinstance(configuration_id, int) or configuration_id <= 0:
        raise JobError(f"Invalid configuration_id: {configuration_id}")

    if not isinstance(job_configuration_data, dict):
        raise JobError("job_configuration_data must be a dictionary")

    try:
        query = """
            INSERT INTO irp_job_configuration
            (batch_id, configuration_id, job_configuration_data, skipped, overridden, override_reason_txt)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        job_config_id = execute_insert(
            query,
            (batch_id, configuration_id, json.dumps(job_configuration_data),
             skipped, overridden, override_reason_txt),
            schema=schema
        )
        return job_config_id
    except DatabaseError as e:
        raise JobError(f"Failed to create job configuration: {str(e)}")


def _create_job(
    batch_id: int,
    job_configuration_id: int,
    parent_job_id: Optional[int] = None,
    schema: str = 'public'
) -> int:
    """
    Create job record in INITIATED status.

    Args:
        batch_id: Batch ID
        job_configuration_id: Job configuration ID
        parent_job_id: Parent job ID (if resubmission)
        schema: Database schema

    Returns:
        Job ID

    Raises:
        JobError: If creation fails
    """
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise JobError(f"Invalid batch_id: {batch_id}")

    if not isinstance(job_configuration_id, int) or job_configuration_id <= 0:
        raise JobError(f"Invalid job_configuration_id: {job_configuration_id}")

    if parent_job_id is not None and (not isinstance(parent_job_id, int) or parent_job_id <= 0):
        raise JobError(f"Invalid parent_job_id: {parent_job_id}")

    try:
        query = """
            INSERT INTO irp_job (batch_id, job_configuration_id, parent_job_id, status)
            VALUES (%s, %s, %s, %s)
        """
        job_id = execute_insert(
            query,
            (batch_id, job_configuration_id, parent_job_id, JobStatus.INITIATED),
            schema=schema
        )
        return job_id
    except DatabaseError as e:
        raise JobError(f"Failed to create job: {str(e)}")


def _submit_job(job_id: int, job_config: Dict[str, Any]) -> Tuple[str, Dict, Dict]:
    """
    Submit job to Moody's workflow API (stubbed for now).

    Args:
        job_id: Job ID
        job_config: Job configuration data

    Returns:
        Tuple of (workflow_id, request_json, response_json)

    NOTE: Currently returns random workflow_id and empty dicts.
          Will be replaced with actual Moody's API call.
    """
    # TODO: Replace with actual Moody's API call
    # For now, generate random workflow_id
    workflow_id = f"MW-{random.randint(100000, 999999)}"

    # Stub request/response
    request_json = {
        'job_id': job_id,
        'configuration': job_config,
        'submitted_at': datetime.now().isoformat()
    }

    response_json = {
        'workflow_id': workflow_id,
        'status': 'ACCEPTED',
        'message': 'Job submitted successfully (stub)'
    }

    return workflow_id, request_json, response_json


def _register_job_submission(
    job_id: int,
    workflow_id: str,
    request: Dict[str, Any],
    response: Dict[str, Any],
    submitted_ts: datetime,
    schema: str = 'public'
) -> None:
    """
    Register job submission in database.

    Updates job with:
    - moodys_workflow_id
    - status = SUBMITTED
    - submitted_ts
    - submission_request
    - submission_response

    Args:
        job_id: Job ID
        workflow_id: Moody's workflow ID
        request: Submission request JSON
        response: Submission response JSON
        submitted_ts: Timestamp of submission
        schema: Database schema

    Raises:
        JobError: If update fails
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    if not workflow_id or not isinstance(workflow_id, str):
        raise JobError(f"Invalid workflow_id: {workflow_id}")

    try:
        query = """
            UPDATE irp_job
            SET moodys_workflow_id = %s,
                status = %s,
                submitted_ts = %s,
                submission_request = %s,
                submission_response = %s,
                updated_ts = NOW()
            WHERE id = %s
        """
        execute_command(
            query,
            (workflow_id, JobStatus.SUBMITTED, submitted_ts,
             json.dumps(request), json.dumps(response), job_id),
            schema=schema
        )
    except DatabaseError as e:
        raise JobError(f"Failed to register job submission: {str(e)}")


def _insert_tracking_log(
    job_id: int,
    workflow_id: str,
    job_status: str,
    tracking_data: Dict[str, Any],
    schema: str = 'public'
) -> int:
    """
    Insert job tracking log entry.

    Args:
        job_id: Job ID
        workflow_id: Moody's workflow ID
        job_status: Job status from tracking
        tracking_data: Response from Moody's API
        schema: Database schema

    Returns:
        Tracking log ID

    Raises:
        JobError: If insert fails
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    try:
        query = """
            INSERT INTO irp_job_tracking_log
            (job_id, moodys_workflow_id, job_status, tracking_data)
            VALUES (%s, %s, %s, %s)
        """
        tracking_id = execute_insert(
            query,
            (job_id, workflow_id, job_status, json.dumps(tracking_data)),
            schema=schema
        )
        return tracking_id
    except DatabaseError as e:
        raise JobError(f"Failed to insert tracking log: {str(e)}")


# ============================================================================
# CORE CRUD OPERATIONS
# ============================================================================

def read_job(job_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Read job by ID.

    Args:
        job_id: Job ID
        schema: Database schema

    Returns:
        Dictionary with job details

    Raises:
        JobError: If job not found
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}. Must be a positive integer.")

    query = """
        SELECT id, batch_id, job_configuration_id, moodys_workflow_id,
               status, skipped, last_error, parent_job_id,
               submitted_ts, completed_ts, last_tracked_ts,
               created_ts, updated_ts,
               submission_request, submission_response
        FROM irp_job
        WHERE id = %s
    """

    df = execute_query(query, (job_id,), schema=schema)

    if df.empty:
        raise JobError(f"Job with id {job_id} not found")

    job = df.iloc[0].to_dict()

    # Parse JSON fields if they're strings
    if isinstance(job.get('submission_request'), str):
        job['submission_request'] = json.loads(job['submission_request'])
    if isinstance(job.get('submission_response'), str):
        job['submission_response'] = json.loads(job['submission_response'])

    return job


def update_job_status(
    job_id: int,
    status: str,
    schema: str = 'public'
) -> bool:
    """
    Update job status with validation.

    Args:
        job_id: Job ID
        status: New status (must be valid JobStatus)
        schema: Database schema

    Returns:
        True if status was updated, False if status unchanged

    Raises:
        JobError: If job not found or invalid status
    """
    # Validate job_id
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}. Must be a positive integer.")

    # Validate status
    if status not in JobStatus.all():
        raise JobError(
            f"Invalid status: {status}. Must be one of {JobStatus.all()}"
        )

    # Read current job
    current_job = read_job(job_id, schema=schema)

    # If status is the same, no update needed
    if current_job['status'] == status:
        return False

    # Update status and timestamp
    query = """
        UPDATE irp_job
        SET status = %s,
            completed_ts = CASE WHEN %s IN (%s, %s, %s) THEN NOW() ELSE completed_ts END,
            last_tracked_ts = NOW(),
            updated_ts = NOW()
        WHERE id = %s
    """

    rows = execute_command(
        query,
        (status, status, JobStatus.FINISHED, JobStatus.FAILED,
         JobStatus.CANCELLED, job_id),
        schema=schema
    )

    return rows > 0


def get_job_config(job_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Get job configuration for a job.

    Args:
        job_id: Job ID
        schema: Database schema

    Returns:
        Dictionary with job configuration details including configuration_data

    Raises:
        JobError: If job not found
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    # Get job to find job_configuration_id
    job = read_job(job_id, schema=schema)

    query = """
        SELECT id, batch_id, configuration_id, job_configuration_data,
               skipped, overridden, override_reason_txt,
               created_ts, updated_ts
        FROM irp_job_configuration
        WHERE id = %s
    """

    df = execute_query(query, (job['job_configuration_id'],), schema=schema)

    if df.empty:
        raise JobError(f"Job configuration not found for job {job_id}")

    config = df.iloc[0].to_dict()

    # Parse JSON configuration data if it's a string
    if isinstance(config.get('job_configuration_data'), str):
        config['job_configuration_data'] = json.loads(config['job_configuration_data'])

    return config


# ============================================================================
# JOB CREATION
# ============================================================================

def create_job(
    batch_id: int,
    configuration_id: int,
    job_configuration_id: Optional[int] = None,
    job_configuration_data: Optional[Dict[str, Any]] = None,
    parent_job_id: Optional[int] = None,
    validate: bool = False,
    schema: str = 'public'
) -> int:
    """
    Create a new job with either existing or new job configuration.

    Process:
    - Requires exactly one of: job_configuration_id OR job_configuration_data
    - If job_configuration_id: Reuse existing configuration
    - If job_configuration_data: Create new configuration (with optional validation)
    - Create job in INITIATED status

    Args:
        batch_id: Batch ID
        configuration_id: Master configuration ID
        job_configuration_id: Reuse existing job config (mutually exclusive with job_configuration_data)
        job_configuration_data: Create new job config (mutually exclusive with job_configuration_id)
        parent_job_id: Parent job ID if this is a resubmission
        validate: Whether to validate configuration data against batch type
        schema: Database schema

    Returns:
        New job ID

    Raises:
        JobError: If neither or both config parameters provided
        JobError: If validation fails (when validate=True)
    """
    # Validate exactly one of job_configuration_id or job_configuration_data is provided
    if (job_configuration_id is None) == (job_configuration_data is None):
        raise JobError(
            "Must provide exactly one of: job_configuration_id or job_configuration_data. "
            f"Got job_configuration_id={job_configuration_id}, job_configuration_data={'provided' if job_configuration_data else 'None'}"
        )

    # Validate inputs
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise JobError(f"Invalid batch_id: {batch_id}")

    if not isinstance(configuration_id, int) or configuration_id <= 0:
        raise JobError(f"Invalid configuration_id: {configuration_id}")

    # Determine which mode we're in
    if job_configuration_id is not None:
        # Mode 1: Reuse existing configuration
        if not isinstance(job_configuration_id, int) or job_configuration_id <= 0:
            raise JobError(f"Invalid job_configuration_id: {job_configuration_id}")

        config_id = job_configuration_id

    else:
        # Mode 2: Create new configuration
        if validate:
            # TODO: Implement validation against batch type
            # For now, always pass validation
            # In future: get batch_type from batch_id, call validator
            pass

        # Determine if this is an override
        overridden = parent_job_id is not None

        config_id = _create_job_configuration(
            batch_id=batch_id,
            configuration_id=configuration_id,
            job_configuration_data=job_configuration_data,
            skipped=False,
            overridden=overridden,
            override_reason_txt="User-provided override" if overridden else None,
            schema=schema
        )

    # Create job
    job_id = _create_job(batch_id, config_id, parent_job_id, schema=schema)

    return job_id


def skip_job(job_id: int, schema: str = 'public') -> None:
    """
    Mark job as skipped.

    Args:
        job_id: Job ID
        schema: Database schema

    Raises:
        JobError: If job not found
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    # Verify job exists
    read_job(job_id, schema=schema)

    try:
        query = """
            UPDATE irp_job
            SET skipped = TRUE, updated_ts = NOW()
            WHERE id = %s
        """
        execute_command(query, (job_id,), schema=schema)
    except DatabaseError as e:
        raise JobError(f"Failed to skip job: {str(e)}")


# ============================================================================
# JOB SUBMISSION AND TRACKING
# ============================================================================

def submit_job(
    job_id: int,
    force: bool = False,
    track_immediately: bool = False,
    schema: str = 'public'
) -> int:
    """
    Submit job to Moody's workflow system.

    Process:
    1. Read job details
    2. Check if already submitted (has moodys_workflow_id)
    3. If not submitted OR force=True:
       - Get job configuration
       - Call _submit_job (stubbed API call)
       - Register submission via _register_job_submission
    4. If track_immediately: Call track_job_status

    Args:
        job_id: Job ID
        force: Resubmit even if already submitted
        track_immediately: Track status after submission
        schema: Database schema

    Returns:
        Job ID

    Raises:
        JobError: If job not found

    TODO:

    1. Implement error check for job submission. If the submission succeeds, 
       job should be updated to SUBMITTED status. If failed, it should be set 
       to ERROR status
    2. Implement error check when resubmitting a failed job. If the submission succeeds, 
       job should be updated to SUBMITTED status. If failed, it should be set 
       to ERROR status
    3. Enhance batch recon. If any job is in ERROR status, the batch should be set 
       to ERROR status

    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    # Read job
    job = read_job(job_id, schema=schema)

    # Check if already submitted
    already_submitted = job.get('moodys_workflow_id') is not None

    if not already_submitted or force:
        # Get job configuration
        job_config = get_job_config(job_id, schema=schema)

        # Submit job (stubbed)
        workflow_id, request, response = _submit_job(
            job_id,
            job_config['job_configuration_data']
        )

        # Register submission
        _register_job_submission(
            job_id,
            workflow_id,
            request,
            response,
            datetime.now(),
            schema=schema
        )


    # Optionally track immediately
    if track_immediately:
        time.sleep(5)
        track_job_status(job_id, schema=schema)

    return job_id


def track_job_status(
    job_id: int,
    moodys_workflow_id: Optional[str] = None,
    schema: str = 'public'
) -> str:
    """
    Track job status on Moody's workflow system.

    Process:
    1. Read job (or use provided workflow_id)
    2. Call Moody's API to get current status (STUBBED)
    3. Insert tracking log entry
    4. Update job status if changed

    STUB BEHAVIOR (for testing):
    - SUBMITTED → QUEUED or PENDING
    - QUEUED → PENDING or RUNNING
    - PENDING → RUNNING
    - RUNNING → RUNNING, COMPLETED, or FAILED (random)

    Args:
        job_id: Job ID
        moodys_workflow_id: Optional workflow ID (uses job's if None)
        schema: Database schema

    Returns:
        Current job status

    Raises:
        JobError: If job not found or has no workflow_id

    NOTE: Currently uses realistic stub. Will be replaced with actual Moody's API.
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    # Read job
    job = read_job(job_id, schema=schema)

    # Get workflow_id
    workflow_id = moodys_workflow_id or job.get('moodys_workflow_id')

    if not workflow_id:
        raise JobError(
            f"Job {job_id} has no moodys_workflow_id. "
            "Job must be submitted before tracking."
        )

    # Get current status
    current_status = job['status']

    # TODO: Replace with actual Moody's API call
    # For now, use realistic stub transitions
    transitions = {
        JobStatus.SUBMITTED: [JobStatus.QUEUED, JobStatus.PENDING],
        JobStatus.QUEUED: [JobStatus.PENDING, JobStatus.RUNNING],
        JobStatus.PENDING: [JobStatus.RUNNING],
        JobStatus.RUNNING: [JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.FINISHED, JobStatus.FAILED],
    }

    if current_status in transitions:
        new_status = random.choice(transitions[current_status])
    else:
        # Already in terminal state or unknown state
        new_status = current_status

    # Stub response data
    tracking_data = {
        'workflow_id': workflow_id,
        'status': new_status,
        'polled_at': datetime.now().isoformat(),
        'message': f'Status transition: {current_status} -> {new_status} (stub)'
    }

    # Insert tracking log
    _insert_tracking_log(job_id, workflow_id, new_status, tracking_data, schema=schema)

    # Update job status if changed
    if new_status != current_status:
        update_job_status(job_id, new_status, schema=schema)

    return new_status


# ============================================================================
# JOB RESUBMISSION
# ============================================================================

def resubmit_job(
    job_id: int,
    job_configuration_data: Optional[Dict[str, Any]] = None,
    override_reason: Optional[str] = None,
    schema: str = 'public'
) -> int:
    """
    Resubmit a job with optional configuration override.

    Process:
    1. Validate: If override data provided, reason is required
    2. Read original job
    3. Create new job:
       - With override: Create new configuration with override data
       - Without override: Reuse original configuration
       - Set parent_job_id to original job
    4. Skip original job (only after new job created successfully)

    Args:
        job_id: Original job ID to resubmit
        job_configuration_data: Optional override configuration
        override_reason: Required if job_configuration_data provided
        schema: Database schema

    Returns:
        New job ID

    Raises:
        JobError: If job not found
        JobError: If override_reason missing when job_configuration_data provided
    """
    if not isinstance(job_id, int) or job_id <= 0:
        raise JobError(f"Invalid job_id: {job_id}")

    # Validate inputs
    if job_configuration_data is not None and not override_reason:
        raise JobError(
            "override_reason is required when providing job_configuration_data"
        )

    # Read original job
    job = read_job(job_id, schema=schema)
    batch_id = job['batch_id']

    # Get configuration_id from job_configuration
    job_config = get_job_config(job_id, schema=schema)
    configuration_id = job_config['configuration_id']

    parent_job_id = job_id

    # Create new job
    if job_configuration_data:
        # Override case: Create new job with override configuration
        # Update override reason in the _create_job_configuration call
        new_config_id = _create_job_configuration(
            batch_id=batch_id,
            configuration_id=configuration_id,
            job_configuration_data=job_configuration_data,
            skipped=False,
            overridden=True,
            override_reason_txt=override_reason,
            schema=schema
        )

        new_job_id = _create_job(
            batch_id=batch_id,
            job_configuration_id=new_config_id,
            parent_job_id=parent_job_id,
            schema=schema
        )

        # Submit the job
        submit_job(new_job_id, schema=schema)
        # TODO Handle submission error
        
    else:
        # Reuse same config
        job_config_id = job['job_configuration_id']

        new_job_id = _create_job(
            batch_id=batch_id,
            job_configuration_id=job_config_id,
            parent_job_id=parent_job_id,
            schema=schema
        )

        # Submit the job
        submit_job(new_job_id, schema=schema)
        # TODO Handle submission error

    # Skip original job after new job is created
    skip_job(job_id, schema=schema)

    return new_job_id
