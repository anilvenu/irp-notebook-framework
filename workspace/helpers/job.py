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
from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from helpers.irp_integration import IRPClient

from helpers.database import (
    execute_query, execute_command, execute_insert, DatabaseError
)
from helpers.constants import JobStatus


class JobError(Exception):
    """Custom exception for job operation errors"""
    pass


# ============================================================================
# JOB CONFIGURATION CRUD OPERATIONS (Layer 2)
# ============================================================================

def create_job_configuration(
    batch_id: int,
    configuration_id: int,
    job_configuration_data: Dict[str, Any],
    skipped: bool = False,
    overridden: bool = False,
    override_reason_txt: Optional[str] = None,
    parent_job_configuration_id: Optional[int] = None,
    schema: str = 'public'
) -> int:
    """
    Create job configuration record.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        batch_id: Batch ID
        configuration_id: Master configuration ID
        job_configuration_data: Configuration data for this specific job
        skipped: Whether this configuration is skipped
        overridden: Whether this is an override configuration
        override_reason_txt: Reason for override (if overridden=True)
        parent_job_configuration_id: Parent job configuration ID (if this is an override)
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

    if parent_job_configuration_id is not None:
        if not isinstance(parent_job_configuration_id, int) or parent_job_configuration_id <= 0:
            raise JobError(f"Invalid parent_job_configuration_id: {parent_job_configuration_id}")

    try:
        query = """
            INSERT INTO irp_job_configuration
            (batch_id, configuration_id, job_configuration_data, skipped, overridden, override_reason_txt, parent_job_configuration_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        job_config_id = execute_insert(
            query,
            (batch_id, configuration_id, json.dumps(job_configuration_data),
             skipped, overridden, override_reason_txt, parent_job_configuration_id),
            schema=schema
        )
        return job_config_id
    except DatabaseError as e:  # pragma: no cover
        raise JobError(f"Failed to create job configuration: {str(e)}") # pragma: no cover


def create_job(
    batch_id: int,
    job_configuration_id: int,
    parent_job_id: Optional[int] = None,
    schema: str = 'public'
) -> int:
    """
    Create job record in INITIATED status.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

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
    except DatabaseError as e: # pragma: no cover
        raise JobError(f"Failed to create job: {str(e)}")   # pragma: no cover


def _submit_job(job_id: int, job_config: Dict[str, Any], batch_type: str, irp_client: 'IRPClient') -> Tuple[Optional[str], Dict, Dict]:
    """
    Submit job to Moody's workflow API.

    Routes to appropriate submission handler based on batch type.

    Args:
        job_id: Job ID
        job_config: Job configuration data
        batch_type: Type of batch (from irp_batch.batch_type)
        irp_client: IRPClient instance

    Returns:
        Tuple of (workflow_id, request_json, response_json)
        Returns (None, request_json, error_response) on failure
    """
    try:
        # Route to appropriate submission handler based on batch type
        if batch_type == 'EDM Creation':
            workflow_id, request_json, response_json = _submit_edm_creation_job(
                job_id, job_config, irp_client
            )
        elif batch_type == 'Portfolio Creation':
            workflow_id, request_json, response_json = _submit_portfolio_creation_job(
                job_id, job_config, irp_client
            )
        elif batch_type == 'MRI Import':
            workflow_id, request_json, response_json = _submit_mri_import_job(
                job_id, job_config, irp_client
            )
        elif batch_type == 'Analysis':
            workflow_id, request_json, response_json = _submit_analysis_job(
                job_id, job_config, irp_client
            )
        # Add more batch types as needed
        else:
            raise ValueError(f"Unsupported batch type: {batch_type}")

        return workflow_id, request_json, response_json

    except Exception as e:
        # Build generic error response
        request_json = {
            'job_id': job_id,
            'batch_type': batch_type,
            'configuration': job_config,
            'submitted_at': datetime.now().isoformat()
        }

        response_json = {
            'status': 'ERROR',
            'error': str(e),
            'error_type': type(e).__name__,
            'message': f'Job submission failed: {str(e)}'
        }

        # Return None for workflow_id on error
        return None, request_json, response_json


def _submit_edm_creation_job(
    job_id: int,
    job_config: Dict[str, Any],
    client: 'IRPClient'
) -> Tuple[str, Dict, Dict]:
    """
    Submit EDM Creation job to Moody's API.

    Follows the pattern from irp_integration.edm.submit_create_edm_jobs:
    - Lookup database server by name
    - Search or create exposure set
    - Submit EDM creation job

    Args:
        job_id: Job ID
        job_config: Job configuration data
        client: IRPClient instance

    Returns:
        Tuple of (workflow_id, request_json, response_json)
    """
    edm_name = job_config.get('Database')
    if not edm_name:
        raise ValueError("Missing required field: Database")

    # Submit EDM creation job
    moody_job_id = client.edm.submit_create_edm_job(
        edm_name=edm_name
    )

    # Build workflow ID
    workflow_id = str(moody_job_id)

    # Build request/response structures
    request_json = {
        'job_id': job_id,
        'batch_type': 'EDM Creation',
        'configuration': job_config,
        'api_request': {
            'edm_name': edm_name
        },
        'submitted_at': datetime.now().isoformat()
    }

    response_json = {
        'workflow_id': workflow_id,
        'moody_job_id': moody_job_id,
        'status': 'ACCEPTED',
        'message': 'EDM creation job submitted successfully'
    }

    return workflow_id, request_json, response_json


def _submit_portfolio_creation_job(
    job_id: int,
    job_config: Dict[str, Any],
    client: 'IRPClient'
) -> Tuple[str, Dict, Dict]:
    """
    Submit Portfolio Creation job to Moody's API.

    TODO: Implement portfolio creation submission logic.

    Args:
        job_id: Job ID
        job_config: Job configuration data
        client: IRPClient instance

    Returns:
        Tuple of (workflow_id, request_json, response_json)
    """
    raise NotImplementedError("Portfolio Creation job submission not yet implemented")


def _submit_mri_import_job(
    job_id: int,
    job_config: Dict[str, Any],
    client: 'IRPClient'
) -> Tuple[str, Dict, Dict]:
    """
    Submit MRI Import job to Moody's API.

    TODO: Implement MRI import submission logic.

    Args:
        job_id: Job ID
        job_config: Job configuration data
        client: IRPClient instance

    Returns:
        Tuple of (workflow_id, request_json, response_json)
    """
    raise NotImplementedError("MRI Import job submission not yet implemented")


def _submit_analysis_job(
    job_id: int,
    job_config: Dict[str, Any],
    client: 'IRPClient'
) -> Tuple[str, Dict, Dict]:
    """
    Submit Analysis job to Moody's API.

    TODO: Implement analysis submission logic.

    Args:
        job_id: Job ID
        job_config: Job configuration data
        client: IRPClient instance

    Returns:
        Tuple of (workflow_id, request_json, response_json)
    """
    raise NotImplementedError("Analysis job submission not yet implemented")


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
    except DatabaseError as e: # pragma: no cover
        raise JobError(f"Failed to register job submission: {str(e)}") # pragma: no cover


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
    except DatabaseError as e: # pragma: no cover
        raise JobError(f"Failed to insert tracking log: {str(e)}") # pragma: no cover


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
               parent_job_configuration_id, skipped_reason_txt, override_job_configuration_id,
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
# JOB WORKFLOW OPERATIONS (Layer 3)
# ============================================================================

def create_job_with_config(
    batch_id: int,
    configuration_id: int,
    job_configuration_data: Dict[str, Any],
    parent_job_id: Optional[int] = None,
    validate: bool = False,
    schema: str = 'public'
) -> int:
    """
    Create a new job with new job configuration atomically.

    LAYER: 3 (Workflow)

    TRANSACTION BEHAVIOR:
        - Uses transaction_context() to ensure atomicity
        - Both configuration and job are created in single transaction
        - If either fails, both are rolled back

    Process:
    - Creates new job configuration
    - Creates job in INITIATED status
    - Both operations are atomic (all-or-nothing)

    Args:
        batch_id: Batch ID
        configuration_id: Master configuration ID
        job_configuration_data: Configuration data for this specific job
        parent_job_id: Parent job ID if this is a resubmission
        validate: Whether to validate configuration data against batch type
        schema: Database schema

    Returns:
        New job ID

    Raises:
        JobError: If validation fails (when validate=True)
        JobError: If creation fails
    """
    # Validate inputs
    if not isinstance(batch_id, int) or batch_id <= 0:
        raise JobError(f"Invalid batch_id: {batch_id}")

    if not isinstance(configuration_id, int) or configuration_id <= 0:
        raise JobError(f"Invalid configuration_id: {configuration_id}")

    if validate:
        # TODO: Implement validation against batch type
        # For now, always pass validation
        # In future: get batch_type from batch_id, call validator
        pass

    # Determine if this is an override
    overridden = parent_job_id is not None

    # Use transaction to ensure atomicity
    # If job creation fails, configuration is rolled back (no orphaned config)
    from helpers.database import transaction_context

    with transaction_context(schema=schema):
        config_id = create_job_configuration(
            batch_id=batch_id,
            configuration_id=configuration_id,
            job_configuration_data=job_configuration_data,
            skipped=False,
            overridden=overridden,
            override_reason_txt="User-provided override" if overridden else None,
            schema=schema
        )

        # Create job
        job_id = create_job(batch_id, config_id, parent_job_id, schema=schema)

        # Both committed together at end of context
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
    except DatabaseError as e:  # pragma: no cover
        raise JobError(f"Failed to skip job: {str(e)}")


def skip_job_configuration(
    job_configuration_id: int,
    skipped_reason_txt: str,
    override_job_configuration_id: Optional[int] = None,
    schema: str = 'public'
) -> None:
    """
    Mark job configuration as skipped.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    This function can be called:
    1. Standalone for manual skipping (with override_job_configuration_id=None)
    2. As part of resubmit_job() workflow (with override_job_configuration_id set)

    Args:
        job_configuration_id: Job configuration ID to skip
        skipped_reason_txt: Mandatory reason for skipping (user input)
        override_job_configuration_id: Optional ID of override configuration (if applicable)
        schema: Database schema

    Raises:
        JobError: If configuration not found or update fails
        JobError: If skipped_reason_txt is empty
    """
    if not isinstance(job_configuration_id, int) or job_configuration_id <= 0:
        raise JobError(f"Invalid job_configuration_id: {job_configuration_id}")

    if not skipped_reason_txt or not isinstance(skipped_reason_txt, str) or not skipped_reason_txt.strip():
        raise JobError("skipped_reason_txt is required and must be a non-empty string")

    if override_job_configuration_id is not None:
        if not isinstance(override_job_configuration_id, int) or override_job_configuration_id <= 0:
            raise JobError(f"Invalid override_job_configuration_id: {override_job_configuration_id}")

    try:
        query = """
            UPDATE irp_job_configuration
            SET skipped = TRUE,
                skipped_reason_txt = %s,
                override_job_configuration_id = %s,
                updated_ts = NOW()
            WHERE id = %s
        """
        rows = execute_command(
            query,
            (skipped_reason_txt, override_job_configuration_id, job_configuration_id),
            schema=schema
        )

        if rows == 0:
            raise JobError(f"Job configuration with id {job_configuration_id} not found")

    except DatabaseError as e:  # pragma: no cover
        raise JobError(f"Failed to skip job configuration: {str(e)}")  # pragma: no cover 


# ============================================================================
# JOB SUBMISSION AND TRACKING
# ============================================================================

def submit_job(
    job_id: int,
    batch_type: str,
    irp_client: 'IRPClient',
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
        batch_type: Batch Type
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

        # Submit job
        workflow_id, request, response = _submit_job(
            job_id,
            job_config['job_configuration_data'],
            batch_type,
            irp_client
        )

        # Check if submission succeeded
        if workflow_id is None:
            # Submission failed - set job to ERROR status
            update_job_status(job_id, JobStatus.ERROR, schema=schema)

            # Get error message and raise to caller
            error_msg = response.get('error', 'Unknown submission error')
            raise JobError(f"Job submission failed: {error_msg}")

        # Submission succeeded - proceed with registration
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
        track_job_status(job_id, irp_client, schema=schema)

    return job_id


def track_job_status(
    job_id: int,
    irp_client: 'IRPClient',
    moodys_workflow_id: Optional[str] = None,
    schema: str = 'public'
) -> str:
    """
    Track job status on Moody's workflow system.

    Process:
    1. Read job (or use provided workflow_id)
    2. Call Moody's API to get current status
    3. Insert tracking log entry
    4. Update job status if changed

    Args:
        job_id: Job ID
        irp_client: IRPClient instance for API calls
        moodys_workflow_id: Optional workflow ID (uses job's if None)
        schema: Database schema

    Returns:
        Current job status

    Raises:
        JobError: If job not found, has no workflow_id, or API call fails
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

    # Poll Moody's API for current job status
    from helpers.irp_integration.exceptions import IRPAPIError

    try:
        # Get job status from Moody's
        # Note: workflow_id from our DB is Moody's job_id
        job_data = irp_client.job.get_risk_data_job(int(workflow_id))

        # Extract status and full data
        new_status = job_data['status']
        tracking_data = job_data  # Store full API response

    except IRPAPIError as e:
        # API error - don't change job status, raise error
        raise JobError(f"Failed to track job {job_id} from Moody's API: {str(e)}")
    except ValueError as e:
        # Invalid workflow_id format
        raise JobError(f"Invalid workflow_id format for job {job_id}: {workflow_id}")
    except Exception as e:
        # Other unexpected error
        raise JobError(f"Unexpected error tracking job {job_id}: {str(e)}")

    # Insert tracking log
    _insert_tracking_log(job_id, workflow_id, new_status, tracking_data, schema=schema)

    # Update job status if changed
    if new_status != current_status:
        update_job_status(job_id, new_status, schema=schema)

    return new_status


# ============================================================================
# JOB RESUBMISSION (Layer 3)
# ============================================================================

def resubmit_job(
    job_id: int,
    irp_client: 'IRPClient',
    batch_type: str,
    job_configuration_data: Optional[Dict[str, Any]] = None,
    override_reason: Optional[str] = None,
    schema: str = 'public'
) -> int:
    """
    Resubmit a job with optional configuration override, atomically.

    LAYER: 3 (Workflow)

    TRANSACTION BEHAVIOR:
        - Uses transaction_context() to ensure atomicity
        - Creates new job (and optionally new config) + skips original job
        - All operations are atomic (all-or-nothing)
        - Submits new job AFTER transaction completes

    Process:
    1. Validate: If override data provided, reason is required
    2. Read original job
    3. Create new job:
       - With override: Create new configuration with override data
       - Without override: Reuse original configuration
       - Set parent_job_id to original job
    4. Skip original job (only after new job created successfully)
    5. Submit new job (after transaction commits)

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

    # Use transaction to ensure atomicity:
    # - Create new job (and optionally new config)
    # - Skip original job
    # If any step fails, all operations are rolled back
    from helpers.database import transaction_context

    with transaction_context(schema=schema):
        # Create new job
        if job_configuration_data:
            # Override case: Create new job configuration with parent reference
            original_config_id = job['job_configuration_id']

            # Create NEW job configuration with parent reference
            new_config_id = create_job_configuration(
                batch_id=batch_id,
                configuration_id=configuration_id,
                job_configuration_data=job_configuration_data,
                skipped=False,
                overridden=True,
                override_reason_txt=override_reason,
                parent_job_configuration_id=original_config_id,
                schema=schema
            )

            # Skip ORIGINAL job configuration (before creating new job)
            skip_job_configuration(
                job_configuration_id=original_config_id,
                skipped_reason_txt=override_reason,
                override_job_configuration_id=new_config_id,
                schema=schema
            )

            # Create new job with new configuration
            new_job_id = create_job(
                batch_id=batch_id,
                job_configuration_id=new_config_id,
                parent_job_id=parent_job_id,
                schema=schema
            )

        else:
            # Reuse same config (no override)
            job_config_id = job['job_configuration_id']

            new_job_id = create_job(
                batch_id=batch_id,
                job_configuration_id=job_config_id,
                parent_job_id=parent_job_id,
                schema=schema
            )

        # Skip original job (must happen within same transaction)
        skip_job(job_id, schema=schema)

        # All database operations committed together at end of context

    # Submit the job AFTER transaction completes successfully
    # Job submission involves external API call, so keep it outside transaction
    submit_job(new_job_id, batch_type=batch_type, irp_client=irp_client, schema=schema)
    # TODO Handle submission error

    return new_job_id
