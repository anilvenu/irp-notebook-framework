# Proposal: Fix Job Error Status Handling

## Issue Summary

When a job submission fails (e.g., due to a code error like `AttributeError`), the job incorrectly ends up in `SUBMITTED` status with `moodys_workflow_id = 'ERROR'` instead of being in `ERROR` status.

This breaks idempotency - re-running a notebook won't retry failed jobs because they appear to have already been submitted.

## Current Design (from CLAUDE.md)

The Job Status flow is already documented:

```
INITIATED → SUBMITTED → QUEUED → PENDING → RUNNING → FINISHED
                                                    → FAILED
                                                    → CANCELLED
            → ERROR (submission failure)
```

**Status definitions per existing design:**

| Status | Meaning | Workflow ID |
|--------|---------|-------------|
| `ERROR` | Submission failure (never reached Moody's) | Should be NULL |
| `FAILED` | Job was submitted, Moody's reported failure | Valid Moody's ID |

**`ready_for_submit()` already includes ERROR** (constants.py line 143-145):
```python
def ready_for_submit(cls):
    """Statuses that are ready for submission (including retry of failed submissions)"""
    return [cls.INITIATED, cls.FAILED, cls.ERROR]
```

The design is correct - the implementation is broken.

## Root Cause

In `workspace/helpers/job.py`, the `submit_job()` function has conflicting logic:

```python
# Line ~1849-1865
if workflow_id is None:
    # Submission failed - set job to ERROR status
    update_job_status(job_id, JobStatus.ERROR, schema=schema)  # ✓ Sets ERROR

    # Store submission info for audit trail even though job failed
    _register_job_submission(
        job_id,
        workflow_id='ERROR',  # ✗ Fake workflow ID
        request=request,
        response=response,
        ...
    )
```

The problem: `_register_job_submission()` always sets `status = SUBMITTED`, overwriting the `ERROR` status:

```python
# _register_job_submission always does:
SET status = SUBMITTED  # ✗ Overwrites ERROR!
```

**Result:** Job ends up with `status=SUBMITTED`, `moodys_workflow_id='ERROR'`

## When This Was Introduced

Commit `6e49e28` on Dec 23, 2025 ("error handling") added the `_register_job_submission` call for failed jobs to preserve request/response data for debugging. The intent was good (audit trail), but it broke the status handling.

## Impact

1. **Jobs stuck in SUBMITTED status** instead of ERROR
2. **No automatic retry**: `submit_job()` sees non-empty `moodys_workflow_id` and skips
3. **Breaks idempotency**: Re-running notebooks doesn't retry failed jobs
4. **Confusing UI**: Jobs show "ERROR" as their Moody's Job ID

## Proposed Fix

Don't call `_register_job_submission` for failed jobs. Instead, directly update request/response fields while keeping `moodys_workflow_id` NULL:

```python
if workflow_id is None:
    # Submission failed - set job to ERROR status
    update_job_status(job_id, JobStatus.ERROR, schema=schema)

    # Store submission info for audit trail (without changing status/workflow_id)
    execute_command(
        """UPDATE irp_job
           SET submission_request = %s,
               submission_response = %s,
               last_error = %s,
               updated_ts = NOW()
           WHERE id = %s""",
        (json.dumps(request), json.dumps(response), response.get('error'), job_id),
        schema=schema
    )

    # Raise to caller
    error_msg = response.get('error', 'Unknown submission error')
    raise JobError(f"Job submission failed: {error_msg}")
```

**Key points:**
- `moodys_workflow_id` remains NULL (we never got one)
- `status` stays ERROR (set by `update_job_status`)
- `submission_request/response` saved for debugging
- `last_error` populated with error message

## Why This Aligns With Existing Design

1. **Status flow matches CLAUDE.md**: `INITIATED → ERROR` for submission failures
2. **`ready_for_submit()` already handles it**: ERROR jobs will be retried
3. **`already_submitted` check works**: NULL workflow_id means not submitted
4. **Batch recon handles it**: ERROR jobs are counted in reconciliation

## Testing

1. Create a batch with jobs that will fail (e.g., code error)
2. Run `submit_batch()` - verify jobs are `ERROR` status with `NULL` workflow_id
3. Fix the underlying issue
4. Re-run `submit_batch()` - verify jobs are retried and complete

## Files to Modify

- `workspace/helpers/job.py`: Fix error handling in `submit_job()` (~line 1849-1865)
