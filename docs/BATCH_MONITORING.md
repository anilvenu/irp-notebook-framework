# Batch Monitoring System

Automated job status tracking and workflow progression for Moody's API submissions.

## Overview

The monitoring system polls Moody's for job status updates, reconciles batch completion, and automatically chains workflow steps. The primary interface is the **Monitor Active Jobs** notebook.

**Location:** `workflows/_Tools/Batch Management/Monitor Active Jobs.ipynb`

## Monitoring Workflow

The notebook executes five sequential sections:

```
1. Find Active Batches for the current Active Cycle
   ↓
2. Poll Jobs (track_job_status for each)
   ↓
3. Reconcile Batch Statuses
   ↓
4. Auto-Execute Next Steps (chaining)
   ↓
5. Display Summary
```

### Section 1: Find Active Batches

Queries all batches with status `ACTIVE` in cycles that are also `ACTIVE`:

```python
from helpers.batch import get_active_batches

batches = get_active_batches()
# Returns: [{'batch_id': 1, 'batch_type': 'Analysis', 'submitted_ts': ...}, ...]
```

### Section 2: Poll Jobs

For each active batch, polls Moody's API for current job status:

```python
from helpers.batch import get_batch_jobs
from helpers.job import track_job_status
from helpers.constants import JobStatus

jobs = get_batch_jobs(batch_id, skipped=False)
for job in jobs:
    if job['status'] not in JobStatus.terminal():
        new_status = track_job_status(job['id'], batch_type, irp_client)
```

**Terminal statuses** (polling stops): `FINISHED`, `FAILED`, `CANCELLED`, `ERROR`

**Non-terminal statuses** (continue polling): `SUBMITTED`, `QUEUED`, `PENDING`, `RUNNING`

### Section 3: Reconcile Batch Statuses

After polling, determines final batch status:

```python
from helpers.batch import recon_batch

final_status = recon_batch(batch_id)
# Returns: 'COMPLETED', 'FAILED', 'CANCELLED', 'ERROR', or 'ACTIVE'
```

### Section 4: Auto-Execute Next Steps

For batches reaching terminal status, checks if workflow should auto-progress:

```python
from helpers.step_chain import should_execute_next_step, get_next_step_info
from helpers.notebook_executor import execute_next_step

if should_execute_next_step(batch_id):
    next_step = get_next_step_info(batch_id)
    result = execute_next_step(next_step['notebook_path'])
```

### Section 5: Summary

Displays metrics:
- Batches found and processed
- Jobs polled and status transitions
- Chain executions triggered
- Errors encountered
- Recommended next run time

## Job Polling

### track_job_status()

Polls Moody's API for current job status based on batch type:

```python
from helpers.job import track_job_status
from helpers.irp_integration import IRPClient

irp_client = IRPClient()
status = track_job_status(job_id, batch_type, irp_client)
```

**API Calls by Batch Type:**

| Batch Type | API Method |
|------------|------------|
| EDM Creation | `irp_client.job.get_risk_data_job()` |
| MRI Import | `irp_client.mri_import.get_import_job()` |
| EDM DB Upgrade | `irp_client.job.get_risk_data_job()` |
| GeoHaz | `irp_client.geohaz.get_geohaz_job()` |
| Analysis | `irp_client.analysis.get_analysis_job()` |
| Grouping | `irp_client.analysis.get_analysis_grouping_job()` |
| Grouping Rollup | `irp_client.analysis.get_analysis_grouping_job()` |
| Export to RDM | `irp_client.rdm.get_rdm_export_job()` |

**Tracking Log:**

Every poll is recorded in `irp_job_tracking_log`:
- `job_id`, `tracked_ts`, `moodys_workflow_id`
- `job_status` (status at time of poll)
- `tracking_data` (full API response as JSON)

## Batch Reconciliation

### recon_batch()

Determines batch status based on job states using priority logic:

```
Priority Order:
1. CANCELLED - All non-skipped jobs are CANCELLED
2. ERROR     - Any non-skipped job is ERROR
3. FAILED    - Any non-skipped job is FAILED
4. COMPLETED - All job configs have ≥1 FINISHED job
5. ACTIVE    - Jobs still in progress
```

**Special Cases:**
- Empty batches (0 jobs) → immediately `COMPLETED`
- Skipped jobs are excluded from status determination
- Each job configuration must have at least one successful job

**Notifications:**

Teams notifications sent automatically when batch reaches:
- `FAILED`
- `ERROR`
- `CANCELLED`

**Reconciliation Log:**

Each reconciliation recorded in `irp_batch_recon_log`:
- `batch_id`, `recon_ts`, `recon_result`
- `recon_summary` (JSON with job counts by status)

## Notebook Chaining

Automatic workflow progression based on batch completion.

### Chain Configurations

| Stage | Step Flow | Trigger Condition |
|-------|-----------|-------------------|
| Stage 02 | Step 1 → Step 2 | COMPLETED |
| Stage 03 | Step 1 → 2 → 3 → ... → 8 | COMPLETED |
| Stage 04 | Step 1 → Step 2 | COMPLETED or FAILED |
| Stage 05 | Step 1 → 2 → 3 | Step 2→3: COMPLETED or FAILED |
| Stage 06 | Step 1 → Step 2 | COMPLETED or FAILED |

**Note:** Some chains trigger on `FAILED` to allow analysts to review partial results.

### Chain Functions

```python
from helpers.step_chain import should_execute_next_step, get_next_step_info

# Check if chaining should occur
if should_execute_next_step(batch_id):
    # Get next step details
    info = get_next_step_info(batch_id)
    # Returns: {
    #   'cycle_name': 'Analysis-2025-Q1',
    #   'stage_num': 3,
    #   'step_num': 4,
    #   'notebook_path': Path('...')
    # }
```

### Notebook Execution

```python
from helpers.notebook_executor import execute_notebook

result = execute_notebook(notebook_path, timeout=3600)
# Returns: {
#   'success': bool,
#   'notebook_path': Path,
#   'execution_time': float,
#   'stdout': str,
#   'stderr': str,
#   'error': str or None
# }
```

## Scheduling

The monitoring notebook should run periodically using JupyterLab's built-in Notebook Jobs feature.

### Setting Up a Scheduled Job

1. Open a new Launcher in JupyterLab
2. In the "Other" section, click **Notebook Jobs**
3. Click the **Notebook Job Definitions** tab
   - If nothing is displayed, the notebook is not yet scheduled
4. Navigate to `/workspace/workflows/_Tools/Batch Management`
5. Right-click on **Monitor Active Jobs.ipynb**
6. Select **Create Notebook Job**
7. Configure the job:
   - Uncheck **Output Formats > Notebook** (prevents output file clutter)
   - Under **Schedule**, select **Run on a schedule**
   - Choose the desired interval (typically **Minute** for continuous monitoring)

## Key Functions

### Job Operations (`helpers.job`)

| Function | Purpose |
|----------|---------|
| `track_job_status(job_id, batch_type, irp_client)` | Poll Moody's for job status |
| `read_job(job_id)` | Get job details from database |
| `get_job_config(job_id)` | Get job's configuration data |

### Batch Operations (`helpers.batch`)

| Function | Purpose |
|----------|---------|
| `get_active_batches()` | Get all ACTIVE batches in ACTIVE cycles |
| `get_batch_jobs(batch_id, skipped, status)` | Get jobs with optional filters |
| `recon_batch(batch_id)` | Determine batch status from job states |

### Step Chaining (`helpers.step_chain`)

| Function | Purpose |
|----------|---------|
| `should_execute_next_step(batch_id)` | Check if auto-chain should trigger |
| `get_next_step_info(batch_id)` | Get next step notebook path |
| `get_chain_config(stage_num)` | Get chain rules for stage |

### Notebook Execution (`helpers.notebook_executor`)

| Function | Purpose |
|----------|---------|
| `execute_notebook(path, timeout)` | Run notebook via nbconvert |
| `execute_next_step(path)` | Execute with workflow logging |

## Error Handling

The monitoring system is designed for resilience:

- **Individual job polling failures** don't stop processing other jobs
- **Batch reconciliation errors** are logged but don't stop other batches
- **Chain execution failures** are logged but don't affect monitoring
- **Teams notifications** sent for all failure states

```python
# Example error handling pattern in monitor notebook
for job in jobs:
    try:
        track_job_status(job['id'], batch_type, irp_client)
    except JobError as e:
        errors.append(f"Job {job['id']}: {e}")
        continue  # Process next job
```
