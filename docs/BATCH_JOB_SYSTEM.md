# Batch & Job Processing System

The batch and job system orchestrates submissions to Moody's Risk Modeler API. Batches group related jobs generated from a master configuration.

## Data Model

```
Configuration (Excel file parsed to JSON)
└── Batch (collection of jobs for one batch type)
    ├── Job Configuration (parameters per job)
    │   └── Job (individual API submission)
    └── Job Configuration
        └── Job
```

## Batch Types

| Batch Type | Execution | Description |
|------------|-----------|-------------|
| Data Extraction | Sync | Extract data from Assurant SQL Server to CSV |
| EDM Creation | Async | Create exposure databases |
| Portfolio Creation | Sync | Create portfolios in EDM |
| MRI Import | Async | Import accounts/locations from CSV |
| Create Reinsurance Treaties | Sync | Create treaty structures |
| EDM DB Upgrade | Async | Upgrade EDM data version |
| GeoHaz | Async | Geocoding and hazard lookup |
| Portfolio Mapping | Sync | Create sub-portfolios via SQL (Moody's Databridge) |
| Analysis | Async | Run DLM/HD analysis jobs |
| Grouping | Async | Create analysis groups |
| Grouping Rollup | Async | Create rollup groups (groups of groups) |
| Export to RDM | Async | Export results to RDM database |

**Execution Patterns:**
- **Synchronous**: Completes immediately. Job status set to FINISHED on success.
- **Asynchronous**: Returns workflow ID. Requires polling via `track_job_status()`.

## Status Flows

### Job Status
```
INITIATED → SUBMITTED → QUEUED → PENDING → RUNNING → FINISHED
                                                    → FAILED
                                                    → CANCELLED
          → ERROR (submission failure)
```

### Batch Status
```
INITIATED → ACTIVE → COMPLETED (all jobs finished)
                   → FAILED (any job failed)
                   → CANCELLED (all jobs cancelled)
                   → ERROR (any job errored)
```

## Standard Workflow

Most batch types follow this pattern (e.g., EDM Creation, MRI Import):

```python
from helpers.batch import create_batch, validate_batch, submit_batch, recon_batch
from helpers.job import track_job_status
from helpers.irp_integration import IRPClient
from helpers.constants import BatchType, JobStatus

# 1. Batch is created during setup (Stage 01/Step 03)
batch_id = create_batch(
    batch_type=BatchType.EDM_CREATION,
    configuration_id=config_id,
    step_id=step_id
)

# 2. Validate before submission
errors = validate_batch(batch_id)
if errors:
    raise ValueError(f"Validation failed: {errors}")

# 3. Submit batch to Moody's
irp_client = IRPClient()
result = submit_batch(batch_id, irp_client, step_id=current_step_id)
# Returns: {'batch_id': 1, 'batch_status': 'ACTIVE', 'submitted_jobs': 5, 'jobs': [...]}

# 4. Poll jobs until complete (async batch types only)
jobs = get_batch_jobs(batch_id, skipped=False)
for job in jobs:
    while job['status'] not in JobStatus.terminal():
        status = track_job_status(job['id'], BatchType.EDM_CREATION, irp_client)
        if status not in JobStatus.terminal():
            time.sleep(30)

# 5. Reconcile batch status
final_status = recon_batch(batch_id)
# Returns: 'COMPLETED', 'FAILED', 'CANCELLED', or 'ERROR'
```

## Interactive Workflow (Analysis)

The Analysis batch type uses an interactive pattern where users decide how to handle existing entities:

```python
from helpers.batch import get_batch_jobs, read_batch, activate_batch
from helpers.job import submit_job, resubmit_job, delete_analyses_for_jobs
from helpers.entity_validator import EntityValidator

# 1. Check which analyses already exist
validator = EntityValidator()
errors, existing_analyses = validator.validate_analysis_batch(analyses)

# 2. User decides for each job:
#    - Missing analysis → submit_job() to create
#    - Existing analysis → skip OR delete and resubmit

# For jobs where analysis is missing
if job['status'] == JobStatus.INITIATED:
    submit_job(job_id, BatchType.ANALYSIS, irp_client)

# For jobs where analysis exists and user wants to re-run
delete_analyses_for_jobs(jobs_to_delete, irp_client)
for job in jobs_to_delete:
    resubmit_job(job['job_id'], irp_client, BatchType.ANALYSIS)

# 3. Activate batch manually (bypasses submit_batch)
activate_batch(batch_id)
```

## Job Resubmission

Failed jobs can be resubmitted with or without configuration changes:

```python
from helpers.job import resubmit_job

# Resubmit with same configuration (transient failure)
new_job_id = resubmit_job(
    job_id=failed_job_id,
    irp_client=irp_client,
    batch_type=BatchType.ANALYSIS
)
# Original job marked skipped, new job created and submitted

# Resubmit with modified configuration (not currently used)
new_job_id = resubmit_job(
    job_id=failed_job_id,
    irp_client=irp_client,
    batch_type=BatchType.ANALYSIS,
    job_configuration_data={'param': 'corrected_value'},
    override_reason="Fixed threshold parameter"
)
# Creates new job config with parent reference, skips original
```

## Validation

`validate_batch()` checks entity existence before submission. Validation rules vary by batch type:

| Batch Type | Pre-requisites | Must Not Exist |
|------------|---------------|----------------|
| EDM Creation | Server exists | EDM |
| Portfolio Creation | EDM exists | Portfolio |
| MRI Import | EDM + Portfolio exist, CSV files exist | Accounts in portfolio |
| Create Treaties | EDM exists | Treaty |
| GeoHaz | EDM + Portfolio exist | - |
| Analysis | EDM + Portfolio + Treaties exist | Analysis |
| Grouping | Analyses/Groups exist | Group |
| Export to RDM | Analyses/Groups exist | RDM |

```python
from helpers.batch import validate_batch

errors = validate_batch(batch_id)
# Returns list of error messages, empty if valid
# Example: ['EDM "RM_EDM_202501_Test" already exists']
```

## Key Functions

### Batch Operations (`helpers.batch`)

| Function | Purpose |
|----------|---------|
| `create_batch(batch_type, config_id, step_id)` | Create batch with jobs from configuration |
| `read_batch(batch_id)` | Get batch details |
| `validate_batch(batch_id)` | Check entity existence before submission |
| `submit_batch(batch_id, irp_client, step_id)` | Submit all eligible jobs |
| `activate_batch(batch_id)` | Set batch to ACTIVE (for interactive flows) |
| `recon_batch(batch_id)` | Determine final batch status from job states |
| `get_batch_jobs(batch_id, skipped, status)` | Get jobs with optional filters |
| `update_batch_step(batch_id, step_id)` | Update associated step (for step chaining) |

### Job Operations (`helpers.job`)

| Function | Purpose |
|----------|---------|
| `submit_job(job_id, batch_type, irp_client)` | Submit single job to Moody's |
| `track_job_status(job_id, batch_type, irp_client)` | Poll Moody's for current status |
| `resubmit_job(job_id, irp_client, batch_type, ...)` | Create new job from failed job |
| `skip_job(job_id)` | Mark job as skipped |
| `read_job(job_id)` | Get job details |
| `get_job_config(job_id)` | Get job's configuration data |
| `delete_analyses_for_jobs(jobs, irp_client)` | Delete analyses in Moody's for resubmission |
| `delete_groups_for_jobs(jobs, irp_client)` | Delete groups in Moody's for resubmission |

## Reconciliation Logic

`recon_batch()` determines batch status based on job states:

1. **CANCELLED**: All non-skipped jobs are CANCELLED
2. **ERROR**: Any non-skipped job is ERROR
3. **FAILED**: Any non-skipped job is FAILED
4. **COMPLETED**: All job configurations have at least one FINISHED job
5. **ACTIVE**: Jobs still in progress

Empty batches (0 jobs) are immediately marked COMPLETED.

## Notifications

Batch failures (FAILED, ERROR, CANCELLED) automatically send Teams notifications with:
- Cycle/stage/step context
- Job status counts
- Link to dashboard and notebook

## Error Handling

```python
from helpers.batch import BatchError
from helpers.job import JobError

try:
    submit_batch(batch_id, irp_client)
except BatchError as e:
    # Configuration invalid, cycle not active, validation failed
    print(f"Batch error: {e}")

try:
    submit_job(job_id, batch_type, irp_client)
except JobError as e:
    # Job skipped, submission failed, API error
    print(f"Job error: {e}")
```
