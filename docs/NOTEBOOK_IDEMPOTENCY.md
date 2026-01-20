# Notebook Idempotency

How stage notebooks handle re-execution and job resubmission.

## Overview

Stage notebooks are designed to be **idempotent** - they can be run multiple times safely. The core principle:

- **Create what's missing** - Submit jobs for entities that don't exist in Moody's
- **Skip what exists** - Don't re-create entities that already exist
- **Resubmit failed jobs** - Automatically handle jobs that need to be retried

This is critical for **chained notebooks** that run automatically without user intervention.

## The Idempotency Pattern

Every submission notebook follows this pattern:

```
1. Retrieve batch created in Stage 01/Step 03
2. Get jobs and their configurations
3. For each job, check if entity exists in Moody's
4. Categorize jobs:
   - INITIATED + entity missing → submit
   - FINISHED/CANCELLED + entity missing → resubmit
   - Any status + entity exists → skip (or prompt user)
   - SUBMITTED/RUNNING → skip (in progress)
5. Execute submission plan
6. Activate batch for monitoring
```

### Entity Validation

The `EntityValidator` class checks whether an entity already exists in Moody's:

```python
from helpers.entity_validator import EntityValidator

validator = EntityValidator()
entity_exists = validator.check_entity_exists_for_job(job_config, batch_type)
```

This check is batch-type aware:
- **EDM Creation**: Checks if database exists
- **Portfolio Creation**: Checks if portfolio exists in EDM
- **Analysis**: Checks if analysis exists
- **Grouping**: Checks if group exists

## Two Idempotency Patterns

### Pattern 1: Auto-Idempotent (Chained Notebooks)

Used by Stage 3 notebooks that run automatically via step chaining. These notebooks must work without user input.

**Behavior:**
- Auto-submit jobs where entity is missing
- Auto-skip jobs where entity exists
- No user prompts
- Log all decisions for audit trail

**Example notebooks:**
- `Stage_03_Data_Import/Step_02_Create_Base_Portfolios (chained).ipynb`
- `Stage_03_Data_Import/Step_03_Submit_MRI_Import_Batch (chained).ipynb`
- All `(chained)` suffix notebooks

**Code pattern:**

```python
# Categorize jobs by status and entity existence
pending_jobs = []      # INITIATED - will be submitted
skipped_jobs = []      # Entity exists - will be skipped
resubmit_jobs = []     # Entity missing but job terminal - will be resubmitted
other_jobs = []        # In progress - skip

for job in jobs:
    config_data = get_job_config(job['job_configuration_id'])

    if job['status'] == JobStatus.INITIATED:
        pending_jobs.append(job)
    elif job['status'] in (JobStatus.FINISHED, JobStatus.CANCELLED):
        entity_exists = validator.check_entity_exists_for_job(config_data, batch_type)
        if entity_exists:
            skipped_jobs.append(job)
        else:
            resubmit_jobs.append(job)
    else:
        other_jobs.append(job)

# Submit batch - handles both pending and resubmit jobs automatically
result = submit_batch(batch_id, irp_client, step_id=step.step_id)
```

The `submit_batch()` function handles the categorization internally and:
- Calls `submit_job()` for INITIATED jobs
- Calls `resubmit_job()` for terminal jobs that need resubmission
- Skips jobs where entity already exists

### Pattern 2: Interactive Idempotent (User-Facing Notebooks)

Used by notebooks where users may want to explicitly re-run items that already exist. These notebooks prompt the user for decisions.

**Behavior:**
- Display what exists vs what's missing
- Prompt user for existing entities: "Delete and re-run?"
- Execute based on user choice

**Example notebooks:**
- `Stage_04_Analysis_Execution/Step_01_Execute_Analysis.ipynb`
- `Stage_05_Grouping/Step_01_Group_Analysis_Results.ipynb`

**Code pattern:**

```python
# Categorize jobs
jobs_missing_analysis = []  # Need to create
jobs_with_analysis = []     # Already exist - prompt user
jobs_in_progress = []       # Skip

for job in jobs:
    analysis_key = f"{edm}/{analysis_name}"
    analysis_exists = analysis_key in existing_analyses

    if job['status'] in IN_PROGRESS_STATUSES:
        jobs_in_progress.append(job)
    elif analysis_exists:
        jobs_with_analysis.append(job)
    else:
        jobs_missing_analysis.append(job)

# Prompt for missing analyses
if jobs_missing_analysis:
    choice = input(f"Create {len(jobs_missing_analysis)} missing analysis(es)? (y/n): ")
    if choice.lower() == 'y':
        jobs_to_create = jobs_missing_analysis

# Prompt for existing analyses
if jobs_with_analysis:
    choice = input(f"Delete and re-run {len(jobs_with_analysis)} existing analysis(es)? (y/n): ")
    if choice.lower() == 'y':
        jobs_to_delete = jobs_with_analysis

# Execute plan
if jobs_to_delete:
    delete_analyses_for_jobs(jobs_to_delete, irp_client)

for job in jobs_to_create + jobs_to_delete:
    if job['status'] == JobStatus.INITIATED:
        submit_job(job['id'], batch_type, irp_client)
    else:
        resubmit_job(job['id'], irp_client, batch_type)

activate_batch(batch_id)
```

## Job Resubmission

When a job needs to be resubmitted (entity missing but job not in INITIATED status):

### When Resubmission Happens

| Job Status | Entity Exists | Action |
|------------|---------------|--------|
| INITIATED | No | `submit_job()` |
| INITIATED | Yes | Skip (or prompt) |
| FINISHED/FAILED/CANCELLED | No | `resubmit_job()` |
| FINISHED/FAILED/CANCELLED | Yes | Skip (or prompt to delete) |
| SUBMITTED/RUNNING | Any | Skip (in progress) |

### Database State Changes

**Original Job:**
```
id: 5
status: FAILED (unchanged)
skipped: TRUE  ← marked as skipped
```

**New Job:**
```
id: 6 (new)
parent_job_id: 5  ← links to original
job_configuration_id: same as original (or new if override)
status: INITIATED → SUBMITTED
skipped: FALSE
```

**Batch:**
```
status: ACTIVE  ← reset so monitoring picks it up
```

### Resubmission Functions

```python
from helpers.job import resubmit_job, submit_job

# For INITIATED jobs - submit directly
submit_job(job_id, batch_type, irp_client)

# For terminal jobs - creates new job and submits
new_job_id = resubmit_job(job_id, irp_client, batch_type)
```

### Tracking Resubmission History

To see the resubmission chain for a job:

```sql
-- Find all jobs in a resubmission chain
WITH RECURSIVE job_chain AS (
    SELECT id, parent_job_id, status, skipped, 1 as depth
    FROM irp_job
    WHERE id = <job_id>

    UNION ALL

    SELECT j.id, j.parent_job_id, j.status, j.skipped, jc.depth + 1
    FROM irp_job j
    JOIN job_chain jc ON j.parent_job_id = jc.id
)
SELECT * FROM job_chain ORDER BY depth;
```

## Deleting Entities for Re-run

For interactive notebooks where users choose to re-run existing entities:

```python
from helpers.job import delete_analyses_for_jobs, delete_groups_for_jobs

# Delete analyses in Moody's before re-creating
deletion_errors = delete_analyses_for_jobs(jobs_to_delete, irp_client)

# Delete groups in Moody's before re-creating
deletion_errors = delete_groups_for_jobs(jobs_to_delete, irp_client)
```

These functions:
1. Call Moody's API to delete the entity
2. Return list of any deletion errors
3. Do not modify job status (that happens during resubmission)

## Key Functions

| Function | Module | Purpose |
|----------|--------|---------|
| `EntityValidator.check_entity_exists_for_job()` | `entity_validator` | Check if entity exists in Moody's |
| `submit_job()` | `job` | Submit INITIATED job to Moody's |
| `resubmit_job()` | `job` | Create new job from terminal job and submit |
| `submit_batch()` | `batch` | Submit all eligible jobs in batch |
| `activate_batch()` | `batch` | Set batch to ACTIVE for monitoring |
| `delete_analyses_for_jobs()` | `job` | Delete analyses before re-creating |
| `delete_groups_for_jobs()` | `job` | Delete groups before re-creating |

## Why Idempotency Matters

### Chained Notebook Execution

Stage 3 notebooks are chained together - when one completes, the next starts automatically. If a chain is interrupted (system restart, error), the notebooks must be safe to re-run:

```
Step 1: Create EDMs → Step 2: Create Portfolios → Step 3: MRI Import → ...
```

If Step 3 fails partway through:
- Some portfolios were imported successfully
- Some were not
- Re-running Step 3 must skip the successful ones and only process the failures

### Manual Re-execution

Users may need to re-run notebooks for various reasons:
- Partial failures need retry
- Data was corrected and needs re-processing
- Testing or validation purposes

Idempotent notebooks handle all these cases safely.
