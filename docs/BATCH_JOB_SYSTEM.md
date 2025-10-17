# Batch & Job Processing System

## Overview

The IRP Notebook Framework includes a batch and job processing system for orchestrating Moody's workflow submissions. The system manages the entire lifecycle from configuration to job completion.

## Key Entities

| Entity | Purpose | Cardinality |
|--------|---------|-------------|
| **Configuration** | Master configuration for a cycle | 1 per cycle (VALID/ACTIVE) |
| **Batch** | Collection of jobs for a specific batch type | Many per configuration |
| **Job Configuration** | Specific configuration for one job | Many per batch |
| **Job** | Individual Moody's workflow submission | 1+ per job configuration |

---

## Configuration

A **Configuration** is the master settings document for a risk analysis cycle. It contains all parameters needed to run the analysis, loaded from an Excel Configuration file.

### Key Operations

```python
from helpers.configuration import load_configuration_file, read_configuration

# Load configuration from Excel
config_id = load_configuration_file(
    cycle_id=1,
    excel_config_path='/path/to/config.xlsx',
    register=True  # Deletes existing configs and registers as NEW
)

# Read configuration
config = read_configuration(config_id)
# Returns: {
#   'id': 1,
#   'cycle_id': 1,
#   'status': 'VALID',
#   'configuration_data': {...}  # Parsed Excel data
# }
```

### Configuration Transformers

Configurations are transformed into job-specific configurations using registered transformers

---

## Batch

A **Batch** represents a collection of related jobs generated from a master configuration. Each batch has a specific type (e.g., 'portfolio_analysis', 'risk_calculation') that determines how jobs are generated.

### Batch Status Lifecycle

```
INITIATED → ACTIVE → COMPLETED
              ↓     
           FAILED  OR CANCELLED
```

### Creating a Batch

```python
from helpers.batch import create_batch

# Create batch (generates jobs via transformer)
batch_id = create_batch(
    batch_type='portfolio_analysis',  # Must be registered transformer
    configuration_id=1,
    step_id=5  # Optional, can be looked up from context
)

# Batch is created with:
# - Status: INITIATED
# - Jobs: Generated via transformer in INITIATED status
# - Job Configurations: One per transformed config
```

**What create_batch() does:**
1. Validates configuration is VALID or ACTIVE
2. Validates batch_type is registered transformer
3. Applies transformer to generate job configurations
4. Creates batch record
5. Creates job configuration records
6. Creates job records (all in INITIATED status)

### Submitting a Batch

```python
from helpers.batch import submit_batch

# Submit all jobs in batch to Moody's
result = submit_batch(batch_id)
# Returns: {
#   'batch_id': 1,
#   'batch_status': 'ACTIVE',
#   'submitted_jobs': 5,
#   'jobs': [{'job_id': 1, 'status': 'SUBMITTED'}, ...]
# }
```

**What submit_batch() does:**
1. Validates batch/configuration/cycle status
2. Gets all jobs in INITIATED status
3. Calls submit_job() for each
4. Updates batch status to ACTIVE
5. Updates configuration status to ACTIVE
6. Sets submitted_ts timestamp

### Batch Reconciliation

```python
from helpers.batch import recon_batch

# Reconcile batch status based on job states
final_status = recon_batch(batch_id)
# Returns: 'COMPLETED', 'FAILED', 'CANCELLED', or 'ACTIVE'
```

**Reconciliation Logic:**
- **CANCELLED**: All non-skipped jobs are CANCELLED
- **FAILED**: At least one non-skipped job is FAILED
- **COMPLETED**: All job configurations have at least one COMPLETED/FORCED_OK job (or are skipped)
- **ACTIVE**: Otherwise (jobs still running or pending)

**Recon creates log entry with:**
```json
{
  "total_configs": 10,
  "fulfilled_configs": 8,
  "total_jobs": 12,
  "job_status_counts": {
    "COMPLETED": 8,
    "FAILED": 2,
    "RUNNING": 1,
    "SKIPPED": 1
  },
  "failed_job_ids": [5, 8]
}
```

TODO - may need to replace with Moody's JOB IDs?

### Querying Batches

```python
from helpers.batch import read_batch, get_batch_jobs, get_batch_job_configurations

# Read batch
batch = read_batch(batch_id)

# Get all jobs (with optional filters)
all_jobs = get_batch_jobs(batch_id)
active_jobs = get_batch_jobs(batch_id, skipped=False)
failed_jobs = get_batch_jobs(batch_id, status='FAILED')

# Get all job configurations
configs = get_batch_job_configurations(batch_id)
non_skipped = get_batch_job_configurations(batch_id, skipped=False)
```

---

## Job Configuration

A **Job Configuration** is the specific configuration data for a single job, derived from the master configuration via a transformer. It can be:
- **Original**: Created by transformer during batch creation
- **Override**: Created during job resubmission with user modifications

### Job Configuration Structure

```sql
CREATE TABLE irp_job_configuration (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    configuration_id INTEGER NOT NULL,  -- Master config
    job_configuration_data JSONB NOT NULL,
    skipped BOOLEAN DEFAULT False,
    overridden BOOLEAN DEFAULT False,
    override_reason_txt VARCHAR(1000),
    created_ts TIMESTAMPTZ DEFAULT NOW()
);
```

### Configuration Modes

**1. Shared Configuration (Reuse)**
Multiple jobs can share the same job configuration:
```python
# Job 1 creates config
job1_id = create_job(batch_id, config_id, job_configuration_data={...})

# Job 2 reuses same config
job1 = read_job(job1_id)
job2_id = create_job(batch_id, config_id, job_configuration_id=job1['job_configuration_id'])
```

**2. Override Configuration**
Created during resubmission with user-provided changes:
```python
new_job_id = resubmit_job(
    original_job_id,
    job_configuration_data={'param': 'new_value'},  # Override
    override_reason="Corrected parameter based on validation error"
)
# Creates new job config with overridden=True
```

---

## Job

A **Job** is an individual Moody's workflow submission. Jobs have a full audit trail including parent-child relationships for resubmissions.

### Job Status Lifecycle


**Terminal States:** COMPLETED, FAILED, CANCELLED

### Creating Jobs

**Option 1: With New Configuration**
```python
from helpers.job import create_job

job_id = create_job(
    batch_id=1,
    configuration_id=1,
    job_configuration_data={'portfolio': 'ABC', 'threshold': 0.95}
)
```

**Option 2: With Existing Configuration**
```python
job_id = create_job(
    batch_id=1,
    configuration_id=1,
    job_configuration_id=5  # Reuse existing
)
```

**Validation:**
- Exactly one of `job_configuration_id` or `job_configuration_data` must be provided
- All inputs are validated in-code (raises `JobError` on invalid input)

### Submitting Jobs

```python
from helpers.job import submit_job

# Submit job to Moody's
submit_job(job_id)

# Force resubmit (even if already submitted)
submit_job(job_id, force=True)

# Submit and track immediately
submit_job(job_id, track_immediately=True)
```

**What submit_job() does:**
1. Reads job details
2. Checks if already submitted (has workflow_id)
3. Gets job configuration
4. Calls Moody's API (currently stubbed)
5. Updates job with workflow_id, status=SUBMITTED, timestamps
6. Stores submission request/response

### Tracking Jobs

```python
from helpers.job import track_job_status

# Poll Moody's for current status
current_status = track_job_status(job_id)
# Returns: 'QUEUED', 'RUNNING', 'COMPLETED', etc.
```

**What track_job_status() does:**
1. Reads job to get workflow_id
2. Calls Moody's API to get status (currently stubbed)
3. Creates tracking log entry
4. Updates job status if changed
5. Returns current status

**Stub Behavior (for testing):**
- SUBMITTED → QUEUED or PENDING
- QUEUED → PENDING or RUNNING
- PENDING → RUNNING
- RUNNING → RUNNING, COMPLETED, or FAILED (random)

### Job Resubmission

**Scenario:** A job fails and needs to be rerun, possibly with corrected parameters.

**Without Override (Same Configuration):**
```python
from helpers.job import resubmit_job

# Resubmit with same configuration
new_job_id = resubmit_job(failed_job_id)

# Result:
# - Original job: skipped=True
# - New job: INITIATED, parent_job_id=failed_job_id, same job_configuration_id
```

**With Override (Modified Configuration):**
```python
# Resubmit with corrected parameters
new_job_id = resubmit_job(
    failed_job_id,
    job_configuration_data={'param': 'corrected_value'},
    override_reason="Fixed validation error in threshold parameter"
)

# Result:
# - Original job: skipped=True
# - New job config: overridden=True, override_reason_txt set
# - New job: INITIATED, parent_job_id=failed_job_id, new job_configuration_id
```

### Skipping Jobs

```python
from helpers.job import skip_job

# Mark job as skipped (won't count in recon)
skip_job(job_id)
```

Skipped jobs:
- Are excluded from batch reconciliation logic
- Don't block batch completion
- Maintain audit trail (still in database)

---

## Common Workflows

### 1. Standard Batch Processing

```python
# Create batch (generates jobs)
batch_id = create_batch('portfolio_analysis', config_id, step_id)

# Submit batch to Moody's
submit_batch(batch_id)

# Poll jobs until complete
jobs = get_batch_jobs(batch_id)
for job in jobs:
    while True:
        status = track_job_status(job['id'])
        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(30)  # Poll every 30 seconds

# Reconcile batch
final_status = recon_batch(batch_id)
```

### 2. Handle Failed Job with Override

```python
# Job failed
failed_job_id = 123

# Investigate issue, determine corrected parameters
corrected_config = {
    'portfolio': 'ABC',
    'threshold': 0.90  # Corrected from 0.95
}

# Resubmit with override
new_job_id = resubmit_job(
    failed_job_id,
    job_configuration_data=corrected_config,
    override_reason="Threshold was too high, caused validation error"
)

# Submit new job
submit_job(new_job_id)

# Track new job
while True:
    status = track_job_status(new_job_id)
    if status in ['COMPLETED', 'FAILED']:
        break

# Reconcile batch
recon_batch(batch_id)
```

### 3. Audit Trail Query

```python
# Get job history
job = read_job(job_id)

# Check for parent
if job['parent_job_id']:
    parent = read_job(job['parent_job_id'])
    print(f"This job is a resubmission of job {parent['id']}")

# Get job configuration
config = get_job_config(job_id)
if config['overridden']:
    print(f"Override reason: {config['override_reason_txt']}")

# Get tracking history
df = execute_query(
    "SELECT * FROM irp_job_tracking_log WHERE job_id = %s ORDER BY tracked_ts",
    (job_id,)
)
# Shows status transitions over time
```

---

## Database Schema Reference

### Table Relationships
TODO

### Key Tables

**irp_batch**
- `configuration_id` - Master configuration (NOT NULL, FK)
- `batch_type` - Transformer type (e.g., 'portfolio_analysis')
- `status` - INITIATED, ACTIVE, COMPLETED, FAILED, CANCELLED
- `submitted_ts` - When batch was submitted to Moody's

**irp_job_configuration**
- `batch_id` - Parent batch
- `configuration_id` - Master configuration reference
- `job_configuration_data` - Specific config for this job (JSONB)
- `overridden` - Whether this is an override config
- `override_reason_txt` - Why it was overridden

**irp_job**
- `batch_id` - Parent batch
- `job_configuration_id` - Which config to use
- `parent_job_id` - Parent job if resubmission
- `moodys_workflow_id` - Moody's workflow identifier
- `skipped` - Whether job is skipped
- `submission_request/response` - API call details (JSONB)

**irp_batch_recon_log**
- `batch_id` - Batch that was reconciled
- `recon_result` - Determined status (COMPLETED, FAILED, etc.)
- `recon_summary` - Detailed counts and job IDs (JSONB)

**irp_job_tracking_log**
- `job_id` - Job that was tracked
- `job_status` - Status from Moody's
- `tracking_data` - API response (JSONB)

---

## Error Handling

### Custom Exceptions

```python
from helpers.batch import BatchError
from helpers.job import JobError
from helpers.configuration import ConfigurationError

try:
    batch_id = create_batch('unknown_type', config_id, step_id)
except BatchError as e:
    print(f"Batch creation failed: {e}")
    # Example: "Unknown batch_type 'unknown_type'. Registered types: ['default', 'multi_job']"

try:
    job_id = create_job(batch_id, config_id)  # Missing both config params
except JobError as e:
    print(f"Job creation failed: {e}")
    # Example: "Must provide exactly one of: job_configuration_id or job_configuration_data"
```

### Input Validation

All functions validate inputs and raise errors **before** database operations:

```python
# Invalid batch_id
read_batch(-1)  # Raises: BatchError("Invalid batch_id: -1. Must be a positive integer.")

# Invalid status
update_batch_status(1, 'INVALID')  # Raises: BatchError("Invalid status: INVALID. Must be one of [...]")

# Invalid transformer
create_batch('nonexistent', config_id, step_id)  # Raises: BatchError("Unknown batch_type...")
```

**API documentation:** Use `help()` on any function for detailed docstrings