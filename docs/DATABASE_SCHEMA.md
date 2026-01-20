# Database Schema

The IRP Notebook Framework uses PostgreSQL to store workflow metadata. All runtime data (cycles, batches, jobs, configurations) is tracked here.

## Data Hierarchy

```
Cycle (e.g., "Analysis-2025-Q4")
├── Configuration (Excel file with job parameters)
├── Stage (Setup, Extract, Process, Submit, Monitor)
│   └── Step (individual notebooks)
│       └── Step Run (execution history)
└── Batch (collection of jobs for Moody's)
    ├── Job Configuration (parameters per job)
    └── Job (individual Moody's submission)
```

## Core Tables

### irp_cycle

Top-level workflow unit. Only one can be ACTIVE at a time.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| cycle_name | VARCHAR(255) | Unique name (e.g., "Analysis-2025-Q4") |
| status | ENUM | `ACTIVE` or `ARCHIVED` |
| created_ts | TIMESTAMPTZ | When created |
| archived_ts | TIMESTAMPTZ | When archived (null if active) |

### irp_configuration

Stores parsed Excel configuration. One per cycle.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| cycle_id | INTEGER | FK to irp_cycle |
| configuration_file_name | VARCHAR(2000) | Path to Excel file |
| configuration_data | JSONB | Parsed configuration as JSON |
| status | ENUM | `NEW` → `VALID` → `ACTIVE` or `ERROR` |
| file_last_updated_ts | TIMESTAMPTZ | Excel file modification time |

### irp_stage

Major workflow phases.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| cycle_id | INTEGER | FK to irp_cycle |
| stage_num | INTEGER | Sequence (1, 2, 3...) |
| stage_name | VARCHAR(255) | e.g., "Setup", "Extract", "Submit" |

### irp_step

Individual notebook within a stage.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| stage_id | INTEGER | FK to irp_stage |
| step_num | INTEGER | Sequence within stage |
| step_name | VARCHAR(255) | Step name |
| notebook_path | VARCHAR(1000) | Path to Jupyter notebook |
| requires_batch | BOOLEAN | Whether step creates Moody's batch |

### irp_step_run

Execution history for each step.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| step_id | INTEGER | FK to irp_step |
| run_num | INTEGER | Execution number (1st, 2nd...) |
| status | ENUM | `ACTIVE` → `COMPLETED` / `FAILED` / `SKIPPED` |
| started_ts | TIMESTAMPTZ | When started |
| completed_ts | TIMESTAMPTZ | When finished |
| started_by | VARCHAR(255) | User who ran it |
| error_message | TEXT | Error details if failed |
| output_data | JSONB | Step results |

### irp_batch

Collection of jobs submitted to Moody's.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| step_id | INTEGER | FK to irp_step |
| configuration_id | INTEGER | FK to irp_configuration |
| batch_type | VARCHAR(255) | e.g., "Analysis", "EDM Creation" |
| status | ENUM | `INITIATED` → `ACTIVE` → `COMPLETED` / `FAILED` / `CANCELLED` / `ERROR` |
| submitted_ts | TIMESTAMPTZ | When submitted |
| completed_ts | TIMESTAMPTZ | When finished |

### irp_job_configuration

Parameters for each job. Supports overrides for resubmission.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| batch_id | INTEGER | FK to irp_batch |
| configuration_id | INTEGER | FK to irp_configuration |
| job_configuration_data | JSONB | Job-specific parameters |
| skipped | BOOLEAN | Whether to skip this job |
| overridden | BOOLEAN | Whether config was overridden |
| override_reason_txt | VARCHAR(1000) | Why overridden |
| parent_job_configuration_id | INTEGER | Original config if overridden |

### irp_job

Individual Moody's API submission.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| batch_id | INTEGER | FK to irp_batch |
| job_configuration_id | INTEGER | FK to irp_job_configuration |
| moodys_workflow_id | VARCHAR(50) | Moody's system workflow ID |
| status | ENUM | See status flow below |
| skipped | BOOLEAN | Whether job is skipped |
| last_error | TEXT | Last error message |
| parent_job_id | INTEGER | Parent job if resubmission |
| submitted_ts | TIMESTAMPTZ | When submitted |
| completed_ts | TIMESTAMPTZ | When completed |
| last_tracked_ts | TIMESTAMPTZ | Last status check |
| submission_request | JSONB | Request sent to API |
| submission_response | JSONB | Response from API |

## Logging Tables

### irp_batch_recon_log

Batch reconciliation history.

| Column | Type | Description |
|--------|------|-------------|
| batch_id | INTEGER | FK to irp_batch |
| recon_ts | TIMESTAMPTZ | When reconciliation ran |
| recon_result | ENUM | Result status |
| recon_summary | JSONB | Detailed breakdown |

### irp_job_tracking_log

Job status polling history.

| Column | Type | Description |
|--------|------|-------------|
| job_id | INTEGER | FK to irp_job |
| tracked_ts | TIMESTAMPTZ | When checked |
| moodys_workflow_id | VARCHAR(50) | Workflow ID |
| job_status | ENUM | Status from Moody's |
| tracking_data | JSONB | Full API response |

## Views

| View | Purpose |
|------|---------|
| `v_irp_job` | Jobs with derived fields: age, report_status, needs_attention |
| `v_irp_job_configuration` | Aggregated stats per config: total/finished/failed jobs, progress % |
| `v_irp_batch` | Batch reporting: completion %, recommended actions |

## Status Flows

**Job Status Flow:**
```
INITIATED → SUBMITTED → QUEUED → PENDING → RUNNING → FINISHED
                                                   → FAILED
                                                   → CANCELLED
          → ERROR (submission failure)
```

**Batch Status Flow:**
```
INITIATED → ACTIVE → COMPLETED
                   → FAILED
                   → CANCELLED
                   → ERROR
```

## Key Rules

- **One Active Cycle**: Only one cycle can have status `ACTIVE` at any time
- **One Configuration Per Cycle**: Each cycle has exactly one configuration
- **Cascade Deletes**: Deleting a cycle removes all related stages, steps, batches, and jobs
- **Job Resubmission**: Failed jobs can be resubmitted; original jobs will have `skipped` == `True`; parent job is tracked via `parent_job_id`; optionally can override configuration (not currently used)

## Accessing the Database

```bash
# Connect via Docker
docker exec -it irp-postgres psql -U irp_user -d irp_db

# Common commands
\dt                          # List tables
\d irp_cycle                 # Describe table
\dv                          # List views

# Common queries
SELECT * FROM irp_cycle;                              # View cycles
SELECT * FROM v_irp_batch WHERE status = 'ACTIVE';    # Active batches
SELECT * FROM v_irp_job WHERE needs_attention;        # Jobs needing action
```
