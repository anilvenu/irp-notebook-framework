# Demo Sets

This demo includes two different sets showcasing various batch execution scenarios.

## SET_01 - Mixed Status Scenarios

**Location:** `demo/set_01/`

**Purpose:** Demonstrates various error scenarios, failures, and edge cases

**Contents:**
- 8 batches with various batch types
- Mix of statuses: COMPLETED, ACTIVE, CANCELLED, INITIATED
- 27 jobs with statuses: FINISHED, RUNNING, PENDING, QUEUED, FAILED, ERROR, CANCELLED
- Demonstrates:
  - Batch failures
  - Job cancellations
  - Resubmissions (parent/child jobs)
  - Skipped configurations
  - Submission errors

## SET_02 - All Successful Execution

**Location:** `demo/set_02/`

**Purpose:** Demonstrates successful execution of all batch types with realistic data

**Contents:**
- 11 batches covering ALL 11 business batch types
- All batches: COMPLETED
- 589 jobs, all FINISHED
- Generated from real Excel configuration file: `workspace/tests/files/valid_excel_configuration.xlsx`

**Realistic job counts per batch type:**

| Batch Type | Job Count |
|------------|-----------|
| EDM Creation | 7 |
| Portfolio Creation | 70 |
| MRI Import | 70 |
| Create Reinsurance Treaties | 3 |
| EDM DB Upgrade | 7 |
| GeoHaz | 70 |
| Portfolio Mapping | 70 |
| Analysis | 93 |
| Grouping | 96 |
| Export to RDM | 96 |
| Staging ETL | 7 |
| **TOTAL** | **589** |

## Usage

### Generate Dashboards for All Sets

```bash
cd irp-notebook-framework
./demo/run_generator.sh
```

The script will:
1. Find all `demo/set_*` directories
2. For each set:
   - Load CSV data to database (`prepare_data.py`)
   - Generate HTML dashboards (`generate_dashboards.py`)
3. Output dashboards to: `demo/<set_name>/html_output/`

### Generate Dashboards for a Single Set

```bash
cd irp-notebook-framework
source .venv/bin/activate

# For set_01
python demo/prepare_data.py set_01
python demo/generate_dashboards.py set_01

# For set_02
python demo/prepare_data.py set_02
python demo/generate_dashboards.py set_02
```

### View Dashboards

After generation, open the cycle dashboard:
- **SET_01:** `demo/set_01/html_output/cycle/Analysis-2025-Q1/index.html`
- **SET_02:** `demo/set_02/html_output/cycle/Analysis-2025-Q1/index.html`

## Regenerating SET_02 Data

If you need to regenerate SET_02 with updated Excel configuration:

```bash
cd irp-notebook-framework
source .venv/bin/activate
python demo/generate_set_02_data.py
```

This script:
- Reads `workspace/tests/files/valid_excel_configuration.xlsx`
- Validates the configuration
- Runs all 11 batch type transformers
- Generates realistic CSV data for batches, job configurations, and jobs
- Writes to `demo/set_02/csv_data/`

## Directory Structure

```
demo/
├── set_01/                          # Mixed status scenarios
│   ├── csv_data/
│   │   ├── batches.csv
│   │   ├── configurations.csv
│   │   ├── cycles.csv
│   │   ├── job_configurations.csv
│   │   ├── jobs.csv
│   │   ├── stages.csv
│   │   └── steps.csv
│   └── html_output/                 # Generated dashboards
│       └── cycle/
│           └── Analysis-2025-Q1/
│               ├── index.html       # Cycle dashboard
│               └── batch/
│                   └── {batch_id}/
│                       └── index.html
│
├── set_02/                          # All successful
│   ├── csv_data/                    # Same structure as set_01
│   └── html_output/                 # Same structure as set_01
│
├── run_generator.sh                 # Generate all sets
├── generate_set_02_data.py          # Regenerate set_02 from Excel
├── prepare_data.py                  # Load CSV to database
└── generate_dashboards.py           # Generate HTML dashboards
```
