# IRP Notebook Framework: End-User Guide

A step-by-step guide for analysts to execute insurance risk analysis cycles using the IRP Notebook Framework.

---

## Overview

The IRP Notebook Framework is a Jupyter-based workflow system that manages complete insurance risk analysis cycles. You'll work through a series of notebooks organized into stages, from initial setup through final data export to RDM.

**Key Concepts:**

- **Cycle**: A complete analysis run (e.g., "2025-Q4-Quarterly"). Only one cycle can be active at a time
- **Stage**: A major phase of the workflow (Setup, Data Extraction, Data Import, etc.)
- **Step**: An individual notebook within a stage that you execute
- **Batch**: A collection of jobs submitted to Moody's for processing
- **Configuration**: An Excel file that defines what analyses, portfolios, and groupings to create

---

## Workflow Summary

```
1. Create New Cycle
   └── Creates workspace from template, archives previous cycle

2. Load Configuration (Stage 01)
   └── Upload Excel file, validate, create batches

3. Extract Data (Stage 02)
   └── Pull data from SQL Server to CSV files

4. Import Data (Stage 03)
   └── Create EDMs, portfolios, treaties, run GeoHaz

5. Run Analyses (Stage 04)
   └── Submit analysis jobs to Moody's

6. Create Groupings (Stage 05)
   └── Group analyses and create rollups

7. Export Results (Stage 06)
   └── Export to RDM for reporting
```

---

## Getting Started

### Accessing the Framework

**JupyterLab** (for executing notebooks):
- Local: `http://localhost:8888`
- Deployed: `http://atl4lexpd001.cead.prd:8888/lab`
- Navigate to `workspace/workflows/`

**IRP Dashboard** (for monitoring progress):
- Local: `http://localhost:8001`
- Deployed: `http://atl4lexpd001.cead.prd:8001`
- Real-time view of batches, jobs, and cycle status

### Directory Structure

```
workflows/
├── _Tools/                    # Management notebooks
│   ├── Cycle Management/
│   ├── Batch Management/
│   └── ...
├── _Template/                 # Master template for new cycles
├── Active_<cycle_name>/       # Your current working cycle
│   ├── files/
│   │   ├── configuration/     # Place Excel config here
│   │   ├── data/              # Extracted CSV files
│   │   └── mapping/           # Mapping files
│   └── notebooks/
│       ├── Stage_01_Setup/
│       ├── Stage_02_Data_Extraction/
│       └── ...
└── _Archive/                  # Previous completed cycles
```

### Running Notebooks

Notebooks are designed to be run in their entirety each time. Use the **Run All** button (▶▶) in JupyterLab's toolbar rather than running cells individually. This ensures:
- Proper initialization and context setup
- Consistent execution order
- Complete step tracking in the database

---

## Step 1: Create a New Cycle

**Notebook:** `_Tools/Cycle Management/New Cycle.ipynb`

Run this notebook to start a new analysis cycle.

### What Happens

1. You'll be prompted to enter a cycle name
2. If a previous cycle exists, it will be automatically archived
3. A new `Active_<cycle_name>/` directory is created from the template
4. The cycle is registered in the database as `ACTIVE`

### Suggested Naming Convention

While any name is accepted, consider a pattern like:
- `2025-Q4-Quarterly`
- `2025-November-Adhoc`
- `2025-Q4-Rerun-v2`

### After Creation

Your new cycle workspace will contain:
- Empty `files/` directories ready for your configuration and data
- Pre-configured `notebooks/` with all stage and step notebooks
- A clean execution history

---

## Step 2: Prepare Your Configuration File

Before running Stage 01, place your Excel configuration file in:

```
Active_<cycle_name>/files/configuration/
```

### Configuration File Structure

The Excel workbook contains multiple sheets that define your analysis:

| Sheet | Purpose |
|-------|---------|
| **Metadata** | Key-value pairs controlling the cycle. See important values below. |
| **Databases** | EDM databases to create (e.g., `RM_EDM_202503_Quarterly_CBAP`) |
| **Portfolios** | Portfolio definitions with database assignments |
| **Analysis Table** | One row per analysis: database, portfolio, profiles, treaties |
| **Reinsurance Treaties** | Treaty names and financial terms |
| **Groupings** | Group definitions with member analyses or sub-groups |
| **GeoHaz Thresholds** | Threshold configurations for GeoHaz processing |
| **Products and Perils** | Product and peril mapping definitions |
| **Moody's Reference Data** | Reference data from Moody's (profiles, event rate schemes) |
| **ExposureGroup <-> Portname** | Exposure group to portfolio name mappings |

### Important Metadata Values

Two metadata values are particularly important:

**Current Date Value**
- Format: `YYYYMM` (e.g., `202503`)
- Used in file naming conventions for Moody's import files
- Used when executing SQL scripts against Assurant or Data Bridge databases
- Appears in database names and export identifiers

**Cycle Type**
- Valid values: `Quarterly`, `Annual`, `Adhoc`, `Test_<test-scenario-name>`
- Determines workflow behavior and naming conventions
- Used when executing SQL scripts against Assurant or Data Bridge databases
- **NOTE**: it is highly recommended to include "test" in the Cycle Type value when not running a production scenario. This ensures that data is unique in Risk Modeler and reduces confusion

### Updating Metadata Values

When you change the **Current Date Value** or **Cycle Type** in the Metadata sheet, most sheets update automatically via formulas. However, two sheets require manual updates:

- **Groupings** sheet
- **Products and Perils** sheet

To update these sheets: use Find & Replace (Ctrl+H) to search for the old Cycle Type value and replace with the new value.

---

## Step 3: Execute Stage 01 - Setup

Navigate to `Active_<cycle_name>/notebooks/Stage_01_Setup/`

### Step 01: Initialize Environment

**Notebook:** `Step_01_Initialize_Environment.ipynb`

Validates that:
- The cycle is active
- Database connectivity is working
- All required directories exist

### Step 02: Validate Configuration File

**Notebook:** `Step_02_Validate_Config_File.ipynb`

1. Lists available Excel files in your configuration directory
2. Prompts you to select which file to use
3. Offers validation options:
   - **Full validation**: **Recommended Choice**; checks entity existence / non-existence against Moody's API (slower, more thorough), as well as file format and cross-references
   - **Structural validation only**: Checks file format and cross-references only (faster, for re-runs or testing); skips entity existence in Moody's checks
4. Displays a preview of the configuration contents
5. Loads the configuration to the database if valid

### Step 03: Create Batches

**Notebook:** `Step_03_Create_Batches.ipynb`

1. Reads your validated configuration
2. Shows a preview of each batch type that will be created:
   - EDM Creation
   - Portfolio Creation
   - MRI Import
   - Create Treaties
   - EDM Upgrade
   - GeoHaz
   - Portfolio Mapping
   - Analysis
   - Grouping / Grouping Rollup
   - Export to RDM
3. Creates all batches and jobs in the database
4. Jobs start in `INITIATED` status, ready for submission

---

## Step 4: Execute Remaining Stages

After Stage 01, work through the remaining stages sequentially. Each stage contains numbered step notebooks.

### Stage 02: Data Extraction

Extracts source data from SQL Server and saves CSV files to your `files/data/` directory.

### Stage 03: Data Import

Eight sequential steps that import data into Moody's:

1. EDM Creation
2. Portfolio Creation
3. MRI Import
4. Create Reinsurance Treaties
5. EDM DB Upgrade
6. GeoHaz
7. Portfolio Mapping
8. Control Totals (verification)

**Note:** These steps are chained—when one completes, the monitoring system automatically starts the next.

### Stage 04: Analysis Execution

Submits analysis jobs to Moody's. This is typically the longest-running stage.

### Stage 05: Grouping

1. Grouping (analysis-only groups)
2. Grouping Rollup (groups of groups)

### Stage 06: Data Export

Exports all analyses and groups to RDM for reporting.

---

## Monitoring Jobs

Once you've submitted batches, you have two ways to track progress:

### IRP Dashboard (Recommended for Viewing)

The web-based dashboard provides a real-time view of your cycle:
- **Cycle overview**: All batches with status indicators
- **Batch details**: Click any batch to see individual job statuses
- **Job filtering**: Search and filter jobs by status
- **No refresh needed**: Data updates on each page load, and automatically every one minute

Access at `http://localhost:8001` (local) or `http://atl4lexpd001.cead.prd:8001` (deployed).

### Verifying in Moody's Risk Modeler

During cycle execution—especially during testing—it's typical to verify activities directly in Moody's. You can confirm that EDMs, portfolios, analyses, etc. appear as expected.

**Moody's Risk Modeler:** https://assurant.rms.com/riskmodeler/datasources/

### Monitor Active Jobs Notebook (Required for Progression)

**Notebook:** `_Tools/Batch Management/Monitor Active Jobs.ipynb`

While the dashboard shows status, this notebook actually advances the workflow:

### What It Does

1. Finds all active batches in your cycle
2. Polls Moody's API for job status updates
3. Updates job statuses in the database
4. Reconciles batch completion status
5. Auto-executes the next step when a batch completes

### Setting Up Scheduled Monitoring

For hands-off operation, schedule the monitoring notebook to run automatically:

Check If Monitoring Exists:
1. Open a new Launcher in JupyterLab
2. In the "Other" section, click **Notebook Jobs**
3. Click the **Notebook Job Definitions** tab; if nothing is displayed, then monitoring needs to be set u

Set Up Monitoring:
1. Navigate to `workspace/workflows/_Tools/Batch Management/`
2. Right-click on **Monitor Active Jobs.ipynb**
3. Select **Create Notebook Job**
4. Configure the job:
   - Uncheck **Output Formats > Notebook** (prevents output file clutter)
   - Under **Schedule**, select **Run on a schedule**
   - Choose the desired interval (typically **Minute** for continuous monitoring)

### Batch Status Meanings

| Status | Meaning |
|--------|---------|
| `INITIATED` | Batch created, not yet submitted |
| `ACTIVE` | Jobs are running |
| `COMPLETED` | All jobs finished successfully |
| `FAILED` | One or more jobs failed |
| `CANCELLED` | All jobs were cancelled |
| `ERROR` | Submission errors occurred |

---

## Notebook Idempotency

**Key Principle**: All notebooks from Stage 02 onward are idempotent—they can be safely re-run any number of times. The goal of each notebook is to fulfill its portion of the configuration file.

### How Idempotency Works

When a notebook runs, it performs two checks for each item:

1. **Prerequisite check**: Verifies that required upstream data exists (e.g., EDM must exist before portfolios can be created)
2. **Existence check**: Verifies that the target data does not already exist in Moody's

If both checks pass, the notebook proceeds with creation. If the data already exists, that item is skipped. This means:

- Re-running a notebook after partial completion will only process the remaining items
- Re-running a fully completed notebook will skip all items (no harm done)
- Failed jobs can be addressed by simply re-running the notebook

### Exception: Stage 01 Batch Creation

The batch creation step (`Stage_01_Setup/Step_03_Create_Batches.ipynb`) is **not** idempotent. Once batches are created from a configuration file:

- The configuration cannot be modified
- Batches cannot be recreated
- If the configuration file needs changes, you must create a new cycle

---

## Handling Failed Jobs

When jobs fail, the resolution depends on the nature of the failure.

### Step 1: Investigate the Failure

Before taking action, understand what went wrong:

- **IRP Dashboard**: Click on the failed batch to see which jobs failed and their configuration data
- **Notebook output**: Check the step notebook's output cells for detailed error messages
- **Moody's Risk Modeler**: Verify the actual state of data in Moody's (what was created, what's missing)
- **Database**: Query the `irp_job` table for full error details if needed

### Step 2: Determine the Resolution Path

Based on your investigation, choose the appropriate action:

**Option A: Re-run the Notebook**

For transient failures (network timeouts, temporary Moody's issues, partial completions):

1. Fix any environmental issues if needed (e.g., wait for Moody's to recover)
2. Re-run the step notebook
3. The notebook's idempotency ensures only incomplete work is retried

**Option B: Manual Intervention in Moody's**

For issues requiring direct fixes in Moody's:

1. Make corrections directly in Moody's Risk Modeler
2. Re-run the notebook to verify completion and continue

**Option C: Start a New Cycle**

For configuration errors that require changes to the Excel file:

1. Archive or delete the current cycle
2. Correct the Excel configuration file
3. Create a new cycle
4. Re-run from Stage 01

This is necessary when the fundamental configuration is wrong—batch creation cannot be undone.

---

## Cycle Completion

When all stages are complete:

1. Your results are exported to RDM
2. The cycle remains `ACTIVE` until you create a new one
3. Creating a new cycle automatically archives the current one

### Manual Cycle Management

Additional tools are available in `_Tools/Cycle Management/`:

- **Delete Cycle**: Permanently removes the active cycle (use for mistakes)
- **Purge Archive**: Removes all archived cycles (periodic cleanup)

---

## Quick Reference

### Typical Daily Workflow

```
1. Open the IRP Dashboard to check cycle progress
   └── Review batch statuses, identify any failures

2. If failures exist, open JupyterLab to address them
   └── Check notebook outputs, resubmit jobs as needed

3. Verify monitoring is running
   └── Check Notebook Jobs in JupyterLab, or run Monitor Active Jobs manually

4. Continue with next manual steps if required
   └── Some steps require manual execution (e.g., Stage 01 setup)
```

### Key Directories

| Path | Purpose |
|------|---------|
| `Active_*/files/configuration/` | Your Excel config file |
| `Active_*/files/data/` | Extracted CSV data |
| `Active_*/notebooks/` | Stage and step notebooks |
| `_Tools/Batch Management/` | Monitoring notebook |
| `_Tools/Cycle Management/` | Cycle creation and cleanup |

### Getting Help

- **IRP Dashboard**: View batch and job statuses at a glance
- **Notebook output**: Check output cells for detailed error messages
- **Database tables**: Query `irp_step_run` for execution history, `irp_job` for job details
- **Documentation**: Consult technical docs in `docs/`
- **Moody's API Documentation**:
  - Platform: https://developer.rms.com/platform
  - Data Bridge: https://developer.rms.com/databridge
  - Risk Modeler: https://developer.rms.com/risk-modeler
