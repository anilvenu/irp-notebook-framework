# Common Issues and Troubleshooting Guide

This document provides solutions for common issues encountered when working with the IRP Notebook Framework.

---

## Quick Reference

| Issue | Root Cause | Jump To |
|-------|------------|---------|
| Files not editable in JupyterLab / SQL scripts cannot execute | File permissions on VM | [Section 1](#1-files-not-editable--sql-scripts-cannot-execute) |
| Kernel restarts unexpectedly while running a notebook | VM RAM limit exceeded | [Section 2](#2-kernel-restarts-unexpectedly) |
| Job statuses on Dashboard are not updating | Batch Monitoring not turned on | [Section 3](#3-job-statuses-not-updating-on-dashboard) |
| Job statuses not updating / Step Chaining not happening | Monitor Active Jobs notebook is failing | [Section 4](#4-job-statuses-not-updating--step-chaining-not-happening) |
| MRI Import or GeoHaz jobs are failing | Import data is invalid | [Section 5](#5-mri-import-or-geohaz-jobs-failing) |
| Analysis or Grouping results are not as expected | Analysis/Grouping settings are incorrect | [Section 6](#6-analysis-or-grouping-results-not-as-expected) |
| Database connectivity timeouts | Network or access configuration issues | [Section 7](#7-database-connectivity-timeouts) |

---

## Issues

### 1. Files Not Editable / SQL Scripts Cannot Execute

**Symptom**: Some files are not editable in JupyterLab, or SQL scripts cannot be executed due to a permissions issue.

**Root Cause**: File permissions on the VM are restricting access.

**Resolution**:

Grant all users read/write/execute access to all files in the irp-notebook-framework directory:

```bash
cd /appdata && sudo chmod 777 -R irp-notebook-framework
```

> **Note**: This grants broad permissions, which is acceptable in our case: only specific PIM users can log on to the VM, and only users within the Assurant network can access JupyterLab.

---

### 2. Kernel Restarts Unexpectedly

**Symptom**: The Jupyter kernel restarts unexpectedly while running a notebook.

**Root Cause**: VM RAM limit exceeded. The most common culprit is executing SQL scripts where large results are stored in memory.

**Resolution**:

1. **Manual garbage collection** - Add explicit garbage collection after processing large datasets:
   ```python
   import gc

   # After processing large data
   del large_dataframe
   gc.collect()
   ```

2. **Code optimization** - Split CSV files or large datasets into smaller parts:
   ```python
   # Instead of loading entire file
   # df = pd.read_csv('large_file.csv')

   # Process in chunks
   chunk_size = 10000
   for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
       process(chunk)
   ```

3. **Monitor memory usage** - Check current memory before running memory-intensive operations:
   ```python
   import psutil
   print(f"Memory usage: {psutil.virtual_memory().percent}%")
   ```

---

### 3. Job Statuses Not Updating on Dashboard

**Symptom**: Job statuses on the Dashboard are not updating.

**Root Cause**: Batch Monitoring is not turned on.

**Resolution**:

Follow the steps to enable Batch Monitoring documented in [BATCH_MONITORING.md](BATCH_MONITORING.md), specifically the **"Scheduling"** section.

Quick summary:
1. Open a new Launcher in JupyterLab
2. In the "Other" section, click **Notebook Jobs**
3. Click the **Notebook Job Definitions** tab
4. Navigate to `/workspace/workflows/_Tools/Batch Management`
5. Right-click on **Monitor Active Jobs.ipynb**
6. Select **Create Notebook Job**
7. Configure to run on a schedule (typically every minute for continuous monitoring)

---

### 4. Job Statuses Not Updating / Step Chaining Not Happening

**Symptom**: Job statuses on the Dashboard are not updating, or automatic Step Chaining is not happening.

**Root Cause**: The Monitor Active Jobs notebook is failing.

**Resolution**:

1. **Navigate to Notebook Job outputs**:
   - Open a new Launcher in JupyterLab
   - In the "Other" section, click **Notebook Jobs**

2. **Download and review output files**:
   - Find the output files for the Monitor Active Jobs notebook
   - Download the notebook outputs for review

3. **Identify the timing of failures**:
   - Look at the timestamps of failed runs
   - Download notebook outputs around the same time as your observed failure
   - Review the error messages in the failed notebook output

---

### 5. MRI Import or GeoHaz Jobs Failing

**Symptom**: MRI Import jobs or GeoHaz jobs are failing.

**Root Cause**: Import data is invalid.

**Resolution**:

1. **Identify the invalid data**:
   - Review the job error messages for specific validation failures
   - Check which records are causing the issue

2. **Fix the source data**:
   - Fix SQL scripts or underlying tables in the Assurant database
   - Correct any data format issues or missing required fields

3. **Re-run the workflow**:
   - Re-run data extraction steps to regenerate the import files
   - Re-run subsequent steps that depend on the corrected data

---

### 6. Analysis or Grouping Results Not as Expected

**Symptom**: Analysis or Grouping results are not as expected.

**Root Cause**: Analysis or Grouping settings are incorrect.

**Resolution**:

This issue requires detailed debugging to compare the automation settings with expected settings:

1. **Run analysis/grouping on the UI manually**:
   - Perform the same operation through the Moody's UI
   - Make note of the request bodies being sent (use browser developer tools)

2. **Analyze automation request bodies**:
   - Use the Tool notebook: `_Tools/Debugging/View Job Request.ipynb`
   - Compare the request body generated by automation with what you captured from the UI

3. **Validate if requests are the same**:
   - Check for differences in parameters, settings, or configuration values

4. **Use the validation notebook**:
   - Open: `_Tools/IRP Integration/Validate_Analysis_Results.ipynb`
   - This notebook works for both analysis and grouping validation
   - It will highlight differences between expected and actual results

5. **Check UI settings**:
   - Use the Moody's UI to view Analysis settings and applied treaties
   - Verify the configuration matches what the automation is using

6. **Apply fixes**:
   - Identify discrepancies between UI and automation
   - Apply necessary code changes to correct the automation behavior

7. **If needed: get help from Moody's Support**
   - Support email: rmssupport@moodys.com
   - Send email explaining the issue; provide as much context as possible

---

### 7. Database Connectivity Timeouts

**Symptom**: Database connections are timing out or failing during data extraction or other database operations.

**Root Cause**: Network or access configuration issues preventing connectivity to external databases.

**Resolution**:

#### For Assurant DB Issues

1. **Verify connection details are valid**:
   - Check that the server hostname, port, and database name are correct
   - Confirm credentials are up to date

2. **Verify AD account access** (requires assistance from Assurant networking / security teams):
   - Ensure the AD account (`bi_riskmodeler_prd`) has proper access to the database
   - Contact the database team to verify permissions if needed

3. **Verify VM network configuration** (requires assistance from Assurant networking teams):
   - Ensure the VM has outbound traffic enabled to access the database
   - Check firewall rules allow connections to the database server

#### For Data Bridge Issues

1. **Verify connection details are valid**:
   - Check that the server hostname, port, and database name are correct
   - Confirm credentials are up to date

2. **Verify VM network configuration** (requires assistance from Assurant networking teams):
   - Ensure the VM has outbound traffic enabled to access the database

3. **Verify IP whitelisting**:
   - Ensure IP whitelisting for the VM is configured on the Data Bridge side
   - If running locally, ensure your local machine's IP is also whitelisted

---

## Related Documentation

- [BATCH_MONITORING.md](BATCH_MONITORING.md) - Automated job monitoring and workflow progression
- [BATCH_JOB_SYSTEM.md](BATCH_JOB_SYSTEM.md) - Detailed batch and job processing guide
- [END_USER_GUIDE.md](END_USER_GUIDE.md) - Step-by-step guide for analysts
- [CONFIGURATION_SYSTEM.md](CONFIGURATION_SYSTEM.md) - Excel configuration and transformation system
