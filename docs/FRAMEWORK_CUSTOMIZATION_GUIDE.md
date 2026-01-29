# Framework Customization Guide

A guide for analysts on key changes you can make to the IRP Notebook Framework and where to make them. See [Updating the Code Repository](UPDATING_CODE_REPOSITORY.md) for instructions.

---

## Overview

This guide covers code-level default values that can be modified to change system behavior. These are values hardcoded in the Python source files that control how jobs are submitted to Moody's.

**Important**: After making changes, you must commit and push them using [Git Cola](UPDATING_CODE_REPOSITORY.md).

---

## SQL Scripts

SQL scripts control how data is extracted from source databases (Assurant, Data Bridge) and transformed into Moody's import files.

### Location

```
workspace/sql/
├── control_totals/           # Validation queries
├── import_files/             # Portfolio data extraction
│   ├── adhoc/
│   ├── annual/
│   ├── quarterly/
│   └── test/
├── portfolio_mapping/        # Sub-portfolio creation
│   ├── adhoc/
│   ├── annual/
│   ├── quarterly/
│   └── test/
└── data_export/              # Analysis export queries
```

### Import File Scripts

These scripts extract portfolio data and create Moody's import files. They are organized by **cycle type** (quarterly, annual, adhoc, test).

**Path pattern**: `workspace/sql/import_files/{cycle_type}/`

**Naming convention**: `2_Create_{PORTFOLIO}_Moodys_ImportFile.sql`

### Template Variables

SQL scripts use template variables that are replaced at runtime:

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `{{ DATE_VALUE }}` | Current date value from configuration | `202503` |
| `{{ CYCLE_TYPE }}` | Cycle type from configuration | `Quarterly` |

### Common SQL Changes

| Change | What to Modify |
|--------|----------------|
| Add/remove columns | Modify SELECT clause |
| Change data filters | Modify WHERE clause |
| Add new portfolio | Create new script following naming convention |
| Adjust joins | Modify JOIN clauses |
| Update business logic | Modify CASE statements or calculations |

---

## Analysis Settings

Controls how portfolio analyses are submitted to Moody's.

**API Documentation**: https://developer.rms.com/platform/reference/createmodeljob

### Location

**File**: `workspace/helpers/irp_integration/analysis.py`

**Function**: `submit_portfolio_analysis_job` (line 154)

### Default Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `franchise_deductible` | `False` | Whether to apply franchise deductible |
| `min_loss_threshold` | `1.0` | Minimum loss threshold value |
| `treat_construction_occupancy_as_unknown` | `True` | Treat construction/occupancy as unknown |
| `num_max_loss_event` | `1` | Number of max loss events to include |

### How to Change

Edit the default value in the function signature:

```python
# Before
def submit_portfolio_analysis_job(
    ...
    min_loss_threshold: float = 1.0,
    ...
)

# After (example: change threshold to 0.5)
def submit_portfolio_analysis_job(
    ...
    min_loss_threshold: float = 0.5,
    ...
)
```

---

## GeoHaz Settings

Controls geocoding and hazard operations on portfolios.

**API Documentation**: https://developer.rms.com/platform/reference/creategeohazjob

### Location

**File**: `workspace/helpers/irp_integration/portfolio.py`

**Function**: `submit_geohaz_job` (line 315)

### Function Parameter Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `version` | `"22.0"` | Geocode version |
| `hazard_eq` | `False` | Enable earthquake hazard |
| `hazard_ws` | `False` | Enable windstorm hazard |

### Geocode Layer Options (line 382)

These control how geocoding is performed:

| Option | Default | Description |
|--------|---------|-------------|
| `aggregateTriggerEnabled` | `"true"` | Enable aggregate trigger |
| `geoLicenseType` | `"0"` | Geocoding license type |
| `skipPrevGeocoded` | `False` | Skip previously geocoded locations |

```python
# Default geocode_layer_options (line 382-386)
geocode_layer_options = {
    "aggregateTriggerEnabled": "true",
    "geoLicenseType": "0",
    "skipPrevGeocoded": False
}
```

### Hazard Layer Options (line 389)

These control how hazard data is applied:

| Option | Default | Description |
|--------|---------|-------------|
| `overrideUserDef` | `False` | Override user-defined hazard values |
| `skipPrevHazard` | `False` | Skip previously hazarded locations |

```python
# Default hazard_layer_options (line 389-392)
hazard_layer_options = {
    "overrideUserDef": False,
    "skipPrevHazard": False
}
```

### How to Change

**Function parameters** - Edit the function signature (line 315):

```python
# Before
def submit_geohaz_job(self,
                      portfolio_name: str,
                      edm_name: str,
                      version: str = "22.0",
                      ...
)

# After (example: update to version 23.0)
def submit_geohaz_job(self,
                      portfolio_name: str,
                      edm_name: str,
                      version: str = "23.0",
                      ...
)
```

**Layer options** - Edit the default dictionaries in the function body:

```python
# Example: Enable skipping previously geocoded locations (line 382)
if geocode_layer_options is None:
    geocode_layer_options = {
        "aggregateTriggerEnabled": "true",
        "geoLicenseType": "0",
        "skipPrevGeocoded": True  # Changed from False
    }
```

---

## Grouping Settings

Controls how analysis groups are created and configured.

**API Documentation**: https://developer.rms.com/platform/reference/creategroupingjob

### Location

**File**: `workspace/helpers/irp_integration/analysis.py`

**Function**: `submit_analysis_grouping_job` (line 688)

### Default Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `simulate_to_plt` | `False` | Whether to simulate to PLT |
| `num_simulations` | `50000` | Number of simulations |
| `propagate_detailed_losses` | `False` | Whether to propagate detailed losses |
| `reporting_window_start` | `"01/01/2021"` | Reporting window start date |
| `simulation_window_start` | `"01/01/2021"` | Simulation window start date |
| `simulation_window_end` | `"12/31/2021"` | Simulation window end date |

### How to Change

Edit the default value in the function signature:

```python
# Before
def submit_analysis_grouping_job(
    ...
    num_simulations: int = 50000,
    reporting_window_start: str = "01/01/2021",
    ...
)

# After (example: increase simulations, update window)
def submit_analysis_grouping_job(
    ...
    num_simulations: int = 100000,
    reporting_window_start: str = "01/01/2025",
    ...
)
```

---

## EDM Creation Settings

Controls the database server where EDMs (Exposure Data Management databases) are created.

**Find your database server**: https://assurant.rms.com/databridge-app/databridge/database-servers

### Location

**File**: `workspace/helpers/irp_integration/edm.py`

**Function**: `submit_create_edm_job` (line 237)

### Default Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `server_name` | `"databridge-1"` | Database server where EDMs are created |

### How to Change

Edit the default value in the function signature:

```python
# Before
def submit_create_edm_job(self, edm_name: str, server_name: str = "databridge-1") -> Tuple[int, Dict[str, Any]]:

# After (example: use a different server)
def submit_create_edm_job(self, edm_name: str, server_name: str = "databridge-2") -> Tuple[int, Dict[str, Any]]:
```

---

## MRI Import Settings

Controls how portfolio data files are imported into Moody's.

### Locations

There are two places where MRI import settings are defined:

1. **Function defaults**: `workspace/helpers/irp_integration/mri_import.py` - `submit_mri_import_job` (line 463)
2. **Actual values used**: `workspace/helpers/job.py` - `_submit_mri_import_job` (line 437)

### Default Values

| Parameter | Function Default | Actual Value Used | Description |
|-----------|------------------|-------------------|-------------|
| `delimiter` | `"COMMA"` | `"TAB"` | File delimiter format |
| `skip_lines` | `1` | `1` | Number of header lines to skip |
| `currency` | `"USD"` | `"USD"` | Currency code for values |
| `append_locations` | `False` | `False` | Whether to append to existing locations |

**Note**: The framework uses TAB-delimited files (not comma-delimited) to handle commas that appear in address data.

### How to Change

**To change the delimiter used by the framework** - Edit `workspace/helpers/job.py` (line 437):

```python
# Current setting (line 437)
workflow_id, http_request_body = client.mri_import.submit_mri_import_job(
    edm_name=edm_name,
    portfolio_name=portfolio_name,
    accounts_file_name=accounts_file,
    locations_file_name=locations_file,
    mapping_file_name=mapping_file_name,
    delimiter="TAB"  # Files are tab-delimited to handle commas in data
)

# Example: Change to comma-delimited (if your files use commas)
    delimiter="COMMA"
```

**To change the function defaults** - Edit `workspace/helpers/irp_integration/mri_import.py` (line 463):

```python
# Before
def submit_mri_import_job(
    ...
    delimiter: str = "COMMA",
    skip_lines: int = 1,
    currency: str = "USD",
    append_locations: bool = False
)

# After (example: change default currency)
def submit_mri_import_job(
    ...
    delimiter: str = "COMMA",
    skip_lines: int = 1,
    currency: str = "EUR",
    append_locations: bool = False
)
```

---

## Import File Naming

Controls the naming convention for CSV files created during data extraction.

### Location

**File**: `workspace/helpers/csv_export.py`

**Function**: `build_import_filename` (line 77)

### Default Pattern

The standard naming pattern is:

```
Modeling_{date_value}_Moodys_{cycle_type}_{portfolio}_{modifier}_{file_type}
```

| Component | Required | Example Values |
|-----------|----------|----------------|
| `Modeling` | Yes | Always "Modeling" |
| `date_value` | Yes | `202503`, `202511` |
| `Moodys` | Yes | Always "Moodys" |
| `cycle_type` | Optional | `Quarterly`, `Annual` |
| `portfolio` | Yes | `USEQ`, `USHU`, `CBHU`, `USFL` |
| `modifier` | Optional | `Full`, `Leak`, `Commercial_Excess` |
| `file_type` | Yes | `Account`, `Location` |

### Example Filenames

| Configuration | Result |
|---------------|--------|
| date=`202503`, portfolio=`CBHU`, type=`Account` | `Modeling_202503_Moodys_CBHU_Account.csv` |
| + cycle_type=`Quarterly` | `Modeling_202503_Moodys_Quarterly_CBHU_Account.csv` |
| + modifier=`Full` | `Modeling_202503_Moodys_Quarterly_CBHU_Full_Account.csv` |

### How to Change

The naming pattern is built in the function body (lines 118-130):

```python
# Current pattern construction (line 118-130)
parts = ['Modeling', date_value, 'Moodys']

if cycle_type:
    parts.append(cycle_type)

parts.append(portfolio)

if modifier:
    parts.append(modifier)

parts.append(file_type)

return '_'.join(parts)
```

**Example changes:**

```python
# Change prefix from "Modeling" to "RiskAnalysis"
parts = ['RiskAnalysis', date_value, 'Moodys']

# Add a fixed company identifier
parts = ['Modeling', date_value, 'Assurant', 'Moodys']

# Change the separator from underscore to hyphen
return '-'.join(parts)  # Instead of '_'.join(parts)

# Reorder components (put portfolio first)
parts = [portfolio, 'Modeling', date_value, 'Moodys']
```

---

## SQL Script Naming

Controls the naming patterns used to locate SQL scripts during workflow execution.

**Important**: When changing script name patterns, you must also rename the actual SQL files in `workspace/sql/` to match.

### Import File Scripts (Data Extraction)

**File**: `workspace/helpers/job.py`

**Function**: `_submit_data_extraction_job` (lines 1198-1199)

These scripts extract portfolio data from source databases:

```python
# Current naming pattern (lines 1198-1199)
account_script_name = f"2_Create_{import_file}_Moodys_ImportFile_Account.sql"
location_script_name = f"2_Create_{import_file}_Moodys_ImportFile_Location.sql"
```

| Variable | Source | Example Value |
|----------|--------|---------------|
| `import_file` | Configuration Excel "Import File" column | `USEQ`, `USHU`, `CBHU` |

**Example filenames**:
- `2_Create_USEQ_Moodys_ImportFile_Account.sql`
- `2_Create_USEQ_Moodys_ImportFile_Location.sql`

**How to change**:

```python
# Example: Change prefix from "2_Create" to "Extract"
account_script_name = f"Extract_{import_file}_Account.sql"
location_script_name = f"Extract_{import_file}_Location.sql"

# Example: Add date to filename
account_script_name = f"2_Create_{import_file}_{date_value}_Account.sql"
```

### Portfolio Mapping Scripts

**File**: `workspace/helpers/irp_integration/portfolio.py`

**Function**: `create_sub_portfolios` (line 632)

These scripts create sub-portfolios in Moody's:

```python
# Current naming pattern (line 632)
sql_script_name = f"2b_Query_To_Create_Sub_Portfolios_{import_file}_RMS_BackEnd.sql"
```

**Example filenames**:
- `2b_Query_To_Create_Sub_Portfolios_USEQ_RMS_BackEnd.sql`
- `2b_Query_To_Create_Sub_Portfolios_USHU_RMS_BackEnd.sql`

**How to change**:

```python
# Example: Simplify the naming pattern
sql_script_name = f"SubPortfolios_{import_file}.sql"
```

### Control Totals Scripts

**Location**: `workspace/sql/control_totals/`

These scripts are called from notebooks and referenced in `workspace/helpers/control_totals.py`:

| Script | Purpose |
|--------|---------|
| `3a_Control_Totals_Working_Table.sql` | Source data counts |
| `3b_Control_Totals_Contract_Import_File_Tables.sql` | Import file counts |
| `3d_RMS_EDM_Control_Totals.sql` | EDM validation counts |
| `3e_GeocodingSummary.sql` | Geocoding results |

**Note**: Control totals scripts are referenced directly by filename in notebooks and documentation. If you rename them, update:
1. The SQL file itself in `workspace/sql/control_totals/`
2. Any notebook cells that execute these scripts
3. Documentation references in `workspace/helpers/control_totals.py`

### Directory Structure

SQL scripts are organized by cycle type:

```
workspace/sql/
├── import_files/
│   ├── quarterly/    # Quarterly cycle scripts
│   ├── annual/       # Annual cycle scripts
│   ├── adhoc/        # Ad-hoc cycle scripts
│   └── test/         # Test cycle scripts
├── portfolio_mapping/
│   ├── quarterly/
│   ├── annual/
│   ├── adhoc/
│   └── test/
└── control_totals/   # Shared across all cycle types
```

The cycle type directory is resolved automatically based on the configuration's "Cycle Type" value.

---

## Quick Reference

| Setting Type | File | Function | Line |
|--------------|------|----------|------|
| Analysis | `workspace/helpers/irp_integration/analysis.py` | `submit_portfolio_analysis_job` | 154 |
| GeoHaz | `workspace/helpers/irp_integration/portfolio.py` | `submit_geohaz_job` | 315 |
| Grouping | `workspace/helpers/irp_integration/analysis.py` | `submit_analysis_grouping_job` | 688 |
| EDM Creation | `workspace/helpers/irp_integration/edm.py` | `submit_create_edm_job` | 237 |
| MRI Import | `workspace/helpers/job.py` | `_submit_mri_import_job` | 437 |
| Import File Naming | `workspace/helpers/csv_export.py` | `build_import_filename` | 77 |
| SQL Script Naming (Import) | `workspace/helpers/job.py` | `_submit_data_extraction_job` | 1198 |
| SQL Script Naming (Mapping) | `workspace/helpers/irp_integration/portfolio.py` | `create_sub_portfolios` | 632 |
| SQL Scripts | `workspace/sql/` | Various `.sql` files | N/A |

---

## Tips

1. **Test changes in a test cycle first** - Use `Cycle Type = Test_<scenario>` to verify changes work correctly

2. **Document your changes** - Add a comment explaining why the default was changed

3. **Commit changes via Git Cola** - See [Updating the Code Repository](UPDATING_CODE_REPOSITORY.md) for instructions

4. **Keep backups** - Note the original values before changing them
