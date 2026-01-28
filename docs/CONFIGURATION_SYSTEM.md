# Configuration System

Excel-based master configuration that drives job generation for all batch types.

## Overview

```
Excel File → Validation → Database Storage → Transformer → Job Creation
```

The configuration system:
1. Parses Excel files with 10 required sheets
2. Validates structure, references, and business rules
3. Stores as JSON in PostgreSQL
4. Transforms into job configurations per batch type

## Excel File Structure

### Required Sheets

| Sheet | Structure | Purpose |
|-------|-----------|---------|
| Metadata | Key-Value | Project settings (versions, dates, names) |
| Databases | Table | EDM database definitions |
| Portfolios | Table | Portfolio-to-database mappings |
| Reinsurance Treaties | Table | Treaty definitions with financial terms |
| GeoHaz Thresholds | Table | Hazard thresholds by portfolio |
| Analysis Table | Table | Analysis definitions (profiles, treaties) |
| Groupings | Dynamic | Group hierarchies (Item1-Item50 columns) |
| Products and Perils | Table | Analysis-to-peril-to-product mappings |
| Moody's Reference Data | Lists | Reference profiles from Moody's |
| ExposureGroup <-> Portname | Table | Exposure group mappings |

### Key Sheets Explained

**Metadata** (Key-Value pairs):
```
Current Date Value     → "202503"
EDM Data Version       → "23.0.0"
Cycle Type             → "quarterly" | "annual" | "adhoc" | "test"
Export RDM Name        → "RM_EDM_202503_Quarterly_Results"
```

**Databases**:
```
| Database                        | Store in Data Bridge? |
|---------------------------------|-----------------------|
| RM_EDM_202503_Quarterly_CBAP    | Y                     |
| RM_EDM_202503_Quarterly_USEQ    | Y                     |
```

**Analysis Table** (one row per analysis):
```
| Database | Portfolio | Analysis Name | Analysis Profile | Output Profile | Treaty 1 | Treaty 2 |
|----------|-----------|---------------|------------------|----------------|----------|----------|
| RM_EDM_* | CBHU      | CBHU_LT       | DLM CBHU v23     | Modeling_*     | Treaty_A | null     |
```

**Groupings** (dynamic columns):
```
| Group_Name            | Item1          | Item2          | Item3     |
|-----------------------|----------------|----------------|-----------|
| 202503_USST_Group     | USST_Analysis1 | USST_Analysis2 | ...       |
| 202503_Rollup_Group   | 202503_USST_*  | 202503_CBAP_*  | ...       |
```

## Configuration Status Flow

```
NEW → VALID → ACTIVE
  ↓
ERROR (validation failed)
```

| Status | Meaning |
|--------|---------|
| NEW | Initial state after parsing |
| VALID | Passed all validation |
| ACTIVE | Being used by batches |
| ERROR | Validation failed |

**Constraints:**
- One configuration per cycle
- Cannot replace configuration that has batches
- Only VALID or ACTIVE configs can create batches

## Validation

### Three Validation Layers

**Layer 1: Structural Validation** (per-sheet)
- Required columns/keys present
- Data types match schema
- Regex patterns validated
- Nullable constraints enforced
- Unique constraints checked

**Layer 2: Cross-Sheet Validation**
- Foreign key references (Portfolio.Database → Databases.Database)
- Analysis profiles exist in Moody's Reference Data
- Grouping items reference valid analyses/portfolios/groups
- Business rules (each database has ≥1 base portfolio)

**Layer 3: API Validation** (optional)
- Model profiles exist in Moody's
- Output profiles exist in Moody's
- Entities don't already exist (EDMs, portfolios, analyses)

### Running Validation

```python
from helpers.configuration import validate_configuration_file

errors, warnings, config_data = validate_configuration_file(
    file_path,
    skip_entity_validation=False  # Set True to skip API checks
)

if errors:
    print(f"Validation failed: {errors}")
else:
    print("Configuration valid")
```

### Validation Results

Stored in `config_data['_validation']`:
```python
{
    'Metadata': {
        'status': 'SUCCESS',
        'errors': [],
        'warnings': [],
        'row_count': 12,
        'validated_at': '2025-01-20T10:30:00Z'
    },
    '_cross_sheet': {
        'status': 'SUCCESS',
        'errors': []
    }
}
```

## Transformer System

Transformers convert configuration data into job-specific configurations.

### How It Works

```python
from helpers.configuration import create_job_configurations

# Input: batch type + full configuration
job_configs = create_job_configurations(batch_type, config_data)

# Output: list of job configurations (one per job to create)
# [
#     {'Database': 'EDM1', 'Portfolio': 'PORT1', ...},
#     {'Database': 'EDM1', 'Portfolio': 'PORT2', ...},
# ]
```

### Transformer Registry

| Batch Type | Input Sheet(s) | Jobs Created |
|------------|----------------|--------------|
| EDM Creation | Databases | 1 per database |
| Portfolio Creation | Portfolios (base only) | 1 per base portfolio |
| MRI Import | Portfolios (base only) | 1 per base portfolio |
| Create Reinsurance Treaties | Treaties + Analysis Table | 1 per treaty-EDM pair |
| EDM DB Upgrade | Databases | 1 per database |
| GeoHaz | Portfolios (base only) | 1 per base portfolio |
| Portfolio Mapping | Portfolios (base only) | 1 per base portfolio |
| Analysis | Analysis Table | 1 per analysis row |
| Grouping | Groupings (analysis-only) | 1 per group |
| Grouping Rollup | Groupings (rollup) | 1 per group |
| Export to RDM | All analyses + groups | Chunked (100 per job) |
| Data Extraction | Portfolios (base only) | 1 per base portfolio |

### Key Transformers Explained

**Data Extraction** - Auto-generates CSV filenames:
```python
{
    'Import File': 'CBHU',
    'accounts_import_file': 'Modeling_202503_Moodys_CBHU_Account.csv',
    'locations_import_file': 'Modeling_202503_Moodys_CBHU_Location.csv',
    # SQL scripts resolved by job.py based on Import File and Cycle Type:
    # - 2_Create_CBHU_Moodys_ImportFile_Account.sql
    # - 2_Create_CBHU_Moodys_ImportFile_Location.sql
}
```

**Grouping vs Grouping Rollup**:
- **Grouping**: Groups containing only analysis/portfolio names
- **Grouping Rollup**: Groups containing references to other groups
- Rollups must be created after analysis-only groups

**Export to RDM** - Chunked for large exports:
```python
# 175 items to export
# Job 1 (seed): Creates RDM with 1 item, is_seed_job=True
# Job 2: 100 items, uses database_id from seed
# Job 3: 74 items, uses database_id from seed
```

## Loading Configuration

### Notebook Workflow

`Stage_01_Setup/Step_02_Validate_Config_File.ipynb`:

1. **Select file** - Browse cycle's `files/configuration/` directory
2. **Validate** - Run all validation layers
3. **Preview** - Show metadata, counts, expected jobs
4. **Load** - Store in database

### Programmatic Loading

```python
from helpers.configuration import load_configuration_file

config_id = load_configuration_file(
    cycle_id=1,
    file_path='/path/to/config.xlsx',
    skip_entity_validation=False
)
# Configuration stored with status='VALID'
```

## Batch Creation

When a batch is created, the transformer generates job configurations:

```python
from helpers.batch import create_batch

batch_id = create_batch(
    batch_type='Analysis',
    configuration_id=config_id,
    step_id=step_id
)
# Creates:
# - 1 batch record (status='INITIATED')
# - N job_configuration records (one per analysis)
# - N job records (one per job_configuration)
```

## Key Functions

### Configuration (`helpers.configuration`)

| Function | Purpose |
|----------|---------|
| `validate_configuration_file(path, skip_entity)` | Run all validation layers |
| `load_configuration_file(cycle_id, path, skip)` | Validate and store in database |
| `create_job_configurations(batch_type, config)` | Transform config to job configs |
| `get_active_configuration(cycle_id)` | Get current config for cycle |
| `read_configuration(config_id)` | Get config by ID |

### Entity Validation (`helpers.entity_validator`)

| Function | Purpose |
|----------|---------|
| `validate_config_entities_not_exist(config)` | Check entities don't exist in Moody's |
| `EntityValidator.validate_analysis_batch(jobs)` | Check analyses before submission |

## Error Handling

```python
from helpers.configuration import ConfigurationError

try:
    config_id = load_configuration_file(cycle_id, path)
except ConfigurationError as e:
    # Validation failed, config not loaded
    print(f"Configuration error: {e}")
```

Common errors:
- Missing required sheets or columns
- Invalid data types or patterns
- Foreign key violations
- Duplicate values where uniqueness required
- Entities already exist in Moody's
