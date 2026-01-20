# SQL Server Integration

External SQL Server connectivity for data extraction, control totals validation, and portfolio mapping.

## Overview

The IRP Framework connects to two types of databases:

| Database | Purpose | Module |
|----------|---------|--------|
| PostgreSQL | Workflow state (cycles, batches, jobs) | `helpers/database.py` |
| SQL Server (MSSQL) | External data sources (Assurant, Databridge) | `helpers/sqlserver.py` |

The SQL Server module provides:
- Multiple named database connections via environment variables
- SQL script execution with parameter substitution
- Support for SQL Server and Windows (Kerberos) authentication
- Multi-result-set support for complex queries

## Connection Configuration

### Environment Variable Pattern

Each connection is configured via environment variables following this pattern:

```
MSSQL_{CONNECTION_NAME}_{SETTING}
```

For example, a connection named `ASSURANT` uses variables like `MSSQL_ASSURANT_SERVER`.

### Per-Connection Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MSSQL_{NAME}_SERVER` | Yes | - | Server hostname or IP address |
| `MSSQL_{NAME}_AUTH_TYPE` | No | `SQL` | Authentication type: `SQL` or `WINDOWS` |
| `MSSQL_{NAME}_USER` | SQL auth only | - | SQL Server username |
| `MSSQL_{NAME}_PASSWORD` | SQL auth only | - | SQL Server password |
| `MSSQL_{NAME}_PORT` | No | `1433` | Server port |
| `MSSQL_{NAME}_DATABASE` | No | - | Default database (can also use USE in SQL) |

### Global Settings

These apply to all connections:

| Variable | Default | Description |
|----------|---------|-------------|
| `MSSQL_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver name |
| `MSSQL_TRUST_CERT` | `yes` | Trust server certificate |
| `MSSQL_TIMEOUT` | `30` | Connection timeout in seconds |

### Example Configuration

```bash
# SQL Server Authentication
MSSQL_DATABRIDGE_SERVER=77aea63098bc8fe4578390278d631a6d.databridge.rms-pe.com
MSSQL_DATABRIDGE_USER=Modeling_Automation
MSSQL_DATABRIDGE_PASSWORD=your_password_here

# Windows Authentication (Kerberos)
MSSQL_ASSURANT_SERVER=vdbpdw-housing-secondary.database.cead.prd
MSSQL_ASSURANT_AUTH_TYPE=WINDOWS
# No USER/PASSWORD needed - uses Kerberos ticket
```

### Standard Connections

| Connection Name | Purpose | Auth Type |
|-----------------|---------|-----------|
| `TEST` | Local SQL Server Express for development | SQL |
| `DATABRIDGE` | Moody's Databridge (RMS backend) | SQL |
| `ASSURANT` | Assurant data warehouse | Windows |

## SQL Script Directory

SQL scripts are stored in `workspace/sql/` and can be executed via `execute_query_from_file()`.

### Folder Structure

```
workspace/sql/
├── README.md                  # SQL script writing guide
├── examples/                  # Example scripts
│   ├── sample_query.sql       # Basic parameter usage
│   ├── list_edm_tables.sql    # USE statement example
│   └── test_multiple_select.sql
├── control_totals/            # Control totals validation
│   ├── 3d_RMS_EDM_Control_Totals.sql
│   └── 3e_GeocodingSummary.sql
├── data_export/               # Data export queries
│   └── rdm_analysis_summary.sql
├── import_files/              # Moody's import file generation
│   ├── adhoc/
│   ├── annual/
│   ├── quarterly/
│   └── test/
└── portfolio_mapping/         # Sub-portfolio creation via SQL
    ├── adhoc/
    ├── annual/
    ├── quarterly/
    └── test/
```

### Organization by Cycle Type

The `import_files/` and `portfolio_mapping/` directories are organized by cycle type (`adhoc`, `annual`, `quarterly`, `test`). This structure exists because:

- **Import file queries** vary by cycle (different date filters, portfolio selections)
- **Portfolio mapping SQL** has cycle-specific business logic
- **Easy maintenance** of variations without complex conditional logic in scripts

**The folder used is determined by the `Cycle Type` metadata value from the configuration file.** When a cycle is created, the `Cycle Type` field in the Excel configuration's Metadata sheet (e.g., `quarterly`, `annual`, `adhoc`, `test`) determines which subfolder to use for SQL scripts.

#### Directory Resolution Logic

The framework uses helper functions to resolve the cycle type to a directory name:

- `_resolve_import_files_directory()` in [job.py](workspace/helpers/job.py)
- `resolve_cycle_type_directory()` in [portfolio.py](workspace/helpers/irp_integration/portfolio.py)

**Resolution rules:**

1. If cycle type contains `test` (case-insensitive), use the `test/` directory
   - Examples: `Test`, `Test_Q1`, `test_annual` → all resolve to `test/`
2. Otherwise, match the cycle type to a directory name (case-insensitive)
   - `Quarterly` → `quarterly/`
   - `Annual` → `annual/`
   - `Adhoc` → `adhoc/`
3. If no matching directory exists, raise an error

**Example resolution:**

| Cycle Type (from config) | Resolved Directory |
|--------------------------|-------------------|
| `quarterly` | `quarterly/` |
| `Quarterly` | `quarterly/` |
| `annual` | `annual/` |
| `test` | `test/` |
| `Test_Q1_2025` | `test/` |
| `adhoc` | `adhoc/` |

### File Path Resolution

When calling `execute_query_from_file()`:

- **Relative paths** resolve from `workspace/sql/`
- **Absolute paths** are used as-is

The framework automatically resolves paths for Data Extraction and Portfolio Mapping jobs using the cycle type from configuration:

```python
# Internal resolution (in job.py for Data Extraction)
cycle_type = job_config['Cycle Type']  # e.g., 'Quarterly'
cycle_type_dir = _resolve_import_files_directory(cycle_type)  # Returns 'quarterly'

sql_script_path = f'import_files/{cycle_type_dir}/2_Create_{import_file}_Moodys_ImportFile.sql'
# Result: 'import_files/quarterly/2_Create_CBHU_Moodys_ImportFile.sql'
```

```python
# Internal resolution (in portfolio.py for Portfolio Mapping)
cycle_type_dir = resolve_cycle_type_directory(cycle_type)  # Returns 'quarterly'

sql_script_path = f'portfolio_mapping/{cycle_type_dir}/2b_Query_To_Create_Sub_Portfolios_{import_file}_RMS_BackEnd.sql'
# Result: 'portfolio_mapping/quarterly/2b_Query_To_Create_Sub_Portfolios_USEQ_RMS_BackEnd.sql'
```

## Parameter Substitution

SQL scripts support named parameters using double-brace syntax with spaces:

```
{{ parameter_name }}
```

### Basic Usage

**SQL Script:**
```sql
SELECT * FROM portfolios
WHERE portfolio_id = {{ portfolio_id }}
  AND created_date >= {{ start_date }}
```

**Python:**
```python
df = execute_query_from_file(
    'my_query.sql',
    params={'portfolio_id': 123, 'start_date': '2025-01-01'},
    connection='DATABRIDGE'
)
```

### Context-Aware Escaping

Parameters are escaped differently based on their context in the SQL:

**Value Context** (escaped and quoted):
```sql
WHERE id = {{ user_id }} AND name = {{ user_name }}
-- With params={'user_id': 123, 'user_name': "O'Brien"}
-- Becomes: WHERE id = 123 AND name = 'O''Brien'
```

**Identifier Context** (raw substitution, no quotes):

Inside square brackets:
```sql
USE [{{ db_name }}]
-- With params={'db_name': 'my_database'}
-- Becomes: USE [my_database]
```

Part of table/column names:
```sql
SELECT * FROM CombinedData_{{ date_val }}_Working
-- With params={'date_val': '20250115'}
-- Becomes: SELECT * FROM CombinedData_20250115_Working
```

Inside string literals:
```sql
SELECT 'Modeling_{{ date_val }}_Moodys' as filename
-- With params={'date_val': '202503'}
-- Becomes: SELECT 'Modeling_202503_Moodys' as filename
```

### Type Handling

| Python Type | SQL Result |
|-------------|------------|
| `str` | Escaped, wrapped in quotes: `'value'` |
| `int`, `float` | Inserted directly: `123` |
| `None` | `NULL` |
| `bool` | `1` (True) or `0` (False) |
| numpy/pandas types | Auto-converted to native Python |

### Security

Parameter substitution automatically prevents SQL injection:
- String values have single quotes escaped (`'` → `''`)
- Identifiers are validated to contain only safe characters
- Never use string concatenation for dynamic SQL values

## Query Execution Functions

### Core Functions

| Function | Purpose | Returns |
|----------|---------|---------|
| `execute_query()` | Execute inline SELECT query | `pd.DataFrame` |
| `execute_query_from_file()` | Execute SELECT from .sql file | `List[pd.DataFrame]` |
| `execute_scalar()` | Get single value from query | `Any` |
| `execute_command()` | Execute INSERT/UPDATE/DELETE | `int` (rows affected) |

### Utility Functions

| Function | Purpose | Returns |
|----------|---------|---------|
| `test_connection()` | Verify database connectivity | `bool` |
| `sql_file_exists()` | Check if SQL script file exists | `bool` |
| `display_result_sets()` | Pretty-print multiple DataFrames | `None` |
| `get_connection()` | Context manager for raw connection | `pyodbc.Connection` |

### execute_query_from_file()

The primary function for executing SQL scripts:

```python
from helpers.sqlserver import execute_query_from_file

dataframes = execute_query_from_file(
    file_path='control_totals/3d_RMS_EDM_Control_Totals.sql',
    params={
        'WORKSPACE_EDM': 'WORKSPACE_EDM_202503',
        'CYCLE_TYPE': 'Quarterly',
        'DATE_VALUE': '202503'
    },
    connection='DATABRIDGE',
    database='master'  # Optional: specify database
)
```

**Parameters:**
- `file_path`: Path to SQL file (relative to `workspace/sql/` or absolute)
- `params`: Dictionary of parameter values for substitution
- `connection`: Name of the connection to use
- `database`: Optional database name (can also use USE statement in SQL)

**Returns:** `List[pd.DataFrame]` - One DataFrame per SELECT statement in the script.

### Multi-Result-Set Support

SQL scripts can contain multiple SELECT statements. Each SELECT returns a separate DataFrame:

```python
# Script has 10 SELECT statements
results = execute_query_from_file('control_totals/3d_RMS_EDM_Control_Totals.sql', ...)

# Access individual result sets
policy_summary = results[0]
location_counts = results[1]
location_values = results[2]
location_deductibles = results[3]
# ... and so on
```

### display_result_sets()

For notebooks, use this to pretty-print multiple DataFrames:

```python
from helpers.sqlserver import execute_query_from_file, display_result_sets

results = execute_query_from_file('control_totals.sql', ...)
display_result_sets(results, max_rows=10)
```

Output:
```
================================================================================
QUERY RESULTS: 4 result set(s)
================================================================================

--------------------------------------------------------------------------------
Result Set 1 of 4
--------------------------------------------------------------------------------
Rows: 15 | Columns: 5

   PORTNAME  PolicyCount  PolicyLimit  ...
0  CBHU            1234     50000000  ...

... (3 more rows not shown)

================================================================================
```

## Windows Authentication (Kerberos)

For SQL Servers that require Windows/Active Directory authentication.

### When to Use

- Corporate SQL Servers that don't accept SQL authentication
- Servers behind Active Directory authentication
- When security policy requires domain authentication

### Configuration

```bash
# Enable Kerberos
KERBEROS_ENABLED=true

# Domain settings
KRB5_REALM=CEAD.PRD
KRB5_PRINCIPAL=bi_riskmodeler_prd@CEAD.PRD

# Authentication (choose one)
KRB5_KEYTAB=/path/to/service.keytab    # Keytab file (preferred)
KRB5_PASSWORD=your_password             # Or password-based
```

### Connection Configuration

```bash
MSSQL_ASSURANT_SERVER=vdbpdw-housing-secondary.database.cead.prd
MSSQL_ASSURANT_AUTH_TYPE=WINDOWS
# No USER/PASSWORD needed
```

### Automatic Ticket Management

When using Windows authentication, the module automatically:

1. Checks if a valid Kerberos ticket exists
2. Renews the ticket if it's expiring within 5 minutes
3. Uses the keytab file or password to obtain a new ticket

This happens transparently when you call any query function.

### Manual Kerberos Functions

```python
from helpers.sqlserver import check_kerberos_status, init_kerberos

# Check current Kerberos status
status = check_kerberos_status()
print(f"Has ticket: {status['has_ticket']}")
print(f"Principal: {status['principal']}")
print(f"Expires: {status['expiration']}")

# Manually initialize Kerberos
success = init_kerberos()  # Uses environment variables
```

## Usage Examples

### Basic Query with Parameters

```python
from helpers.sqlserver import execute_query_from_file

df = execute_query_from_file(
    'import_files/quarterly/2_Create_CBHU_Moodys_ImportFile.sql',
    params={
        'DATE_VALUE': '202503',
        'CYCLE_TYPE': 'Quarterly'
    },
    connection='ASSURANT',
    database='DW_EXP_MGMT_USER'
)

print(f"Extracted {len(df)} rows")
```

### Control Totals Validation

```python
from helpers.sqlserver import execute_query_from_file, display_result_sets

results = execute_query_from_file(
    'control_totals/3d_RMS_EDM_Control_Totals.sql',
    params={
        'WORKSPACE_EDM': 'WORKSPACE_EDM_202503',
        'CYCLE_TYPE': 'Quarterly',
        'DATE_VALUE': '202503'
    },
    connection='DATABRIDGE'
)

# Display all 10 result sets
display_result_sets(results)

# Or access individually
policy_summary_df = results[0]
location_counts_df = results[1]
```

### Check Script Exists Before Executing

```python
from helpers.sqlserver import execute_query_from_file, sql_file_exists

sql_script = f'portfolio_mapping/quarterly/2b_Query_To_Create_Sub_Portfolios_{portfolio_name}_RMS_BackEnd.sql'

if sql_file_exists(sql_script):
    result = execute_query_from_file(sql_script, params=params, connection='DATABRIDGE')
else:
    print(f"Skipping - script not found: {sql_script}")
```

### Test Connection

```python
from helpers.sqlserver import test_connection

if test_connection('DATABRIDGE'):
    print("Connection successful!")
else:
    print("Connection failed - check configuration")
```

### Inline Query (No File)

```python
from helpers.sqlserver import execute_query

df = execute_query(
    "SELECT TOP 10 * FROM portfolios WHERE value > {{ min_value }}",
    params={'min_value': 1000000},
    connection='DATABRIDGE',
    database='DataWarehouse'
)
```

## Error Handling

### Exception Types

| Exception | When Raised |
|-----------|-------------|
| `SQLServerConnectionError` | Cannot connect to server |
| `SQLServerConfigurationError` | Missing environment variables, invalid auth type |
| `SQLServerQueryError` | Query execution failed, missing parameter |

### Example

```python
from helpers.sqlserver import (
    execute_query_from_file,
    SQLServerConnectionError,
    SQLServerConfigurationError,
    SQLServerQueryError
)

try:
    results = execute_query_from_file('my_query.sql', params={...}, connection='DATABRIDGE')
except SQLServerConnectionError as e:
    print(f"Connection failed: {e}")
except SQLServerConfigurationError as e:
    print(f"Configuration error: {e}")
except SQLServerQueryError as e:
    print(f"Query failed: {e}")
```

## Key Functions Reference

### Connection Management

```python
# Get connection configuration
config = get_connection_config('DATABRIDGE')
# Returns: {'server': '...', 'user': '...', 'auth_type': 'SQL', ...}

# Build ODBC connection string
conn_str = build_connection_string('DATABRIDGE', database='MyDB')

# Context manager for raw connection
with get_connection('DATABRIDGE') as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    print(cursor.fetchone())
```

### Kerberos Functions

```python
# Check Kerberos status
status = check_kerberos_status()

# Initialize Kerberos ticket
success = init_kerberos()

# Check if ticket is valid (with time buffer)
is_valid = is_ticket_valid(min_remaining_minutes=5)

# Ensure valid ticket (auto-renews if needed)
ensure_valid_kerberos_ticket()
```

## Integration with Other Modules

### CSV Export

The `csv_export` module works directly with SQL Server query results:

```python
from helpers.sqlserver import execute_query_from_file
from helpers.csv_export import save_dataframes_to_csv, build_import_filenames

# Execute SQL and get DataFrames
dataframes = execute_query_from_file(
    'import_files/quarterly/2_Create_CBHU_Moodys_ImportFile.sql',
    params={'DATE_VALUE': '202503'},
    connection='ASSURANT'
)

# Generate standardized filenames
filenames = build_import_filenames('CBHU', '202503')
# Returns: ['Modeling_202503_Moodys_CBHU_Account.csv', 'Modeling_202503_Moodys_CBHU_Location.csv']

# Save to working files directory
save_dataframes_to_csv(dataframes, filenames)
```

### Control Totals

The `control_totals` module uses SQL Server queries for validation:

```python
from helpers.control_totals import compare_3d_vs_3e

# Uses execute_query_from_file() internally
comparison_df, all_matched = compare_3d_vs_3e(results_3d, results_3e)
```

## Writing SQL Scripts

See `workspace/sql/README.md` for complete guidelines. Key points:

1. **Header comments** - Document purpose, parameters, and expected output
2. **Parameter syntax** - Use `{{ param_name }}` with spaces
3. **Multiple result sets** - Each SELECT becomes a DataFrame in the returned list
4. **USE statements** - Can switch databases within scripts
5. **No credentials** - Never hardcode passwords in scripts

### Example Script Header

```sql
-- ============================================================================
-- Script: extract_policies.sql
-- Purpose: Extract policy data for a specific cycle
-- Parameters:
--   {{ cycle_name }} - Name of the analysis cycle (e.g., 'Q1-2025')
--   {{ run_date }} - Date of extraction (format: 'YYYY-MM-DD')
-- Returns: Policy records with associated risk metrics
-- Author: Data Analytics Team
-- Created: 2025-01-15
-- ============================================================================

USE [{{ database_name }}]

SELECT p.policy_id, p.policy_name, r.risk_score
FROM policies p
LEFT JOIN risks r ON p.policy_id = r.policy_id
WHERE p.cycle_name = {{ cycle_name }}
  AND p.extraction_date = {{ run_date }};
```
