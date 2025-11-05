# SQL Scripts Directory

This directory contains SQL scripts for executing operations against external MSSQL databases in the IRP Notebook Framework.

## Purpose

SQL scripts stored here are independent of workflow cycles and can be executed from any notebook step. They provide a centralized location for:

- Data extraction queries
- Data transformation scripts
- Reporting queries
- Database maintenance operations

## Directory Structure

```
workspace/sql/
├── README.md           # This file
├── examples/           # Example SQL scripts
│   └── sample_query.sql
└── [your_scripts].sql  # Your SQL scripts
```

## Using SQL Scripts in Notebooks

### Basic Query Execution

```python
from helpers.sqlserver import execute_query_from_file

# Execute query and get results as DataFrame
df = execute_query_from_file(
    'my_query.sql',
    params={'portfolio_id': 123},
    connection='AWS_DW'
)

print(df.head())
```

### Script Execution (INSERT/UPDATE/DELETE)

```python
from helpers.sqlserver import execute_script_file

# Execute script and get rows affected
rows_affected = execute_script_file(
    'update_portfolios.sql',
    params={'status': 'ACTIVE', 'min_value': 100000},
    connection='ANALYTICS'
)

print(f"Updated {rows_affected} rows")
```

## Parameter Syntax

Use `{param_name}` for named parameters in your SQL scripts:

```sql
-- Good: Named parameters
SELECT * FROM portfolios
WHERE portfolio_id = {portfolio_id}
  AND created_date >= {start_date}
  AND portfolio_value > {min_value}

-- These will be automatically converted to parameterized queries
```

### Parameter Example

**SQL Script (workspace/sql/get_portfolio.sql):**
```sql
-- Get portfolio with risk metrics
-- Parameters: {portfolio_id}, {risk_type}

SELECT
    p.portfolio_name,
    p.portfolio_value,
    r.risk_type,
    r.risk_value,
    r.calculated_ts
FROM portfolios p
INNER JOIN risks r ON p.id = r.portfolio_id
WHERE p.id = {portfolio_id}
  AND r.risk_type = {risk_type}
ORDER BY r.calculated_ts DESC;
```

**Usage in Notebook:**
```python
from helpers.sqlserver import execute_query_from_file

df = execute_query_from_file(
    'get_portfolio.sql',
    params={
        'portfolio_id': 123,
        'risk_type': 'VaR_95'
    },
    connection='AWS_DW'
)
```

## SQL Script Best Practices

### 1. Add Header Comments

Start each script with a comment block describing:
- Purpose of the script
- Required parameters
- Expected output
- Author and date

```sql
-- ============================================================================
-- Script: extract_policies.sql
-- Purpose: Extract policy data for a specific cycle
-- Parameters:
--   {cycle_name} - Name of the analysis cycle (e.g., 'Q1-2025')
--   {run_date} - Date of extraction (format: 'YYYY-MM-DD')
-- Returns: Policy records with associated risk metrics
-- Author: Data Analytics Team
-- Created: 2025-01-15
-- ============================================================================

SELECT p.policy_id, p.policy_name, r.risk_score
FROM policies p
LEFT JOIN risks r ON p.policy_id = r.policy_id
WHERE p.cycle_name = {cycle_name}
  AND p.extraction_date = {run_date};
```

### 2. Use Clear Naming

- Use descriptive filenames: `extract_policies_for_cycle.sql` not `query1.sql`
- Use snake_case for file names
- Group related scripts with prefixes: `extract_*.sql`, `update_*.sql`, `report_*.sql`

### 3. Format for Readability

- Use consistent indentation (2 or 4 spaces)
- Align SQL keywords (SELECT, FROM, WHERE, etc.)
- One column per line in SELECT statements for complex queries
- Add blank lines between logical sections

### 4. Handle NULL Values

Always consider NULL handling in your queries:

```sql
-- Good: Explicit NULL handling
SELECT
    portfolio_id,
    COALESCE(risk_value, 0) as risk_value,
    ISNULL(status, 'UNKNOWN') as status
FROM portfolios
WHERE portfolio_value IS NOT NULL;
```

### 5. Test with Sample Data

Before using in production:
1. Test script with sample parameters
2. Verify results match expectations
3. Check performance on realistic data volumes
4. Document any known limitations

## File Paths

Scripts can be referenced by:

1. **Relative path (from workspace/sql/):**
   ```python
   execute_query_from_file('my_query.sql', ...)
   execute_query_from_file('examples/sample_query.sql', ...)
   ```

2. **Absolute path:**
   ```python
   execute_query_from_file('/full/path/to/script.sql', ...)
   ```

## Database Connections

Database connections are configured via environment variables. Available connections:

- `TEST` - SQL Server Express test container (for development/testing)
- `AWS_DW` - AWS Data Warehouse (production, if configured)
- `ANALYTICS` - Analytics Database (production, if configured)

Contact your system administrator for connection names and access.

## Multi-Statement Scripts

Scripts can contain multiple SQL statements separated by semicolons:

```sql
-- Multi-statement script
UPDATE portfolios SET status = {new_status} WHERE value < {min_value};
DELETE FROM temp_portfolios WHERE processed = 1;
INSERT INTO audit_log (action, timestamp) VALUES ('cleanup', GETDATE());
```

When executed with `execute_script_file()`, all statements run in a single transaction.

## Error Handling

If a script fails:

1. Check the error message for details (includes connection name and file path)
2. Verify parameters are correctly named and provided
3. Test the connection: `from helpers.sqlserver import test_connection; test_connection('AWS_DW')`
4. Verify the SQL syntax is correct for SQL Server
5. Check database permissions

## Security Notes

- **Never hardcode credentials** in SQL scripts
- **Never commit sensitive data** to version control
- Use parameters for dynamic values, not string concatenation
- Review scripts before execution in production databases
- Limit permissions to read-only when possible

## Examples

See the `examples/` directory for sample SQL scripts demonstrating:
- Parameter usage
- Common query patterns
- Proper formatting and documentation

## Support

For questions or issues:
- Review the main project README
- Check `docs/` directory for additional documentation
- Contact the development team