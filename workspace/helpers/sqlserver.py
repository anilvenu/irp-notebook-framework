"""
IRP Notebook Framework - SQL Server Database Operations

This module provides SQL Server (MSSQL) database connectivity and operations
for the IRP Notebook Framework. It supports multiple named database connections
and provides utilities for executing queries and SQL scripts.

================================================================================
CONNECTION MANAGEMENT
================================================================================

The framework supports multiple named MSSQL database connections configured
via environment variables. Each connection requires:

Environment Variable Pattern:
    MSSQL_{CONNECTION_NAME}_SERVER    - Server hostname or IP (required)
    MSSQL_{CONNECTION_NAME}_USER      - SQL Server authentication username (required)
    MSSQL_{CONNECTION_NAME}_PASSWORD  - SQL Server authentication password (required)
    MSSQL_{CONNECTION_NAME}_PORT      - Port (optional, defaults to 1433)
    MSSQL_{CONNECTION_NAME}_DATABASE  - Database name (optional, use USE statements in SQL scripts)

Example Configuration:
    # AWS Data Warehouse connection (no default database, use USE statements in SQL)
    MSSQL_AWS_DW_SERVER=aws-db.company.com
    MSSQL_AWS_DW_USER=irp_service
    MSSQL_AWS_DW_PASSWORD=secretpassword

    # Analytics Database connection (no default database, use USE statements in SQL)
    MSSQL_ANALYTICS_SERVER=analytics.company.com
    MSSQL_ANALYTICS_USER=irp_service
    MSSQL_ANALYTICS_PASSWORD=secretpassword

Usage in Notebooks:
    from helpers.sqlserver import execute_query, execute_script_file

    # Option 1: Specify database via connection parameter
    df = execute_query(
        "SELECT * FROM portfolios WHERE value > {{ min_value }}",
        params={'min_value': 1000000},
        connection='AWS_DW',
        database='DataWarehouse'
    )

    # Option 2: Include USE statement in SQL (if script needs multiple databases)
    query = "USE DataWarehouse; SELECT * FROM portfolios WHERE value > {{ min_value }}"
    df = execute_query(query, params={'min_value': 1000000}, connection='AWS_DW')

    # Execute SQL script from file with database parameter
    rows_affected = execute_script_file(
        'workspace/sql/extract_policies.sql',
        params={'cycle_name': 'Q1-2025', 'run_date': '2025-01-15'},
        connection='ANALYTICS',
        database='AnalyticsDB'
    )

================================================================================
DATABASE SPECIFICATION
================================================================================

All SQL Server methods accept an optional 'database' parameter to specify which
database to use. There are two ways to specify the database:

1. **database parameter** (Recommended): Pass database name to any execute method
   - Cleanest approach when working with a single database
   - Connection string includes DATABASE= clause
   - Example: execute_query(..., database='DataWarehouse')

2. **USE statement in SQL**: Include USE [database] in your SQL script
   - Useful when script needs to access multiple databases
   - Example: "USE DataWarehouse; SELECT * FROM portfolios"

If neither is specified, connection will not default to any database and queries
must use fully qualified table names (database.schema.table) or will fail.

================================================================================
PARAMETER SUBSTITUTION
================================================================================

SQL queries and scripts support named parameters using {{ param_name }} syntax:

    SELECT * FROM portfolios
    WHERE portfolio_id = {{ portfolio_id }}
      AND created_date >= {{ start_date }}

Parameters are safely substituted with SQL injection protection:
- String values are escaped (single quotes doubled)
- Numeric values are inserted directly
- NULL values handled appropriately
- Numpy and pandas types are converted to native Python types

================================================================================
DESIGN PHILOSOPHY
================================================================================

This module follows the same patterns as database.py:
- Simple connection management (no pooling for notebook environments)
- Context managers for automatic resource cleanup
- Clear error messages with connection context
- Type conversion for numpy/pandas compatibility
- Thread-safe operation

Differences from PostgreSQL integration (database.py):
- No schema context (MSSQL uses databases instead of schemas)
- Named connections instead of single DB_CONFIG
- No default database (use USE statements in SQL scripts to specify database)
- Focus on read operations and script execution (not full ORM)
- SQL Server specific features (SCOPE_IDENTITY(), etc.)
"""

import os
from pathlib import Path
from contextlib import contextmanager
from typing import List, Optional, Dict, Any, Union, Tuple
from string import Template
from dataclasses import dataclass
from helpers.constants import WORKSPACE_PATH
import pandas as pd
import numpy as np

try:
    import pyodbc
except ImportError as e:
    raise ImportError(
        "pyodbc is required for SQL Server operations. "
        "Install it with: pip install pyodbc\n"
        "Note: Microsoft ODBC Driver 18 for SQL Server must also be installed."
    ) from e


# ============================================================================
# TEMPLATE SYSTEM
# ============================================================================

class ExpressionTemplate(Template):
    """
    Custom Template class for SQL parameter substitution.

    Uses {{ PARAM }} syntax with space padding to avoid conflicts with SQL syntax.
    Example: SELECT * FROM table WHERE id = {{ ID }}
    """
    delimiter = '{{'
    # Pattern matches: {{ PARAM_NAME }}
    # Template class will compile this pattern automatically
    pattern = r'''
    \{\{\s*
    (?:
    (?P<escaped>\{\{)|
    (?P<named>[_a-zA-Z][_a-zA-Z0-9]*)\s*\}\}|
    (?P<braced>[_a-zA-Z][_a-zA-Z0-9]*)\s*\}\}|
    (?P<invalid>)
    )
    ''' # type: ignore


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ScriptResult:
    """Result from executing a SQL script file.

    Attributes:
        rows_affected: Total rows affected by DML statements (INSERT/UPDATE/DELETE)
        statements_executed: Total number of statements executed
        result_sets: Number of SELECT statements that returned result sets
        success: Whether the script executed successfully
    """
    rows_affected: int = 0
    statements_executed: int = 0
    result_sets: int = 0
    success: bool = True

    def __str__(self) -> str:
        """Return a formatted string representation for notebook display."""
        status = "✓ SUCCESS" if self.success else "✗ FAILED"
        lines = [
            f"Script Execution Result: {status}",
            f"  Statements executed: {self.statements_executed}",
            f"  Rows affected:       {self.rows_affected}",
            f"  Result sets:         {self.result_sets}"
        ]
        return "\n".join(lines)


# ============================================================================
# EXCEPTIONS
# ============================================================================

class SQLServerError(Exception):
    """Base exception for SQL Server operations"""
    pass


class SQLServerConnectionError(SQLServerError):
    """Connection-related errors"""
    pass


class SQLServerConfigurationError(SQLServerError):
    """Configuration or setup errors"""
    pass


class SQLServerQueryError(SQLServerError):
    """Query execution errors"""
    pass


# ============================================================================
# CONNECTION CONFIGURATION
# ============================================================================

def get_connection_config(connection_name: str = 'TEST') -> Dict[str, str]:
    """
    Get connection configuration for a named MSSQL connection.

    Args:
        connection_name: Name of the connection (e.g., 'AWS_DW', 'ANALYTICS', 'TEST')

    Returns:
        Dictionary with connection parameters

    Raises:
        SQLServerConfigurationError: If required environment variables are missing

    Example:
        config = get_connection_config('AWS_DW')
        # Returns: {
        #     'server': 'aws-db.company.com',
        #     'user': 'irp_service',
        #     'password': 'secret',
        #     'port': '1433'
        # }
    """
    connection_name = connection_name.upper()

    # Build environment variable names
    prefix = f'MSSQL_{connection_name}_'

    config = {
        'server': os.getenv(f'{prefix}SERVER'),
        'user': os.getenv(f'{prefix}USER'),
        'password': os.getenv(f'{prefix}PASSWORD'),
        'port': os.getenv(f'{prefix}PORT', '1433'),
        'driver': os.getenv('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
        'trust_cert': os.getenv('MSSQL_TRUST_CERT', 'yes'),
        'timeout': os.getenv('MSSQL_TIMEOUT', '30')
    }

    # Validate required fields
    required = ['server', 'user', 'password']
    missing = [field for field in required if not config[field]]

    if missing:
        raise SQLServerConfigurationError(
            f"SQL Server connection '{connection_name}' is not properly configured.\n"
            f"Missing environment variables: {', '.join([f'{prefix}{f.upper()}' for f in missing])}\n"
            f"Required format:\n"
            f"  MSSQL_{connection_name}_SERVER=<server>\n"
            f"  MSSQL_{connection_name}_USER=<user>\n"
            f"  MSSQL_{connection_name}_PASSWORD=<password>\n"
        )

    return config


def build_connection_string(connection_name: str = 'TEST', database: Optional[str] = None) -> str:
    """
    Build ODBC connection string for SQL Server.

    Args:
        connection_name: Name of the connection
        database: Optional database name to connect to

    Returns:
        ODBC connection string

    Example:
        conn_str = build_connection_string('AWS_DW')
        # Returns: "DRIVER={ODBC Driver 18 for SQL Server};SERVER=aws-db.company.com,1433;..."

        conn_str = build_connection_string('AWS_DW', database='DataWarehouse')
        # Returns: "...DATABASE=DataWarehouse;..."
    """
    config = get_connection_config(connection_name)

    connection_string = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
    )

    # Only include DATABASE if explicitly provided
    if database:
        connection_string += f"DATABASE={database};"

    connection_string += (
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"TrustServerCertificate={config['trust_cert']};"
        f"Connection Timeout={config['timeout']};"
    )

    return connection_string


@contextmanager
def get_connection(connection_name: str = 'TEST', database: Optional[str] = None):
    """
    Context manager for SQL Server database connections.

    Automatically handles connection lifecycle:
    - Opens connection
    - Yields connection for use
    - Closes connection on exit (even if exception occurs)

    Args:
        connection_name: Name of the connection to use
        database: Optional database name to connect to

    Yields:
        pyodbc.Connection object

    Raises:
        SQLServerConnectionError: If connection fails

    Example:
        with get_connection('AWS_DW') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM portfolios")
            rows = cursor.fetchall()

        with get_connection('AWS_DW', database='DataWarehouse') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM portfolios")
            rows = cursor.fetchall()
    """
    connection_string = build_connection_string(connection_name, database=database)
    conn = None

    try:
        conn = pyodbc.connect(connection_string)
        yield conn
    except pyodbc.Error as e:
        # Only wrap actual connection errors, not query execution errors
        if conn is None:
            # Connection failed
            raise SQLServerConnectionError(
                f"Failed to connect to SQL Server (connection: {connection_name}): {e}"
            ) from e
        else:
            # Connection succeeded but query failed - let the error propagate
            raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass  # Ignore errors during cleanup


def test_connection(connection_name: str = 'TEST') -> bool:
    """
    Test if a SQL Server connection is working.

    Args:
        connection_name: Name of the connection to test

    Returns:
        True if connection successful, False otherwise

    Example:
        if test_connection('AWS_DW'):
            print("Connection successful!")
        else:
            print("Connection failed!")
    """
    try:
        with get_connection(connection_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except Exception as e:
        print(e)
        return False


# ============================================================================
# PARAMETER CONVERSION
# ============================================================================

def _escape_sql_value(value: Any) -> str:
    """
    Escape parameter values for safe SQL substitution.

    Prevents SQL injection by properly escaping values based on type.

    Args:
        value: Parameter value to escape

    Returns:
        Escaped string representation safe for SQL substitution

    Example:
        _escape_sql_value(123) -> "123"
        _escape_sql_value("O'Brien") -> "'O''Brien'"
        _escape_sql_value(None) -> "NULL"
    """
    if value is None:
        return 'NULL'
    elif isinstance(value, bool):
        # Handle bool before int (bool is subclass of int in Python)
        return '1' if value else '0'
    elif isinstance(value, (int, float)):
        # Numbers are safe - no injection risk
        return str(value)
    elif isinstance(value, str):
        # Escape single quotes by doubling them (SQL standard)
        # This prevents breaking out of string literals
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    else:
        # Convert to string and escape (handles dates, etc.)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


def _convert_param_value(value: Any) -> Any:
    """
    Convert numpy/pandas types to native Python types for pyodbc.

    Args:
        value: Parameter value (may be numpy/pandas type)

    Returns:
        Native Python type that pyodbc can handle
    """
    # Handle None
    if value is None:
        return None

    # Convert numpy arrays to lists (check before pandas/numpy scalars)
    if isinstance(value, np.ndarray):
        return value.tolist()

    # Convert pandas Series to list (check before pandas NA check)
    if isinstance(value, pd.Series):
        return value.tolist()

    # Convert pandas NA to None (check after Series)
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # pd.isna() fails on arrays/series, which we already handled above
        pass

    # Convert numpy scalar types to native Python
    if hasattr(value, 'item'):
        return value.item()

    # Return as-is (native Python types)
    return value


def _convert_params_to_native_types(params: Union[Dict[str, Any], Tuple, None]) -> Union[Dict[str, Any], Tuple, None]:
    """
    Convert all parameter values to native Python types.

    Args:
        params: Dictionary or tuple of parameters

    Returns:
        Converted parameters in same structure
    """
    if params is None:
        return None

    if isinstance(params, dict):
        return {key: _convert_param_value(value) for key, value in params.items()}
    elif isinstance(params, (tuple, list)):
        return tuple(_convert_param_value(value) for value in params)
    else:
        return params


def _substitute_named_parameters(query: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Substitute named parameters {{ param_name }} using Template system with context-aware escaping.

    Parameters are escaped differently based on their context:
    - IDENTIFIER CONTEXTS (no quoting - raw substitution):
      * Inside square brackets: [{{ param }}]
      * Inside string literals: '...{{ param }}...'
      * As part of object names: table_{{ param }}_suffix or {{ param }}_table
    - VALUE CONTEXTS (escaped and quoted appropriately):
      * Strings: Quoted with single quotes, SQL injection protected
      * Numbers: Unquoted
      * NULL: NULL keyword

    Args:
        query: SQL query with {{ param_name }} placeholders
        params: Dictionary of parameter values

    Returns:
        SQL query with parameters substituted

    Examples:
        # Value context (quoted/escaped):
        query = "SELECT * FROM table WHERE id = {{ user_id }} AND name = {{ user_name }}"
        params = {'user_id': 123, 'user_name': 'John'}
        # Returns: "SELECT * FROM table WHERE id = 123 AND name = 'John'"

        # Identifier in brackets (not quoted):
        query = "USE [{{ db_name }}]"
        params = {'db_name': 'my_database'}
        # Returns: "USE [my_database]"

        # Identifier in table name (not quoted):
        query = "SELECT * FROM CombinedData_{{ date_val }}_Working"
        params = {'date_val': '20250115'}
        # Returns: "SELECT * FROM CombinedData_20250115_Working"

        # Inside string literal (not quoted):
        query = "SELECT 'Modeling_{{ date_val }}_Moodys' as table_name"
        params = {'date_val': '202501'}
        # Returns: "SELECT 'Modeling_202501_Moodys' as table_name"
    """
    print ('Parameterizing ...')
    if not params:
        return query

    # Convert numpy/pandas types to native Python types first
    converted_params = _convert_params_to_native_types(params)

    # Ensure we have a dict (not tuple) after conversion
    if not isinstance(converted_params, dict):
        return query

    # Import re for pattern matching
    import re

    # Create parameter dict with context-aware escaping
    escaped_params = {}

    for key, value in converted_params.items():
        # Check if this parameter appears in an identifier context (no quoting needed)
        # Pattern 1: Inside square brackets: [{{ param }}]
        # Pattern 2: Inside string literals: '...{{ param }}...'
        # Pattern 3: As part of a table/column name: word_{{ param }}_word or word_{{ param }} or {{ param }}_word
        identifier_patterns = [
            rf'\[\s*\{{\{{\s*{re.escape(key)}\s*\}}\}}\s*\]',  # [{{ param }}]
            rf"'[^']*\{{\{{\s*{re.escape(key)}\s*\}}\}}[^']*'",  # '...{{ param }}...' (inside string literal)
            rf'\w+_\{{\{{\s*{re.escape(key)}\s*\}}\}}',  # word_{{ param }}
            rf'\{{\{{\s*{re.escape(key)}\s*\}}\}}_\w+',  # {{ param }}_word
        ]

        is_identifier = any(re.search(pattern, query) for pattern in identifier_patterns)

        if is_identifier:
            # Identifier context: no quoting, just validate and use raw value
            if isinstance(value, str):
                # Basic validation for identifiers (alphanumeric, underscore, hyphen)
                # Allow more characters for string literal context
                if not all(c.isalnum() or c in ('_', '-', ' ', '/') for c in value):
                    raise ValueError(
                        f"Invalid identifier value for parameter '{key}': {value}. "
                        f"Identifiers can only contain alphanumeric characters, underscores, hyphens, and spaces."
                    )
            escaped_params[key] = str(value)
        else:
            # Value context: use normal escaping with quotes for strings
            escaped_params[key] = _escape_sql_value(value)

    try:
        # Use ExpressionTemplate for substitution
        template = ExpressionTemplate(query)
        substituted_query = template.substitute(escaped_params)
        return substituted_query
    except KeyError as e:
        # Template raises KeyError for missing parameters
        raise SQLServerQueryError(
            f"Missing required parameter: {str(e)}\n"
            f"Query requires parameter that was not provided.\n"
            f"Provided parameters: {', '.join(converted_params.keys())}"
        ) from e
    except ValueError as e:
        # Template raises ValueError for invalid placeholders or identifier validation
        raise SQLServerQueryError(
            f"Parameter substitution error: {e}"
        ) from e


# ============================================================================
# QUERY OPERATIONS
# ============================================================================

def execute_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame.

    Args:
        query: SQL SELECT query (supports {{ param_name }} placeholders)
        params: Query parameters as dictionary
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        pandas DataFrame with query results

    Raises:
        SQLServerQueryError: If query execution fails

    Example:
        df = execute_query(
            "SELECT * FROM portfolios WHERE value > {{ min_value }}",
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
    """
    try:
        # Substitute named parameters if dict provided
        if isinstance(params, dict):
            query = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            df = pd.read_sql(query, conn)

        return df

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Query execution failed (connection: {connection}): {e}"
        ) from e


def execute_scalar(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> Any:
    """
    Execute query and return single scalar value (first column of first row).

    Args:
        query: SQL query returning single value
        params: Query parameters
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        Single value from query result (or None if no results)

    Example:
        count = execute_scalar(
            "SELECT COUNT(*) FROM portfolios WHERE value > {{ min_value }}",
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
    """
    try:
        # Substitute named parameters if dict provided
        if isinstance(params, dict):
            query = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            return row[0] if row else None

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Scalar query execution failed (connection: {connection}): {e}\n"
            f"Query: {query[:200]}{'...' if len(query) > 200 else ''}"
        ) from e


def execute_command(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> int:
    """
    Execute non-query command (INSERT, UPDATE, DELETE) and return rows affected.

    Args:
        query: SQL command
        params: Query parameters
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        Number of rows affected

    Example:
        rows = execute_command(
            "UPDATE portfolios SET status = {{ status }} WHERE value < {{ min_value }}",
            params={'status': 'INACTIVE', 'min_value': 100000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
        print(f"Updated {rows} rows")
    """
    try:
        # Substitute named parameters if dict provided
        if isinstance(params, dict):
            query = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return cursor.rowcount

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Command execution failed (connection: {connection}): {e}\n"
            f"Query: {query[:200]}{'...' if len(query) > 200 else ''}"
        ) from e


# ============================================================================
# FILE-BASED OPERATIONS
# ============================================================================

def _read_sql_file(file_path: Union[str, Path]) -> str:
    """
    Read SQL script from file.

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)

    Returns:
        SQL script content

    Raises:
        SQLServerQueryError: If file not found or cannot be read
    """
    print('Reading SQL script ...')
    file_path = Path(file_path)

    # If relative path, try workspace/sql/ directory
    if not file_path.is_absolute():
        sql_dir = WORKSPACE_PATH / 'sql'
        file_path = sql_dir / file_path

    if not file_path.exists():
        raise SQLServerQueryError(
            f"SQL script file not found: {file_path}\n"
            f"Ensure the file exists in workspace/sql/ directory"
        )

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        raise SQLServerQueryError(
            f"Failed to read SQL script file: {file_path}: {e}"
        ) from e




def execute_query_from_file(
    file_path: Union[str, Path],
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> List[pd.DataFrame]:
    """
    Execute SELECT query from SQL file and return DataFrame.

    Handles both single-statement queries and multi-statement scripts
    (e.g., scripts with USE statements followed by SELECT).

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)
        params: Query parameters (supports {{ param_name }} placeholders)
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        pandas DataFrame with query results

    Example:
        # Standard query with parameters
        df = execute_query_from_file(
            'portfolio_query.sql',
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )

        # Script with USE statements (automatically handled)
        df = execute_query_from_file(
            'list_edm_tables.sql',
            params={'EDM_FULL_NAME': 'CBHU_Automated_FxHJ'},
            connection='DATABRIDGE'
        )
    """
    query = _read_sql_file(file_path)

    try:
        # Substitute named parameters if dict provided
        if isinstance(params, dict):
            query = _substitute_named_parameters(query, params)

        dataframes = []

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            if cursor.description is not None:
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                # Convert to DataFrame (convert Row objects to tuples for pandas compatibility)
                data = [tuple(row) for row in rows]
                dataframes.append(pd.DataFrame.from_records(data, columns=columns))

            while cursor.nextset():
                if cursor.description is not None:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()

                    # Convert to DataFrame (convert Row objects to tuples for pandas compatibility)
                    data = [tuple(row) for row in rows]
                    dataframes.append(pd.DataFrame.from_records(data, columns=columns))

        if len(dataframes) == 0:
            raise SQLServerQueryError(
                f"Query file does not return a result set: {file_path}"
            )

        return dataframes

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Query execution failed (connection: {connection}, file: {file_path}): {e}"
        ) from e


def execute_script_file(
    file_path: Union[str, Path],
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> ScriptResult:
    """
    Execute SQL script from file (supports multi-statement scripts).

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)
        params: Script parameters (supports {{ param_name }} placeholders)
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        ScriptResult object with execution summary:
        - rows_affected: Total rows from INSERT/UPDATE/DELETE
        - statements_executed: Total statements processed
        - result_sets: Number of SELECT queries executed
        - success: Whether execution succeeded

    Example:
        # Create workspace/sql/update_portfolios.sql with content:
        # UPDATE portfolios SET status = {{ status }} WHERE value < {{ min_value }};
        # DELETE FROM portfolios WHERE status = 'CLOSED';

        result = execute_script_file(
            'update_portfolios.sql',
            params={'status': 'INACTIVE', 'min_value': 100000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
        print(f"Executed {result.statements_executed} statements")
        print(f"Rows affected: {result.rows_affected}")
    """
    
    script = _read_sql_file(file_path)

    try:
        # Substitute named parameters if dict provided
        if isinstance(params, dict):
            script = _substitute_named_parameters(script, params)

        result = ScriptResult()

        with get_connection(connection, database=database) as conn:
            print('Executing SQL ...')
            cursor = conn.cursor()
            cursor.execute(script)

            # Process first result set
            result.statements_executed += 1
            if cursor.description is not None:
                # SELECT statement
                result.result_sets += 1
            elif cursor.rowcount >= 0:
                # DML statement (INSERT/UPDATE/DELETE)
                result.rows_affected += cursor.rowcount

            # Process additional result sets
            while cursor.nextset():
                result.statements_executed += 1
                if cursor.description is not None:
                    # SELECT statement
                    result.result_sets += 1
                elif cursor.rowcount >= 0:
                    # DML statement
                    result.rows_affected += cursor.rowcount

            conn.commit()

        return result

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Script execution failed (connection: {connection}, file: {file_path}): {e}"
        ) from e


# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def initialize_database(sql_file_path: Union[str, Path], connection: str = 'TEST') -> bool:
    """
    Initialize a database by executing a SQL script file.

    This is useful for setting up test databases or initial schemas.
    The script should handle creating the database if needed.

    Args:
        sql_file_path: Path to SQL initialization script
        connection: Name of the SQL Server connection to use

    Returns:
        True if successful, False otherwise

    Example:
        from helpers.sqlserver import initialize_database

        success = initialize_database(
            'init_sqlserver.sql',
            connection='TEST'
        )
    """
    try:
        # Read the SQL script
        script_path = Path(sql_file_path)
        if not script_path.is_absolute():
            # Try relative to workspace/helpers/db
            from helpers.constants import WORKSPACE_PATH
            script_path = WORKSPACE_PATH / 'helpers' / 'db' / script_path.name

        with open(script_path, 'r', encoding='utf-8') as f:
            script = f.read()

        # Connect to master database (always exists)
        config = get_connection_config(connection)
        master_conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']},{config['port']};"
            f"DATABASE=master;"
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"TrustServerCertificate={config['trust_cert']};"
        )

        conn = pyodbc.connect(master_conn_str)
        conn.autocommit = True  # Required for CREATE DATABASE
        cursor = conn.cursor()

        # Split script by GO statements and execute each batch
        batches = [batch.strip() for batch in script.split('GO') if batch.strip()]

        for batch in batches:
            if batch:
                try:
                    cursor.execute(batch)
                except Exception as e:
                    # Some statements may fail if objects exist - that's okay
                    if 'already exists' not in str(e).lower():
                        print(f"Warning executing batch: {str(e)[:100]}")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        raise SQLServerError(f"Failed to initialize database: {e}") from e


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # Data classes
    'ScriptResult',

    # Exceptions
    'SQLServerError',
    'SQLServerConnectionError',
    'SQLServerConfigurationError',
    'SQLServerQueryError',

    # Connection management
    'get_connection_config',
    'build_connection_string',
    'get_connection',
    'test_connection',

    # Query operations
    'execute_query',
    'execute_scalar',
    'execute_command',

    # File-based operations
    'execute_query_from_file',
    'execute_script_file',

    # Database initialization
    'initialize_database',
]