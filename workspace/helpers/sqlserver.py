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
        "SELECT * FROM portfolios WHERE value > {min_value}",
        params={'min_value': 1000000},
        connection='AWS_DW',
        database='DataWarehouse'
    )

    # Option 2: Include USE statement in SQL (if script needs multiple databases)
    query = "USE DataWarehouse; SELECT * FROM portfolios WHERE value > {min_value}"
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

SQL queries and scripts support named parameters using {param_name} syntax:

    SELECT * FROM portfolios
    WHERE portfolio_id = {portfolio_id}
      AND created_date >= {start_date}

Parameters are automatically converted to pyodbc-compatible format and types.
Numpy and pandas types are converted to native Python types.

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
import re
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, Any, Union, Tuple
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


def _substitute_named_parameters(query: str, params: Optional[Dict[str, Any]] = None) -> Tuple[str, Optional[Tuple]]:
    """
    Convert named parameters {param_name} to pyodbc ? placeholders.

    Note: This function removes SQL comments before processing to avoid
    matching parameter placeholders in comments.

    Args:
        query: SQL query with {param_name} placeholders
        params: Dictionary of parameter values

    Returns:
        Tuple of (converted_query, ordered_params_tuple)

    Example:
        query = "SELECT * FROM table WHERE id = {user_id} AND name = {user_name}"
        params = {'user_id': 123, 'user_name': 'John'}

        new_query, new_params = _substitute_named_parameters(query, params)
        # new_query: "SELECT * FROM table WHERE id = ? AND name = ?"
        # new_params: (123, 'John')
    """
    if not params:
        return query, None

    # Remove SQL comments to avoid matching parameters in comments
    # Remove single-line comments (-- comment)
    query_no_comments = re.sub(r'--[^\n]*', '', query)
    # Remove multi-line comments (/* comment */)
    query_no_comments = re.sub(r'/\*.*?\*/', '', query_no_comments, flags=re.DOTALL)

    # Find all {param_name} patterns in order (including duplicates)
    param_pattern = re.compile(r'\{(\w+)\}')
    param_names = param_pattern.findall(query_no_comments)

    if not param_names:
        return query, None

    # Validate all parameters are provided
    unique_param_names = list(dict.fromkeys(param_names))  # Preserve order, remove duplicates
    missing_params = [name for name in unique_param_names if name not in params]
    if missing_params:
        raise SQLServerQueryError(
            f"Missing required parameters: {', '.join(missing_params)}\n"
            f"Query requires: {', '.join(unique_param_names)}\n"
            f"Provided: {', '.join(params.keys())}"
        )

    # Replace {param_name} with ? in the ORIGINAL query (to preserve formatting)
    converted_query = param_pattern.sub('?', query)

    # Build ordered tuple of parameter values (one value per placeholder, even if same param used multiple times)
    ordered_params = tuple(params[name] for name in param_names)

    return converted_query, ordered_params


# ============================================================================
# QUERY OPERATIONS
# ============================================================================

def execute_query(
    query: str,
    params: Optional[Union[Dict[str, Any], Tuple]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame.

    Args:
        query: SQL SELECT query (supports {param_name} placeholders)
        params: Query parameters (dict with named params or tuple for ? placeholders)
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        pandas DataFrame with query results

    Raises:
        SQLServerQueryError: If query execution fails

    Example:
        # With named parameters and database specification
        df = execute_query(
            "SELECT * FROM portfolios WHERE value > {min_value}",
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )

        # With positional parameters
        df = execute_query(
            "SELECT * FROM portfolios WHERE value > ?",
            params=(1000000,),
            connection='AWS_DW'
        )
    """
    try:
        # Convert numpy/pandas types to native Python types
        params = _convert_params_to_native_types(params)

        # Handle named parameters if dict provided
        if isinstance(params, dict):
            query, params = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            df = pd.read_sql(query, conn, params=params)

        return df

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Query execution failed (connection: {connection}): {e}\n"
            f"Parameters: {params}"
        ) from e


def execute_scalar(
    query: str,
    params: Optional[Union[Dict[str, Any], Tuple]] = None,
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
            "SELECT COUNT(*) FROM portfolios WHERE value > {min_value}",
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
    """
    try:
        # Convert numpy/pandas types to native Python types
        params = _convert_params_to_native_types(params)

        # Handle named parameters if dict provided
        if isinstance(params, dict):
            query, params = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
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
    params: Optional[Union[Dict[str, Any], Tuple]] = None,
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
            "UPDATE portfolios SET status = {status} WHERE value < {min_value}",
            params={'status': 'INACTIVE', 'min_value': 100000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
        print(f"Updated {rows} rows")
    """
    try:
        # Convert numpy/pandas types to native Python types
        params = _convert_params_to_native_types(params)

        # Handle named parameters if dict provided
        if isinstance(params, dict):
            query, params = _substitute_named_parameters(query, params)

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
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
    from helpers.constants import WORKSPACE_PATH

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


def _substitute_params_direct(query: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Perform direct string substitution of parameters (case-insensitive).

    WARNING: This bypasses SQL injection protection. Only use with trusted input
    or when parameterized queries are not supported (e.g., USE statements).

    Args:
        query: SQL query with {param_name} placeholders (case-insensitive)
        params: Dictionary of parameter values

    Returns:
        Query with parameters directly substituted

    Example:
        query = "USE [{database_name}]"
        params = {'DATABASE_NAME': 'MyDB'}  # or 'database_name': 'MyDB'
        result = _substitute_params_direct(query, params)
        # Returns: "USE [MyDB]"
    """
    if not params:
        return query

    # Create case-insensitive parameter lookup
    params_lower = {k.lower(): v for k, v in params.items()}

    def replace_param(match):
        param_name = match.group(1).lower()
        if param_name not in params_lower:
            raise SQLServerQueryError(
                f"Missing required parameter: {match.group(1)}\n"
                f"Available parameters: {', '.join(params.keys())}"
            )
        value = params_lower[param_name]
        # Convert value to string representation
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            return value
        else:
            return str(value)

    # Replace {param_name} with actual values (case-insensitive)
    result = re.sub(r'\{(\w+)\}', replace_param, query, flags=re.IGNORECASE)
    return result


def execute_query_from_file(
    file_path: Union[str, Path],
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None,
    use_direct_substitution: bool = False
) -> pd.DataFrame:
    """
    Execute SELECT query from SQL file and return DataFrame.

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)
        params: Query parameters (supports {param_name} placeholders, case-insensitive)
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)
        use_direct_substitution: If True, use direct string substitution instead of
                                parameterized queries. Required for scripts with USE
                                statements or other commands that don't support parameters.
                                WARNING: Only use with trusted input.

    Returns:
        pandas DataFrame with query results

    Example:
        # With parameterized query (safe from SQL injection)
        df = execute_query_from_file(
            'portfolio_query.sql',
            params={'min_value': 1000000},
            connection='AWS_DW',
            database='DataWarehouse'
        )

        # With direct substitution (for USE statements)
        df = execute_query_from_file(
            'list_edm_tables.sql',
            params={'EDM_FULL_NAME': 'CBHU_Automated_FxHJ'},
            connection='DATABRIDGE',
            use_direct_substitution=True
        )
    """
    query = _read_sql_file(file_path)

    if use_direct_substitution:
        # Direct string substitution (for USE statements, etc.)
        query = _substitute_params_direct(query, params)

        # Handle multi-statement scripts (e.g., USE followed by SELECT)
        try:
            with get_connection(connection, database=database) as conn:
                cursor = conn.cursor()

                # Execute the entire script at once
                # SQL Server can handle multiple statements separated by semicolons
                cursor.execute(query)

                # Move through result sets until we find one with data
                # (USE statements produce no results, so we need to skip to the SELECT)
                while cursor.description is None:
                    if not cursor.nextset():
                        # No more result sets and no data found
                        cursor.close()
                        return pd.DataFrame()

                # Now we have a result set with data
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                cursor.close()

                # Convert pyodbc Row objects to tuples for pandas compatibility
                data = [tuple(row) for row in rows]
                df = pd.DataFrame(data, columns=columns)
                return df

        except (SQLServerConnectionError, SQLServerConfigurationError):
            raise  # Re-raise connection/configuration errors as-is
        except Exception as e:
            raise SQLServerQueryError(
                f"Query execution failed (connection: {connection}): {e}\n"
                f"Query: {query[:500]}{'...' if len(query) > 500 else ''}"
            ) from e
    else:
        # Parameterized query (safe from SQL injection)
        return execute_query(query, params=params, connection=connection, database=database)


def execute_script_file(
    file_path: Union[str, Path],
    params: Optional[Dict[str, Any]] = None,
    connection: str = 'TEST',
    database: Optional[str] = None
) -> int:
    """
    Execute SQL script from file (supports multi-statement scripts).

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)
        params: Script parameters (supports {param_name} placeholders)
        connection: Name of the SQL Server connection to use
        database: Optional database name to connect to (overrides connection config)

    Returns:
        Total number of rows affected (sum across all statements)

    Example:
        # Create workspace/sql/update_portfolios.sql with content:
        # UPDATE portfolios SET status = {status} WHERE value < {min_value};
        # DELETE FROM portfolios WHERE status = 'CLOSED';

        rows = execute_script_file(
            'update_portfolios.sql',
            params={'status': 'INACTIVE', 'min_value': 100000},
            connection='AWS_DW',
            database='DataWarehouse'
        )
        print(f"Total rows affected: {rows}")
    """
    script = _read_sql_file(file_path)

    try:
        # Convert numpy/pandas types to native Python types
        params = _convert_params_to_native_types(params)

        # Handle named parameters if dict provided
        if isinstance(params, dict):
            script, params = _substitute_named_parameters(script, params)

        total_rows = 0

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()

            # Execute script (pyodbc handles multiple statements)
            cursor.execute(script, params or ())

            # Sum up rows affected across all statements
            total_rows = cursor.rowcount

            # If there are multiple result sets, iterate through them
            while cursor.nextset():
                total_rows += cursor.rowcount

            conn.commit()

        return total_rows

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