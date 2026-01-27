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
    MSSQL_{CONNECTION_NAME}_AUTH_TYPE - Authentication type: 'SQL' or 'WINDOWS' (optional, defaults to SQL)
    MSSQL_{CONNECTION_NAME}_USER      - SQL Server authentication username (required for SQL auth)
    MSSQL_{CONNECTION_NAME}_PASSWORD  - SQL Server authentication password (required for SQL auth)
    MSSQL_{CONNECTION_NAME}_PORT      - Port (optional, defaults to 1433)
    MSSQL_{CONNECTION_NAME}_DATABASE  - Database name (optional, use USE statements in SQL scripts)

Authentication Types:
    SQL     - Standard SQL Server authentication with username/password (default)
    WINDOWS - Windows Authentication using Kerberos (Trusted_Connection)
              Requires valid Kerberos ticket (kinit or keytab)

Example Configuration:
    # SQL Server authentication (default)
    MSSQL_AWS_DW_SERVER=aws-db.company.com
    MSSQL_AWS_DW_AUTH_TYPE=SQL
    MSSQL_AWS_DW_USER=irp_service
    MSSQL_AWS_DW_PASSWORD=secretpassword

    # Windows Authentication (Kerberos)
    MSSQL_CORP_SQL_SERVER=sqlserver.corp.company.com
    MSSQL_CORP_SQL_AUTH_TYPE=WINDOWS
    # No USER/PASSWORD needed - uses Kerberos ticket

Usage in Notebooks:
    from helpers.sqlserver import execute_query, execute_query_from_file

    # Option 1: Execute inline query with database parameter
    df = execute_query(
        "SELECT * FROM portfolios WHERE value > {{ min_value }}",
        params={'min_value': 1000000},
        connection='AWS_DW',
        database='DataWarehouse'
    )

    # Option 2: Include USE statement in SQL (if script needs multiple databases)
    query = "USE DataWarehouse; SELECT * FROM portfolios WHERE value > {{ min_value }}"
    df = execute_query(query, params={'min_value': 1000000}, connection='AWS_DW')

    # Option 3: Execute SQL script from file (returns list of DataFrames)
    dataframes = execute_query_from_file(
        'extract_policies.sql',
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

import gc
import os
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import List, Optional, Dict, Any, Union, Tuple
from string import Template
from helpers.constants import WORKSPACE_PATH
import pandas as pd
import numpy as np

# Configure module logger
logger = logging.getLogger(__name__)

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

    Supports both SQL Server native authentication and Windows Authentication (Kerberos).

    Authentication Types:
        SQL     - Standard SQL Server authentication with username/password (default)
        WINDOWS - Windows Authentication using Kerberos (Trusted_Connection)

    Args:
        connection_name: Name of the connection (e.g., 'AWS_DW', 'ANALYTICS', 'TEST')

    Returns:
        Dictionary with connection parameters including 'auth_type'

    Raises:
        SQLServerConfigurationError: If required environment variables are missing

    Environment Variables:
        MSSQL_{CONNECTION_NAME}_SERVER      - Server hostname or IP (required)
        MSSQL_{CONNECTION_NAME}_AUTH_TYPE   - 'SQL' or 'WINDOWS' (default: SQL)
        MSSQL_{CONNECTION_NAME}_USER        - Username (required for SQL auth)
        MSSQL_{CONNECTION_NAME}_PASSWORD    - Password (required for SQL auth)
        MSSQL_{CONNECTION_NAME}_PORT        - Port (optional, defaults to 1433)

    Example:
        # SQL authentication
        config = get_connection_config('AWS_DW')
        # Returns: {
        #     'server': 'aws-db.company.com',
        #     'auth_type': 'SQL',
        #     'user': 'irp_service',
        #     'password': 'secret',
        #     'port': '1433',
        #     ...
        # }

        # Windows authentication
        config = get_connection_config('CORP_SQL')
        # Returns: {
        #     'server': 'sqlserver.corp.local',
        #     'auth_type': 'WINDOWS',
        #     'port': '1433',
        #     ...
        # }
    """
    connection_name = connection_name.upper()

    # Build environment variable names
    prefix = f'MSSQL_{connection_name}_'

    # Determine authentication type (default to SQL for backward compatibility)
    auth_type = os.getenv(f'{prefix}AUTH_TYPE', 'SQL').upper()

    if auth_type not in ('SQL', 'WINDOWS'):
        raise SQLServerConfigurationError(
            f"Invalid authentication type '{auth_type}' for connection '{connection_name}'.\n"
            f"Valid values are: SQL, WINDOWS"
        )

    config = {
        'server': os.getenv(f'{prefix}SERVER'),
        'port': os.getenv(f'{prefix}PORT', '1433'),
        'driver': os.getenv('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
        'trust_cert': os.getenv('MSSQL_TRUST_CERT', 'yes'),
        'timeout': os.getenv('MSSQL_TIMEOUT', '30'),
        'auth_type': auth_type,
    }

    # For SQL authentication, require user and password
    if auth_type == 'SQL':
        config['user'] = os.getenv(f'{prefix}USER')
        config['password'] = os.getenv(f'{prefix}PASSWORD')

        # Validate required fields for SQL auth
        required = ['server', 'user', 'password']
        missing = [field for field in required if not config.get(field)]

        if missing:
            raise SQLServerConfigurationError(
                f"SQL Server connection '{connection_name}' is not properly configured.\n"
                f"Authentication type: SQL\n"
                f"Missing environment variables: {', '.join([f'{prefix}{f.upper()}' for f in missing])}\n"
                f"Required format:\n"
                f"  MSSQL_{connection_name}_SERVER=<server>\n"
                f"  MSSQL_{connection_name}_USER=<user>\n"
                f"  MSSQL_{connection_name}_PASSWORD=<password>\n"
            )

    else:  # WINDOWS authentication
        # For Windows auth, only server is required
        if not config.get('server'):
            raise SQLServerConfigurationError(
                f"SQL Server connection '{connection_name}' is not properly configured.\n"
                f"Authentication type: WINDOWS\n"
                f"Missing environment variable: {prefix}SERVER\n"
                f"\nNote: Windows Authentication requires valid Kerberos credentials.\n"
                f"Ensure you have run 'kinit' or have a valid keytab configured."
            )

    return config


def build_connection_string(connection_name: str = 'TEST', database: Optional[str] = None) -> str:
    """
    Build ODBC connection string for SQL Server.

    Supports both SQL Server native authentication and Windows Authentication (Kerberos).

    Args:
        connection_name: Name of the connection
        database: Optional database name to connect to

    Returns:
        ODBC connection string

    Example:
        # SQL authentication
        conn_str = build_connection_string('AWS_DW')
        # Returns: "DRIVER={...};SERVER=...;UID=user;PWD=pass;..."

        # Windows authentication
        conn_str = build_connection_string('CORP_SQL')
        # Returns: "DRIVER={...};SERVER=...;Trusted_Connection=yes;..."

        # With database
        conn_str = build_connection_string('AWS_DW', database='DataWarehouse')
        # Returns: "...DATABASE=DataWarehouse;..."
    """
    config = get_connection_config(connection_name)

    auth_type = config.get('auth_type', 'SQL')

    connection_string = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
    )

    # Only include DATABASE if explicitly provided
    if database:
        connection_string += f"DATABASE={database};"

    # Authentication-specific connection string parts
    if auth_type == 'WINDOWS':
        # Windows Authentication (Kerberos)
        connection_string += "Trusted_Connection=yes;"
    else:
        # SQL Server Authentication
        connection_string += (
            f"UID={config['user']};"
            f"PWD={config['password']};"
        )

    connection_string += (
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

    For Windows Authentication connections, this function automatically
    checks if the Kerberos ticket is valid and renews it if necessary
    (using the configured keytab file).

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
    # Check if this is a Windows auth connection - if so, ensure valid ticket
    try:
        config = get_connection_config(connection_name)
        if config.get('auth_type') == 'WINDOWS':
            if not ensure_valid_kerberos_ticket():
                raise SQLServerConnectionError(
                    f"Windows Authentication requires a valid Kerberos ticket.\n"
                    f"Automatic renewal failed. Please check:\n"
                    f"  - KERBEROS_ENABLED=true in environment\n"
                    f"  - KRB5_KEYTAB points to valid keytab file\n"
                    f"  - KRB5_PRINCIPAL is set correctly\n"
                    f"Or manually renew with: kinit -kt <keytab> <principal>"
                )
    except SQLServerConfigurationError:
        raise  # Re-raise config errors

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
# KERBEROS SUPPORT
# ============================================================================

def check_kerberos_status() -> Dict[str, Any]:
    """
    Check the status of Kerberos configuration and credentials.

    This function checks if Kerberos is enabled and if there is a valid
    Kerberos ticket available for Windows Authentication.

    Returns:
        Dictionary with Kerberos status information:
        - enabled: Whether Kerberos is enabled (KERBEROS_ENABLED env var)
        - has_ticket: Whether a valid Kerberos ticket exists
        - principal: Current principal (if ticket exists)
        - expiration: Ticket expiration time (if ticket exists)
        - error: Error message (if any)

    Example:
        status = check_kerberos_status()
        if status['has_ticket']:
            print(f"Authenticated as: {status['principal']}")
        else:
            print(f"No ticket: {status['error']}")
    """
    import subprocess
    import re

    result = {
        'enabled': os.getenv('KERBEROS_ENABLED', 'false').lower() == 'true',
        'has_ticket': False,
        'principal': None,
        'expiration': None,
        'error': None,
    }

    if not result['enabled']:
        result['error'] = "Kerberos is not enabled (KERBEROS_ENABLED != true)"
        return result

    try:
        klist_output = subprocess.run(
            ['klist'],
            capture_output=True,
            text=True,
            timeout=10
        )

        if klist_output.returncode == 0:
            output = klist_output.stdout

            # Parse principal from klist output
            principal_match = re.search(r'Default principal:\s+(\S+)', output)
            if principal_match:
                result['principal'] = principal_match.group(1)

            # Parse expiration time
            expires_match = re.search(r'(\d{2}/\d{2}/\d{2,4}\s+\d{2}:\d{2}:\d{2})', output)
            if expires_match:
                result['expiration'] = expires_match.group(1)

            result['has_ticket'] = True
        else:
            result['error'] = klist_output.stderr.strip() or "No valid Kerberos ticket found"

    except subprocess.TimeoutExpired:
        result['error'] = "klist command timed out"
    except FileNotFoundError:
        result['error'] = "klist command not found. Kerberos tools may not be installed."
    except Exception as e:
        result['error'] = str(e)

    return result


def init_kerberos(keytab_path: Optional[str] = None, principal: Optional[str] = None, password: Optional[str] = None) -> bool:
    """
    Initialize Kerberos credentials using a keytab file or password.

    This function runs 'kinit' with the specified keytab or password and principal
    to obtain a Kerberos ticket for Windows Authentication.

    Authentication methods (tried in order):
        1. Keytab file (if KRB5_KEYTAB is set and file exists)
        2. Password (if KRB5_PASSWORD is set)

    Args:
        keytab_path: Path to the keytab file. Defaults to KRB5_KEYTAB env var.
        principal: Kerberos principal. Defaults to KRB5_PRINCIPAL env var.
        password: Kerberos password. Defaults to KRB5_PASSWORD env var.

    Returns:
        True if successful, False otherwise

    Raises:
        SQLServerConfigurationError: If principal not specified or no auth method available

    Example:
        # Using environment variables (keytab or password)
        if init_kerberos():
            print("Kerberos initialized successfully")

        # Explicit keytab
        if init_kerberos('/etc/krb5/svc_jupyter.keytab', 'svc_jupyter@CORP.LOCAL'):
            print("Kerberos initialized successfully")

        # Explicit password
        if init_kerberos(principal='svc_jupyter@CORP.LOCAL', password='secret'):
            print("Kerberos initialized successfully")
    """
    import subprocess

    keytab = keytab_path or os.getenv('KRB5_KEYTAB')
    princ = principal or os.getenv('KRB5_PRINCIPAL')
    pwd = password or os.getenv('KRB5_PASSWORD')

    if not princ:
        raise SQLServerConfigurationError(
            "Kerberos principal not specified.\n"
            "Set KRB5_PRINCIPAL environment variable or pass principal parameter."
        )

    # Method 1: Try keytab if available
    if keytab and os.path.exists(keytab):
        try:
            result = subprocess.run(
                ['kinit', '-k', '-t', keytab, princ],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                logger.info(f"Kerberos initialized via keytab for principal: {princ}")
                return True
            else:
                logger.warning(f"Keytab auth failed: {result.stderr.strip()}. Trying password auth...")

        except subprocess.TimeoutExpired:
            logger.warning("kinit with keytab timed out. Trying password auth...")
        except FileNotFoundError:
            raise SQLServerConfigurationError(
                "kinit command not found. Kerberos tools may not be installed."
            )
        except Exception as e:
            logger.warning(f"Keytab auth error: {e}. Trying password auth...")

    # Method 2: Try password if available
    if pwd:
        try:
            result = subprocess.run(
                ['kinit', princ],
                input=pwd + '\n',
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                logger.info(f"Kerberos initialized via password for principal: {princ}")
                return True
            else:
                logger.error(f"Password auth failed: {result.stderr.strip()}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("kinit with password timed out")
            return False
        except FileNotFoundError:
            raise SQLServerConfigurationError(
                "kinit command not found. Kerberos tools may not be installed."
            )
        except Exception as e:
            logger.error(f"Password auth error: {e}")
            return False

    # No authentication method available
    raise SQLServerConfigurationError(
        "No Kerberos credentials available.\n"
        "Set KRB5_KEYTAB (with valid keytab file) or KRB5_PASSWORD environment variable."
    )


def is_ticket_valid(min_remaining_minutes: int = 5) -> bool:
    """
    Check if the current Kerberos ticket is valid and not expiring soon.

    Args:
        min_remaining_minutes: Minimum minutes of validity required (default: 5)

    Returns:
        True if ticket exists and has at least min_remaining_minutes before expiry
    """
    import subprocess
    import re
    from datetime import datetime

    try:
        result = subprocess.run(
            ['klist'],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            return False

        output = result.stdout

        # Parse expiration time from klist output
        # Format: "11/27/2025 08:01:31" or "Nov 27 2025 08:01:31"
        # Look for line with "Expires" or parse the ticket entry
        expires_match = re.search(
            r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})',
            output
        )

        if not expires_match:
            # Try alternative format (some systems)
            expires_match = re.search(
                r'(\w{3}\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}:\d{2})',
                output
            )

        if not expires_match:
            logger.warning("Could not parse ticket expiration time")
            return False

        expiry_str = expires_match.group(1)

        # Parse the expiration time
        try:
            # Try MM/DD/YYYY HH:MM:SS format first
            expiry_time = datetime.strptime(expiry_str, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            try:
                # Try "Mon DD YYYY HH:MM:SS" format
                expiry_time = datetime.strptime(expiry_str, '%b %d %Y %H:%M:%S')
            except ValueError:
                logger.warning(f"Could not parse expiration time: {expiry_str}")
                return False

        # Check if ticket has enough time remaining
        now = datetime.now()
        remaining = expiry_time - now
        remaining_minutes = remaining.total_seconds() / 60

        if remaining_minutes < min_remaining_minutes:
            logger.info(f"Kerberos ticket expiring soon ({remaining_minutes:.1f} minutes remaining)")
            return False

        return True

    except Exception as e:
        logger.warning(f"Error checking ticket validity: {e}")
        return False


def ensure_valid_kerberos_ticket(min_remaining_minutes: int = 5) -> bool:
    """
    Ensure a valid Kerberos ticket exists, renewing if necessary.

    This function checks if the current ticket is valid and has sufficient
    time remaining. If not, it automatically renews the ticket using the
    configured keytab file.

    Args:
        min_remaining_minutes: Minimum minutes of validity required (default: 5)

    Returns:
        True if a valid ticket exists (or was successfully renewed)

    Example:
        # Called automatically by get_connection() for Windows auth
        # Can also be called manually:
        if ensure_valid_kerberos_ticket():
            print("Kerberos ticket is valid")
    """
    # Check if Kerberos is enabled
    if os.getenv('KERBEROS_ENABLED', 'false').lower() != 'true':
        logger.debug("Kerberos not enabled, skipping ticket check")
        return True  # Not an error - Kerberos simply not configured

    # Check if current ticket is valid
    if is_ticket_valid(min_remaining_minutes):
        logger.debug("Kerberos ticket is valid")
        return True

    # Ticket expired or expiring - try to renew
    logger.info("Kerberos ticket expired or expiring, attempting renewal...")

    try:
        if init_kerberos():
            logger.info("Kerberos ticket renewed successfully")
            return True
        else:
            logger.error("Failed to renew Kerberos ticket")
            return False
    except SQLServerConfigurationError as e:
        logger.error(f"Cannot renew Kerberos ticket: {e}")
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
        # Pattern 2: Inside string literals: '...{{ param }}...' (single line only)
        # Pattern 3: As part of a table/column name: word_{{ param }}_word or word_{{ param }} or {{ param }}_word
        identifier_patterns = [
            rf'\[\s*\{{\{{\s*{re.escape(key)}\s*\}}\}}\s*\]',  # [{{ param }}]
            rf"'[^'\n\r]*\{{\{{\s*{re.escape(key)}\s*\}}\}}[^'\n\r]*'",  # '...{{ param }}...' (inside string literal, single line only)
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


def sql_file_exists(file_path: Union[str, Path]) -> bool:
    """
    Check if a SQL script file exists.

    Args:
        file_path: Path to SQL file (absolute or relative to workspace/sql/)

    Returns:
        True if file exists, False otherwise

    Example:
        # Check if optional SQL script exists before executing
        sql_script = f'portfolio_mapping/{portfolio_name}.sql'
        if sql_file_exists(sql_script):
            result = execute_query_from_file(sql_script, params=params)
        else:
            print(f"Skipping - script not found: {sql_script}")
    """
    file_path = Path(file_path)

    # If relative path, try workspace/sql/ directory
    if not file_path.is_absolute():
        sql_dir = WORKSPACE_PATH / 'sql'
        file_path = sql_dir / file_path

    return file_path.exists() and file_path.is_file()




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

        logger.info(f"Executing query from file: {file_path}")
        print(f"Executing SQL query: {file_path}")

        with get_connection(connection, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            stmt_num = 1
            if cursor.description is not None:
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                # Convert to DataFrame (convert Row objects to tuples for pandas compatibility)
                data = [tuple(row) for row in rows]
                del rows  # Free fetchall result to reduce peak memory

                df = pd.DataFrame.from_records(data, columns=columns)
                del data  # Free tuple list to reduce peak memory

                dataframes.append(df)
                logger.debug(f"Statement {stmt_num}: Retrieved {len(df)} rows")
                stmt_num += 1

            while cursor.nextset():
                if cursor.description is not None:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()

                    # Convert to DataFrame (convert Row objects to tuples for pandas compatibility)
                    data = [tuple(row) for row in rows]
                    del rows  # Free fetchall result to reduce peak memory

                    df = pd.DataFrame.from_records(data, columns=columns)
                    del data  # Free tuple list to reduce peak memory

                    dataframes.append(df)
                    logger.debug(f"Statement {stmt_num}: Retrieved {len(df)} rows")
                    stmt_num += 1

            # Force garbage collection after processing all result sets
            gc.collect()

            conn.commit()

        if not dataframes:
            logger.warning(f"No result sets returned from {file_path}")
            print("⚠ Warning: No result sets returned. This may occur if:")
            print("  • Script contains only DDL (CREATE/DROP/ALTER)")
            print("  • Script contains only DML (INSERT/UPDATE/DELETE)")
            print("  • Script uses dynamic SQL (EXEC) that returns results to client, not cursor")
            print("  → Note: This function is designed for SELECT queries that return data")

        logger.info(f"Query completed: {len(dataframes)} result sets returned")
        if dataframes:
            print(f"✓ Retrieved {len(dataframes)} result set(s)")

        return dataframes

    except (SQLServerConnectionError, SQLServerConfigurationError):
        raise  # Re-raise connection/configuration errors as-is
    except Exception as e:
        raise SQLServerQueryError(
            f"Query execution failed (connection: {connection}, file: {file_path}): {e}"
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
    conn = None
    cursor = None

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

        return True

    except Exception as e:
        raise SQLServerError(f"Failed to initialize database: {e}") from e

    finally:
        # Ensure cursor and connection are properly closed
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================================
# DISPLAY UTILITIES
# ============================================================================

def display_result_sets(dataframes: List[pd.DataFrame], max_rows: int = 10) -> None:
    """
    Display multiple result sets in a clean, readable format for Jupyter notebooks.

    This function provides a much cleaner output than print(dataframes) which shows
    the messy Python list representation across multiple lines.

    Args:
        dataframes: List of DataFrames returned from execute_query_from_file()
        max_rows: Maximum number of rows to display per DataFrame (default: 10)

    Example:
        # Instead of: print(result)
        result = execute_query_from_file('control_totals.sql', ...)
        display_result_sets(result)
    """
    from IPython.display import display

    if not dataframes:
        print("No result sets to display")
        return

    print(f"\n{'='*80}")
    print(f"QUERY RESULTS: {len(dataframes)} result set(s)")
    print(f"{'='*80}\n")

    for i, df in enumerate(dataframes, 1):
        print(f"\n{'-'*80}")
        print(f"Result Set {i} of {len(dataframes)}")
        print(f"{'-'*80}")

        if df.empty:
            print(f"Empty DataFrame")
            print(f"Columns: {list(df.columns)}")
        else:
            print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
            print()

            # Display the DataFrame (uses pandas default display settings)
            if len(df) > max_rows:
                display(df.head(max_rows))
                print(f"\n... ({len(df) - max_rows:,} more rows not shown)")
            else:
                display(df)

    print(f"\n{'='*80}\n")


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

    # Kerberos support
    'check_kerberos_status',
    'init_kerberos',
    'is_ticket_valid',
    'ensure_valid_kerberos_ticket',

    # Query operations
    'execute_query',
    'execute_scalar',
    'execute_command',

    # File-based operations
    'sql_file_exists',
    'execute_query_from_file',

    # Display utilities
    'display_result_sets',

    # Database initialization
    'initialize_database',
]