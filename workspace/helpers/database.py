"""
IRP Notebook Framework - Database Operations

This module provides a comprehensive interface for all database operations in the
IRP Notebook Framework. 

It also handles connections, CRUD query execution, and CRUD operations
for cycles, stages, steps, and step runs.

================================================================================
SCHEMA CONTROL
================================================================================

The framework provides three ways to control which PostgreSQL schema is used for
database operations. Understanding the precedence and behavior is critical:

PRECEDENCE ORDER (highest to lowest):
--------------------------------------
1. EXPLICIT SCHEMA PARAMETER: schema='my_schema' passed to function
2. CONTEXT SCHEMA: Set via set_schema() or schema_context()
3. ENVIRONMENT VARIABLE: DB_SCHEMA environment variable
+  DEFAULT: 'public' schema

1: Explicit Schema Parameter (Highest Priority)
-------------------------------------------------------
Low-level functions (execute_query, execute_insert, execute_command, etc.) accept
a schema parameter that ALWAYS overrides context and environment settings.

    # execute_query uses 'test' schema regardless of context or environment
    df = execute_query("SELECT * FROM irp_cycle", schema='test')

    # execute_insert uses 'dev' schema for this operation only
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name) VALUES (%s)",
        ('Q1-2024',),
        schema='dev'
    )

IMPORTANT: Domain functions (get_cycle_by_name, register_cycle, etc.) do NOT
have schema parameters. They use context only. To use a different schema with
domain functions, change the context first:

    # WRONG - domain functions don't have schema parameter
    # cycle = get_cycle_by_name('Q1-2024', schema='dev')  # TypeError!

    # CORRECT - change context first
    with schema_context('dev'):
        cycle = get_cycle_by_name('Q1-2024')  # Uses 'dev'

Use explicit schema parameter when:
- Using low-level execute_ functions directly
- Need temporary schema override for single query
- Doing cross-schema operations

2: Context Schema (Medium Priority)
-------------------------------------------
Set the schema for all operations in the current thread until changed.
Two ways to set context:

i) Persistent schema change with set_schema():

    set_schema('test')
    cycle_id = register_cycle('Q1-2024')    # Uses 'test'
    stage_id = get_or_create_stage(...)      # Uses 'test'
    # ALL subsequent operations use 'test' until you call set_schema() again

ii) Temporary schema change with schema_context():

    set_schema('public')  # Currently on 'public'

    with schema_context('test'):
        cycle_id = register_cycle('test_cycle')  # Uses 'test'
        stage_id = get_or_create_stage(...)      # Uses 'test'

    # Automatically restored to 'public' after context exits
    cycle = get_active_cycle()  # Uses 'public' again

C) Reset to default:

    reset_schema()  # Returns to 'public'

Use this when:
- Running tests (isolate test data in separate schema)
- Batch processing across multiple operations
- Need automatic cleanup (use context manager)

METHOD 3: Environment Variable (Low Priority)
----------------------------------------------
Set DB_SCHEMA environment variable to change the default schema for the entire
application/process. This is automatically loaded when the module is imported.

    # In shell or docker-compose.yml:
    export DB_SCHEMA=production

    # In Python, all operations now default to 'production':
    cycle = get_active_cycle()  # Uses 'production'

The environment variable is read ONCE at module import via init_from_environment().

Use this when:
- Configuring different environments (dev/staging/prod)
- Docker containers with different schemas
- Want consistent schema for entire application

METHOD 4: Default Behavior (Lowest Priority)
---------------------------------------------
If nothing is set, all operations use 'public' schema.

    # No context, no env var, no explicit schema
    cycle = get_active_cycle()  # Uses 'public'

COMPLETE EXAMPLES:
-----------------

Example 1: Test Isolation
    # Tests use dedicated schema, production uses public
    with schema_context('test_cycle_operations'):
        cycle_id = register_cycle('test_cycle')
        stage_id = get_or_create_stage(cycle_id, 1, 'Test Stage')
        # All test data in 'test_cycle_operations' schema
    # Automatically back to 'public'

Example 2: Multi-Environment Setup
    # docker-compose.yml
    environment:
      - DB_SCHEMA=production

    # All operations automatically use 'production' schema
    cycle = get_active_cycle()  # production.irp_cycle

Example 3: Cross-Schema Query
    set_schema('current_analysis')

    # Most operations use current_analysis
    cycle = get_active_cycle()  # current_analysis.irp_cycle

    # But explicitly query archive schema
    archived = execute_query(
        "SELECT * FROM irp_cycle WHERE status = 'ARCHIVED'",
        schema='archive'
    )

Example 4: Nested Contexts (Advanced)
    set_schema('public')

    with schema_context('dev'):
        create_cycle('dev_cycle')  # In 'dev'

        with schema_context('test'):
            create_cycle('test_cycle')  # In 'test'

        create_cycle('dev_cycle_2')  # Back to 'dev'

    create_cycle('prod_cycle')  # Back to 'public'

CHECKING CURRENT SCHEMA:
------------------------
    current = get_current_schema()
    print(f"Currently using schema: {current}")

THREAD SAFETY:
--------------
Schema context uses thread-local storage, so each thread has its own schema
setting. This prevents race conditions in multi-threaded applications.

================================================================================
OUTPUT FORMATS
================================================================================

Functions return different types based on their purpose:

1. execute_query() -> pd.DataFrame
   - SELECT queries return pandas DataFrame
   - Empty results return empty DataFrame (not None)
   - All columns preserved with proper data types

   Example:
       df = execute_query("SELECT * FROM irp_cycle")
       # Returns DataFrame with columns: id, cycle_name, status, created_ts, archived_ts

2. execute_scalar() -> Any
   - Returns single value (first column of first row)
   - Returns None if no results found
   - Useful for COUNT, MAX, SUM, or single column lookups

   Example:
       count = execute_scalar("SELECT COUNT(*) FROM irp_cycle")
       # Returns: 5 (integer)

3. execute_command() -> int
   - INSERT/UPDATE/DELETE return number of rows affected
   - Returns 0 if no rows were affected
   - Does not return the inserted/updated data

   Example:
       rows = execute_command("UPDATE irp_cycle SET status = %s WHERE id = %s",
                             ('ARCHIVED', 123))
       # Returns: 1 (one row updated)

4. execute_insert() -> int
   - INSERT returns the new record's ID
   - Automatically adds "RETURNING id" clause
   - Returns None if insert somehow fails to return ID

   Example:
       new_id = execute_insert("INSERT INTO irp_cycle (cycle_name) VALUES (%s)",
                              ('Q1-2024',))
       # Returns: 42 (the new cycle's ID)

5. bulk_insert() -> List[int]
   - Returns list of IDs for all inserted records
   - Order matches input params_list order
   - Returns empty list if params_list is empty
   - All inserts in single transaction (all-or-nothing)

   Example:
       ids = bulk_insert(
           "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
           [(1, 1, 'Stage 1'), (1, 2, 'Stage 2'), (1, 3, 'Stage 3')]
       )
       # Returns: [101, 102, 103] (three new stage IDs)

6. Domain-specific getters -> Optional[Dict[str, Any]]
   - Functions like get_active_cycle(), get_cycle_by_name(), etc.
   - Return dict with record fields if found
   - Return None if not found
   - Dict keys match database column names

   Example:
       cycle = get_active_cycle()
       if cycle:
           print(f"Active cycle: {cycle['cycle_name']}")
           print(f"ID: {cycle['id']}, Status: {cycle['status']}")
       else:
           print("No active cycle")

================================================================================
PARAMETER HANDLING
================================================================================

Query parameters use %s placeholders (psycopg2 style) and are automatically
converted to SQLAlchemy format for safe execution:

Examples:
    execute_query("SELECT * FROM irp_cycle WHERE cycle_name = %s", ('Q1-2024',))
    execute_insert("INSERT INTO irp_cycle (cycle_name) VALUES (%s)", ('Q1-2024',))

JSONB Handling in bulk_insert():
    # For JSONB columns, you can pass Python dicts directly
    bulk_insert(
        "INSERT INTO irp_step_run (step_id, output_data) VALUES (%s, %s)",
        [(1, {'key': 'value'}), (2, {'foo': 'bar'})],
        jsonb_columns=[1]  # Column index 1 contains JSONB
    )
    # Dicts automatically converted to JSON strings

================================================================================
ERROR HANDLING
================================================================================

All database errors are wrapped in DatabaseError exception:

    try:
        cycle_id = register_cycle('Q1-2024')
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        # Original error message included for debugging

Transactions:
- Each operation is atomic (auto-commit)
- Failures trigger automatic rollback
- bulk_insert() uses single transaction (all-or-nothing)

Connection Management:
- Uses NullPool (no connection pooling)
- Each operation creates fresh connection
- Connections auto-close after operation
- Suitable for notebook/Jupyter environments

================================================================================
INITIALIZATION
================================================================================

Database initialization:
    # Create tables in 'public' schema
    init_database()

    # Create tables in custom schema
    init_database(schema='test')

    # Use custom SQL file
    init_database(schema='dev', sql_file_name='custom_init.sql')

The SQL file must be located in: workspace/helpers/db/[sql_file_name]
"""

import os
from contextlib import contextmanager
from threading import local
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from helpers.constants import DB_CONFIG, CycleStatus, StepStatus

# ============================================================================
# SCHEMA CONTEXT MANAGEMENT
# ============================================================================

# Thread-local storage for schema context
# Each thread maintains its own schema setting to prevent race conditions
_context = local()

def get_current_schema() -> str:
    """
    Get the current schema for database operations.

    This function returns the schema that will be used for database operations
    when no explicit schema parameter is provided to database functions.

    Precedence:
        1. Schema set via set_schema() or schema_context() in current thread
        2. Schema from DB_SCHEMA environment variable (if set at import time)
        3. Default: 'public'

    Returns:
        str: Current schema name ('public' if nothing set)

    Example:
        >>> get_current_schema()
        'public'

        >>> set_schema('test')
        >>> get_current_schema()
        'test'

        >>> with schema_context('dev'):
        ...     print(get_current_schema())
        'dev'

    Thread Safety:
        Each thread has its own schema context, so this is safe in
        multi-threaded environments.
    """
    return getattr(_context, 'schema', 'public')


def set_schema(schema: str):
    """
    Set the schema for all subsequent database operations in the current thread.

    This affects ALL database operations that don't explicitly override the schema
    parameter. The schema remains set until you call set_schema() again or
    reset_schema().

    Args:
        schema: Schema name to use for database operations
                Cannot be empty or None

    Raises:
        ValueError: If schema is empty or None

    Example:
        # Set schema for testing
        set_schema('test_schema')
        cycle_id = register_cycle('test')  # Uses 'test_schema'
        stage_id = get_or_create_stage(cycle_id, 1, 'Stage 1')  # Also 'test_schema'

        # Change schema
        set_schema('production')
        cycle = get_active_cycle()  # Uses 'production'

        # Reset to default
        reset_schema()
        cycle = get_active_cycle()  # Uses 'public'

    Warning:
        This sets the schema for ALL subsequent operations in the current thread
        until changed. Use schema_context() if you need automatic restoration.

    See Also:
        schema_context(): For temporary schema changes with automatic restoration
        reset_schema(): To reset back to 'public'
        get_current_schema(): To check what schema is currently set
    """
    if not schema:
        raise ValueError("Schema name cannot be empty")
    _context.schema = schema


@contextmanager
def schema_context(schema: str):
    """
    Context manager for temporary schema selection with automatic restoration.

    The schema is automatically restored to its previous value when exiting
    the context, even if an exception occurs. This is the recommended way to
    temporarily change schemas.

    Args:
        schema: Schema name to use within the context
                Cannot be empty or None

    Yields:
        None

    Raises:
        ValueError: If schema is empty or None

    Example:
        # Basic usage - automatic restoration
        >>> set_schema('public')
        >>> with schema_context('test'):
        ...     cycle_id = register_cycle('test_cycle')  # Uses 'test'
        ...     print(get_current_schema())
        'test'
        >>> print(get_current_schema())  # Restored automatically
        'public'

        >>> # Nested contexts
        >>> with schema_context('dev'):
        ...     print(get_current_schema())  # 'dev'
        ...     with schema_context('test'):
        ...         print(get_current_schema())  # 'test'
        ...     print(get_current_schema())  # 'dev' (restored)
        'dev'
        'test'
        'dev'

        >>> # Exception handling - still restores
        >>> with schema_context('test'):
        ...     try:
        ...         execute_query("INVALID SQL")
        ...     except DatabaseError:
        ...         pass
        >>> print(get_current_schema())  # Still restored to 'public'
        'public'

    Best Practices:
        - Use this for test isolation
        - Preferred over set_schema() for temporary changes
        - Safe even if exceptions occur
        - Can be nested for complex scenarios

    See Also:
        set_schema(): For persistent schema changes
        get_current_schema(): To check current schema
    """
    if not schema:
        raise ValueError("Schema name cannot be empty")

    # Save current schema
    old_schema = get_current_schema()

    # Set new schema
    set_schema(schema)

    try:
        yield
    finally:
        # Always restore original schema, even if exception occurred
        set_schema(old_schema)


def reset_schema():
    """
    Reset schema to default ('public').

    Useful for cleanup after tests or when you want to ensure you're back
    to the production/default schema.

    Example:
        >>> set_schema('test')
        >>> get_current_schema()
        'test'

        >>> reset_schema()
        >>> get_current_schema()
        'public'

        >>> # Common pattern in test cleanup
        >>> def teardown():
        ...     reset_schema()
        ...     # ... other cleanup

    See Also:
        set_schema(): To set a different schema
        get_current_schema(): To check current schema
    """
    set_schema('public')


def get_schema_from_env() -> str:
    """
    Get schema from environment variable.

    Checks DB_SCHEMA environment variable, defaults to 'public' if not set.
    This function reads the CURRENT environment variable value, not the
    cached value from module import.

    Returns:
        str: Schema name from environment or 'public'

    Example:
        >>> # In shell
        >>> # export DB_SCHEMA=dev

        >>> # In Python
        >>> get_schema_from_env()
        'dev'

        >>> # If not set
        >>> # (DB_SCHEMA not set)
        >>> get_schema_from_env()
        'public'

    Note:
        The module automatically calls init_from_environment() on import,
        which reads this value ONCE. Changing the environment variable
        after import requires calling init_from_environment() again.

    See Also:
        init_from_environment(): To apply env var to current context
    """
    return os.environ.get('DB_SCHEMA', 'public')


def init_from_environment():
    """
    Initialize schema context from DB_SCHEMA environment variable.

    If DB_SCHEMA environment variable is set and not 'public', sets it as
    the current schema context using set_schema(). This is automatically
    called when the module is imported.

    Example:
        >>> # In docker-compose.yml or shell
        >>> # export DB_SCHEMA=production

        >>> # At application startup (or module import)
        >>> init_from_environment()

        >>> # Now all operations use 'production' schema by default
        >>> get_current_schema()
        'production'
        >>> cycle_id = register_cycle('Q1')  # Uses 'production'

        >>> # Can still override with explicit schema or context
        >>> with schema_context('test'):
        ...     cycle_id = register_cycle('test')  # Uses 'test'

    Behavior:
        - If DB_SCHEMA is not set: Does nothing (stays at 'public')
        - If DB_SCHEMA='public': Does nothing (already public)
        - If DB_SCHEMA='anything_else': Sets context to that schema

    Note:
        This is called automatically at module import, so you typically
        don't need to call it manually unless you change the environment
        variable after import.

    See Also:
        get_schema_from_env(): To just read the env var without applying it
        set_schema(): To manually set the schema
    """
    env_schema = get_schema_from_env()
    if env_schema != 'public':
        set_schema(env_schema)


# Auto-initialize from environment on module import
# This ensures DB_SCHEMA environment variable is respected by default
init_from_environment()


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


# SQLAlchemy engine
def get_engine(schema: str = 'public'):
    """
    Get SQLAlchemy engine

    Args:
        schema: Database schema to use (default: 'public')

    Returns:
        SQLAlchemy engine
    """
    db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url, poolclass=NullPool)

    # Note: We set search_path per connection, not here
    return engine


def _set_search_path(conn, schema: str):
    """

    Set the PostgreSQL search_path for a database connection.

    This function is the FINAL STEP in the schema resolution chain:

    1. Application Level (Python):
       - User code calls: execute_query("SELECT * FROM irp_cycle")
       - Schema determined by precedence: explicit param > context > env > 'public'

    2. Connection Level (SQL) Managed by THIS FUNCTION:
       - Takes the resolved schema from step 1
       - Executes PostgreSQL command: SET search_path TO {schema}, public
       - This tells PostgreSQL which schema to search for tables

    3. Query Execution:
       - PostgreSQL uses the search_path to find tables
       - Query "SELECT * FROM irp_cycle" searches:
           a) {schema}.irp_cycle (first)
           b) public.irp_cycle (fallback if not found in {schema})

    USING WITH set_schema:
    ----------------------
    Mental model:
    - set_schema('test') = "Remember to use 'test' schema" (Python memory)
    - _set_search_path(conn, 'test') = "Hey PostgreSQL, look in 'test' first" (SQL command)

    1. set_schema('my_schema')
       Stores 'my_schema' in thread-local Python variable, setting the CONTEXT for your thread
    2. execute_query("SELECT * FROM irp_cycle")
       - Gets 'my_schema' from Python context
       - Opens database connection
       - Calls _set_search_path(conn, 'my_schema')  ← THIS FUNCTION
       - PostgreSQL now knows to look in 'my_schema' first
       - Executes your query

    Args:
        conn: Active SQLAlchemy database connection
        schema: Schema name to use in PostgreSQL's search_path

    SQL Behavior:
        - schema='test': Executes "SET search_path TO test, public"
        - schema='public': Executes "SET search_path TO public"

    Why 'public' as fallback?
        If a table doesn't exist in your schema, PostgreSQL checks 'public' next.
        This allows system tables and shared resources to still be accessible.

    Connection Lifecycle:
        - This is a CONNECTION-LEVEL setting (SQL command)
        - It does NOT affect Python's schema context
        - Search_path resets when connection closes
        - That's why we call this for every database operation        - We use NullPool, so each operation = new connection = must set search_path again
    """
    if schema and schema != 'public':
        # Execute SQL: SET search_path TO {schema}, public
        # This tells PostgreSQL: "Look in {schema} first, then fall back to public"
        conn.execute(text(f"SET search_path TO {schema}, public"))
    else:
        # Execute SQL: SET search_path TO public
        # Standard/default behavior
        conn.execute(text("SET search_path TO public"))


def _convert_query_params(query: str, params: tuple = None):
    """
    Convert psycopg2-style (%s) parameters to SQLAlchemy-style (:paramN) parameters.

    This function bridges the gap between psycopg2's positional parameter style
    and SQLAlchemy's named parameter style. It's used internally by all execute
    functions to ensure parameter safety.

    Args:
        query: SQL query string with %s placeholders
        params: Tuple of parameter values (optional)

    Returns:
        Tuple of (modified_query, param_dict):
            - modified_query: Query with %s replaced by :param0, :param1, etc.
            - param_dict: Dict mapping param names to values

    Example:
        >>> query = "SELECT * FROM tbl WHERE id = %s AND name = %s"
        >>> params = (42, 'test')
        >>> _convert_query_params(query, params)
        ("SELECT * FROM tbl WHERE id = :param0 AND name = :param1",
         {'param0': 42, 'param1': 'test'})

    Implementation Notes:
        - Processes placeholders in REVERSE order to maintain string positions
        - Each %s is replaced with :paramN where N is the position (0-indexed)
        - Returns empty dict if no params provided
    """
    if not params:
        return query, {}

    import re
    param_dict = {}
    modified_query = query

    # Find all %s placeholders in the query
    placeholders = list(re.finditer(r'%s', query))

    # Replace each %s with :param0, :param1, etc.
    # IMPORTANT: Process in REVERSE order to maintain string positions as we modify
    for i, match in enumerate(reversed(placeholders)):
        # Calculate the actual parameter index (since we're iterating reversed)
        param_name = f'param{len(placeholders) - i - 1}'
        param_dict[param_name] = params[len(placeholders) - i - 1]

        # Replace %s with :paramN at the match position
        modified_query = modified_query[:match.start()] + f':{param_name}' + modified_query[match.end():]

    return modified_query, param_dict


def _convert_params_to_native_types(params: tuple) -> tuple:
    """
    Convert numpy/pandas types to Python native types for psycopg2 compatibility.

    This function ensures that parameter values are compatible with psycopg2,
    which doesn't natively support numpy data types. When you query data using
    execute_query() (returns pandas DataFrame), the column values are numpy types.
    This function automatically converts them to Python native types.

    execute_query() returns pandas DataFrame
    DataFrame columns use numpy types (numpy.int64, numpy.float64, etc.)
    When you pass those values to execute_command() or execute_insert():
    - psycopg2 receives numpy types
    - psycopg2 doesn't know how to handle them
    - Raises: "ProgrammingError: can't adapt type 'numpy.int64'"
    This function converts numpy → Python types automatically

    COMMON SCENARIO:
    ----------------
    # Query returns DataFrame with numpy types
    df = execute_query("SELECT id FROM irp_cycle WHERE status = 'ACTIVE'")

    # df['id'] values are numpy.int64, not Python int
    for _, row in df.iterrows():
        cycle_id = row['id']  # This is numpy.int64
        archive_cycle(cycle_id)  # Would fail without conversion!

    # This function automatically converts:
    # numpy.int64(42) → int(42)
    # numpy.float64(3.14) → float(3.14)
    # numpy.bool_(True) → bool(True)

    Args:
        params: Tuple of parameter values (may contain numpy types)

    Returns:
        tuple: Same parameters converted to Python native types

    Conversion Map:
        numpy.int64, numpy.int32, etc.     → int
        numpy.float64, numpy.float32, etc. → float
        numpy.bool_                        → bool
        numpy.str_                         → str
        Python native types                → unchanged (passthrough)
        None, dict, list, etc.             → unchanged (passthrough)

    Example:
        >>> import numpy as np
        >>> params = (np.int64(42), np.float64(3.14), 'text', None)
        >>> _convert_params_to_native_types(params)
        (42, 3.14, 'text', None)

        >>> # Real-world usage (happens automatically):
        >>> df = execute_query("SELECT id FROM irp_cycle")
        >>> cycle_id = df.iloc[0]['id']  # numpy.int64
        >>> # When passed to execute_command, automatically converted:
        >>> archive_cycle(cycle_id)  # Works! (converted to int internally)

    Implementation Notes:
        - Uses isinstance() to check numpy types
        - Handles all common numpy scalar types
        - Preserves None and other Python types unchanged
        - Returns tuple (same as input)
        - Performance: Negligible overhead (~microseconds for typical params)

    See Also:
        execute_command(): Uses this function to convert params
        execute_insert(): Uses this function to convert params
        bulk_insert(): Uses this function to convert params
    """
    if not params:
        return params

    converted = []
    for param in params:
        # Check for numpy integer types
        if isinstance(param, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            converted.append(int(param))

        # Check for numpy floating point types
        elif isinstance(param, (np.floating, np.float64, np.float32)):
            converted.append(float(param))

        # Check for numpy boolean type
        elif isinstance(param, np.bool_):
            converted.append(bool(param))

        # Check for numpy string types
        elif isinstance(param, np.str_):
            converted.append(str(param))

        # All other types (including Python natives): pass through unchanged
        else:
            converted.append(param)

    return tuple(converted)


def test_connection(schema: str = 'public') -> bool:
    """
    Test database connectivity

    Args:
        schema: Database schema to test (default: 'public')

    Returns:
        True if connection successful
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            _set_search_path(conn, schema)
            conn.execute(text("SELECT 1"))
        return True
    except: 
        return False 


def execute_query(query: str, params: tuple = None, schema: str = None) -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (optional, uses context if not provided)

    Returns:
        DataFrame with query results
    """
    try:
        # Use provided schema, or get from context
        active_schema = schema if schema is not None else get_current_schema()

        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_search_path(conn, active_schema)
            df = pd.read_sql_query(text(converted_query), conn, params=param_dict)
        return df
    except Exception as e:
        raise DatabaseError(f"Query failed: {str(e)}") # pragma: no cover


def execute_scalar(query: str, params: tuple = None, schema: str = None) -> Any:
    """
    Execute query and return single scalar value

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (optional, uses context if not provided)

    Returns:
        Single value from query
    """
    try:
        # Use provided schema, or get from context
        active_schema = schema if schema is not None else get_current_schema()

        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_search_path(conn, active_schema)
            result = conn.execute(text(converted_query), param_dict)
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Scalar query failed: {str(e)}") # paragma: no cover


def execute_command(query: str, params: tuple = None, schema: str = None) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return rows affected

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (optional, uses context if not provided)

    Returns:
        Number of rows affected
    """
    try:
        # Use provided schema, or get from context
        active_schema = schema if schema is not None else get_current_schema()

        # Convert numpy types to Python native types for psycopg2 compatibility
        params = _convert_params_to_native_types(params)

        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_search_path(conn, active_schema)
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            return result.rowcount
    except Exception as e:
        raise DatabaseError(f"Command failed: {str(e)}") # pragma: no cover


def execute_insert(query: str, params: tuple = None, schema: str = None) -> int:
    """
    Execute INSERT and return new record ID

    Args:
        query: SQL INSERT query string
        params: Query parameters (optional)
        schema: Database schema to use (optional, uses context if not provided)

    Returns:
        ID of newly inserted record
    """
    try:
        # Use provided schema, or get from context
        active_schema = schema if schema is not None else get_current_schema()

        # Add RETURNING id if not present
        if "RETURNING" not in query.upper():
            query = query + " RETURNING id"

        # Convert numpy types to Python native types for psycopg2 compatibility
        params = _convert_params_to_native_types(params)

        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_search_path(conn, active_schema)
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Insert failed: {str(e)}") # pragma: no cover


def bulk_insert(query: str, params_list: List[tuple], jsonb_columns: List[int] = None, schema: str = None) -> List[int]:
    """
    Execute bulk INSERT and return list of new record IDs

    This function efficiently inserts multiple records in a single transaction.
    If any insert fails, the entire operation is rolled back.

    Args:
        query: SQL INSERT query string with placeholders (%s)
        params_list: List of tuples, each containing parameters for one insert
        jsonb_columns: Optional list of column indices (0-based) that contain JSONB data.
                      Dicts at these positions will be automatically converted to JSON strings.
        schema: Database schema to use (optional, uses context if not provided)

    Returns:
        List of IDs for newly inserted records (in order)

    Example:
        query = "INSERT INTO tbl (name, status, json_data) VALUES (%s, %s, %s)"
        params = [
            ('n1', 'ACTIVE', {'key': 'value1'}),
            ('n2', 'ACTIVE', {'key': 'value2'})
        ]
        ids = bulk_insert(query, params, jsonb_columns=[2])
    """
    import json

    if not params_list:
        return []

    try:
        # Use provided schema, or get from context
        active_schema = schema if schema is not None else get_current_schema()

        # Add RETURNING id if not present
        if "RETURNING" not in query.upper():
            query = query + " RETURNING id"

        # Process JSONB columns if specified
        processed_params = []
        for params in params_list:
            # Convert numpy types to Python native types for psycopg2 compatibility
            params = _convert_params_to_native_types(params)

            if jsonb_columns:
                # Convert to list for modification
                params_list_item = list(params)
                for col_idx in jsonb_columns:
                    if col_idx < len(params_list_item) and params_list_item[col_idx] is not None:
                        # Convert dict to JSON string if needed
                        if isinstance(params_list_item[col_idx], dict):
                            params_list_item[col_idx] = json.dumps(params_list_item[col_idx])
                processed_params.append(tuple(params_list_item))
            else:
                processed_params.append(params)

        engine = get_engine()
        inserted_ids = []

        with engine.connect() as conn:
            _set_search_path(conn, active_schema)

            # Execute all inserts in a single transaction
            for params in processed_params:
                # Convert query params for each insert
                converted_query, param_dict = _convert_query_params(query, params)
                result = conn.execute(text(converted_query), param_dict)
                row = result.fetchone()
                if row:
                    inserted_ids.append(row[0])

            # Commit all inserts at once
            conn.commit()

        return inserted_ids

    except Exception as e:
        raise DatabaseError(f"Bulk insert failed: {str(e)}")


def init_database(schema: str = 'public', sql_file_name: str = 'init_database.sql') -> bool:
    """
    Initialize database by running SQL script

    Args:
        schema: Database schema to use (default: 'public')
        sql_file_name: Name of SQL initialization file (default: 'init_database.sql')

    Returns:
        True if successful
    """
    try:
        from pathlib import Path

        # Read SQL initialization script
        sql_file = Path(__file__).parent / 'db' / sql_file_name

        if not sql_file.exists():
            raise DatabaseError(f"SQL file not found: {sql_file}")

        with open(sql_file, 'r') as f:
            sql_script = f.read()

        # Execute script
        engine = get_engine()
        with engine.connect() as conn:
            # Create schema if not public
            if schema != 'public':
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                _set_search_path(conn, schema)

            # Execute the SQL script
            conn.execute(text(sql_script))
            conn.commit()

        print(f"Database initialized successfully (schema: {schema})")
        return True

    except Exception as e:
        print(f"Database initialization failed: {str(e)}")
        return False

# ============================================================================
# CYCLE OPERATIONS
# ============================================================================
#
# Cycles represent analysis periods (e.g., "Analysis-2025-Q1"). They are the
# top-level organizational unit in the IRP Notebook Framework hierarchy:
#
#   Cycle (Analysis-2025-Q1)
#     └─ Stage 1 (Data Collection)
#         └─ Step 1 (Load Raw Data)
#             └─ Step Run 1 (Execution Record)
#
# SCHEMA BEHAVIOR:
# - All cycle operations respect the schema context/override pattern
# - Use schema_context() for test isolation
# - Cycles in different schemas are completely independent
#
# ============================================================================

def get_active_cycle() -> Optional[Dict[str, Any]]:
    """
    Get the currently active cycle from the current schema.

    Returns the most recently created ACTIVE cycle. There should only be one
    active cycle at a time, but this ensures you get the latest if multiple exist.

    Schema Behavior:
        - Uses current schema from context (get_current_schema())
        - To query a different schema, use set_schema() or schema_context()
        - Searches only within the current context schema

    Returns:
        Optional[Dict[str, Any]]: Dictionary with cycle fields or None
            {
                'id': int,           # Unique cycle ID
                'cycle_name': str,   # e.g., 'Analysis-2025-Q1'
                'status': str,       # 'ACTIVE' or 'ARCHIVED'
                'created_ts': datetime
            }

    Example:
        >>> cycle = get_active_cycle()
        >>> if cycle:
        ...     print(f"Working on: {cycle['cycle_name']}")
        ... else:
        ...     print("No active cycle - need to create one")
        Working on: Analysis-2025-Q1

        >>> # Test isolation
        >>> with schema_context('test'):
        ...     test_cycle = get_active_cycle()  # Searches test schema only
    """
    query = """
        SELECT id, cycle_name, status, created_ts
        FROM irp_cycle
        WHERE status = 'ACTIVE'
        ORDER BY created_ts DESC
        LIMIT 1
    """
    df = execute_query(query)
    return df.iloc[0].to_dict() if not df.empty else None


def get_cycle_by_name(cycle_name: str) -> Optional[Dict[str, Any]]:
    """
    Get cycle by its name from the current schema.

    Retrieves a cycle regardless of status (ACTIVE or ARCHIVED). Cycle names
    must be unique within a schema.

    Args:
        cycle_name: Name of the cycle to retrieve (e.g., 'Analysis-2025-Q1')

    Schema Behavior:
        - Uses current schema from context
        - To query a different schema, use schema_context() first

    Returns:
        Optional[Dict[str, Any]]: Dictionary with cycle fields or None if not found
            {
                'id': int,
                'cycle_name': str,
                'status': str,         # 'ACTIVE' or 'ARCHIVED'
                'created_ts': datetime,
                'archived_ts': datetime | None
            }

    Example:
        >>> cycle = get_cycle_by_name('Analysis-2024-Q4')
        >>> if cycle:
        ...     if cycle['status'] == 'ARCHIVED':
        ...         print("This cycle is archived")
        ... else:
        ...     print("Cycle not found")
    """
    query = """
        SELECT id, cycle_name, status, created_ts, archived_ts
        FROM irp_cycle
        WHERE cycle_name = %s
    """
    df = execute_query(query, (cycle_name,))
    return df.iloc[0].to_dict() if not df.empty else None


def register_cycle(cycle_name: str) -> int:
    """
    Create a new cycle with ACTIVE status.

    Creates a new analysis cycle in the current schema. The cycle is automatically
    set to ACTIVE status.

    Args:
        cycle_name: Unique name for the cycle (e.g., 'Analysis-2025-Q1')
                   Should follow naming convention: 'Analysis-YYYY-QN'

    Schema Behavior:
        - Creates cycle in current schema from context
        - To create in different schema, use schema_context() or set_schema() first
        - Cycle names must be unique within their schema

    Returns:
        int: ID of the newly created cycle

    Raises:
        DatabaseError: If cycle name already exists in the schema

    Example:
        >>> cycle_id = register_cycle('Analysis-2025-Q2')
        >>> print(f"Created cycle with ID: {cycle_id}")
        Created cycle with ID: 42

        >>> # Test environment
        >>> with schema_context('test'):
        ...     test_id = register_cycle('test-cycle')  # Isolated in test schema

    See Also:
        archive_cycle(): To mark a cycle as archived
        get_cycle_by_name(): To check if cycle already exists
    """
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """
    return execute_insert(query, (cycle_name, CycleStatus.ACTIVE))


def archive_cycle(cycle_id: int) -> bool:
    """
    Archive a cycle by setting its status to ARCHIVED.

    Marks a cycle as archived and records the archival timestamp. This does not
    delete any data - the cycle and all its stages/steps remain in the database.

    Args:
        cycle_id: ID of the cycle to archive

    Schema Behavior:
        - Updates cycle in current schema from context
        - To update in different schema, use schema_context() or set_schema() first

    Returns:
        bool: True if cycle was archived, False if cycle_id not found

    Example:
        >>> cycle = get_cycle_by_name('Analysis-2024-Q1')
        >>> if archive_cycle(cycle['id']):
        ...     print("Cycle archived successfully")
        ... else:
        ...     print("Cycle not found")
        Cycle archived successfully

        >>> # Verify archival
        >>> cycle = get_cycle_by_name('Analysis-2024-Q1')
        >>> print(f"Status: {cycle['status']}, Archived: {cycle['archived_ts']}")
        Status: ARCHIVED, Archived: 2025-01-15 10:30:00

    Note:
        Archiving a cycle does not cascade to stages or steps. Only the cycle
        status changes. All related data remains queryable.

    See Also:
        register_cycle(): To create a new cycle
        delete_cycle(): To permanently delete a cycle (use with caution)
    """
    query = """
        UPDATE irp_cycle
        SET status = %s, archived_ts = NOW()
        WHERE id = %s
    """
    rows = execute_command(query, (CycleStatus.ARCHIVED, cycle_id))
    return rows > 0

def delete_cycle(cycle_id: int) -> bool:
    """
    Permanently delete a cycle and all associated data.

    WARNING: This is a hard delete that will CASCADE to all related records:
    - All stages in the cycle
    - All steps in those stages
    - All step runs for those steps
    - All associated execution history

    This operation CANNOT be undone. Consider using archive_cycle() instead
    for most use cases.

    Args:
        cycle_id: ID of the cycle to delete

    Schema Behavior:
        - Deletes from current schema from context
        - To delete from different schema, use schema_context() or set_schema() first

    Returns:
        bool: True if cycle was deleted, False if cycle_id not found

    Example:
        >>> # Typical use: Clean up test data
        >>> with schema_context('test'):
        ...     test_cycle = get_cycle_by_name('test-cycle')
        ...     if test_cycle:
        ...         delete_cycle(test_cycle['id'])
        ...         print("Test data cleaned up")

    Warnings:
        - USE WITH EXTREME CAUTION in production
        - Cannot be undone
        - Cascades to all related records
        - Consider archive_cycle() for production use

    See Also:
        archive_cycle(): Safer alternative that preserves data
        get_cycle_by_name(): To verify before deleting
    """
    query = """
        DELETE FROM irp_cycle
        WHERE id = %s
    """
    rows = execute_command(query, (cycle_id,)) # It needs to be a tuple
    return rows > 0


# ============================================================================
# STAGE OPERATIONS
# ============================================================================

def get_or_create_stage(cycle_id: int, stage_num: int, stage_name: str) -> int:
    """Get existing stage or create new one"""
    
    # Try to get existing
    query = "SELECT id FROM irp_stage WHERE cycle_id = %s AND stage_num = %s"
    stage_id = execute_scalar(query, (cycle_id, stage_num))
    
    if stage_id:
        return stage_id
    
    # Create new
    query = """
        INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
        VALUES (%s, %s, %s)
    """
    return execute_insert(query, (cycle_id, stage_num, stage_name))

# ============================================================================
# STEP OPERATIONS
# ============================================================================

def get_or_create_step(
    stage_id: int,
    step_num: int,
    step_name: str,
    notebook_path: str = None,
) -> int:
    """Get existing step or create new one"""
    
    # Try to get existing
    query = "SELECT id FROM irp_step WHERE stage_id = %s AND step_num = %s"
    step_id = execute_scalar(query, (stage_id, step_num))
    
    if step_id:
        return step_id
    
    # Create new
    query = """
        INSERT INTO irp_step (stage_id, step_num, step_name, notebook_path)
        VALUES (%s, %s, %s, %s)
    """
    return execute_insert(query, (stage_id, step_num, step_name, notebook_path))


def get_step_info(step_id: int) -> Optional[Dict[str, Any]]:
    """Get step information"""
    query = """
        SELECT 
            st.id, st.step_num, st.step_name, st.notebook_path,
            sg.stage_num, sg.stage_name,
            c.cycle_name, c.id as cycle_id
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        WHERE st.id = %s
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None

# ============================================================================
# STEP RUN OPERATIONS
# ============================================================================

def get_last_step_run(step_id: int) -> Optional[Dict[str, Any]]:
    """Get the most recent run for a step"""
    query = """
        SELECT id, run_num, status, started_ts, completed_ts, 
               started_by, error_message, output_data
        FROM irp_step_run
        WHERE step_id = %s
        ORDER BY run_num DESC
        LIMIT 1
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def create_step_run(step_id: int, started_by: str) -> Tuple[int, int]:
    """
    Create new step run
    
    Returns:
        Tuple of (run_id, run_num)
    """
    # Get next run number
    query = "SELECT COALESCE(MAX(run_num), 0) + 1 FROM irp_step_run WHERE step_id = %s"
    run_num = execute_scalar(query, (step_id,))
    
    # Create run
    query = """
        INSERT INTO irp_step_run (step_id, run_num, status, started_by)
        VALUES (%s, %s, 'ACTIVE', %s)
    """
    run_id = execute_insert(query, (step_id, run_num, started_by))
    
    return run_id, run_num


def update_step_run(
    run_id: int,
    status: str,
    error_message: str = None,
    output_data: Dict = None
) -> bool:
    """Update step run with completion status"""
    import json
    
    query = """
        UPDATE irp_step_run
        SET status = %s,
            completed_ts = CASE WHEN %s IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN NOW() ELSE completed_ts END,
            error_message = %s,
            output_data = %s
        WHERE id = %s
    """
    
    rows = execute_command(query, (
        status,
        status,
        error_message,
        json.dumps(output_data) if output_data else None,
        run_id
    ))
    
    return rows > 0

# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_cycle_progress(cycle_name: str) -> pd.DataFrame:
    """Get progress for all steps in a cycle"""
    query = """
        SELECT 
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            sr.status as last_status,
            sr.run_num as last_run,
            sr.completed_ts as last_completed
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        LEFT JOIN LATERAL (
            SELECT status, run_num, completed_ts
            FROM irp_step_run
            WHERE step_id = st.id
            ORDER BY run_num DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE c.cycle_name = %s
        ORDER BY sg.stage_num, st.step_num
    """
    return execute_query(query, (cycle_name,))


def get_step_history(cycle_name: str, stage_num: int = None, step_num: int = None) -> pd.DataFrame:
    """Get execution history for steps"""
    
    base_query = """
        SELECT 
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            sr.run_num,
            sr.status,
            sr.started_ts,
            sr.completed_ts,
            sr.started_by,
            EXTRACT(EPOCH FROM (sr.completed_ts - sr.started_ts)) as duration_seconds
        FROM irp_step_run sr
        INNER JOIN irp_step st ON sr.step_id = st.id
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        WHERE c.cycle_name = %s
    """
    
    params = [cycle_name]
    
    if stage_num is not None:
        base_query += " AND sg.stage_num = %s"
        params.append(stage_num)
    
    if step_num is not None:
        base_query += " AND st.step_num = %s"
        params.append(step_num)
    
    base_query += " ORDER BY sr.started_ts DESC"
    
    return execute_query(base_query, tuple(params))