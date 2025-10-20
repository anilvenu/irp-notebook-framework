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
from typing import List, Any
from helpers.constants import DB_CONFIG, StepStatus

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


@contextmanager
def transaction_context(schema: str = None):
    """
    Context manager for executing multiple database operations in a single transaction.

    This provides ACID guarantees across multiple operations. All operations within
    the context share a single database connection and are committed together when
    the context exits successfully, or rolled back if an exception occurs.

    - All database operations (execute_query, execute_insert, execute_command, etc.)
      within this context will use the SAME connection
    - Operations are NOT committed until the context exits
    - If ANY operation raises an exception, ALL operations are rolled back
    - Nesting transaction contexts is NOT supported (will raise error)
    - Thread-safe: Each thread has its own transaction context

    Args:
        schema: Schema to use for this transaction (optional, uses current schema if None)

    Yields:
        SQLAlchemy connection object (can be ignored in most cases)

    Raises:
        DatabaseError: If transaction fails or if nested transactions attempted

    """
    # Check for nested transactions
    if hasattr(_context, 'transaction_conn') and _context.transaction_conn is not None:
        raise DatabaseError(
            "Nested transactions are not supported. "
            "Complete the outer transaction_context before starting a new one."
        )

    # Determine schema to use
    active_schema = schema if schema is not None else get_current_schema()

    # Get engine and start transaction
    engine = get_engine()

    try:
        # Use engine.begin() for automatic commit/rollback
        with engine.begin() as conn:
            # Set search path for this connection
            _set_search_path(conn, active_schema)

            # Store connection in thread-local storage
            _context.transaction_conn = conn

            try:
                yield conn
                # If we reach here without exception, commit happens automatically
            finally:
                # Always clean up thread-local storage
                _context.transaction_conn = None

    except Exception as e:
        # Rollback happens automatically via engine.begin()
        # Re-raise as DatabaseError for consistency
        if isinstance(e, DatabaseError):
            raise
        else:
            raise DatabaseError(f"Transaction failed: {str(e)}")


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

    Transaction Behavior:
        - If called within transaction_context(): Uses shared connection
        - If called outside transaction: Creates connection (read-only, no commit needed)
    """
    try:
        # Check if we're in a transaction context
        if hasattr(_context, 'transaction_conn') and _context.transaction_conn is not None:
            # Use existing transaction connection
            conn = _context.transaction_conn

            # Convert query params
            converted_query, param_dict = _convert_query_params(query, params)

            # Execute query using shared connection
            df = pd.read_sql_query(text(converted_query), conn, params=param_dict)
            return df
        else:
            # New connection
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

    Transaction Behavior:
        - If called within transaction_context(): Uses shared connection
        - If called outside transaction: Creates connection (read-only, no commit needed)
    """
    try:
        # Check if we're in a transaction context
        if hasattr(_context, 'transaction_conn') and _context.transaction_conn is not None:
            # Use existing transaction connection
            conn = _context.transaction_conn

            # Convert query params
            converted_query, param_dict = _convert_query_params(query, params)

            # Execute query using shared connection
            result = conn.execute(text(converted_query), param_dict)
            row = result.fetchone()
            return row[0] if row else None
        else:
            # New connection
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

    Transaction Behavior:
        - If called within transaction_context(): Uses shared connection, no commit
        - If called outside transaction: Creates connection, auto-commits
    """
    try:
        # Check if we're in a transaction context
        if hasattr(_context, 'transaction_conn') and _context.transaction_conn is not None:
            # Use existing transaction connection (don't commit)
            conn = _context.transaction_conn

            # Convert numpy types to Python native types for psycopg2 compatibility
            params = _convert_params_to_native_types(params)

            # Convert query params
            converted_query, param_dict = _convert_query_params(query, params)

            # Execute without commit (transaction will commit)
            result = conn.execute(text(converted_query), param_dict)
            return result.rowcount
        else:
            # New connection + auto-commit
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

    Transaction Behavior:
        - If called within transaction_context(): Uses shared connection, no commit
        - If called outside transaction: Creates connection, auto-commits
    """
    try:
        # Add RETURNING id if not present
        if "RETURNING" not in query.upper():
            query = query + " RETURNING id"

        # Check if we're in a transaction context
        if hasattr(_context, 'transaction_conn') and _context.transaction_conn is not None:
            # Use existing transaction connection (don't commit)
            conn = _context.transaction_conn

            # Convert numpy types to Python native types for psycopg2 compatibility
            params = _convert_params_to_native_types(params)

            # Convert query params
            converted_query, param_dict = _convert_query_params(query, params)

            # Execute without commit (transaction will commit)
            result = conn.execute(text(converted_query), param_dict)
            row = result.fetchone()
            return row[0] if row else None
        else:
            # Original behavior: fresh connection + auto-commit
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
# STAGE OPERATIONS
# ============================================================================
# NOTE: Cycle operations have been moved to helpers/cycle.py
# Import from there: from helpers.cycle import get_active_cycle, etc.
# NOTE: Stage operations have been moved to helpers/stage.py
# Import from there: from helpers.stage import get_or_create_stage, etc.
# ============================================================================

# ============================================================================
# STEP OPERATIONS
# ============================================================================
# NOTE: Step operations have been moved to helpers/step.py
# Import from there: from helpers.step import get_or_create_step, get_step_info, etc.
# NOTE: Step run operations have been moved to helpers/step.py
# Import from there: from helpers.step import get_last_step_run, create_step_run, update_step_run
# ============================================================================

# ============================================================================
# QUERY HELPERS
# ============================================================================
# NOTE: Cycle-related query helpers (get_cycle_progress, get_step_history)
# have been moved to helpers/cycle.py
# Import from there: from helpers.cycle import get_cycle_progress, etc.
# ============================================================================