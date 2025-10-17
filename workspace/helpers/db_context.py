"""
Database context management for schema selection

This module provides a context manager for setting the database schema
used by all database operations. It uses thread-local storage to ensure
thread-safety in concurrent environments.

Usage:
    # Production code (uses 'public' by default)
    from helpers.cycle import create_cycle
    cycle_id = create_cycle('Q1-2024')  # Uses 'public' schema

    # Test code (explicit schema)
    from helpers.db_context import schema_context
    with schema_context('test_cycle'):
        cycle_id = create_cycle('test_cycle_1')  # Uses 'test_cycle' schema

    # Set schema for multiple operations
    from helpers.db_context import set_schema
    set_schema('dev')
    cycle_id = create_cycle('dev_cycle')  # Uses 'dev' schema
    batch_id = create_batch(...)          # Also uses 'dev' schema
"""

import os
from contextlib import contextmanager
from threading import local

# Thread-local storage for schema context
_context = local()


def get_current_schema() -> str:
    """
    Get the current schema for database operations.

    Returns 'public' if no schema is explicitly set.

    Returns:
        str: Current schema name

    Example:
        schema = get_current_schema()
        print(f"Current schema: {schema}")
    """
    return getattr(_context, 'schema', 'public')


def set_schema(schema: str):
    """
    Set the schema for subsequent database operations.

    This affects all database operations in the current thread until
    changed or reset. Use with caution in production code.

    Args:
        schema: Schema name to use for database operations

    Example:
        # Set schema for all subsequent operations
        set_schema('test_cycle')
        cycle_id = create_cycle('test')  # Uses 'test_cycle' schema
        batch_id = create_batch(...)     # Also uses 'test_cycle' schema

        # Reset to default
        set_schema('public')
    """
    if not schema:
        raise ValueError("Schema name cannot be empty")
    _context.schema = schema


@contextmanager
def schema_context(schema: str):
    """
    Context manager for temporary schema selection.

    The schema is automatically restored to its previous value
    when exiting the context, even if an exception occurs.

    Args:
        schema: Schema name to use within the context

    Yields:
        None

    Example:
        # Temporary schema change
        with schema_context('test_cycle'):
            cycle_id = create_cycle('test')  # Uses 'test_cycle'
        # Automatically restored to previous schema

        # Nested contexts
        set_schema('public')
        with schema_context('dev'):
            # Uses 'dev' schema
            with schema_context('test'):
                # Uses 'test' schema
                pass
            # Back to 'dev' schema
        # Back to 'public' schema
    """
    if not schema:
        raise ValueError("Schema name cannot be empty")

    old_schema = get_current_schema()
    set_schema(schema)
    try:
        yield
    finally:
        set_schema(old_schema)


def reset_schema():
    """
    Reset schema to default ('public').

    Useful for cleanup after tests or when you want to ensure
    you're back to the production schema.

    Example:
        # After tests
        reset_schema()
        assert get_current_schema() == 'public'
    """
    set_schema('public')


def get_schema_from_env() -> str:
    """
    Get schema from environment variable.

    Checks DB_SCHEMA environment variable, defaults to 'public' if not set.

    Returns:
        str: Schema name from environment or 'public'

    Example:
        # Set environment variable
        export DB_SCHEMA=dev

        # In code
        schema = get_schema_from_env()
        print(schema)  # 'dev'
    """
    return os.environ.get('DB_SCHEMA', 'public')


def init_from_environment():
    """
    Initialize schema from environment variable.

    If DB_SCHEMA environment variable is set and not 'public',
    sets it as the current schema context.

    Example:
        # At application startup
        init_from_environment()

        # Now all operations use schema from environment
        cycle_id = create_cycle('Q1')  # Uses $DB_SCHEMA
    """
    env_schema = get_schema_from_env()
    if env_schema != 'public':
        set_schema(env_schema)


# Auto-initialize from environment on module import (optional)
# Uncomment the next line to enable automatic initialization
# init_from_environment()
