"""
pytest configuration and shared fixtures for IRP Notebook Framework tests

This module provides:
- Automatic test schema management (derived from test filename)
- Database connection verification
- Shared fixtures for common test scenarios
- Custom pytest options (--preserve-schema)
"""

import pytest
from pathlib import Path
from helpers.database import (
    init_database,
    get_engine,
    test_connection,
    execute_insert,
    execute_query,
    DatabaseError,
    set_schema
)
from helpers.constants import ConfigurationStatus
from sqlalchemy import text
from datetime import datetime
import json


# ==============================================================================
# PYTEST CONFIGURATION
# ==============================================================================

def pytest_addoption(parser):
    """Add custom command-line options for pytest"""
    parser.addoption(
        "--preserve-schema",
        action="store_true",
        default=False,
        help="Preserve test schemas after tests complete (for debugging)"
    )


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests for individual functions"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests for component interactions"
    )
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests for complete workflows"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take more than 5 seconds"
    )
    config.addinivalue_line(
        "markers", "database: Tests that require database connection"
    )
    config.addinivalue_line(
        "markers", "moody_api: Tests that require Moody's API (when implemented)"
    )


# ==============================================================================
# SESSION-LEVEL FIXTURES
# ==============================================================================

@pytest.fixture(scope="session", autouse=True)
def verify_database_connection():
    """
    Verify database is accessible before running any tests.

    This fixture runs once per test session and fails fast if database
    is not available.
    """
    if not test_connection():
        pytest.fail(
            "Database connection failed. Please ensure:\n"
            "  1. PostgreSQL container is running\n"
            "  2. test_db and test_user exist\n"
            "  3. Environment variables are set correctly\n"
            "Run: docker ps | grep postgres"
        )


# ==============================================================================
# MODULE-LEVEL FIXTURES
# ==============================================================================

@pytest.fixture(scope="module")
def test_schema(request):
    """
    Automatically create and manage test schema for each test module.

    Schema name is derived from the test file name:
    - test_database.py → schema: test_database
    - test_job.py → schema: test_job
    - test_batch.py → schema: test_batch

    This allows multiple test files to run in parallel without conflicts.

    Lifecycle:
    1. Drop existing schema (cleanup from previous run)
    2. Create and initialize new schema
    3. Yield schema name to tests
    4. Drop schema after tests (unless --preserve-schema flag)

    Usage:
        def test_something(test_schema):
            job_id = create_job(..., schema=test_schema)
    """
    # Derive schema name from test file
    schema = Path(request.fspath).stem

    preserve_mode = request.config.getoption("--preserve-schema")

    print(f"\n{'='*80}")
    print(f"SETUP: Initializing Test Schema '{schema}'")
    if preserve_mode:
        print(f"⚠️  PRESERVE MODE: Schema will be kept after tests")
    print(f"{'='*80}")

    # Cleanup any existing schema from previous run
    # This is critical: drops previously preserved schemas
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # Check if schema exists first
            result = conn.execute(text(
                f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'"
            ))
            existing = result.fetchone()

            if existing:
                print(f"⚠️  Found existing schema '{schema}' (from previous --preserve run)")

            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.commit()

        if existing:
            print(f"✓ Dropped previously preserved schema '{schema}'")
        else:
            print(f"✓ No existing schema found")
    except Exception as e:
        print(f"⚠️  Warning: Could not drop existing schema: {e}")

    # Initialize new schema
    try:
        success = init_database(schema=schema)
        if not success:
            pytest.fail(f"Failed to initialize test schema '{schema}'")
        print(f"✓ Test schema '{schema}' initialized successfully")
    except Exception as e:
        pytest.fail(f"Failed to setup test schema '{schema}': {e}")

    # Set schema context for all tests in this module
    # This allows tests to call functions without schema= parameter
    set_schema(schema)
    print(f"✓ Schema context set to '{schema}'\n")

    # Provide schema to tests
    yield schema

    # Cleanup (unless preserve flag is set)
    if not request.config.getoption("--preserve-schema"):
        print(f"\n{'='*80}")
        print(f"CLEANUP: Dropping Test Schema '{schema}'")
        print(f"{'='*80}")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
                conn.commit()
            print(f"✓ Test schema '{schema}' dropped successfully")
        except Exception as e:
            print(f"Warning: Cleanup failed for schema '{schema}': {e}")
    else:
        print(f"\n⚠️  Schema '{schema}' preserved for debugging")

    # Reset schema context to 'public'
    set_schema('public')
    print(f"✓ Schema context reset to 'public'")


# ==============================================================================
# FUNCTION-LEVEL FIXTURES
# ==============================================================================

@pytest.fixture
def sample_cycle(test_schema):
    """
    Create a sample cycle for testing.

    Returns:
        int: cycle_id
    """
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('test_cycle', 'ACTIVE'),
        schema=test_schema
    )
    return cycle_id


@pytest.fixture
def sample_hierarchy(test_schema):
    """
    Create a complete test hierarchy: cycle → stage → step → configuration.

    Returns:
        tuple: (cycle_id, stage_id, step_id, config_id)
    """
    # Create cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('test_cycle', 'ACTIVE'),
        schema=test_schema
    )

    # Create stage
    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'test_stage'),
        schema=test_schema
    )

    # Create step
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'test_step'),
        schema=test_schema
    )

    # Create configuration
    config_data = {
        'param1': 'value1',
        'param2': 100,
        'nested': {'key': 'value'}
    }

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    return cycle_id, stage_id, step_id, config_id


@pytest.fixture
def sample_batch(sample_hierarchy, test_schema):
    """
    Create a sample batch with hierarchy.

    Returns:
        dict: {
            'batch_id': int,
            'cycle_id': int,
            'stage_id': int,
            'step_id': int,
            'config_id': int
        }
    """
    from helpers.constants import BatchStatus

    cycle_id, stage_id, step_id, config_id = sample_hierarchy

    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'default', BatchStatus.INITIATED),
        schema=test_schema
    )

    return {
        'batch_id': batch_id,
        'cycle_id': cycle_id,
        'stage_id': stage_id,
        'step_id': step_id,
        'config_id': config_id
    }


# ==============================================================================
# HELPER FUNCTIONS (for use in tests)
# ==============================================================================

def create_test_hierarchy(cycle_name, schema):
    """
    Helper function to create test hierarchy with custom cycle name.

    Args:
        cycle_name: Name for the cycle
        schema: Database schema

    Returns:
        tuple: (cycle_id, stage_id, step_id, config_id)
    """
    # Create cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
        schema=schema
    )

    # Create stage
    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'test_stage'),
        schema=schema
    )

    # Create step
    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'test_step'),
        schema=schema
    )

    # Create configuration
    config_data = {'param1': 'value1', 'param2': 100}
    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.VALID, datetime.now()),
        schema=schema
    )

    return cycle_id, stage_id, step_id, config_id
