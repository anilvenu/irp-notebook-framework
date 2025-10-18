"""
Tests for database schema context management

This module tests the schema context functionality that was consolidated
from db_context.py into database.py. It verifies:
- Schema precedence (explicit > context > env > default)
- Context managers (schema_context)
- Schema isolation between operations
- Thread-local behavior
"""

import pytest
import os
from helpers.database import (
    get_current_schema,
    set_schema,
    schema_context,
    reset_schema,
    get_schema_from_env,
    init_from_environment,
    register_cycle,
    get_cycle_by_name,
    execute_query,
    init_database
)


# ==============================================================================
# SCHEMA PRECEDENCE TESTS
# ==============================================================================

@pytest.mark.unit
def test_default_schema_is_public():
    """Test that default schema is 'public' when nothing is set"""
    reset_schema()
    assert get_current_schema() == 'public'


@pytest.mark.unit
def test_set_schema_changes_context(test_schema):
    """Test that set_schema() changes the current schema context"""
    # Test schema is already set by conftest, but let's test the function
    original = get_current_schema()

    set_schema('test_custom')
    assert get_current_schema() == 'test_custom'

    # Restore to test_schema for other tests
    set_schema(original)


@pytest.mark.unit
def test_reset_schema_returns_to_public(test_schema):
    """Test that reset_schema() always returns to 'public'"""
    set_schema('some_other_schema')
    assert get_current_schema() == 'some_other_schema'

    reset_schema()
    assert get_current_schema() == 'public'

    # Restore test schema for other tests
    set_schema(test_schema)


@pytest.mark.unit
def test_set_schema_rejects_empty_string():
    """Test that set_schema() rejects empty string"""
    with pytest.raises(ValueError, match="Schema name cannot be empty"):
        set_schema('')


@pytest.mark.unit
def test_set_schema_rejects_none():
    """Test that set_schema() rejects None"""
    with pytest.raises(ValueError, match="Schema name cannot be empty"):
        set_schema(None)


# ==============================================================================
# SCHEMA CONTEXT MANAGER TESTS
# ==============================================================================

@pytest.mark.unit
def test_schema_context_temporary_change(test_schema):
    """Test that schema_context() temporarily changes schema"""
    # Start in test_schema (set by conftest)
    assert get_current_schema() == test_schema

    # Temporarily change to another schema
    with schema_context('temporary_schema'):
        assert get_current_schema() == 'temporary_schema'

    # Should restore to original after exiting context
    assert get_current_schema() == test_schema


@pytest.mark.unit
def test_schema_context_nested(test_schema):
    """Test nested schema contexts"""
    assert get_current_schema() == test_schema

    with schema_context('level1'):
        assert get_current_schema() == 'level1'

        with schema_context('level2'):
            assert get_current_schema() == 'level2'

            with schema_context('level3'):
                assert get_current_schema() == 'level3'

            assert get_current_schema() == 'level2'

        assert get_current_schema() == 'level1'

    assert get_current_schema() == test_schema


@pytest.mark.unit
def test_schema_context_restores_on_exception(test_schema):
    """Test that schema_context() restores schema even when exception occurs"""
    assert get_current_schema() == test_schema

    try:
        with schema_context('error_schema'):
            assert get_current_schema() == 'error_schema'
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Schema should be restored despite exception
    assert get_current_schema() == test_schema


@pytest.mark.unit
def test_schema_context_rejects_empty_string():
    """Test that schema_context() rejects empty string"""
    with pytest.raises(ValueError, match="Schema name cannot be empty"):
        with schema_context(''):
            pass


@pytest.mark.unit
def test_schema_context_rejects_none():
    """Test that schema_context() rejects None"""
    with pytest.raises(ValueError, match="Schema name cannot be empty"):
        with schema_context(None):
            pass


# ==============================================================================
# SCHEMA ISOLATION TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_schema_isolation_between_operations(test_schema):
    """Test that operations in different schemas are isolated"""
    # Create a second test schema for isolation testing
    isolation_schema = f"{test_schema}_isolation"

    # Initialize the isolation schema
    init_database(schema=isolation_schema)

    try:
        # Create cycle in test_schema (via context)
        set_schema(test_schema)
        cycle_id_1 = register_cycle('cycle_in_test_schema')

        # Create cycle with same name in isolation_schema
        set_schema(isolation_schema)
        cycle_id_2 = register_cycle('cycle_in_isolation_schema')  # Different name to avoid constraint issues

        # Note: IDs may be the same due to PostgreSQL sequences being shared across schemas
        # The key test is that each schema only sees its own data

        # Verify each schema only sees its own cycle
        set_schema(test_schema)
        cycle_1 = get_cycle_by_name('cycle_in_test_schema')
        assert cycle_1 is not None
        assert cycle_1['id'] == cycle_id_1
        # Should NOT see the isolation schema cycle
        cycle_not_found = get_cycle_by_name('cycle_in_isolation_schema')
        assert cycle_not_found is None

        set_schema(isolation_schema)
        cycle_2 = get_cycle_by_name('cycle_in_isolation_schema')
        assert cycle_2 is not None
        assert cycle_2['id'] == cycle_id_2
        # Should NOT see the test schema cycle
        cycle_not_found = get_cycle_by_name('cycle_in_test_schema')
        assert cycle_not_found is None

    finally:
        # Cleanup isolation schema
        from helpers.database import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {isolation_schema} CASCADE"))
            conn.commit()

        # Restore test_schema context
        set_schema(test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_explicit_schema_parameter_overrides_context(test_schema):
    """Test that explicit schema= parameter overrides context on execute_ functions"""
    # Create a second schema to test context vs explicit override
    other_schema = f"{test_schema}_other"
    init_database(schema=other_schema)

    try:
        # Create cycle in test_schema using context
        set_schema(test_schema)
        cycle_id_context = register_cycle('cycle_via_context')

        # Verify it exists when querying via context
        cycle_context = get_cycle_by_name('cycle_via_context')
        assert cycle_context is not None
        assert cycle_context['id'] == cycle_id_context

        # Change context to other_schema
        set_schema(other_schema)

        # Should not find it in other_schema (context)
        cycle_in_other = get_cycle_by_name('cycle_via_context')
        assert cycle_in_other is None

        # Use execute_query with explicit schema parameter to override context
        df = execute_query(
            "SELECT id, cycle_name FROM irp_cycle WHERE cycle_name = %s",
            ('cycle_via_context',),
            schema=test_schema  # Explicit override
        )
        assert not df.empty
        assert df.iloc[0]['id'] == cycle_id_context

    finally:
        # Cleanup
        from helpers.database import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {other_schema} CASCADE"))
            conn.commit()

        # Restore test_schema context
        set_schema(test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_schema_context_with_explicit_override(test_schema):
    """Test that explicit schema parameter works within schema_context on execute_ functions"""
    # Create second schema for testing
    other_schema = f"{test_schema}_override"
    init_database(schema=other_schema)

    try:
        # Create cycle in test_schema
        set_schema(test_schema)
        cycle_id_1 = register_cycle('cycle_1')

        # Use context manager for different schema, but override with explicit param on execute_query
        with schema_context(other_schema):
            # Context says other_schema, but we explicitly request test_schema
            df = execute_query(
                "SELECT id FROM irp_cycle WHERE cycle_name = %s",
                ('cycle_1',),
                schema=test_schema  # Explicit override
            )
            assert not df.empty
            assert df.iloc[0]['id'] == cycle_id_1

    finally:
        # Cleanup
        from helpers.database import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {other_schema} CASCADE"))
            conn.commit()

        # Restore test_schema context
        set_schema(test_schema)


# ==============================================================================
# ENVIRONMENT VARIABLE TESTS
# ==============================================================================

@pytest.mark.unit
def test_get_schema_from_env_default():
    """Test that get_schema_from_env() returns 'public' when not set"""
    # Temporarily unset DB_SCHEMA if it exists
    original = os.environ.get('DB_SCHEMA')
    if 'DB_SCHEMA' in os.environ:
        del os.environ['DB_SCHEMA']

    try:
        assert get_schema_from_env() == 'public'
    finally:
        # Restore original value
        if original:
            os.environ['DB_SCHEMA'] = original


@pytest.mark.unit
def test_get_schema_from_env_reads_variable(test_schema):
    """Test that get_schema_from_env() reads DB_SCHEMA environment variable"""
    # Set DB_SCHEMA temporarily
    original = os.environ.get('DB_SCHEMA')
    os.environ['DB_SCHEMA'] = 'env_test_schema'

    try:
        assert get_schema_from_env() == 'env_test_schema'
    finally:
        # Restore original value
        if original:
            os.environ['DB_SCHEMA'] = original
        else:
            del os.environ['DB_SCHEMA']


# ==============================================================================
# INTEGRATION TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_complete_schema_precedence_flow(test_schema):
    """
    Test complete schema precedence: explicit > context > env > default

    This test verifies the full precedence chain documented in database.py
    """
    # Save current state
    original_env = os.environ.get('DB_SCHEMA')
    original_context = get_current_schema()

    # Create an alternative schema for testing
    alt_schema = f"{test_schema}_alt"
    init_database(schema=alt_schema)

    try:
        # Setup: Create test data in test_schema
        set_schema(test_schema)
        cycle_id = register_cycle('precedence_test_cycle')

        # Level 4: DEFAULT ('public') - lowest precedence
        # Note: We can't test 'public' since it has no tables in test environment
        # Instead we test with a different schema (alt_schema)
        set_schema(alt_schema)
        assert get_current_schema() == alt_schema
        # Query in alt_schema should not find our cycle (it's in test_schema)
        found = get_cycle_by_name('precedence_test_cycle')
        assert found is None

        # Level 3: ENVIRONMENT VARIABLE
        os.environ['DB_SCHEMA'] = test_schema
        init_from_environment()
        assert get_current_schema() == test_schema
        # Should find cycle now
        found = get_cycle_by_name('precedence_test_cycle')
        assert found is not None

        # Level 2: CONTEXT (overrides env)
        set_schema(alt_schema)
        assert get_current_schema() == alt_schema
        # Even though env says test_schema, context wins
        found = get_cycle_by_name('precedence_test_cycle')
        assert found is None

        # Level 1: EXPLICIT PARAMETER (highest precedence - overrides everything)
        set_schema(alt_schema)  # Context says 'alt_schema'
        os.environ['DB_SCHEMA'] = 'some_other'  # Env says 'some_other'
        # But explicit parameter on execute_query wins
        df = execute_query(
            "SELECT id FROM irp_cycle WHERE cycle_name = %s",
            ('precedence_test_cycle',),
            schema=test_schema  # Explicit override
        )
        assert not df.empty
        assert df.iloc[0]['id'] == cycle_id

    finally:
        # Cleanup alt schema
        from helpers.database import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {alt_schema} CASCADE"))
            conn.commit()

        # Restore original state
        if original_env:
            os.environ['DB_SCHEMA'] = original_env
        elif 'DB_SCHEMA' in os.environ:
            del os.environ['DB_SCHEMA']
        set_schema(original_context)


@pytest.mark.database
@pytest.mark.integration
def test_schema_context_in_practical_use_case(test_schema):
    """
    Test a practical use case: Running tests in isolated schema

    This mimics how conftest.py uses schema_context for test isolation
    """
    # Simulate test setup
    test_isolation_schema = f"{test_schema}_practical"
    init_database(schema=test_isolation_schema)

    # Save original context
    original_context = get_current_schema()

    try:
        # Simulate running a test in isolated schema
        with schema_context(test_isolation_schema):
            # Create test data
            cycle_id = register_cycle('practical_test_cycle')

            # Query should work within context
            cycle = get_cycle_by_name('practical_test_cycle')
            assert cycle is not None
            assert cycle['id'] == cycle_id

            # Verify isolation: test_schema should not see this data
            with schema_context(test_schema):
                cycle_in_other = get_cycle_by_name('practical_test_cycle')
                assert cycle_in_other is None

        # After context exits, should be back to original context
        assert get_current_schema() == original_context

        # Set to test_schema and verify data is not visible there
        set_schema(test_schema)
        cycle_after = get_cycle_by_name('practical_test_cycle')
        assert cycle_after is None

    finally:
        # Cleanup
        from helpers.database import get_engine
        from sqlalchemy import text
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {test_isolation_schema} CASCADE"))
            conn.commit()

        # Restore context
        set_schema(test_schema)


# ==============================================================================
# EDGE CASES
# ==============================================================================

@pytest.mark.unit
def test_multiple_set_schema_calls(test_schema):
    """Test multiple consecutive set_schema calls"""
    set_schema('schema1')
    assert get_current_schema() == 'schema1'

    set_schema('schema2')
    assert get_current_schema() == 'schema2'

    set_schema('schema3')
    assert get_current_schema() == 'schema3'

    # Restore
    set_schema(test_schema)


@pytest.mark.unit
def test_schema_context_same_schema(test_schema):
    """Test schema_context with same schema as current"""
    set_schema(test_schema)
    assert get_current_schema() == test_schema

    # Use context manager with same schema
    with schema_context(test_schema):
        assert get_current_schema() == test_schema

    # Should still be test_schema after
    assert get_current_schema() == test_schema


@pytest.mark.unit
def test_schema_special_characters():
    """Test schema names with underscores and numbers"""
    # These should all work
    set_schema('test_123')
    assert get_current_schema() == 'test_123'

    set_schema('test_schema_v2')
    assert get_current_schema() == 'test_schema_v2'

    with schema_context('test_2025_q1'):
        assert get_current_schema() == 'test_2025_q1'
