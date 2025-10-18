"""
Test suite for database operations (pytest version)

This test file demonstrates and validates database functionality including:
- bulk_insert with JSONB support
- Error handling and transaction rollback
- Performance comparisons
- Numpy type conversion for psycopg2 compatibility

All tests run in the 'test_database' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_database.py
    pytest workspace/tests/test_database.py -v
    pytest workspace/tests/test_database.py --preserve-schema
"""

import time
from datetime import datetime
import pytest

from helpers.database import (
    bulk_insert,
    execute_query,
    execute_insert,
    execute_command,
    DatabaseError,
    _convert_params_to_native_types
)


# ============================================================================
# Tests
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_basic_bulk_insert(test_schema):
    """Test basic bulk insert without JSONB fields"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    params_list = [
        ('test_cycle_1', 'ACTIVE'),
        ('test_cycle_2', 'ACTIVE'),
        ('test_cycle_3', 'ARCHIVED'),
        ('test_cycle_4', 'ACTIVE'),
        ('test_cycle_5', 'ACTIVE'),
    ]

    # Perform bulk insert
    start_time = time.time()
    ids = bulk_insert(query, params_list, schema=test_schema)
    elapsed = time.time() - start_time

    # Assertions
    assert len(ids) == len(params_list), f"Expected {len(params_list)} IDs, got {len(ids)}"
    assert all(isinstance(id, int) for id in ids), "All IDs should be integers"
    assert elapsed < 1.0, f"Bulk insert took too long: {elapsed:.4f}s"

    # Verify inserts
    df = execute_query(
        "SELECT * FROM irp_cycle WHERE cycle_name LIKE 'test_cycle_%' ORDER BY id",
        schema=test_schema
    )
    assert len(df) == len(params_list), f"Expected {len(params_list)} records, found {len(df)}"

    # Verify specific data
    assert df.iloc[0]['cycle_name'] == 'test_cycle_1'
    assert df.iloc[2]['status'] == 'ARCHIVED'


@pytest.mark.database
@pytest.mark.unit
def test_error_handling_rollback(test_schema):
    """Test error handling and transaction rollback on duplicate key"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    # Create a duplicate entry scenario
    params_list = [
        ('rollback_1', 'ACTIVE'),
        ('rollback_2', 'ACTIVE'),
        ('rollback_1', 'ACTIVE'),  # Duplicate - should fail
    ]

    # Count before insert
    before_count = execute_query(
        "SELECT COUNT(*) as count FROM irp_cycle WHERE cycle_name LIKE 'rollback_%'",
        schema=test_schema
    ).iloc[0]['count']

    # Attempt insert - should fail
    with pytest.raises(DatabaseError) as exc_info:
        bulk_insert(query, params_list, schema=test_schema)

    # Verify error message contains expected info
    assert "duplicate" in str(exc_info.value).lower() or "unique" in str(exc_info.value).lower()

    # Verify rollback - no records should be inserted
    after_count = execute_query(
        "SELECT COUNT(*) as count FROM irp_cycle WHERE cycle_name LIKE 'rollback_%'",
        schema=test_schema
    ).iloc[0]['count']

    assert after_count == before_count, \
        f"Transaction not rolled back - {after_count - before_count} records inserted"


@pytest.mark.database
@pytest.mark.integration
def test_configuration_jsonb_insert(test_schema):
    """Test bulk insert into irp_configuration table with JSONB config_data"""

    # Setup: Create cycle hierarchy
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('config_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'config_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'config_step'),
        schema=test_schema
    )

    # Bulk insert configurations with JSONB
    query = """
        INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, file_last_updated_ts)
        VALUES (%s, %s, %s, %s)
    """

    params_list = [
        (cycle_id, 'file-A.xlsx', {
            'portfolio': 'Portfolio_A',
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'parameters': {'risk_level': 'high', 'threshold': 0.95}
        }, datetime.now()),
        (cycle_id, 'file-B.xlsx', {
            'portfolio': 'Portfolio_B',
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'parameters': {'risk_level': 'medium', 'threshold': 0.85}
        }, datetime.now()),
        (cycle_id, 'file-C.xlsx', {
            'portfolio': 'Portfolio_C',
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'parameters': {'risk_level': 'low', 'threshold': 0.75}
        }, datetime.now()),
    ]

    # Perform bulk insert with JSONB column
    start_time = time.time()
    ids = bulk_insert(query, params_list, jsonb_columns=[2], schema=test_schema)
    elapsed = time.time() - start_time

    # Assertions
    assert len(ids) == len(params_list), f"Expected {len(params_list)} IDs, got {len(ids)}"
    assert elapsed < 1.0, f"Bulk insert took too long: {elapsed:.4f}s"

    # Verify inserts
    df = execute_query(
        "SELECT * FROM irp_configuration WHERE cycle_id = %s ORDER BY id",
        (cycle_id,),
        schema=test_schema
    )
    assert len(df) == len(params_list), f"Expected {len(params_list)} configurations, found {len(df)}"

    # Verify JSONB data integrity
    config_a = df[df['configuration_file_name'] == 'file-A.xlsx'].iloc[0]
    assert config_a['configuration_data']['portfolio'] == 'Portfolio_A'
    assert config_a['configuration_data']['parameters']['risk_level'] == 'high'
    assert config_a['configuration_data']['parameters']['threshold'] == 0.95

    config_b = df[df['configuration_file_name'] == 'file-B.xlsx'].iloc[0]
    assert config_b['configuration_data']['parameters']['threshold'] == 0.85


@pytest.mark.database
@pytest.mark.unit
def test_bulk_insert_returns_correct_ids(test_schema):
    """Test that bulk_insert returns IDs in insertion order"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    params_list = [
        ('ordered_1', 'ACTIVE'),
        ('ordered_2', 'ACTIVE'),
        ('ordered_3', 'ACTIVE'),
    ]

    ids = bulk_insert(query, params_list, schema=test_schema)

    # Verify IDs are sequential and in order
    assert ids[0] < ids[1] < ids[2], "IDs should be in ascending order"

    # Verify each ID corresponds to correct record
    for idx, id in enumerate(ids):
        df = execute_query(
            "SELECT cycle_name FROM irp_cycle WHERE id = %s",
            (id,),
            schema=test_schema
        )
        expected_name = params_list[idx][0]
        assert df.iloc[0]['cycle_name'] == expected_name, \
            f"ID {id} should correspond to {expected_name}"


@pytest.mark.database
@pytest.mark.unit
def test_empty_bulk_insert(test_schema):
    """Test bulk_insert behavior with empty params list"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """

    params_list = []

    ids = bulk_insert(query, params_list, schema=test_schema)

    # Should return empty list
    assert ids == [], "Empty params should return empty list"


@pytest.mark.database
@pytest.mark.integration
def test_multiple_jsonb_columns(test_schema):
    """Test bulk insert with multiple JSONB columns"""

    # Setup cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('multi_jsonb_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'multi_jsonb_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'multi_jsonb_step'),
        schema=test_schema
    )

    # Insert configuration with multiple JSONB columns
    # Note: configuration_data is JSONB at index 2
    query = """
        INSERT INTO irp_configuration
        (cycle_id, configuration_file_name, configuration_data, file_last_updated_ts)
        VALUES (%s, %s, %s, %s)
    """

    config_data = {
        'portfolio': 'Portfolio_X',
        'nested': {
            'level1': {
                'level2': {'value': 42}
            }
        }
    }

    params_list = [
        (cycle_id, 'multi_jsonb.xlsx', config_data, datetime.now())
    ]

    ids = bulk_insert(query, params_list, jsonb_columns=[2], schema=test_schema)

    # Verify nested JSONB access
    df = execute_query(
        "SELECT configuration_data FROM irp_configuration WHERE id = %s",
        (ids[0],),
        schema=test_schema
    )

    retrieved_data = df.iloc[0]['configuration_data']
    assert retrieved_data['nested']['level1']['level2']['value'] == 42, \
        "Nested JSONB data should be preserved"


# ============================================================================
# Numpy Type Conversion Tests
# ============================================================================

@pytest.mark.unit
def test_convert_params_to_native_types_with_numpy_int64():
    """Test conversion of numpy.int64 to Python int"""
    import numpy as np

    params = (np.int64(42), np.int64(100), np.int64(-5))
    result = _convert_params_to_native_types(params)

    # Verify conversion
    assert result == (42, 100, -5)
    assert all(isinstance(val, int) for val in result)
    assert all(not isinstance(val, np.integer) for val in result)


@pytest.mark.unit
def test_convert_params_to_native_types_with_numpy_float64():
    """Test conversion of numpy.float64 to Python float"""
    import numpy as np

    params = (np.float64(3.14), np.float64(2.718), np.float64(0.0))
    result = _convert_params_to_native_types(params)

    # Verify conversion
    assert result == (3.14, 2.718, 0.0)
    assert all(isinstance(val, float) for val in result)
    assert all(not isinstance(val, np.floating) for val in result)


@pytest.mark.unit
def test_convert_params_to_native_types_with_numpy_bool():
    """Test conversion of numpy.bool_ to Python bool"""
    import numpy as np

    params = (np.bool_(True), np.bool_(False), np.bool_(True))
    result = _convert_params_to_native_types(params)

    # Verify conversion
    assert result == (True, False, True)
    assert all(isinstance(val, bool) for val in result)
    assert all(not isinstance(val, np.bool_) for val in result)


@pytest.mark.unit
def test_convert_params_to_native_types_with_numpy_str():
    """Test conversion of numpy.str_ to Python str"""
    import numpy as np

    params = (np.str_('hello'), np.str_('world'), np.str_('test'))
    result = _convert_params_to_native_types(params)

    # Verify conversion
    assert result == ('hello', 'world', 'test')
    assert all(isinstance(val, str) for val in result)


@pytest.mark.unit
def test_convert_params_to_native_types_with_mixed_types():
    """Test conversion with mixed numpy and Python types"""
    import numpy as np

    params = (
        np.int64(42),
        'native_string',
        np.float64(3.14),
        None,
        123,  # native int
        True,  # native bool
        np.bool_(False)
    )
    result = _convert_params_to_native_types(params)

    # Verify conversion - numpy types converted, Python types unchanged
    assert result[0] == 42 and isinstance(result[0], int)
    assert result[1] == 'native_string' and isinstance(result[1], str)
    assert result[2] == 3.14 and isinstance(result[2], float)
    assert result[3] is None
    assert result[4] == 123 and isinstance(result[4], int)
    assert result[5] is True and isinstance(result[5], bool)
    assert result[6] is False and isinstance(result[6], bool)


@pytest.mark.unit
def test_convert_params_to_native_types_with_empty_tuple():
    """Test conversion with empty tuple"""
    params = ()
    result = _convert_params_to_native_types(params)

    assert result == ()


@pytest.mark.unit
def test_convert_params_to_native_types_with_none():
    """Test conversion with None params"""
    result = _convert_params_to_native_types(None)

    assert result is None


@pytest.mark.unit
def test_convert_params_to_native_types_preserves_complex_types():
    """Test that complex Python types (dict, list) pass through unchanged"""
    params = (
        {'key': 'value'},
        [1, 2, 3],
        {'nested': {'data': 42}},
    )
    result = _convert_params_to_native_types(params)

    # Should be unchanged
    assert result == params
    assert isinstance(result[0], dict)
    assert isinstance(result[1], list)


@pytest.mark.unit
def test_convert_params_to_native_types_with_various_numpy_int_types():
    """Test conversion of various numpy integer types"""
    import numpy as np

    params = (
        np.int8(8),
        np.int16(16),
        np.int32(32),
        np.int64(64),
    )
    result = _convert_params_to_native_types(params)

    # All should be converted to Python int
    assert result == (8, 16, 32, 64)
    assert all(type(val) == int for val in result)


@pytest.mark.unit
def test_convert_params_to_native_types_with_various_numpy_float_types():
    """Test conversion of various numpy float types"""
    import numpy as np

    params = (
        np.float32(1.5),
        np.float64(2.5),
    )
    result = _convert_params_to_native_types(params)

    # All should be converted to Python float
    assert len(result) == 2
    assert all(type(val) == float for val in result)
    assert abs(result[0] - 1.5) < 0.0001
    assert abs(result[1] - 2.5) < 0.0001


@pytest.mark.database
@pytest.mark.integration
def test_numpy_int64_in_real_database_operation(test_schema):
    """
    Integration test: Verify numpy.int64 values work in real database operations.

    This simulates the common scenario where:
    1. execute_query() returns DataFrame with numpy types
    2. Those values are passed to execute_command() or execute_insert()
    3. The conversion happens automatically
    """
    import numpy as np

    # Create a cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('numpy_test_cycle', 'ACTIVE'),
        schema=test_schema
    )

    # Query it back - DataFrame returns numpy.int64
    df = execute_query(
        "SELECT id FROM irp_cycle WHERE cycle_name = %s",
        ('numpy_test_cycle',),
        schema=test_schema
    )

    # This is numpy.int64, not Python int
    retrieved_id = df.iloc[0]['id']
    assert isinstance(retrieved_id, (np.integer, np.int64))

    # Now use that numpy.int64 value in an UPDATE command
    # This should work without explicit conversion (handled internally)
    rows = execute_command(
        "UPDATE irp_cycle SET status = %s WHERE id = %s",
        ('ARCHIVED', retrieved_id),  # retrieved_id is numpy.int64
        schema=test_schema
    )

    # Verify it worked
    assert rows == 1

    # Verify the update
    df_check = execute_query(
        "SELECT status FROM irp_cycle WHERE id = %s",
        (cycle_id,),
        schema=test_schema
    )
    assert df_check.iloc[0]['status'] == 'ARCHIVED'
