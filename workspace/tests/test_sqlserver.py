"""
Tests for SQL Server (MSSQL) integration module

This module tests the sqlserver.py helper module which provides MSSQL
database connectivity and operations for the IRP Notebook Framework.

Test Coverage:
- Connection management (success and failure scenarios)
- Query execution with parameters
- Scalar queries
- Command execution (INSERT/UPDATE/DELETE)
- File-based query execution (returns DataFrames)
- Parameter substitution ({{ param_name }} style with context-aware escaping)
- Type conversions (numpy/pandas to native Python)
- Error handling
- Configuration validation
- Database parameter handling
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from helpers import sqlserver
from helpers.sqlserver import (
    SQLServerError,
    SQLServerConfigurationError,
    SQLServerQueryError,
    get_connection_config,
    build_connection_string,
    get_connection,
    test_connection as check_connection,  # Renamed to avoid pytest collection
    execute_query,
    execute_scalar,
    execute_command,
    sql_file_exists,
    execute_query_from_file,
    _convert_param_value,
    _convert_params_to_native_types,
    _substitute_named_parameters,
)

# Mark all tests in this module as SQL Server tests
# These tests require SQL Server container and are excluded from default test runs
# Run with: ./test_sqlserver.sh or pytest -m sqlserver
pytestmark = pytest.mark.sqlserver


# ==============================================================================
# CONNECTION TESTS
# ==============================================================================

def test_get_connection_config_success(mssql_env):
    """Test getting connection configuration for TEST connection"""
    config = get_connection_config('TEST')

    assert config['server'] == 'localhost'
    assert config['user'] == 'sa'
    assert config['port'] == '1433'
    assert 'password' in config
    # database field should not be present (no default database)
    assert 'database' not in config


def test_get_connection_config_missing_vars(monkeypatch):
    """Test error when required environment variables are missing"""
    # Clear all MSSQL environment variables
    monkeypatch.delenv('MSSQL_TEST_SERVER', raising=False)
    monkeypatch.delenv('MSSQL_TEST_DATABASE', raising=False)
    monkeypatch.delenv('MSSQL_TEST_USER', raising=False)
    monkeypatch.delenv('MSSQL_TEST_PASSWORD', raising=False)

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        get_connection_config('TEST')

    assert 'not properly configured' in str(exc_info.value)
    assert 'MSSQL_TEST_SERVER' in str(exc_info.value)


def test_build_connection_string(mssql_env):
    """Test building ODBC connection string without database"""
    conn_str = build_connection_string('TEST')

    assert 'DRIVER={ODBC Driver 18 for SQL Server}' in conn_str
    assert 'SERVER=localhost,1433' in conn_str
    assert 'DATABASE=' not in conn_str  # No default database
    assert 'UID=sa' in conn_str
    assert 'TrustServerCertificate=yes' in conn_str


def test_build_connection_string_with_database(mssql_env):
    """Test building ODBC connection string with database parameter"""
    conn_str = build_connection_string('TEST', database='test_db')

    assert 'DRIVER={ODBC Driver 18 for SQL Server}' in conn_str
    assert 'SERVER=localhost,1433' in conn_str
    assert 'DATABASE=test_db' in conn_str
    assert 'UID=sa' in conn_str
    assert 'TrustServerCertificate=yes' in conn_str


def test_connection_context_manager(mssql_env, wait_for_sqlserver):
    """Test connection context manager opens and closes properly"""
    with get_connection('TEST', database='test_db') as conn:
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_test_connection_success(mssql_env, wait_for_sqlserver):
    """Test successful connection test"""
    result = check_connection('TEST')
    assert result is True


def test_test_connection_failure(monkeypatch):
    """Test connection test with invalid configuration"""
    # Set invalid server
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'invalid-server-that-does-not-exist')

    result = check_connection('TEST')
    assert result is False


# ==============================================================================
# PARAMETER CONVERSION TESTS
# ==============================================================================

def test_convert_param_value_none():
    """Test converting None value"""
    assert _convert_param_value(None) is None


def test_convert_param_value_numpy_int():
    """Test converting numpy integer"""
    value = np.int64(123)
    result = _convert_param_value(value)
    assert result == 123
    assert isinstance(result, int)


def test_convert_param_value_numpy_float():
    """Test converting numpy float"""
    value = np.float64(123.45)
    result = _convert_param_value(value)
    assert result == 123.45
    assert isinstance(result, float)


def test_convert_param_value_pandas_na():
    """Test converting pandas NA to None"""
    result = _convert_param_value(pd.NA)
    assert result is None


def test_convert_param_value_numpy_array():
    """Test converting numpy array to list"""
    value = np.array([1, 2, 3])
    result = _convert_param_value(value)
    assert result == [1, 2, 3]
    assert isinstance(result, list)


def test_convert_param_value_pandas_series():
    """Test converting pandas Series to list"""
    value = pd.Series([1, 2, 3])
    result = _convert_param_value(value)
    assert result == [1, 2, 3]
    assert isinstance(result, list)


def test_convert_params_dict():
    """Test converting dictionary of parameters"""
    params = {
        'int_val': np.int64(123),
        'float_val': np.float64(45.67),
        'none_val': None,
        'str_val': 'hello'
    }

    result = _convert_params_to_native_types(params)

    assert result['int_val'] == 123
    assert isinstance(result['int_val'], int)
    assert result['float_val'] == 45.67
    assert isinstance(result['float_val'], float)
    assert result['none_val'] is None
    assert result['str_val'] == 'hello'


def test_convert_params_tuple():
    """Test converting tuple of parameters"""
    params = (np.int64(123), np.float64(45.67), None, 'hello')

    result = _convert_params_to_native_types(params)

    assert result[0] == 123
    assert isinstance(result[0], int)
    assert result[1] == 45.67
    assert isinstance(result[1], float)
    assert result[2] is None
    assert result[3] == 'hello'


# ==============================================================================
# PARAMETER SUBSTITUTION TESTS
# ==============================================================================

def test_substitute_named_parameters_simple():
    """Test simple named parameter substitution with Template"""
    query = "SELECT * FROM table WHERE id = {{ user_id }}"
    params = {'user_id': 123}

    new_query = _substitute_named_parameters(query, params)

    assert "123" in new_query  # Value substituted directly


def test_substitute_named_parameters_multiple():
    """Test multiple named parameter substitution with Template"""
    query = "SELECT * FROM table WHERE id = {{ user_id }} AND name = {{ user_name }} AND age > {{ min_age }}"
    params = {'user_id': 123, 'user_name': 'John', 'min_age': 21}

    new_query = _substitute_named_parameters(query, params)

    assert "123" in new_query
    assert "'John'" in new_query  # String values are quoted
    assert "21" in new_query


def test_substitute_named_parameters_sql_injection_protection():
    """Test that SQL injection attempts are escaped"""
    query = "SELECT * FROM table WHERE name = {{ name }}"
    params = {'name': "'; DROP TABLE users; --"}

    new_query = _substitute_named_parameters(query, params)

    # Single quotes should be doubled (escaped)
    assert "''; DROP TABLE users; --'" in new_query


def test_substitute_named_parameters_missing_param():
    """Test error when required parameter is missing"""
    query = "SELECT * FROM table WHERE id = {{ user_id }} AND name = {{ user_name }}"
    params = {'user_id': 123}  # Missing user_name

    with pytest.raises(SQLServerQueryError) as exc_info:
        _substitute_named_parameters(query, params)

    assert 'Missing required parameter' in str(exc_info.value)


def test_substitute_named_parameters_no_params():
    """Test query with no parameters"""
    query = "SELECT * FROM table"
    params = None

    new_query = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table"


def test_substitute_named_parameters_empty_dict():
    """Test query with parameters but none in query"""
    query = "SELECT * FROM table"
    params = {'unused': 123}

    new_query = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table"


# ==============================================================================
# QUERY EXECUTION TESTS
# ==============================================================================

def test_execute_query_simple(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test simple SELECT query"""
    df = execute_query("SELECT * FROM test_portfolios", connection='TEST', database='test_db')

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'portfolio_name' in df.columns
    assert 'portfolio_value' in df.columns


def test_execute_query_with_named_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with named parameters"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {{ min_value }}",
        params={'min_value': 1000000},
        connection='TEST', database='test_db'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Portfolio B and D have values > 1M
    assert all(df['portfolio_value'] > 1000000)


def test_execute_query_with_multiple_named_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with multiple named parameters"""
    df = execute_query(
        """
        SELECT p.*, r.risk_type, r.risk_value
        FROM test_portfolios p
        INNER JOIN test_risks r ON p.id = r.portfolio_id
        WHERE p.id = {{ portfolio_id }} AND r.risk_type = {{ risk_type }}
        """,
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST', database='test_db'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio A'
    assert df.iloc[0]['risk_type'] == 'VaR_95'


def test_execute_query_with_numpy_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with numpy parameter types"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {{ min_value }}",
        params={'min_value': np.float64(1000000.0)},
        connection='TEST', database='test_db'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


def test_execute_query_empty_result(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query that returns no results"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {{ min_value }}",
        params={'min_value': 99999999},
        connection='TEST', database='test_db'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_execute_query_connection_error():
    """Test query with invalid connection"""
    with pytest.raises(SQLServerConfigurationError):
        execute_query("SELECT 1", connection='NONEXISTENT')


# ==============================================================================
# SCALAR QUERY TESTS
# ==============================================================================

def test_execute_scalar_count(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test scalar query for COUNT"""
    count = execute_scalar("SELECT COUNT(*) FROM test_portfolios", connection='TEST', database='test_db')

    assert isinstance(count, int)
    assert count == 5


def test_execute_scalar_with_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test scalar query with parameters"""
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios WHERE portfolio_value > {{ min_value }}",
        params={'min_value': 1000000},
        connection='TEST', database='test_db'
    )

    assert isinstance(count, int)
    assert count == 2


def test_execute_scalar_null_result(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test scalar query that returns no results"""
    result = execute_scalar(
        "SELECT portfolio_name FROM test_portfolios WHERE id = 9999",
        connection='TEST', database='test_db'
    )

    assert result is None


# ==============================================================================
# COMMAND EXECUTION TESTS
# ==============================================================================

def test_execute_command_insert(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test INSERT command"""
    rows = execute_command(
        """
        INSERT INTO test_portfolios (portfolio_name, portfolio_value, status)
        VALUES ({{ name }}, {{ value }}, {{ status }})
        """,
        params={'name': 'Test Portfolio F', 'value': 3000000.0, 'status': 'ACTIVE'},
        connection='TEST', database='test_db'
    )

    assert rows == 1

    # Verify insert
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_name = {{ name }}",
        params={'name': 'Test Portfolio F'},
        connection='TEST', database='test_db'
    )
    assert len(df) == 1
    assert df.iloc[0]['portfolio_value'] == 3000000.0


def test_execute_command_update(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test UPDATE command"""
    rows = execute_command(
        "UPDATE test_portfolios SET status = {{ status }} WHERE portfolio_value < {{ min_value }}",
        params={'status': 'LOW_VALUE', 'min_value': 1000000},
        connection='TEST', database='test_db'
    )

    assert rows == 3  # Portfolio A, C, and E have values < 1M

    # Verify update
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE status = 'LOW_VALUE'",
        connection='TEST', database='test_db'
    )
    assert len(df) == 3


def test_execute_command_delete(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test DELETE command"""
    # First insert a record to delete
    execute_command(
        "INSERT INTO test_portfolios (portfolio_name, portfolio_value, status) VALUES ('TO_DELETE', 100, 'TEMP')",
        connection='TEST', database='test_db'
    )

    # Now delete it
    rows = execute_command(
        "DELETE FROM test_portfolios WHERE status = 'TEMP'",
        connection='TEST', database='test_db'
    )

    assert rows == 1

    # Verify deletion
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios WHERE status = 'TEMP'",
        connection='TEST', database='test_db'
    )
    assert count == 0


# ==============================================================================
# FILE-BASED OPERATION TESTS
# ==============================================================================

def test_sql_file_exists():
    """Test checking if SQL files exist"""
    # Test with existing file (relative path)
    assert sql_file_exists('examples/sample_query.sql') == True

    # Test with non-existent file
    assert sql_file_exists('nonexistent/missing.sql') == False

    # Test with absolute path to existing file
    from helpers.constants import WORKSPACE_PATH
    absolute_path = WORKSPACE_PATH / 'sql' / 'examples' / 'sample_query.sql'
    assert sql_file_exists(absolute_path) == True

    # Test with absolute path to non-existent file
    absolute_path = WORKSPACE_PATH / 'sql' / 'missing.sql'
    assert sql_file_exists(absolute_path) == False

    # Test that it returns False for directory (not a file)
    from pathlib import Path
    dir_path = WORKSPACE_PATH / 'sql' / 'examples'
    if dir_path.exists():
        assert sql_file_exists('examples') == False


def test_execute_query_from_file(mssql_env, wait_for_sqlserver, init_sqlserver_db, sample_sql_file):
    """Test executing query from SQL file"""
    dataframes = execute_query_from_file(
        sample_sql_file,
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST', database='test_db'
    )

    assert isinstance(dataframes, list)
    assert len(dataframes) == 1
    df = dataframes[0]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio A'
    assert df.iloc[0]['risk_type'] == 'VaR_95'


def test_execute_query_from_file_relative_path(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test executing query from SQL file using relative path"""
    dataframes = execute_query_from_file(
        'examples/sample_query.sql',
        params={'portfolio_id': 2, 'risk_type': 'VaR_99'},
        connection='TEST', database='test_db'
    )

    assert isinstance(dataframes, list)
    assert len(dataframes) == 1
    df = dataframes[0]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio B'


def test_execute_query_from_file_not_found(mssql_env):
    """Test error when SQL file doesn't exist"""
    with pytest.raises(SQLServerQueryError) as exc_info:
        execute_query_from_file('nonexistent.sql', connection='TEST', database='test_db')

    assert 'not found' in str(exc_info.value)




# ==============================================================================
# ERROR HANDLING TESTS
# ==============================================================================

def test_execute_query_syntax_error(mssql_env, wait_for_sqlserver):
    """Test query with SQL syntax error"""
    with pytest.raises(SQLServerQueryError):
        execute_query("INVALID SQL SYNTAX HERE", connection='TEST', database='test_db')


def test_execute_query_invalid_table(mssql_env, wait_for_sqlserver):
    """Test query referencing non-existent table"""
    with pytest.raises(SQLServerQueryError):
        execute_query("SELECT * FROM nonexistent_table", connection='TEST', database='test_db')


def test_execute_command_constraint_violation(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test command that violates database constraints"""
    with pytest.raises(SQLServerQueryError):
        # Try to insert risk with non-existent portfolio_id
        execute_command(
            "INSERT INTO test_risks (portfolio_id, risk_type, risk_value) VALUES (9999, 'VaR_95', 100.0)",
            connection='TEST', database='test_db'
        )


# ==============================================================================
# ADDITIONAL ERROR HANDLING TESTS (for coverage)
# ==============================================================================

def test_convert_params_with_none():
    """Test _convert_params_to_native_types with None input"""
    result = _convert_params_to_native_types(None)
    assert result is None


def test_execute_scalar_connection_error():
    """Test execute_scalar with invalid connection configuration"""
    # This should trigger SQLServerConfigurationError which is re-raised
    with pytest.raises(SQLServerConfigurationError):
        execute_scalar("SELECT 1", connection='INVALID_CONNECTION_NAME')


def test_execute_command_connection_error():
    """Test execute_command with invalid connection configuration"""
    # This should trigger SQLServerConfigurationError which is re-raised
    with pytest.raises(SQLServerConfigurationError):
        execute_command("SELECT 1", connection='INVALID_CONNECTION_NAME')


def test_execute_query_from_file_nonexistent():
    """Test execute_query_from_file with non-existent file"""
    from helpers.sqlserver import execute_query_from_file

    with pytest.raises(SQLServerQueryError) as exc_info:
        execute_query_from_file('nonexistent_file.sql', connection='TEST', database='test_db')

    assert 'SQL script file not found' in str(exc_info.value)




# ==============================================================================
# DATABASE PARAMETER TESTS
# ==============================================================================

def test_execute_query_with_database_parameter(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that execute_query works with database parameter"""
    df = execute_query(
        "SELECT * FROM test_portfolios",
        connection='TEST',
        database='test_db'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5


def test_execute_query_without_database_parameter_fails(mssql_env, wait_for_sqlserver):
    """Test that queries without database parameter fail when table is not fully qualified"""
    with pytest.raises(SQLServerQueryError):
        # This should fail because no database is specified and table is not fully qualified
        execute_query("SELECT * FROM test_portfolios", connection='TEST')


def test_execute_scalar_with_database_parameter(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that execute_scalar works with database parameter"""
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios",
        connection='TEST',
        database='test_db'
    )

    assert count == 5


def test_execute_command_with_database_parameter(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that execute_command works with database parameter"""
    rows = execute_command(
        "UPDATE test_portfolios SET status = 'TEST' WHERE id = 1",
        connection='TEST',
        database='test_db'
    )

    assert rows == 1

    # Verify the update
    df = execute_query(
        "SELECT status FROM test_portfolios WHERE id = 1",
        connection='TEST',
        database='test_db'
    )
    assert df.iloc[0]['status'] == 'TEST'


def test_execute_query_from_file_with_database_parameter(mssql_env, wait_for_sqlserver, clean_sqlserver_db, sample_sql_file):
    """Test that execute_query_from_file works with database parameter"""
    dataframes = execute_query_from_file(
        sample_sql_file,
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST',
        database='test_db'
    )

    assert isinstance(dataframes, list)
    assert len(dataframes) == 1
    df = dataframes[0]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1




def test_parameterized_database_in_brackets(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that database names in brackets can be parameterized (like USE [{{ db_name }}])"""
    # Note: pd.read_sql doesn't support multi-statement queries (USE + SELECT)
    # So we test the parameterization logic directly with _substitute_named_parameters
    from helpers.sqlserver import _substitute_named_parameters

    query = "USE [{{ db_name }}]"
    params = {'db_name': 'test_db'}

    result = _substitute_named_parameters(query, params)
    assert result == "USE [test_db]"
    assert "'" not in result  # Should NOT be quoted


def test_parameterized_value_in_string_literal(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that parameters inside string literals are substituted without extra quotes"""
    # This pattern is used extensively in the control totals SQL files
    df = execute_query(
        "SELECT 'Modeling_{{ date_val }}_Moodys_{{ cycle_type }}' as table_name",
        params={'date_val': '202501', 'cycle_type': 'Full'},
        connection='TEST',
        database='test_db'
    )

    assert len(df) == 1
    assert df.iloc[0]['table_name'] == 'Modeling_202501_Moodys_Full'


def test_use_statement_with_table_name_parameter(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test that table names can be parameterized (like CombinedData_{{ DATE_VALUE }}_Working)"""
    # This pattern is used in CBHU_Control_Totals_Working_Table.sql
    # Create a test table with date suffix
    execute_command(
        "CREATE TABLE dbo.TestData_{{ date_val }}_Working (id INT, value VARCHAR(50))",
        params={'date_val': '20250115'},
        connection='TEST',
        database='test_db'
    )

    # Insert test data
    execute_command(
        "INSERT INTO dbo.TestData_{{ date_val }}_Working (id, value) VALUES (1, {{ test_value }})",
        params={'date_val': '20250115', 'test_value': 'test'},
        connection='TEST',
        database='test_db'
    )

    # Query the parameterized table
    df = execute_query(
        "SELECT * FROM dbo.TestData_{{ date_val }}_Working WHERE id = {{ id_val }}",
        params={'date_val': '20250115', 'id_val': 1},
        connection='TEST',
        database='test_db'
    )

    assert len(df) == 1
    assert df.iloc[0]['value'] == 'test'

    # Cleanup
    execute_command(
        "DROP TABLE dbo.TestData_{{ date_val }}_Working",
        params={'date_val': '20250115'},
        connection='TEST',
        database='test_db'
    )


def test_connection_with_database_parameter(mssql_env, wait_for_sqlserver):
    """Test that get_connection works with database parameter"""
    with get_connection('TEST', database='test_db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        assert db_name == 'test_db'


def test_connection_without_database_parameter(mssql_env, wait_for_sqlserver):
    """Test that get_connection works without database parameter (no default database)"""
    with get_connection('TEST') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        # When no database is specified, DB_NAME() returns NULL or master
        # This is expected behavior - no default database


# ==============================================================================
# ESCAPE VALUE TESTS (for better coverage)
# ==============================================================================

def test_escape_sql_value_bool_true():
    """Test escaping boolean True value"""
    from helpers.sqlserver import _escape_sql_value
    result = _escape_sql_value(True)
    assert result == '1'


def test_escape_sql_value_bool_false():
    """Test escaping boolean False value"""
    from helpers.sqlserver import _escape_sql_value
    result = _escape_sql_value(False)
    assert result == '0'


def test_escape_sql_value_datetime():
    """Test escaping datetime object (falls through to else clause)"""
    from helpers.sqlserver import _escape_sql_value
    from datetime import datetime

    dt = datetime(2025, 1, 15, 12, 30, 0)
    result = _escape_sql_value(dt)

    # Should be quoted and any quotes escaped
    assert result.startswith("'")
    assert result.endswith("'")
    assert '2025' in result


def test_escape_sql_value_with_quotes_in_object():
    """Test escaping non-string object that contains quotes when converted to string"""
    from helpers.sqlserver import _escape_sql_value

    class CustomObj:
        def __str__(self):
            return "Object's value"

    result = _escape_sql_value(CustomObj())
    # Single quotes should be doubled
    assert "Object''s value" in result


# ==============================================================================
# IDENTIFIER VALIDATION TESTS
# ==============================================================================

def test_substitute_parameters_identifier_validation_invalid_chars():
    """Test that invalid characters in identifiers raise error"""
    from helpers.sqlserver import _substitute_named_parameters

    # Identifier with SQL injection attempt
    query = "SELECT * FROM table_{{ table_suffix }}"
    params = {'table_suffix': "'; DROP TABLE users--"}

    with pytest.raises(ValueError) as exc_info:
        _substitute_named_parameters(query, params)

    assert 'Invalid identifier value' in str(exc_info.value)


def test_substitute_parameters_identifier_in_brackets_valid():
    """Test that valid identifier in brackets works"""
    from helpers.sqlserver import _substitute_named_parameters

    query = "USE [{{ db_name }}]"
    params = {'db_name': 'my_database'}

    result = _substitute_named_parameters(query, params)
    assert result == "USE [my_database]"


def test_substitute_parameters_identifier_as_table_prefix():
    """Test that identifier as table prefix works"""
    from helpers.sqlserver import _substitute_named_parameters

    query = "SELECT * FROM {{ prefix }}_table"
    params = {'prefix': 'test'}

    result = _substitute_named_parameters(query, params)
    assert result == "SELECT * FROM test_table"


def test_substitute_parameters_value_context_escaping():
    """Test that value context properly escapes and quotes"""
    from helpers.sqlserver import _substitute_named_parameters

    query = "SELECT * FROM table WHERE name = {{ name }} AND id = {{ id }}"
    params = {'name': "O'Brien", 'id': 123}

    result = _substitute_named_parameters(query, params)
    assert "'O''Brien'" in result  # Quoted and escaped
    assert "id = 123" in result  # Not quoted (no extra spaces around it)


# ==============================================================================
# DISPLAY UTILITIES TESTS
# ==============================================================================

def test_display_result_sets_empty_list():
    """Test display_result_sets with empty list"""
    from helpers.sqlserver import display_result_sets
    import io
    import sys

    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        display_result_sets([])
        output = captured_output.getvalue()
        assert "No result sets to display" in output
    finally:
        sys.stdout = sys.__stdout__


def test_display_result_sets_single_dataframe(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test display_result_sets with single DataFrame"""
    from helpers.sqlserver import display_result_sets
    import io
    import sys

    # Get a dataframe
    dataframes = execute_query_from_file(
        'examples/sample_query.sql',
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST',
        database='test_db'
    )

    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        display_result_sets(dataframes)
        output = captured_output.getvalue()
        assert "QUERY RESULTS:" in output
        assert "Result Set 1 of 1" in output
        assert "Rows:" in output
    finally:
        sys.stdout = sys.__stdout__


def test_display_result_sets_empty_dataframe():
    """Test display_result_sets with empty DataFrame"""
    from helpers.sqlserver import display_result_sets
    import io
    import sys

    # Create empty DataFrame with columns
    empty_df = pd.DataFrame(columns=['id', 'name', 'value'])

    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        display_result_sets([empty_df])
        output = captured_output.getvalue()
        assert "Empty DataFrame" in output
        assert "Columns:" in output
    finally:
        sys.stdout = sys.__stdout__


def test_display_result_sets_large_dataframe(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test display_result_sets with DataFrame larger than max_rows"""
    from helpers.sqlserver import display_result_sets
    import io
    import sys

    # Get all risks (10 rows)
    df = execute_query(
        "SELECT * FROM test_risks",
        connection='TEST',
        database='test_db'
    )

    # Capture stdout
    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        # Display with max_rows=5
        display_result_sets([df], max_rows=5)
        output = captured_output.getvalue()
        assert "more rows not shown" in output
    finally:
        sys.stdout = sys.__stdout__


# ==============================================================================
# INITIALIZE DATABASE TESTS
# ==============================================================================

def test_initialize_database_success(mssql_env, wait_for_sqlserver, temp_sql_file):
    """Test initialize_database with valid SQL script"""
    from helpers.sqlserver import initialize_database
    import pyodbc

    # Create a simple database initialization script
    script_content = """
    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'test_init_db')
    BEGIN
        CREATE DATABASE test_init_db
    END
    GO
    USE test_init_db
    GO
    CREATE TABLE test_table (id INT, name VARCHAR(50))
    GO
    """

    script_path = temp_sql_file(script_content)

    # Should succeed
    result = initialize_database(script_path, connection='TEST')
    assert result is True

    # Verify database was created
    with get_connection('TEST', database='test_init_db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        assert db_name == 'test_init_db'
        cursor.close()
    # Connection is now closed after exiting the with block

    # Add a small delay to ensure connection is fully released
    import time
    time.sleep(0.5)

    # Cleanup - need to use raw connection with autocommit for DROP DATABASE
    config = get_connection_config('TEST')
    master_conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE=master;"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"TrustServerCertificate={config['trust_cert']};"
    )
    conn = pyodbc.connect(master_conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Kill any active connections before dropping database
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.databases WHERE name = 'test_init_db')
            BEGIN
                ALTER DATABASE test_init_db SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE test_init_db;
            END
        """)
    finally:
        cursor.close()
        conn.close()


def test_initialize_database_with_existing_objects(mssql_env, wait_for_sqlserver, temp_sql_file):
    """Test initialize_database handles existing objects gracefully"""
    from helpers.sqlserver import initialize_database
    import pyodbc
    import io
    import sys

    # Create database first using raw connection with autocommit
    config = get_connection_config('TEST')
    master_conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE=master;"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"TrustServerCertificate={config['trust_cert']};"
    )
    conn = pyodbc.connect(master_conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'test_init_db2') CREATE DATABASE test_init_db2")
    cursor.close()
    conn.close()

    # Script that tries to create existing database
    script_content = """
    CREATE DATABASE test_init_db2
    GO
    """

    script_path = temp_sql_file(script_content)

    # Capture stdout to check warning message
    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        # Should succeed but print warning
        result = initialize_database(script_path, connection='TEST')
        assert result is True
    finally:
        sys.stdout = sys.__stdout__

    # Add a small delay to ensure connection is fully released
    import time
    time.sleep(0.5)

    # Cleanup - use raw connection with autocommit for DROP DATABASE
    conn = pyodbc.connect(master_conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Kill any active connections before dropping database
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.databases WHERE name = 'test_init_db2')
            BEGIN
                ALTER DATABASE test_init_db2 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE test_init_db2;
            END
        """)
    finally:
        cursor.close()
        conn.close()


def test_initialize_database_file_not_found():
    """Test initialize_database with non-existent file"""
    from helpers.sqlserver import initialize_database, SQLServerError

    with pytest.raises(SQLServerError) as exc_info:
        initialize_database('nonexistent_file.sql', connection='TEST')

    assert 'Failed to initialize database' in str(exc_info.value)


# ==============================================================================
# AUTHENTICATION TYPE TESTS
# ==============================================================================

def test_get_connection_config_sql_auth_default(monkeypatch):
    """Test that SQL authentication is the default when AUTH_TYPE is not set"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_USER', 'testuser')
    monkeypatch.setenv('MSSQL_TEST_PASSWORD', 'testpass')
    monkeypatch.delenv('MSSQL_TEST_AUTH_TYPE', raising=False)

    config = get_connection_config('TEST')

    assert config['auth_type'] == 'SQL'
    assert config['user'] == 'testuser'
    assert config['password'] == 'testpass'


def test_get_connection_config_sql_auth_explicit(monkeypatch):
    """Test explicit SQL Server authentication configuration"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'SQL')
    monkeypatch.setenv('MSSQL_TEST_USER', 'testuser')
    monkeypatch.setenv('MSSQL_TEST_PASSWORD', 'testpass')

    config = get_connection_config('TEST')

    assert config['auth_type'] == 'SQL'
    assert config['user'] == 'testuser'
    assert config['password'] == 'testpass'


def test_get_connection_config_windows_auth(monkeypatch):
    """Test Windows Authentication configuration"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'WINDOWS')
    monkeypatch.delenv('MSSQL_TEST_USER', raising=False)
    monkeypatch.delenv('MSSQL_TEST_PASSWORD', raising=False)

    config = get_connection_config('TEST')

    assert config['auth_type'] == 'WINDOWS'
    assert 'user' not in config
    assert 'password' not in config


def test_get_connection_config_windows_auth_lowercase(monkeypatch):
    """Test Windows Authentication configuration with lowercase auth_type"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'windows')

    config = get_connection_config('TEST')

    assert config['auth_type'] == 'WINDOWS'


def test_get_connection_config_invalid_auth_type(monkeypatch):
    """Test error when invalid authentication type is specified"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'INVALID')

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        get_connection_config('TEST')

    assert 'Invalid authentication type' in str(exc_info.value)
    assert 'INVALID' in str(exc_info.value)


def test_get_connection_config_windows_auth_missing_server(monkeypatch):
    """Test error when Windows auth is used but server is missing"""
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'WINDOWS')
    monkeypatch.delenv('MSSQL_TEST_SERVER', raising=False)

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        get_connection_config('TEST')

    assert 'WINDOWS' in str(exc_info.value)
    assert 'SERVER' in str(exc_info.value)


def test_build_connection_string_sql_auth(monkeypatch):
    """Test connection string for SQL authentication"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'SQL')
    monkeypatch.setenv('MSSQL_TEST_USER', 'testuser')
    monkeypatch.setenv('MSSQL_TEST_PASSWORD', 'testpass')

    conn_str = build_connection_string('TEST')

    assert 'UID=testuser' in conn_str
    assert 'PWD=testpass' in conn_str
    assert 'Trusted_Connection' not in conn_str


def test_build_connection_string_windows_auth(monkeypatch):
    """Test connection string for Windows authentication"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'WINDOWS')

    conn_str = build_connection_string('TEST')

    assert 'Trusted_Connection=yes' in conn_str
    assert 'UID=' not in conn_str
    assert 'PWD=' not in conn_str


def test_build_connection_string_windows_auth_with_database(monkeypatch):
    """Test Windows auth connection string with database"""
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'testserver')
    monkeypatch.setenv('MSSQL_TEST_AUTH_TYPE', 'WINDOWS')

    conn_str = build_connection_string('TEST', database='test_db')

    assert 'Trusted_Connection=yes' in conn_str
    assert 'DATABASE=test_db' in conn_str


# ==============================================================================
# KERBEROS FUNCTION TESTS
# ==============================================================================

def test_check_kerberos_status_disabled(monkeypatch):
    """Test Kerberos status when disabled"""
    from helpers.sqlserver import check_kerberos_status

    monkeypatch.setenv('KERBEROS_ENABLED', 'false')

    status = check_kerberos_status()

    assert status['enabled'] == False
    assert status['has_ticket'] == False
    assert 'not enabled' in status['error']


def test_check_kerberos_status_not_set(monkeypatch):
    """Test Kerberos status when env var not set"""
    from helpers.sqlserver import check_kerberos_status

    monkeypatch.delenv('KERBEROS_ENABLED', raising=False)

    status = check_kerberos_status()

    assert status['enabled'] == False
    assert 'not enabled' in status['error']


def test_init_kerberos_missing_keytab(monkeypatch):
    """Test init_kerberos error when keytab not specified"""
    from helpers.sqlserver import init_kerberos

    monkeypatch.delenv('KRB5_KEYTAB', raising=False)
    monkeypatch.setenv('KRB5_PRINCIPAL', 'user@REALM')

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        init_kerberos()

    assert 'keytab path not specified' in str(exc_info.value)


def test_init_kerberos_missing_principal(monkeypatch, tmp_path):
    """Test init_kerberos error when principal not specified"""
    from helpers.sqlserver import init_kerberos

    # Create a temp keytab file
    keytab_file = tmp_path / "test.keytab"
    keytab_file.write_text("dummy")

    monkeypatch.setenv('KRB5_KEYTAB', str(keytab_file))
    monkeypatch.delenv('KRB5_PRINCIPAL', raising=False)

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        init_kerberos()

    assert 'principal not specified' in str(exc_info.value)


def test_init_kerberos_keytab_not_found(monkeypatch):
    """Test init_kerberos error when keytab file doesn't exist"""
    from helpers.sqlserver import init_kerberos

    monkeypatch.setenv('KRB5_KEYTAB', '/nonexistent/path/test.keytab')
    monkeypatch.setenv('KRB5_PRINCIPAL', 'user@REALM')

    with pytest.raises(SQLServerConfigurationError) as exc_info:
        init_kerberos()

    assert 'keytab file not found' in str(exc_info.value)


def test_init_kerberos_explicit_params(monkeypatch, tmp_path):
    """Test init_kerberos with explicit parameters (kinit not available)"""
    from helpers.sqlserver import init_kerberos

    # Create a temp keytab file
    keytab_file = tmp_path / "test.keytab"
    keytab_file.write_text("dummy")

    # In CI/test environment, kinit is not available, so this should raise
    with pytest.raises(SQLServerConfigurationError) as exc_info:
        init_kerberos(str(keytab_file), 'user@REALM')

    assert 'kinit command not found' in str(exc_info.value)


# ==============================================================================
# CONNECTION ERROR PATH TESTS
# ==============================================================================

def test_get_connection_closes_on_error(mssql_env, wait_for_sqlserver):
    """Test that get_connection properly closes connection on error"""
    # This tests the finally block in get_connection
    try:
        with get_connection('TEST', database='test_db') as conn:
            cursor = conn.cursor()
            # Cause an error after connection is established
            cursor.execute("INVALID SQL SYNTAX")
    except Exception:
        # Error is expected
        pass
    # If we get here without hanging, the connection was closed properly