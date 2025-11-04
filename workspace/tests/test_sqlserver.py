"""
Tests for SQL Server (MSSQL) integration module

This module tests the sqlserver.py helper module which provides MSSQL
database connectivity and operations for the IRP Notebook Framework.

Test Coverage:
- Connection management (success and failure scenarios)
- Query execution with parameters
- Scalar queries
- Command execution (INSERT/UPDATE/DELETE)
- File-based query execution
- File-based script execution
- Parameter substitution ({param_name} style)
- Type conversions (numpy/pandas to native Python)
- Error handling
- Configuration validation
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from helpers import sqlserver
from helpers.sqlserver import (
    SQLServerError,
    SQLServerConnectionError,
    SQLServerConfigurationError,
    SQLServerQueryError,
    get_connection_config,
    build_connection_string,
    get_connection,
    test_connection,
    execute_query,
    execute_scalar,
    execute_command,
    execute_query_from_file,
    execute_script_file,
    _convert_param_value,
    _convert_params_to_native_types,
    _substitute_named_parameters,
)


# ==============================================================================
# CONNECTION TESTS
# ==============================================================================

def test_get_connection_config_success(mssql_env):
    """Test getting connection configuration for TEST connection"""
    config = get_connection_config('TEST')

    assert config['server'] == 'localhost'
    assert config['database'] == 'test_db'
    assert config['user'] == 'sa'
    assert config['port'] == '1433'
    assert 'password' in config


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
    """Test building ODBC connection string"""
    conn_str = build_connection_string('TEST')

    assert 'DRIVER={ODBC Driver 18 for SQL Server}' in conn_str
    assert 'SERVER=localhost,1433' in conn_str
    assert 'DATABASE=test_db' in conn_str
    assert 'UID=sa' in conn_str
    assert 'TrustServerCertificate=yes' in conn_str


def test_connection_context_manager(mssql_env, wait_for_sqlserver):
    """Test connection context manager opens and closes properly"""
    with get_connection('TEST') as conn:
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_test_connection_success(mssql_env, wait_for_sqlserver):
    """Test successful connection test"""
    result = test_connection('TEST')
    assert result is True


def test_test_connection_failure(monkeypatch):
    """Test connection test with invalid configuration"""
    # Set invalid server
    monkeypatch.setenv('MSSQL_TEST_SERVER', 'invalid-server-that-does-not-exist')

    result = test_connection('TEST')
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
    """Test simple named parameter substitution"""
    query = "SELECT * FROM table WHERE id = {user_id}"
    params = {'user_id': 123}

    new_query, new_params = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table WHERE id = ?"
    assert new_params == (123,)


def test_substitute_named_parameters_multiple():
    """Test multiple named parameter substitution"""
    query = "SELECT * FROM table WHERE id = {user_id} AND name = {user_name} AND age > {min_age}"
    params = {'user_id': 123, 'user_name': 'John', 'min_age': 21}

    new_query, new_params = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table WHERE id = ? AND name = ? AND age > ?"
    assert new_params == (123, 'John', 21)


def test_substitute_named_parameters_missing_param():
    """Test error when required parameter is missing"""
    query = "SELECT * FROM table WHERE id = {user_id} AND name = {user_name}"
    params = {'user_id': 123}  # Missing user_name

    with pytest.raises(SQLServerQueryError) as exc_info:
        _substitute_named_parameters(query, params)

    assert 'Missing required parameters' in str(exc_info.value)
    assert 'user_name' in str(exc_info.value)


def test_substitute_named_parameters_no_params():
    """Test query with no parameters"""
    query = "SELECT * FROM table"
    params = None

    new_query, new_params = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table"
    assert new_params is None


def test_substitute_named_parameters_empty_dict():
    """Test query with parameters but none in query"""
    query = "SELECT * FROM table"
    params = {'unused': 123}

    new_query, new_params = _substitute_named_parameters(query, params)

    assert new_query == "SELECT * FROM table"
    assert new_params is None


# ==============================================================================
# QUERY EXECUTION TESTS
# ==============================================================================

def test_execute_query_simple(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test simple SELECT query"""
    df = execute_query("SELECT * FROM test_portfolios", connection='TEST')

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'portfolio_name' in df.columns
    assert 'portfolio_value' in df.columns


def test_execute_query_with_positional_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with positional parameters"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE id = ?",
        params=(1,),
        connection='TEST'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio A'


def test_execute_query_with_named_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with named parameters"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {min_value}",
        params={'min_value': 1000000},
        connection='TEST'
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
        WHERE p.id = {portfolio_id} AND r.risk_type = {risk_type}
        """,
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio A'
    assert df.iloc[0]['risk_type'] == 'VaR_95'


def test_execute_query_with_numpy_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query with numpy parameter types"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {min_value}",
        params={'min_value': np.float64(1000000.0)},
        connection='TEST'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


def test_execute_query_empty_result(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test query that returns no results"""
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_value > {min_value}",
        params={'min_value': 99999999},
        connection='TEST'
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
    count = execute_scalar("SELECT COUNT(*) FROM test_portfolios", connection='TEST')

    assert isinstance(count, int)
    assert count == 5


def test_execute_scalar_with_params(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test scalar query with parameters"""
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios WHERE portfolio_value > {min_value}",
        params={'min_value': 1000000},
        connection='TEST'
    )

    assert isinstance(count, int)
    assert count == 2


def test_execute_scalar_null_result(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test scalar query that returns no results"""
    result = execute_scalar(
        "SELECT portfolio_name FROM test_portfolios WHERE id = 9999",
        connection='TEST'
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
        VALUES ({name}, {value}, {status})
        """,
        params={'name': 'Test Portfolio F', 'value': 3000000.0, 'status': 'ACTIVE'},
        connection='TEST'
    )

    assert rows == 1

    # Verify insert
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE portfolio_name = {name}",
        params={'name': 'Test Portfolio F'},
        connection='TEST'
    )
    assert len(df) == 1
    assert df.iloc[0]['portfolio_value'] == 3000000.0


def test_execute_command_update(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test UPDATE command"""
    rows = execute_command(
        "UPDATE test_portfolios SET status = {status} WHERE portfolio_value < {min_value}",
        params={'status': 'LOW_VALUE', 'min_value': 1000000},
        connection='TEST'
    )

    assert rows == 3  # Portfolio A, C, and E have values < 1M

    # Verify update
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE status = 'LOW_VALUE'",
        connection='TEST'
    )
    assert len(df) == 3


def test_execute_command_delete(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test DELETE command"""
    # First insert a record to delete
    execute_command(
        "INSERT INTO test_portfolios (portfolio_name, portfolio_value, status) VALUES ('TO_DELETE', 100, 'TEMP')",
        connection='TEST'
    )

    # Now delete it
    rows = execute_command(
        "DELETE FROM test_portfolios WHERE status = 'TEMP'",
        connection='TEST'
    )

    assert rows == 1

    # Verify deletion
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios WHERE status = 'TEMP'",
        connection='TEST'
    )
    assert count == 0


# ==============================================================================
# FILE-BASED OPERATION TESTS
# ==============================================================================

def test_execute_query_from_file(mssql_env, wait_for_sqlserver, init_sqlserver_db, sample_sql_file):
    """Test executing query from SQL file"""
    df = execute_query_from_file(
        sample_sql_file,
        params={'portfolio_id': 1, 'risk_type': 'VaR_95'},
        connection='TEST'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio A'
    assert df.iloc[0]['risk_type'] == 'VaR_95'


def test_execute_query_from_file_relative_path(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test executing query from SQL file using relative path"""
    df = execute_query_from_file(
        'examples/sample_query.sql',
        params={'portfolio_id': 2, 'risk_type': 'VaR_99'},
        connection='TEST'
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['portfolio_name'] == 'Test Portfolio B'


def test_execute_query_from_file_not_found(mssql_env):
    """Test error when SQL file doesn't exist"""
    with pytest.raises(SQLServerQueryError) as exc_info:
        execute_query_from_file('nonexistent.sql', connection='TEST')

    assert 'not found' in str(exc_info.value)


def test_execute_script_file_single_statement(mssql_env, wait_for_sqlserver, init_sqlserver_db, temp_sql_file):
    """Test executing single-statement script from file"""
    # Create a simple UPDATE script
    script_content = """
    UPDATE test_portfolios
    SET status = {new_status}
    WHERE portfolio_value < {min_value}
    """
    script_path = temp_sql_file(script_content)

    rows = execute_script_file(
        script_path,
        params={'new_status': 'UPDATED', 'min_value': 1000000},
        connection='TEST'
    )

    assert rows == 3

    # Verify update
    count = execute_scalar(
        "SELECT COUNT(*) FROM test_portfolios WHERE status = 'UPDATED'",
        connection='TEST'
    )
    assert count == 3


def test_execute_script_file_multi_statement(mssql_env, wait_for_sqlserver, init_sqlserver_db, temp_sql_file):
    """Test executing multi-statement script from file"""
    script_content = """
    UPDATE test_portfolios SET status = 'STAGE1' WHERE id = 1;
    UPDATE test_portfolios SET status = 'STAGE2' WHERE id = 2;
    """
    script_path = temp_sql_file(script_content)

    rows = execute_script_file(script_path, connection='TEST')

    # Verify both updates
    df = execute_query(
        "SELECT * FROM test_portfolios WHERE id IN (1, 2)",
        connection='TEST'
    )
    assert df[df['id'] == 1]['status'].iloc[0] == 'STAGE1'
    assert df[df['id'] == 2]['status'].iloc[0] == 'STAGE2'


# ==============================================================================
# ERROR HANDLING TESTS
# ==============================================================================

def test_execute_query_syntax_error(mssql_env, wait_for_sqlserver):
    """Test query with SQL syntax error"""
    with pytest.raises(SQLServerQueryError):
        execute_query("INVALID SQL SYNTAX HERE", connection='TEST')


def test_execute_query_invalid_table(mssql_env, wait_for_sqlserver):
    """Test query referencing non-existent table"""
    with pytest.raises(SQLServerQueryError):
        execute_query("SELECT * FROM nonexistent_table", connection='TEST')


def test_execute_command_constraint_violation(mssql_env, wait_for_sqlserver, clean_sqlserver_db):
    """Test command that violates database constraints"""
    with pytest.raises(SQLServerQueryError):
        # Try to insert risk with non-existent portfolio_id
        execute_command(
            "INSERT INTO test_risks (portfolio_id, risk_type, risk_value) VALUES (9999, 'VaR_95', 100.0)",
            connection='TEST'
        )