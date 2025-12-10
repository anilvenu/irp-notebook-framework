"""
pytest configuration and shared fixtures for IRP Notebook Framework tests

This module provides:
- Automatic test schema management (derived from test filename)
- Database connection verification
- Shared fixtures for common test scenarios
- Custom pytest options (--preserve-schema)
- Environment variable setup for test database
- Python path configuration for workspace imports
"""

# Set environment variables BEFORE any imports
# The helpers.constants module reads DB_CONFIG from environment on import
# So we MUST set these before importing helpers.database or helpers.constants
import os
import sys
import pytest
from pathlib import Path
from sqlalchemy import text
from datetime import datetime
import json

# Set test database environment variables FIRST
# FORCE test database configuration to prevent using production irp_db
# This is critical when running from VSCode Testing or pytest directly
os.environ['DB_SERVER'] = 'localhost'
os.environ['DB_PORT'] = '5432'
os.environ['DB_NAME'] = 'test_db'
os.environ['DB_USER'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'

# Set MSSQL test database environment variables
os.environ['MSSQL_TEST_SERVER'] = 'localhost'
os.environ['MSSQL_TEST_PORT'] = '1433'
os.environ['MSSQL_TEST_DATABASE'] = 'test_db'
os.environ['MSSQL_TEST_USER'] = 'sa'
os.environ['MSSQL_TEST_PASSWORD'] = os.getenv('MSSQL_SA_PASSWORD', 'TestPass123!')
os.environ['MSSQL_DRIVER'] = 'ODBC Driver 18 for SQL Server'
os.environ['MSSQL_TRUST_CERT'] = 'yes'

# Disable Teams notifications during tests
os.environ['TEAMS_NOTIFICATION_ENABLED'] = 'false'

# Add workspace directory to Python path for imports
workspace_path = Path(__file__).parent.parent.resolve()
workspace_path_str = str(workspace_path)
if workspace_path_str not in sys.path:
    sys.path.insert(0, workspace_path_str)

# NOW import from helpers (DB_CONFIG will use test_db)
from helpers.database import (
    init_database,
    get_engine,
    test_connection,
    execute_insert,
    set_schema
)
from helpers.constants import ConfigurationStatus


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
    """
    Configure pytest with custom markers and environment setup.

    This runs once at the start of the test session and sets up:
    - Custom pytest markers
    """
    # Configure custom markers
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
def verify_database_connection(request):
    """
    Verify database is accessible before running any tests.

    This fixture runs once per test session and fails fast if database
    is not available. Skips PostgreSQL check when running SQL Server tests.
    """
    # Skip PostgreSQL check if running SQL Server tests
    markexpr = request.config.option.markexpr
    if markexpr and 'sqlserver' in markexpr:
        return  # Skip PostgreSQL verification for SQL Server tests

    if not test_connection():

        # Checking for PostgreSQL container
        result = os.popen("docker ps | grep postgres").read()
        if not result:
            pytest.fail("PostgreSQL container is not running.")

        # Checking if the DB_SERVER, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD match the test database settings
        db_server = os.environ.get('DB_SERVER')
        db_port = os.environ.get('DB_PORT')
        db_name = os.environ.get('DB_NAME')
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        print(f"Server: {db_server}, Port: {db_port}, DB Name: {db_name}, User: {db_user}, Password: {db_password}")
        


        pytest.fail(
            "Database connection failed. Please ensure:\n"
            "  1. PostgreSQL container is running\n"
            "  2. test_db and test_user exist\n"
            "  3. Environment variables are set correctly\n"
            "Run: docker ps | grep postgres"
            f"Server: {db_server}, Port: {db_port}, DB Name: {db_name}, User: {db_user}, Password: {db_password}"
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
        print(f"⚠  PRESERVE MODE: Schema will be kept after tests")
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
                print(f"⚠  Found existing schema '{schema}' (from previous --preserve run)")

            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.commit()

        if existing:
            print(f"✓ Dropped previously preserved schema '{schema}'")
        else:
            print(f"✓ No existing schema found")
    except Exception as e:
        print(f"⚠  Warning: Could not drop existing schema: {e}")

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
        print(f"\n⚠ Schema '{schema}' preserved for debugging")

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


@pytest.fixture
def mock_irp_client(mocker):
    """
    Mock IRPClient for testing without actual API calls.

    This fixture provides a mock IRPClient that simulates Moody's API responses
    without making actual HTTP requests. It's configured with default behaviors
    that work for most test scenarios.

    The mock tracks:
    - Submitted EDM creation jobs
    - Job status queries
    - Job IDs and their statuses

    Default behaviors:
    - submit_create_edm_job(): Returns incrementing job IDs (1, 2, 3...)
    - get_risk_data_job(): Returns job with FINISHED status

    Also mocks:
    - _validate_batch_submission: Returns empty list (no validation errors)
      This prevents entity validation from making real API calls during tests.

    You can customize behavior per test using side_effect or return_value:

    Example - Simulate API failure:
        def test_submit_failure(mock_irp_client):
            from helpers.irp_integration.exceptions import IRPAPIError
            mock_irp_client.edm.submit_create_edm_job.side_effect = IRPAPIError("Connection failed")
            # Test error handling...

    Example - Simulate job progression:
        def test_job_tracking(mock_irp_client):
            # Simulate job transitioning from PENDING -> RUNNING -> FINISHED
            mock_irp_client.job.get_risk_data_job.side_effect = [
                {'status': 'PENDING', 'progress': 0},
                {'status': 'RUNNING', 'progress': 50},
                {'status': 'FINISHED', 'progress': 100}
            ]
            # Test polling logic...

    Returns:
        MagicMock: Configured mock IRPClient instance
    """
    from unittest.mock import MagicMock

    # Mock entity validation to prevent real API calls during batch submission tests
    mocker.patch('helpers.batch._validate_batch_submission', return_value=[])

    # Create main mock client
    mock_client = MagicMock()

    # Configure EDM manager mock
    mock_edm = MagicMock()

    # Track submitted EDM jobs (for stateful testing if needed)
    submitted_edms = []

    def mock_submit_edm(edm_name, server_name="databridge-1"):
        """Mock EDM submission - returns incrementing job IDs"""
        job_id = len(submitted_edms) + 1
        submitted_edms.append({
            'job_id': job_id,
            'edm_name': edm_name,
            'server_name': server_name
        })
        return job_id

    mock_edm.submit_create_edm_job.side_effect = mock_submit_edm

    # Configure Job manager mock
    mock_job_manager = MagicMock()

    # Default job status response - FINISHED
    def mock_get_job(job_id):
        """Mock job status retrieval - returns FINISHED status by default"""
        return {
            'jobId': job_id,
            'status': 'FINISHED',
            'progress': 100,
            'message': 'Job completed successfully',
            'createdDate': '2024-01-01T00:00:00Z',
            'completedDate': '2024-01-01T00:01:00Z'
        }

    mock_job_manager.get_risk_data_job.side_effect = mock_get_job

    # Attach managers to client
    mock_client.edm = mock_edm
    mock_client.job = mock_job_manager

    # Store submitted_edms for inspection in tests (if needed)
    mock_client._test_submitted_edms = submitted_edms

    return mock_client


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


# ==============================================================================
# MSSQL FIXTURES
# ==============================================================================

@pytest.fixture(scope="session", autouse=True)
def mssql_env():
    """
    Configure MSSQL test environment variables from .env file.

    This fixture loads environment variables from .env and sets defaults
    for test-specific MSSQL configuration that may not be in docker-compose.yml.
    """
    from dotenv import load_dotenv
    from pathlib import Path

    # Load .env from project root
    env_file = Path(__file__).parent.parent.parent / '.env.test'
    if env_file.exists():
        load_dotenv(env_file)

    # Set test-specific defaults if not already set
    # Note: No DATABASE env var - we pass database parameter to execute functions
    test_defaults = {
        'MSSQL_TEST_SERVER': 'localhost',  # Connect from host machine to Docker
        'MSSQL_TEST_PORT': '1433',
        'MSSQL_TEST_USER': 'sa',
        'MSSQL_TEST_PASSWORD': os.getenv('MSSQL_SA_PASSWORD', 'TestPass123!'),
    }

    # Only set if not already in environment
    for key, default_value in test_defaults.items():
        if key not in os.environ:
            os.environ[key] = default_value

    # Verify required variables are now set (DATABASE not required)
    required_vars = ['MSSQL_TEST_SERVER', 'MSSQL_TEST_USER', 'MSSQL_TEST_PASSWORD']
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        pytest.skip(
            f"MSSQL environment variables not set: {', '.join(missing)}\n"
            f"Configure these in .env file or set as environment variables."
        )

    return True


@pytest.fixture(scope="session")
def wait_for_sqlserver(mssql_env):
    """
    Wait for SQL Server to be ready before running tests.

    This fixture attempts to connect to SQL Server and waits up to 60 seconds
    for the server to become available. It's useful for CI/CD environments
    where SQL Server may still be starting up.
    """
    import time
    from helpers.sqlserver import test_connection

    max_attempts = 30
    wait_seconds = 2

    print("\nWaiting for SQL Server to be ready...")

    for attempt in range(1, max_attempts + 1):
        try:
            if test_connection('TEST'):
                print(f"✓ SQL Server is ready (attempt {attempt})")
                return True
        except Exception as e:
            if attempt == max_attempts:
                pytest.skip(
                    f"SQL Server not available after {max_attempts} attempts: {e}\n"
                    f"Ensure SQL Server Express container is running:\n"
                    f"  docker-compose up -d sqlserver"
                )

        if attempt < max_attempts:
            print(f"  Attempt {attempt}/{max_attempts} failed, retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)

    return False


@pytest.fixture(scope="session")
def init_sqlserver_db(wait_for_sqlserver):
    """
    Initialize SQL Server test database with schema and sample data.

    This fixture runs once per test session and ensures that the test_db
    database exists with the correct schema (test_portfolios, test_risks)
    and sample data for testing.
    """
    import subprocess
    from pathlib import Path

    print("\nInitializing SQL Server test database...")

    # Path to init script
    init_script = Path(__file__).parent.parent / 'helpers' / 'db' / 'init_sqlserver.sql'

    if not init_script.exists():
        pytest.skip(f"SQL Server init script not found: {init_script}")

    # Run initialization script using sqlcmd
    server = os.getenv('MSSQL_TEST_SERVER', 'localhost')
    user = os.getenv('MSSQL_TEST_USER', 'sa')
    password = os.getenv('MSSQL_TEST_PASSWORD', 'TestPass123!')

    try:
        # Try to run sqlcmd in Docker container
        result = subprocess.run(
            [
                'docker', 'exec', 'irp-sqlserver-test',
                '/opt/mssql-tools18/bin/sqlcmd',
                '-S', 'localhost',
                '-U', user,
                '-P', password,
                '-C',
                '-i', f'/docker-entrypoint-initdb.d/init_sqlserver.sql'
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✓ SQL Server test database initialized successfully")
            if result.stdout:
                print(result.stdout)
        else:
            print(f"Warning: SQL Server initialization completed with warnings:")
            if result.stderr:
                print(result.stderr)

    except subprocess.TimeoutExpired:
        pytest.skip("SQL Server initialization timed out")
    except FileNotFoundError:
        pytest.skip(
            "Docker command not found. Ensure Docker is installed and SQL Server container is running."
        )
    except Exception as e:
        pytest.skip(f"Failed to initialize SQL Server test database: {e}")

    return True


@pytest.fixture
def clean_sqlserver_db(init_sqlserver_db):
    """
    Reset SQL Server test database to clean state before each test.

    This fixture truncates test tables and re-inserts the initial 5 portfolios
    and 10 risks to ensure test isolation.
    """
    from helpers import sqlserver

    # Truncate tables (this will cascade to risks due to FK)
    try:
        sqlserver.execute_command("DELETE FROM test_risks", connection='TEST', database='test_db')
        sqlserver.execute_command("DELETE FROM test_portfolios", connection='TEST', database='test_db')
        sqlserver.execute_command("DBCC CHECKIDENT ('test_portfolios', RESEED, 0)", connection='TEST', database='test_db')
        sqlserver.execute_command("DBCC CHECKIDENT ('test_risks', RESEED, 0)", connection='TEST', database='test_db')


        # Re-insert initial data
        portfolios = [
            ("Test Portfolio A", 750000.00, "ACTIVE"),
            ("Test Portfolio B", 1250000.00, "ACTIVE"),
            ("Test Portfolio C", 500000.00, "ACTIVE"),
            ("Test Portfolio D", 2000000.00, "ACTIVE"),
            ("Test Portfolio E", 300000.00, "INACTIVE"),
        ]

        for name, value, status in portfolios:
            sqlserver.execute_command(
                "INSERT INTO test_portfolios (portfolio_name, portfolio_value, status) VALUES ({{ name }}, {{ value }}, {{ status }})",
                params={'name': name, 'value': value, 'status': status},
                connection='TEST', database='test_db'
            )

        # Re-insert risk data
        risks = [
            (1, 'VaR_95', 45000.00),
            (1, 'VaR_99', 65000.00),
            (2, 'VaR_95', 75000.00),
            (2, 'VaR_99', 110000.00),
            (3, 'VaR_95', 30000.00),
            (3, 'VaR_99', 45000.00),
            (4, 'VaR_95', 120000.00),
            (4, 'VaR_99', 180000.00),
            (5, 'VaR_95', 18000.00),
            (5, 'VaR_99', 27000.00),
        ]

        for portfolio_id, risk_type, risk_value in risks:
            sqlserver.execute_command(
                "INSERT INTO test_risks (portfolio_id, risk_type, risk_value) VALUES ({{ portfolio_id }}, {{ risk_type }}, {{ risk_value }})",
                params={'portfolio_id': portfolio_id, 'risk_type': risk_type, 'risk_value': risk_value},
                connection='TEST', database='test_db'
            )

    except Exception as e:
        pytest.skip(f"Failed to reset SQL Server test database: {e}")

    return True


@pytest.fixture
def sample_sql_file(tmp_path):
    """
    Create a temporary SQL file for testing file-based query operations.

    Returns:
        Path: Path to temporary SQL file
    """
    sql_content = """
-- Sample query with parameters
SELECT
    p.portfolio_name,
    p.portfolio_value,
    p.created_ts,
    r.risk_type,
    r.risk_value,
    r.calculated_ts
FROM test_portfolios p
INNER JOIN test_risks r ON p.id = r.portfolio_id
WHERE p.id = {{ portfolio_id }}
  AND r.risk_type = {{ risk_type }}
ORDER BY r.calculated_ts DESC;
"""

    sql_file = tmp_path / "test_query.sql"
    sql_file.write_text(sql_content)

    return sql_file


@pytest.fixture
def temp_sql_file(tmp_path):
    """
    Factory fixture to create temporary SQL files with custom content.

    Usage:
        def test_something(temp_sql_file):
            script_path = temp_sql_file("SELECT 1")
            # Use script_path in test
    """
    def _create_sql_file(content):
        sql_file = tmp_path / f"script_{id(content)}.sql"
        sql_file.write_text(content)
        return sql_file

    return _create_sql_file
