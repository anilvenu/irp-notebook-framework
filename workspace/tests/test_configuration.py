"""
Test suite for configuration management operations (pytest version)

This test file validates configuration functionality including:
- Reading configuration from database
- Updating configuration status
- Loading and validating Excel configuration files
- Configuration transformers for job generation

All tests run in the 'test_configuration' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_configuration.py
    pytest workspace/tests/test_configuration.py -v
    pytest workspace/tests/test_configuration.py --preserve-schema
"""

import pytest
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

from helpers.database import execute_insert, execute_query
from helpers.configuration import (
    read_configuration,
    create_configuration,
    update_configuration_status,
    load_configuration_file,
    ConfigurationError,
    ConfigurationTransformer
)
from helpers.constants import ConfigurationStatus


# Test Excel file path
TEST_EXCEL_PATH = str(Path(__file__).parent / 'files/test_configuration.xlsx')


# ============================================================================
# Helper Functions
# ============================================================================

def create_test_cycle(test_schema, cycle_name='test_cycle'):
    """
    Helper to create a test cycle.

    Archives any existing ACTIVE cycles first to ensure only one ACTIVE cycle exists,
    matching production behavior where get_active_cycle_id() expects a single ACTIVE cycle.
    """
    from helpers.database import execute_command

    # Archive all existing active cycles
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE status = 'ACTIVE'",
        schema=test_schema
    )

    # Create new active cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
        schema=test_schema
    )
    return cycle_id


# ============================================================================
# Tests - CRUD Operations
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_success(test_schema):
    """Test creating a configuration successfully"""
    cycle_id = create_test_cycle(test_schema, 'test-create')

    config_data = {
        'TAB-A': [
            {'A-1': 'val1', 'A-2': 'data1', 'A-3': 'info1'}
        ],
        'TAB-B': [
            {'B-1': 100, 'B-2': 10.5}
        ]
    }

    # Create configuration
    config_id = create_configuration(
        cycle_id=cycle_id,
        configuration_file_name='/test/config.xlsx',
        configuration_data=config_data,
        status=ConfigurationStatus.NEW,
        file_last_updated_ts=datetime.now(),
        schema=test_schema
    )

    # Verify creation
    assert isinstance(config_id, int)
    assert config_id > 0

    # Verify it can be read back
    config = read_configuration(config_id, schema=test_schema)
    assert config['id'] == config_id
    assert config['cycle_id'] == cycle_id
    assert config['status'] == ConfigurationStatus.NEW
    assert config['configuration_data'] == config_data


@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_invalid_cycle_id(test_schema):
    """Test create_configuration with invalid cycle_id"""
    config_data = {'test': 'data'}

    # Test with cycle_id = 0
    with pytest.raises(ConfigurationError, match="Invalid cycle_id"):
        create_configuration(
            cycle_id=0,
            configuration_file_name='/test/config.xlsx',
            configuration_data=config_data,
            file_last_updated_ts=datetime.now(),
            schema=test_schema
        )

    # Test with negative cycle_id
    with pytest.raises(ConfigurationError, match="Invalid cycle_id"):
        create_configuration(
            cycle_id=-1,
            configuration_file_name='/test/config.xlsx',
            configuration_data=config_data,
            file_last_updated_ts=datetime.now(),
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_invalid_status(test_schema):
    """Test create_configuration with invalid status"""
    cycle_id = create_test_cycle(test_schema, 'test-invalid-status')

    config_data = {'test': 'data'}

    # Test with invalid status
    with pytest.raises(ConfigurationError, match="Invalid status: BOGUS_STATUS"):
        create_configuration(
            cycle_id=cycle_id,
            configuration_file_name='/test/config.xlsx',
            configuration_data=config_data,
            status='BOGUS_STATUS',
            file_last_updated_ts=datetime.now(),
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_invalid_filename(test_schema):
    """Test create_configuration with invalid filename"""
    cycle_id = create_test_cycle(test_schema, 'test-invalid-filename')

    config_data = {'test': 'data'}

    # Test with empty filename
    with pytest.raises(ConfigurationError, match="Invalid configuration_file_name"):
        create_configuration(
            cycle_id=cycle_id,
            configuration_file_name='',
            configuration_data=config_data,
            file_last_updated_ts=datetime.now(),
            schema=test_schema
        )

    # Test with whitespace-only filename
    with pytest.raises(ConfigurationError, match="Invalid configuration_file_name"):
        create_configuration(
            cycle_id=cycle_id,
            configuration_file_name='   ',
            configuration_data=config_data,
            file_last_updated_ts=datetime.now(),
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_missing_timestamp(test_schema):
    """Test create_configuration without providing file_last_updated_ts"""
    cycle_id = create_test_cycle(test_schema, 'test-no-timestamp')

    config_data = {'test': 'data'}

    # Test without providing timestamp (should raise error)
    with pytest.raises(ConfigurationError, match="file_last_updated_ts must be provided"):
        create_configuration(
            cycle_id=cycle_id,
            configuration_file_name='/test/config.xlsx',
            configuration_data=config_data,
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.unit
def test_create_configuration_with_all_statuses(test_schema):
    """Test creating configurations with all valid status values"""
    cycle_id = create_test_cycle(test_schema, 'test-all-statuses')

    config_data = {'test': 'data'}
    timestamp = datetime.now()

    # Test each valid status
    for status in [ConfigurationStatus.NEW, ConfigurationStatus.VALID,
                   ConfigurationStatus.ACTIVE, ConfigurationStatus.ERROR]:
        config_id = create_configuration(
            cycle_id=cycle_id,
            configuration_file_name=f'/test/config_{status}.xlsx',
            configuration_data=config_data,
            status=status,
            file_last_updated_ts=timestamp,
            schema=test_schema
        )

        # Verify status
        config = read_configuration(config_id, schema=test_schema)
        assert config['status'] == status


@pytest.mark.database
@pytest.mark.unit
def test_read_configuration(test_schema):
    """Test reading configuration from database"""
    cycle_id = create_test_cycle(test_schema, 'test-read')

    config_data = {
        'TAB-A': [
            {'A-1': 'val1', 'A-2': 'data1', 'A-3': 'info1'},
            {'A-1': 'val2', 'A-2': 'data2', 'A-3': 'info2'}
        ],
        'TAB-B': [
            {'B-1': 100, 'B-2': 10.5}
        ]
    }

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps(config_data),
         ConfigurationStatus.NEW, datetime.now()),
        schema=test_schema
    )

    # Test reading configuration
    result = read_configuration(config_id, schema=test_schema)

    # Assertions
    assert result['id'] == config_id
    assert result['cycle_id'] == cycle_id
    assert result['status'] == ConfigurationStatus.NEW
    assert 'TAB-A' in result['configuration_data']
    assert 'TAB-B' in result['configuration_data']


@pytest.mark.database
@pytest.mark.unit
def test_update_configuration_status(test_schema):
    """Test updating configuration status"""
    cycle_id = create_test_cycle(test_schema, 'test-update')

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps({'test': 'data'}),
         ConfigurationStatus.NEW, datetime.now()),
        schema=test_schema
    )

    # Test updating status
    result = update_configuration_status(config_id, ConfigurationStatus.VALID, schema=test_schema)
    assert result == True, "Update should return True"

    # Verify the update
    config = read_configuration(config_id, schema=test_schema)
    assert config['status'] == ConfigurationStatus.VALID

    # Test updating to same status (should return False)
    result = update_configuration_status(config_id, ConfigurationStatus.VALID, schema=test_schema)
    assert result == False, "Update to same status should return False"


@pytest.mark.database
@pytest.mark.unit
def test_update_configuration_status_to_error(test_schema):
    """Test updating configuration status to ERROR"""
    cycle_id = create_test_cycle(test_schema, 'test-error-status')

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps({'test': 'data'}),
         ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    result = update_configuration_status(config_id, ConfigurationStatus.ERROR, schema=test_schema)
    assert result == True

    config = read_configuration(config_id, schema=test_schema)
    assert config['status'] == ConfigurationStatus.ERROR


# ============================================================================
# Tests - File Loading
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(TEST_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_file_success(test_schema):
    """Test loading valid configuration file"""
    cycle_id = create_test_cycle(test_schema, 'test-load-success')

    # Test loading configuration
    config_id = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=TEST_EXCEL_PATH,
        register=True,
        schema=test_schema
    )

    # Verify the configuration was created
    config = read_configuration(config_id, schema=test_schema)
    assert config['id'] == config_id
    assert config['cycle_id'] == cycle_id

    # Verify configuration data
    config_data = config['configuration_data']

    # Check TAB-A
    assert 'TAB-A' in config_data
    tab_a_data = config_data['TAB-A']
    assert len(tab_a_data) == 3
    assert all(key in tab_a_data[0] for key in ['A-1', 'A-2', 'A-3'])

    # Check TAB-B
    assert 'TAB-B' in config_data
    tab_b_data = config_data['TAB-B']
    assert len(tab_b_data) == 3
    assert all(key in tab_b_data[0] for key in ['B-1', 'B-2'])

    # Check validation status
    assert '_validation' in config_data
    validation = config_data['_validation']
    for tab, status_info in validation.items():
        assert status_info['status'] == 'SUCCESS'

    # Verify final status is VALID
    assert config['status'] == ConfigurationStatus.VALID


@pytest.mark.database
@pytest.mark.integration
def test_load_configuration_file_validation_errors(test_schema):
    """Test loading configuration with validation errors"""
    cycle_id = create_test_cycle(test_schema, 'test-load-error')

    # Create a test Excel file with missing columns
    bad_file_path = Path(__file__).parent / 'test_config_bad.xlsx'

    with pd.ExcelWriter(bad_file_path, engine='openpyxl') as writer:
        df = pd.DataFrame({'A-1': ['val'], 'A-2': ['data']})  # Missing A-3
        df.to_excel(writer, sheet_name='TAB-A', index=False)

    try:
        # Test loading - should fail
        with pytest.raises(ConfigurationError):
            load_configuration_file(
                cycle_id=cycle_id,
                excel_config_path=str(bad_file_path),
                register=True,
                schema=test_schema
            )
    finally:
        # Clean up
        if bad_file_path.exists():
            bad_file_path.unlink()


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(TEST_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_active_cycle(test_schema):
    """Test loading configuration for active cycle"""
    active_cycle_id = create_test_cycle(test_schema, 'test-active')

    # Loading config for active cycle should succeed
    config_id = load_configuration_file(
        cycle_id=active_cycle_id,
        excel_config_path=TEST_EXCEL_PATH,
        register=True,
        schema=test_schema
    )

    assert isinstance(config_id, int)
    assert config_id > 0


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(TEST_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_archived_cycle_fails(test_schema):
    """Test that loading configuration for archived cycle fails"""
    # Create archived cycle
    archived_cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('archived_cycle', 'ARCHIVED'),
        schema=test_schema
    )

    # Loading config for archived cycle should fail
    with pytest.raises(ConfigurationError):
        load_configuration_file(
            cycle_id=archived_cycle_id,
            excel_config_path=TEST_EXCEL_PATH,
            register=True,
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(TEST_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_duplicate_active_fails(test_schema):
    """Test that duplicate ACTIVE configurations are prevented"""
    cycle_id = create_test_cycle(test_schema, 'test-duplicate')

    # Load first configuration
    config_id_1 = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=TEST_EXCEL_PATH,
        register=True,
        schema=test_schema
    )

    # Update to ACTIVE status
    update_configuration_status(config_id_1, ConfigurationStatus.ACTIVE, schema=test_schema)

    # Try to load second configuration - should fail
    with pytest.raises(ConfigurationError):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=TEST_EXCEL_PATH,
            register=True,
            schema=test_schema
        )


# ============================================================================
# Tests - Configuration Transformers
# ============================================================================

@pytest.mark.unit
def test_configuration_transformer_default():
    """Test ConfigurationTransformer default type"""
    config = {
        'param1': 'value1',
        'param2': 100,
        'nested': {'key': 'value'}
    }

    result = ConfigurationTransformer.create_job_configurations('default', config)

    # Verify results
    assert len(result) == 1, "Should return single job config"
    assert result[0] == config, "Should copy config as-is"
    assert result[0] is not config, "Should be a copy, not the same object"


@pytest.mark.unit
def test_configuration_transformer_passthrough():
    """Test ConfigurationTransformer passthrough type"""
    config = {'data': 'test', 'count': 5}

    result = ConfigurationTransformer.create_job_configurations('passthrough', config)

    # Verify results
    assert len(result) == 1, "Should return single job config"
    assert result[0] == config, "Should return same config"
    assert result[0] is config, "Should be the same object (not a copy)"


@pytest.mark.unit
def test_configuration_transformer_multi_job_with_jobs():
    """Test ConfigurationTransformer multi_job type with jobs list"""
    config_with_jobs = {
        'batch_type': 'test_batch',
        'jobs': [
            {'job_id': 1, 'param': 'A'},
            {'job_id': 2, 'param': 'B'},
            {'job_id': 3, 'param': 'C'}
        ]
    }

    result = ConfigurationTransformer.create_job_configurations('multi_job', config_with_jobs)

    assert len(result) == 3, "Should return 3 job configs"
    assert result[0] == {'job_id': 1, 'param': 'A'}
    assert result[1] == {'job_id': 2, 'param': 'B'}
    assert result[2] == {'job_id': 3, 'param': 'C'}


@pytest.mark.unit
def test_configuration_transformer_multi_job_fallback():
    """Test ConfigurationTransformer multi_job type without jobs list (fallback)"""
    config_no_jobs = {'single_job': 'data'}

    result = ConfigurationTransformer.create_job_configurations('multi_job', config_no_jobs)

    assert len(result) == 1, "Should return single job config"
    assert result[0] == config_no_jobs


@pytest.mark.unit
def test_configuration_transformer_unknown_type():
    """Test ConfigurationTransformer unknown type error"""
    config = {'data': 'test'}

    with pytest.raises(ConfigurationError) as exc_info:
        ConfigurationTransformer.create_job_configurations('nonexistent_type', config)

    assert 'nonexistent_type' in str(exc_info.value)
    assert 'Available types' in str(exc_info.value)


@pytest.mark.unit
def test_configuration_transformer_list_types():
    """Test listing registered transformer types"""
    types = ConfigurationTransformer.list_types()

    # Verify expected types are registered
    assert 'default' in types, "Should have 'default' type"
    assert 'passthrough' in types, "Should have 'passthrough' type"
    assert 'multi_job' in types, "Should have 'multi_job' type"


@pytest.mark.unit
def test_configuration_transformer_custom_registration():
    """Test custom transformer registration"""
    # Register a custom transformer for testing
    @ConfigurationTransformer.register('test_custom')
    def transform_custom(config):
        """Custom transformer that doubles values"""
        return [
            {'value': config.get('value', 0) * 2},
            {'value': config.get('value', 0) * 3}
        ]

    # Test the custom transformer
    config = {'value': 10}
    result = ConfigurationTransformer.create_job_configurations('test_custom', config)

    assert len(result) == 2, "Should return 2 job configs"
    assert result[0] == {'value': 20}, "First job should have doubled value"
    assert result[1] == {'value': 30}, "Second job should have tripled value"


# ============================================================================
# Tests - Error Cases and Edge Conditions
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_read_configuration_not_found(test_schema):
    """Test read_configuration with non-existent configuration ID"""
    # Try to read configuration that doesn't exist
    with pytest.raises(ConfigurationError, match="Configuration with id 99999 not found"):
        read_configuration(99999, schema=test_schema)


@pytest.mark.database
@pytest.mark.unit
def test_update_configuration_status_invalid_status(test_schema):
    """Test update_configuration_status with invalid/bogus status"""
    cycle_id = create_test_cycle(test_schema, 'test-bogus-status')

    config_id = execute_insert(
        """INSERT INTO irp_configuration
           (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
           VALUES (%s, %s, %s, %s, %s)""",
        (cycle_id, '/test/config.xlsx', json.dumps({'test': 'data'}),
         ConfigurationStatus.NEW, datetime.now()),
        schema=test_schema
    )

    # Try to update with bogus status
    with pytest.raises(ConfigurationError, match="Invalid status: BOGUS"):
        update_configuration_status(config_id, 'BOGUS', schema=test_schema)


@pytest.mark.database
@pytest.mark.integration
def test_load_configuration_file_not_found(test_schema):
    """Test load_configuration_file with non-existent file path"""
    cycle_id = create_test_cycle(test_schema, 'test-file-not-found')

    # Try to load configuration with non-existent path
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path='/non/existent/path/config.xlsx',
            register=True,
            schema=test_schema
        )


@pytest.mark.database
@pytest.mark.integration
def test_load_configuration_file_not_excel(test_schema):
    """Test load_configuration_file with file that is not really Excel"""
    cycle_id = create_test_cycle(test_schema, 'test-not-excel')

    # Create a text file with .xlsx extension
    fake_excel_path = Path(__file__).parent / 'files/Not_an_Excel.xlsx'
    fake_excel_path.parent.mkdir(exist_ok=True)

    with open(fake_excel_path, 'w') as f:
        f.write("This is not an Excel file, just plain text!")

    try:
        # Try to load the fake Excel file
        with pytest.raises(ConfigurationError, match="Failed to read Excel file"):
            load_configuration_file(
                cycle_id=cycle_id,
                excel_config_path=str(fake_excel_path),
                register=True,
                schema=test_schema
            )
    finally:
        # Clean up
        if fake_excel_path.exists():
            fake_excel_path.unlink()


@pytest.mark.database
@pytest.mark.integration
def test_load_configuration_without_register(test_schema):
    """Test load_configuration_file with register=False"""
    cycle_id = create_test_cycle(test_schema, 'test-no-register')

    # Create a valid test Excel file path
    if not Path(TEST_EXCEL_PATH).exists():
        pytest.skip("Test Excel file not found")

    # Try to load without registering (dry-run mode)
    with pytest.raises(ConfigurationError, match="validated successfully but not registered"):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=TEST_EXCEL_PATH,
            register=False,
            schema=test_schema
        )
