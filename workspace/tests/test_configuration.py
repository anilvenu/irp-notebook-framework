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
    validate_configuration_file,
    ConfigurationError,
    create_job_configurations,
    BATCH_TYPE_TRANSFORMERS
)
from helpers.constants import ConfigurationStatus


# Test Excel file path
VALID_EXCEL_PATH = str(Path(__file__).parent / 'files/valid_excel_configuration.xlsx')
INVALID_EXCEL_PATH = str(Path(__file__).parent / 'files/invalid_excel_configuration.xlsx')


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
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_file_success(test_schema, mocker):
    """Test loading valid configuration file"""
    # Mock API validation to avoid real API calls
    mocker.patch('helpers.configuration.validate_reference_data_with_api', return_value=[])
    mock_validator = mocker.patch('helpers.configuration.EntityValidator')
    mock_validator.return_value.validate_config_entities_not_exist.return_value = []

    cycle_id = create_test_cycle(test_schema, 'test-load-success')

    # Test loading configuration
    config_id = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Verify the configuration was created
    config = read_configuration(config_id, schema=test_schema)
    assert config['id'] == config_id
    assert config['cycle_id'] == cycle_id

    # Verify configuration data
    config_data = config['configuration_data']

    # Check Metadata
    assert 'Metadata' in config_data
    metadata = config_data['Metadata']
    assert metadata == {'Hazard Version': '23.0.0', 
                        'Export RDM Name': 'RM_RDM_202503_QEM_USAP', 
                        'Geocode Version': '23.0.0', 
                        'EDM Data Version': '23.0.0', 
                        'DLM Model Version': 23, 
                        'Current Date Value': '202503', 
                        'SCS HD Model Version': 1, 
                        'Wildfire HD Model Version': 2, 
                        'Validate HD Model Versions?': 'Y', 
                        'Validate DLM Model Versions?': 'Y', 
                        'Inland Flood HD Model Version': 1.2,
                        'Cycle Type': 'Quarterly'
                        }

    # Check Databases
    assert 'Databases' in config_data
    databases = config_data['Databases']
    assert len(databases) == 7
    print(databases)
    assert all(key in databases[0] for key in ['Database', 'Store in Data Bridge?'])

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
                        schema=test_schema
            )
    finally:
        # Clean up
        if bad_file_path.exists():
            bad_file_path.unlink()


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_active_cycle(test_schema, mocker):
    """Test loading configuration for active cycle"""
    # Mock API validation to avoid real API calls
    mocker.patch('helpers.configuration.validate_reference_data_with_api', return_value=[])
    mock_validator = mocker.patch('helpers.configuration.EntityValidator')
    mock_validator.return_value.validate_config_entities_not_exist.return_value = []

    active_cycle_id = create_test_cycle(test_schema, 'test-active')

    # Loading config for active cycle should succeed
    config_id = load_configuration_file(
        cycle_id=active_cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    assert isinstance(config_id, int)
    assert config_id > 0


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Test Excel file not found")
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
            excel_config_path=VALID_EXCEL_PATH,
                schema=test_schema
        )


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_replace_when_no_batches(test_schema, mocker):
    """Test that configuration can be replaced when no batches exist (even if ACTIVE)"""
    # Mock API validation to avoid real API calls
    mocker.patch('helpers.configuration.validate_reference_data_with_api', return_value=[])
    mock_validator = mocker.patch('helpers.configuration.EntityValidator')
    mock_validator.return_value.validate_config_entities_not_exist.return_value = []

    from helpers.database import execute_command
    cycle_id = create_test_cycle(test_schema, 'test-replace-no-batches')

    # Load first configuration
    config_id_1 = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Update to ACTIVE status (simulating workflow progress without batch creation)
    update_configuration_status(config_id_1, ConfigurationStatus.ACTIVE, schema=test_schema)

    # Load second configuration - should succeed since no batches exist
    config_id_2 = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Verify the old configuration was replaced
    assert config_id_2 != config_id_1
    old_config = execute_query(
        "SELECT * FROM irp_configuration WHERE id = %s",
        (config_id_1,),
        schema=test_schema
    )
    assert old_config.empty, "Old configuration should be deleted"


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Test Excel file not found")
def test_load_configuration_fails_when_batches_exist(test_schema, mocker):
    """Test that configuration cannot be replaced when batches exist"""
    # Mock API validation to avoid real API calls
    mocker.patch('helpers.configuration.validate_reference_data_with_api', return_value=[])
    mock_validator = mocker.patch('helpers.configuration.EntityValidator')
    mock_validator.return_value.validate_config_entities_not_exist.return_value = []

    cycle_id = create_test_cycle(test_schema, 'test-batches-exist')

    # Load first configuration
    config_id_1 = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Create a stage for this cycle (required for step)
    stage_id = execute_insert(
        """INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
           VALUES (%s, 1, 'Setup')""",
        (cycle_id,),
        schema=test_schema
    )

    # Create a step for the batch (required for batch)
    step_id = execute_insert(
        """INSERT INTO irp_step (stage_id, step_num, step_name, notebook_path)
           VALUES (%s, 1, 'Test Step', '/test/path')""",
        (stage_id,),
        schema=test_schema
    )

    # Create a batch linked to this configuration
    execute_insert(
        """INSERT INTO irp_batch (batch_type, configuration_id, step_id, status)
           VALUES (%s, %s, %s, %s)""",
        ('EDM Creation', config_id_1, step_id, 'INITIATED'),
        schema=test_schema
    )

    # Try to load second configuration - should fail because batches exist
    with pytest.raises(ConfigurationError, match="batch.*have been created"):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=VALID_EXCEL_PATH,
            schema=test_schema
        )


# ============================================================================
# Tests - Configuration Transformers
# ============================================================================

@pytest.mark.unit
def test_batch_type_transformers_registry():
    """Test that all expected batch types are registered"""
    expected_business_types = [
        'EDM Creation',
        'Portfolio Creation',
        'MRI Import',
        'Create Reinsurance Treaties',
        'EDM DB Upgrade',
        'GeoHaz',
        'Portfolio Mapping',
        'Analysis',
        'Grouping',
        'Grouping Rollup',
        'Export to RDM',
        'Staging ETL',
        'Data Extraction'
    ]

    expected_test_types = [
        'test_default',
        'test_multi_job'
    ]

    # Verify all business batch types are registered
    for batch_type in expected_business_types:
        assert batch_type in BATCH_TYPE_TRANSFORMERS, f"Business batch type '{batch_type}' should be registered"

    # Verify test batch types are registered
    for batch_type in expected_test_types:
        assert batch_type in BATCH_TYPE_TRANSFORMERS, f"Test batch type '{batch_type}' should be registered"

    assert len(BATCH_TYPE_TRANSFORMERS) == 15, "Should have 13 business + 2 test batch types (total 15)"


@pytest.mark.unit
def test_create_job_configurations_unknown_type():
    """Test create_job_configurations with unknown batch type"""
    config = {'data': 'test'}

    with pytest.raises(ConfigurationError) as exc_info:
        create_job_configurations('Unknown Batch Type', config)

    assert 'Unknown batch type' in str(exc_info.value)
    assert 'Available types' in str(exc_info.value)


@pytest.mark.unit
def test_transform_edm_creation():
    """Test EDM Creation transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503', 'EDM Data Version': '23.0.0'},
        'Databases': [
            {'Database': 'RMS_EDM_202503_DB1', 'Code': 'DB1'},
            {'Database': 'RMS_EDM_202503_DB2', 'Code': 'DB2'}
        ]
    }

    result = create_job_configurations('EDM Creation', config)

    assert len(result) == 2, "Should create one job per database"
    assert result[0]['Metadata'] == config['Metadata'], "Should include metadata"
    assert result[0]['Database'] == 'RMS_EDM_202503_DB1'
    assert result[1]['Database'] == 'RMS_EDM_202503_DB2'


@pytest.mark.unit
def test_transform_portfolio_creation():
    """Test Portfolio Creation transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Portfolios': [
            {'Portfolio': 'P1', 'Database': 'DB1', 'Base Portfolio?': 'Y'},
            {'Portfolio': 'P2', 'Database': 'DB1', 'Base Portfolio?': 'N'},
            {'Portfolio': 'P3', 'Database': 'DB1', 'Base Portfolio?': 'Y'}
        ]
    }

    result = create_job_configurations('Portfolio Creation', config)

    assert len(result) == 2, "Should create one job per base portfolio"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Portfolio'] == 'P1'
    assert result[1]['Portfolio'] == 'P3'


@pytest.mark.unit
def test_transform_mri_import():
    """Test MRI Import transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Portfolios': [
            {'Portfolio': 'P1', 'Database': 'DB1', 'Base Portfolio?': 'Y', 'Import File': 'USEQ'},
            {'Portfolio': 'P2', 'Database': 'DB1', 'Base Portfolio?': 'Y', 'Import File': 'USHU'}
        ]
    }

    result = create_job_configurations('MRI Import', config)

    assert len(result) == 2, "Should create one job per portfolio"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Portfolio'] == 'P1'


@pytest.mark.unit
def test_transform_create_reinsurance_treaties():
    """Test Create Reinsurance Treaties transformer.

    Creates one job per unique treaty-EDM combination from Analysis Table.
    """
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Reinsurance Treaties': [
            {'Treaty Name': 'T1', 'Treaty Type': 'QS'},
            {'Treaty Name': 'T2', 'Treaty Type': 'XOL'}
        ],
        'Analysis Table': [
            {'Database': 'EDM1', 'Reinsurance Treaty 1': 'T1', 'Reinsurance Treaty 2': 'T2'},
            {'Database': 'EDM2', 'Reinsurance Treaty 1': 'T1'}
        ]
    }

    result = create_job_configurations('Create Reinsurance Treaties', config)

    # Should create 3 unique treaty-EDM combinations: (T1, EDM1), (T1, EDM2), (T2, EDM1)
    assert len(result) == 3, "Should create one job per unique treaty-EDM combination"
    assert result[0]['Metadata'] == config['Metadata']

    # Results are sorted by (treaty_name, edm), so order is: (T1, EDM1), (T1, EDM2), (T2, EDM1)
    assert result[0]['Treaty Name'] == 'T1'
    assert result[0]['Database'] == 'EDM1'
    assert result[1]['Treaty Name'] == 'T1'
    assert result[1]['Database'] == 'EDM2'
    assert result[2]['Treaty Name'] == 'T2'
    assert result[2]['Database'] == 'EDM1'


@pytest.mark.unit
def test_transform_analysis():
    """Test Analysis transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Portfolio': 'P1'},
            {'Analysis Name': 'A2', 'Portfolio': 'P2'}
        ]
    }

    result = create_job_configurations('Analysis', config)

    assert len(result) == 2, "Should create one job per analysis"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Analysis Name'] == 'A1'
    assert result[1]['Analysis Name'] == 'A2'


@pytest.mark.unit
def test_transform_grouping():
    """Test Grouping transformer includes analysis_edm_map and group_names"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Database': 'EDM1'},
            {'Analysis Name': 'A2', 'Database': 'EDM2'}
        ],
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['A1', 'A2']},
            {'Group_Name': 'G2', 'items': ['A1']}
        ]
    }

    result = create_job_configurations('Grouping', config)

    assert len(result) == 2, "Should create one job per group"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Group_Name'] == 'G1'
    assert result[0]['items'] == ['A1', 'A2']

    # Verify analysis_edm_map is included
    assert 'analysis_edm_map' in result[0], "Should include analysis_edm_map"
    assert result[0]['analysis_edm_map'] == {'A1': 'EDM1', 'A2': 'EDM2'}

    # Verify group_names is included (as list for JSON serialization)
    assert 'group_names' in result[0], "Should include group_names"
    assert set(result[0]['group_names']) == {'G1', 'G2'}

    # Verify same data in second job config
    assert result[1]['analysis_edm_map'] == {'A1': 'EDM1', 'A2': 'EDM2'}
    assert set(result[1]['group_names']) == {'G1', 'G2'}


@pytest.mark.unit
def test_transform_grouping_rollup():
    """Test Grouping Rollup transformer includes analysis_edm_map and group_names"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Database': 'EDM1'},
            {'Analysis Name': 'A2', 'Database': 'EDM2'}
        ],
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['A1', 'A2']},  # Analysis-only group
            {'Group_Name': 'G2', 'items': ['G1', 'A1']}   # Rollup group (contains G1)
        ]
    }

    result = create_job_configurations('Grouping Rollup', config)

    # Only rollup groups (those referencing other groups)
    assert len(result) == 1, "Should create one job for rollup group G2"
    assert result[0]['Group_Name'] == 'G2'
    assert result[0]['items'] == ['G1', 'A1']

    # Verify analysis_edm_map is included
    assert 'analysis_edm_map' in result[0], "Should include analysis_edm_map"
    assert result[0]['analysis_edm_map'] == {'A1': 'EDM1', 'A2': 'EDM2'}

    # Verify group_names is included (as list for JSON serialization)
    assert 'group_names' in result[0], "Should include group_names"
    assert set(result[0]['group_names']) == {'G1', 'G2'}


@pytest.mark.unit
def test_build_analysis_edm_map():
    """Test _build_analysis_edm_map helper function"""
    from helpers.configuration import _build_analysis_edm_map

    config = {
        'Analysis Table': [
            {'Analysis Name': 'Analysis_1', 'Database': 'EDM_A'},
            {'Analysis Name': 'Analysis_2', 'Database': 'EDM_B'},
            {'Analysis Name': 'Analysis_3', 'Database': 'EDM_A'},
            {'Analysis Name': None, 'Database': 'EDM_C'},  # Missing analysis name
            {'Analysis Name': 'Analysis_4', 'Database': None},  # Missing database
        ]
    }

    result = _build_analysis_edm_map(config)

    # Should only include rows with both analysis name and database
    assert len(result) == 3
    assert result['Analysis_1'] == 'EDM_A'
    assert result['Analysis_2'] == 'EDM_B'
    assert result['Analysis_3'] == 'EDM_A'
    assert 'Analysis_4' not in result


@pytest.mark.unit
def test_build_analysis_edm_map_empty():
    """Test _build_analysis_edm_map with empty/missing data"""
    from helpers.configuration import _build_analysis_edm_map

    # Empty Analysis Table
    result = _build_analysis_edm_map({'Analysis Table': []})
    assert result == {}

    # Missing Analysis Table
    result = _build_analysis_edm_map({})
    assert result == {}


@pytest.mark.unit
def test_get_group_names():
    """Test _get_group_names helper function"""
    from helpers.configuration import _get_group_names

    config = {
        'Groupings': [
            {'Group_Name': 'Group_A', 'items': ['A1', 'A2']},
            {'Group_Name': 'Group_B', 'items': ['A3']},
            {'Group_Name': None, 'items': ['A4']},  # Missing group name
        ]
    }

    result = _get_group_names(config)

    # Should only include rows with group name
    assert isinstance(result, set)
    assert result == {'Group_A', 'Group_B'}


@pytest.mark.unit
def test_get_group_names_empty():
    """Test _get_group_names with empty/missing data"""
    from helpers.configuration import _get_group_names

    # Empty Groupings
    result = _get_group_names({'Groupings': []})
    assert result == set()

    # Missing Groupings
    result = _get_group_names({})
    assert result == set()


@pytest.mark.unit
def test_classify_groupings():
    """Test classify_groupings separates analysis-only and rollup groups"""
    from helpers.configuration import classify_groupings

    config = {
        'Analysis Table': [
            {'Analysis Name': 'A1'},
            {'Analysis Name': 'A2'},
            {'Analysis Name': 'A3'}
        ],
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['A1', 'A2']},       # Analysis-only
            {'Group_Name': 'G2', 'items': ['A3']},              # Analysis-only
            {'Group_Name': 'G3', 'items': ['G1', 'G2']},        # Rollup (contains groups)
            {'Group_Name': 'G4', 'items': ['G1', 'A3']},        # Rollup (contains G1)
            {'Group_Name': 'G5', 'items': []}                   # Empty - excluded
        ]
    }

    analysis_only, rollup = classify_groupings(config)

    # Analysis-only groups
    assert len(analysis_only) == 2
    assert analysis_only[0]['Group_Name'] == 'G1'
    assert analysis_only[1]['Group_Name'] == 'G2'

    # Rollup groups (contain references to other groups)
    assert len(rollup) == 2
    assert rollup[0]['Group_Name'] == 'G3'
    assert rollup[1]['Group_Name'] == 'G4'


@pytest.mark.unit
def test_transform_grouping_with_rollup_groups_excluded():
    """Test that Grouping transformer excludes rollup groups"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Database': 'EDM1'},
            {'Analysis Name': 'A2', 'Database': 'EDM1'}
        ],
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['A1', 'A2']},  # Analysis-only
            {'Group_Name': 'G2', 'items': ['G1']}         # Rollup - should be excluded
        ]
    }

    result = create_job_configurations('Grouping', config)

    # Only analysis-only groups should be included
    assert len(result) == 1, "Should only include analysis-only groups"
    assert result[0]['Group_Name'] == 'G1'


@pytest.mark.unit
def test_transform_grouping_rollup_excludes_analysis_only():
    """Test that Grouping Rollup transformer excludes analysis-only groups"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Database': 'EDM1'},
            {'Analysis Name': 'A2', 'Database': 'EDM1'}
        ],
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['A1', 'A2']},  # Analysis-only - should be excluded
            {'Group_Name': 'G2', 'items': ['G1']}         # Rollup
        ]
    }

    result = create_job_configurations('Grouping Rollup', config)

    # Only rollup groups should be included
    assert len(result) == 1, "Should only include rollup groups"
    assert result[0]['Group_Name'] == 'G2'


@pytest.mark.unit
def test_transform_export_to_rdm():
    """Test Export to RDM transformer - single job when <=100 items"""
    config = {
        'Metadata': {'Current Date Value': '202503', 'Export RDM Name': 'TestRDM'},
        'Analysis Table': [
            {'Analysis Name': 'A1', 'Database': 'EDM1'},
            {'Analysis Name': 'A2', 'Database': 'EDM2'}
        ],
        'Groupings': [
            {'Group_Name': 'G1'},
            {'Group_Name': 'G2'}
        ]
    }

    result = create_job_configurations('Export to RDM', config)

    # Should create single job with all analyses + groups when <=100 items
    assert len(result) == 1, "Should create single job when <=100 items"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['rdm_name'] == 'TestRDM'
    assert result[0]['server_name'] == 'databridge-1'
    assert result[0]['analysis_names'] == ['A1', 'A2', 'G1', 'G2']
    assert result[0]['analysis_count'] == 2
    assert result[0]['group_count'] == 2
    assert result[0]['is_seed_job'] is False
    assert result[0]['database_id'] is None

    # Verify analysis_edm_map is included
    assert 'analysis_edm_map' in result[0], "Should include analysis_edm_map"
    assert result[0]['analysis_edm_map'] == {'A1': 'EDM1', 'A2': 'EDM2'}

    # Verify group_names_set is included (as list for JSON serialization)
    assert 'group_names_set' in result[0], "Should include group_names_set"
    assert set(result[0]['group_names_set']) == {'G1', 'G2'}


@pytest.mark.unit
def test_transform_export_to_rdm_chunking():
    """Test Export to RDM transformer - chunking when >100 items"""
    # Create config with 150 analyses + 1 group = 151 total items (requires seed job + 2 chunks)
    # seed (1) + remaining 150 split into [100, 50] = 3 jobs total
    analysis_names = [{'Analysis Name': f'Analysis_{i}', 'Database': f'EDM_{i % 3}'} for i in range(150)]
    config = {
        'Metadata': {'Current Date Value': '202503', 'Export RDM Name': 'LargeRDM'},
        'Analysis Table': analysis_names,
        'Groupings': [{'Group_Name': 'TestGroup'}]
    }

    result = create_job_configurations('Export to RDM', config)

    # Should create seed job (1 item) + 2 chunks (100 + 50)
    assert len(result) == 3, "Should create seed + 2 chunks for 151 items"

    # First job is seed job with 1 analysis
    assert result[0]['is_seed_job'] is True
    assert len(result[0]['analysis_names']) == 1
    assert result[0]['analysis_names'][0] == 'Analysis_0'
    assert result[0]['database_id'] is None

    # Second job has first 100 of remaining
    assert result[1]['is_seed_job'] is False
    assert len(result[1]['analysis_names']) == 100
    assert result[1]['analysis_names'][0] == 'Analysis_1'
    assert result[1]['database_id'] is None

    # Third job has remaining 50 (49 analyses + 1 group)
    assert result[2]['is_seed_job'] is False
    assert len(result[2]['analysis_names']) == 50
    assert result[2]['analysis_names'][0] == 'Analysis_101'
    assert 'TestGroup' in result[2]['analysis_names']  # Group should be in last chunk

    # Verify analysis_edm_map and group_names_set are included in ALL chunks
    for i, job in enumerate(result):
        assert 'analysis_edm_map' in job, f"Job {i} should include analysis_edm_map"
        assert 'group_names_set' in job, f"Job {i} should include group_names_set"
        assert len(job['analysis_edm_map']) == 150, f"Job {i} should have full EDM map"
        assert job['group_names_set'] == ['TestGroup'], f"Job {i} should have group names"


@pytest.mark.unit
def test_transform_export_to_rdm_large_chunking():
    """Test Export to RDM transformer - multiple chunks for very large exports"""
    # Create config with 250 analyses
    # seed (1) + remaining 249 split into [100, 100, 49] = 4 jobs total
    analysis_names = [{'Analysis Name': f'Analysis_{i}'} for i in range(250)]
    config = {
        'Metadata': {'Current Date Value': '202503', 'Export RDM Name': 'VeryLargeRDM'},
        'Analysis Table': analysis_names,
        'Groupings': []
    }

    result = create_job_configurations('Export to RDM', config)

    # Should create seed job + 3 chunks
    assert len(result) == 4, "Should create seed + 3 chunks for 250 items"

    # Seed job
    assert result[0]['is_seed_job'] is True
    assert len(result[0]['analysis_names']) == 1

    # First chunk (100 analyses)
    assert result[1]['is_seed_job'] is False
    assert len(result[1]['analysis_names']) == 100

    # Second chunk (100 analyses)
    assert result[2]['is_seed_job'] is False
    assert len(result[2]['analysis_names']) == 100

    # Third chunk (remaining 49)
    assert result[3]['is_seed_job'] is False
    assert len(result[3]['analysis_names']) == 49


@pytest.mark.unit
def test_transform_staging_etl():
    """Test Staging ETL transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Databases': [
            {'Database': 'RMS_EDM_202503_DB1'},
            {'Database': 'RMS_EDM_202503_DB2'}
        ]
    }

    result = create_job_configurations('Staging ETL', config)

    assert len(result) == 2, "Should create one job per database"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Database'] == 'RMS_EDM_202503_DB1'


@pytest.mark.unit
def test_transform_edm_db_upgrade():
    """Test EDM DB Upgrade transformer creates one job per database with target version"""
    config = {
        'Metadata': {'Current Date Value': '202503', 'EDM Data Version': '22.0.0'},
        'Databases': [
            {'Database': 'RMS_EDM_202503_DB1', 'Store in Data Bridge?': 'Y'},
            {'Database': 'RMS_EDM_202503_DB2', 'Store in Data Bridge?': 'N'}
        ]
    }

    result = create_job_configurations('EDM DB Upgrade', config)

    assert len(result) == 2, "Should create one job per database"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Database'] == 'RMS_EDM_202503_DB1'
    assert result[0]['Store in Data Bridge?'] == 'Y'
    assert result[0]['target_edm_version'] == '22', "Should extract major version from '22.0.0'"
    assert result[1]['Database'] == 'RMS_EDM_202503_DB2'
    assert result[1]['Store in Data Bridge?'] == 'N'
    assert result[1]['target_edm_version'] == '22'


@pytest.mark.unit
def test_transform_geohaz():
    """Test GeoHaz transformer creates one job per base portfolio.
    Only portfolios with 'Base Portfolio?' == 'Y' are included.
    Geocode version is extracted from Metadata (converted from "22.0.0" to "22.0").
    """
    config = {
        'Metadata': {'Current Date Value': '202503', 'Geocode Version': '22.0.0'},
        'Portfolios': [
            {'Portfolio': 'PORTFOLIO_1', 'Database': 'RMS_EDM_202503_DB1', 'Import File': 'file1.csv', 'Base Portfolio?': 'Y'},
            {'Portfolio': 'PORTFOLIO_2', 'Database': 'RMS_EDM_202503_DB1', 'Import File': 'file2.csv', 'Base Portfolio?': 'Y'},
            {'Portfolio': 'PORTFOLIO_3', 'Database': 'RMS_EDM_202503_DB1', 'Import File': 'file3.csv', 'Base Portfolio?': 'N'}
        ]
    }

    result = create_job_configurations('GeoHaz', config)

    assert len(result) == 2, "Should create one job per base portfolio (excludes non-base)"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Portfolio'] == 'PORTFOLIO_1'
    assert result[0]['Import File'] == 'file1.csv'
    assert result[0]['geocode_version'] == '22.0', "Should convert '22.0.0' to '22.0'"
    assert result[1]['Portfolio'] == 'PORTFOLIO_2'
    assert result[1]['Import File'] == 'file2.csv'


@pytest.mark.unit
def test_transform_portfolio_mapping():
    """Test Portfolio Mapping transformer creates one job per base portfolio.
    Only portfolios with 'Base Portfolio?' == 'Y' are included.
    """
    config = {
        'Metadata': {'Current Date Value': '202503', 'EDM Data Version': 'v1.2.3'},
        'Portfolios': [
            {'Portfolio': 'PORTFOLIO_A', 'Database': 'RMS_EDM_202503_DB1', 'Import File': 'USEQ', 'Base Portfolio?': 'Y'},
            {'Portfolio': 'PORTFOLIO_B', 'Database': 'RMS_EDM_202503_DB1', 'Import File': 'USFL', 'Base Portfolio?': 'N'},
            {'Portfolio': 'PORTFOLIO_C', 'Database': 'RMS_EDM_202503_DB2', 'Import File': 'USHU', 'Base Portfolio?': 'Y'}
        ]
    }

    result = create_job_configurations('Portfolio Mapping', config)

    assert len(result) == 2, "Should create one job per base portfolio (excludes non-base)"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Portfolio'] == 'PORTFOLIO_A'
    assert result[0]['Database'] == 'RMS_EDM_202503_DB1'
    assert result[0]['Import File'] == 'USEQ'
    assert result[0]['Base Portfolio?'] == 'Y'
    assert result[1]['Portfolio'] == 'PORTFOLIO_C'
    assert result[1]['Database'] == 'RMS_EDM_202503_DB2'
    assert result[1]['Import File'] == 'USHU'


@pytest.mark.unit
def test_get_transformer_list():
    """Test get_transformer_list() function"""
    from helpers.configuration import get_transformer_list

    # Get list without test transformers
    transformers = get_transformer_list(include_test=False)

    assert isinstance(transformers, list)
    assert len(transformers) > 0
    assert 'EDM Creation' in transformers
    assert 'Portfolio Creation' in transformers
    assert 'Analysis' in transformers

    # Verify test transformers are excluded
    assert not any(t.startswith('test_') for t in transformers), "Test transformers should be excluded"


@pytest.mark.unit
def test_get_transformer_list_with_test():
    """Test get_transformer_list() with include_test=True"""
    from helpers.configuration import get_transformer_list

    # Get list with test transformers
    transformers = get_transformer_list(include_test=True)

    assert isinstance(transformers, list)
    assert len(transformers) > 0

    # Get list without test transformers for comparison
    transformers_no_test = get_transformer_list(include_test=False)

    # List with test should be longer or equal
    assert len(transformers) >= len(transformers_no_test)


@pytest.mark.unit
def test_transformer_empty_data():
    """Test transformer with empty data"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Databases': []
    }

    result = create_job_configurations('EDM Creation', config)

    assert len(result) == 0, "Should return empty list when no data rows"


@pytest.mark.unit
def test_transformer_missing_metadata():
    """Test transformer with missing metadata"""
    config = {
        'Databases': [
            {'Database': 'RMS_EDM_202503_DB1'}
        ]
    }

    result = create_job_configurations('EDM Creation', config)

    assert len(result) == 1, "Should still create job"
    assert result[0]['Metadata'] == {}, "Should have empty metadata dict"
    assert result[0]['Database'] == 'RMS_EDM_202503_DB1'


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
                        schema=test_schema
            )
    finally:
        # Clean up
        if fake_excel_path.exists():
            fake_excel_path.unlink()


@pytest.mark.database
@pytest.mark.integration
def test_validate_configuration_file(test_schema, mocker):
    """Test validate_configuration_file (validation only, no DB insert)"""
    # Mock API validation to avoid real API calls
    mocker.patch('helpers.configuration.validate_reference_data_with_api', return_value=[])
    mock_validator = mocker.patch('helpers.configuration.EntityValidator')
    mock_validator.return_value.validate_config_entities_not_exist.return_value = []

    cycle_id = create_test_cycle(test_schema, 'test-validate-only')

    # Create a valid test Excel file path
    if not Path(VALID_EXCEL_PATH).exists():
        pytest.skip("Test Excel file not found")

    # Validate without loading to database
    result = validate_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH
    )

    # Verify result structure
    assert 'validation_passed' in result
    assert 'configuration_data' in result
    assert 'file_info' in result
    assert result['validation_passed'] is True


# ============================================================================
# Tests - Reference Data API Validation
# ============================================================================

@pytest.mark.unit
def test_validate_reference_data_with_api_all_valid(mocker):
    """Test successful validation when all reference data exists in API"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Configure mock responses - all found
    # Model profile includes perilCode and modelRegionCode for event rate scheme lookup
    mock_ref_data.get_model_profile_by_name.return_value = {
        'count': 1,
        'items': [{'id': 1, 'name': 'Profile1', 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]
    }
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1, 'name': 'Output1'}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'id': 1, 'name': 'Scheme1'}]}

    analysis_job_configs = [
        {'Analysis Profile': 'Profile1', 'Output Profile': 'Output1', 'Event Rate': 'Scheme1'},
        {'Analysis Profile': 'Profile1', 'Output Profile': 'Output1', 'Event Rate': None},  # Event Rate nullable
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 0


@pytest.mark.unit
def test_validate_reference_data_with_api_model_profile_not_found(mocker):
    """Test error when model profile not found in API"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Model profile not found (count=0) - event rate will not be validated since model profile lookup fails
    mock_ref_data.get_model_profile_by_name.return_value = {'count': 0, 'items': []}
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    # Event rate scheme lookup won't have perilCode/modelRegionCode since model profile not found
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'id': 1}]}

    analysis_job_configs = [
        {'Analysis Profile': 'InvalidProfile', 'Output Profile': 'ValidOutput', 'Event Rate': 'ValidScheme'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    # Two errors: model profile not found, and event rate lookup without peril/region context
    assert len(errors) >= 1
    assert any('Model Profile' in e and 'InvalidProfile' in e for e in errors)
    assert any("not found in Moody's system" in e for e in errors)


@pytest.mark.unit
def test_validate_reference_data_with_api_output_profile_not_found(mocker):
    """Test error when output profile not found in API"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Output profile not found (empty list)
    mock_ref_data.get_model_profile_by_name.return_value = {
        'count': 1,
        'items': [{'id': 1, 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]
    }
    mock_ref_data.get_output_profile_by_name.return_value = []  # Empty list = not found
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'id': 1}]}

    analysis_job_configs = [
        {'Analysis Profile': 'ValidProfile', 'Output Profile': 'InvalidOutput', 'Event Rate': 'ValidScheme'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 1
    assert 'Output Profile' in errors[0]
    assert 'InvalidOutput' in errors[0]
    assert "not found in Moody's system" in errors[0]


@pytest.mark.unit
def test_validate_reference_data_with_api_event_rate_not_found(mocker):
    """Test error when event rate scheme not found in API"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Event rate scheme not found (count=0)
    mock_ref_data.get_model_profile_by_name.return_value = {
        'count': 1,
        'items': [{'id': 1, 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]
    }
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 0, 'items': []}

    analysis_job_configs = [
        {'Analysis Profile': 'ValidProfile', 'Output Profile': 'ValidOutput', 'Event Rate': 'InvalidScheme'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 1
    assert 'Event Rate Scheme' in errors[0]
    assert 'InvalidScheme' in errors[0]
    assert "not found in Moody's system" in errors[0]


@pytest.mark.unit
def test_validate_reference_data_with_api_multiple_errors(mocker):
    """Test multiple reference data validation errors"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # All not found - model profile not found means no perilCode/modelRegionCode for event rate lookup
    mock_ref_data.get_model_profile_by_name.return_value = {'count': 0, 'items': []}
    mock_ref_data.get_output_profile_by_name.return_value = []
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 0, 'items': []}

    analysis_job_configs = [
        {'Analysis Profile': 'BadProfile', 'Output Profile': 'BadOutput', 'Event Rate': 'BadScheme'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 3
    assert any('Model Profile' in e and 'BadProfile' in e for e in errors)
    assert any('Output Profile' in e and 'BadOutput' in e for e in errors)
    assert any('Event Rate Scheme' in e and 'BadScheme' in e for e in errors)


@pytest.mark.unit
def test_validate_reference_data_with_api_handles_api_error(mocker):
    """Test handling of API errors during validation"""
    from helpers.configuration import validate_reference_data_with_api
    from helpers.irp_integration.exceptions import IRPAPIError

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # API call raises error - model profile error means event rate can't be validated with context
    mock_ref_data.get_model_profile_by_name.side_effect = IRPAPIError("Connection timeout")
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'id': 1}]}

    analysis_job_configs = [
        {'Analysis Profile': 'SomeProfile', 'Output Profile': 'ValidOutput', 'Event Rate': 'ValidScheme'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) >= 1
    assert any('Failed to validate reference data' in e and 'Connection timeout' in e for e in errors)


@pytest.mark.unit
def test_validate_reference_data_with_api_empty_job_configs(mocker):
    """Test validation with empty job configs list"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient (should not be called)
    mock_client = mocker.patch('helpers.configuration.IRPClient')

    analysis_job_configs = []

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 0


@pytest.mark.unit
def test_validate_reference_data_with_api_unique_values_only(mocker):
    """Test that API is called only once per unique value/combination"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # All found - include perilCode and modelRegionCode for event rate scheme lookup
    mock_ref_data.get_model_profile_by_name.return_value = {
        'count': 1,
        'items': [{'id': 1, 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]
    }
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'id': 1}]}

    # Same profile used in multiple rows
    analysis_job_configs = [
        {'Analysis Profile': 'Profile1', 'Output Profile': 'Output1', 'Event Rate': 'Scheme1'},
        {'Analysis Profile': 'Profile1', 'Output Profile': 'Output1', 'Event Rate': 'Scheme1'},
        {'Analysis Profile': 'Profile1', 'Output Profile': 'Output1', 'Event Rate': 'Scheme1'},
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 0

    # Each API method should only be called once (unique values)
    # Event rate is validated per unique (scheme, perilCode, modelRegionCode) combination
    assert mock_ref_data.get_model_profile_by_name.call_count == 1
    assert mock_ref_data.get_output_profile_by_name.call_count == 1
    assert mock_ref_data.get_event_rate_scheme_by_name.call_count == 1


@pytest.mark.unit
def test_validate_reference_data_event_rate_uses_model_profile_context(mocker):
    """Test that event rate scheme validation uses perilCode and modelRegionCode from model profile"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Model profile with specific perilCode and modelRegionCode
    mock_ref_data.get_model_profile_by_name.return_value = {
        'count': 1,
        'items': [{'id': 1, 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]
    }
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'eventRateSchemeId': 69}]}

    analysis_job_configs = [
        {'Analysis Profile': 'DLM USST Low Frq v23', 'Output Profile': 'Output1', 'Event Rate': 'RMS 2013 Stochastic Event Rates'}
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 0

    # Verify event rate scheme was called with perilCode and modelRegionCode from model profile
    mock_ref_data.get_event_rate_scheme_by_name.assert_called_once_with(
        'RMS 2013 Stochastic Event Rates',
        peril_code='CS',
        model_region_code='NACS'
    )


@pytest.mark.unit
def test_validate_reference_data_different_model_profiles_different_event_rate_lookups(mocker):
    """Test that different model profiles result in separate event rate validations"""
    from helpers.configuration import validate_reference_data_with_api

    # Mock IRPClient
    mock_client = mocker.patch('helpers.configuration.IRPClient')
    mock_ref_data = mock_client.return_value.reference_data

    # Different model profiles return different peril/region combinations
    def mock_model_profile(profile_name):
        if profile_name == 'CS Profile':
            return {'count': 1, 'items': [{'id': 1, 'perilCode': 'CS', 'modelRegionCode': 'NACS'}]}
        elif profile_name == 'WS Profile':
            return {'count': 1, 'items': [{'id': 2, 'perilCode': 'WS', 'modelRegionCode': 'NAWS'}]}
        return {'count': 0, 'items': []}

    mock_ref_data.get_model_profile_by_name.side_effect = mock_model_profile
    mock_ref_data.get_output_profile_by_name.return_value = [{'id': 1}]
    mock_ref_data.get_event_rate_scheme_by_name.return_value = {'count': 1, 'items': [{'eventRateSchemeId': 1}]}

    # Same event rate scheme name, but different model profiles with different peril/region
    analysis_job_configs = [
        {'Analysis Profile': 'CS Profile', 'Output Profile': 'Output1', 'Event Rate': 'RMS 2013 Stochastic Event Rates'},
        {'Analysis Profile': 'WS Profile', 'Output Profile': 'Output1', 'Event Rate': 'RMS 2013 Stochastic Event Rates'},
    ]

    errors = validate_reference_data_with_api(analysis_job_configs)
    assert len(errors) == 0

    # Event rate scheme should be called twice - once for each peril/region combination
    assert mock_ref_data.get_event_rate_scheme_by_name.call_count == 2

    # Verify both calls were made with correct parameters
    calls = mock_ref_data.get_event_rate_scheme_by_name.call_args_list
    call_args = [(call[0][0], call[1].get('peril_code'), call[1].get('model_region_code')) for call in calls]
    assert ('RMS 2013 Stochastic Event Rates', 'CS', 'NACS') in call_args
    assert ('RMS 2013 Stochastic Event Rates', 'WS', 'NAWS') in call_args