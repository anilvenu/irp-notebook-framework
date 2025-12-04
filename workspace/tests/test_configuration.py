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
def test_load_configuration_file_success(test_schema):
    """Test loading valid configuration file"""
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
def test_load_configuration_active_cycle(test_schema):
    """Test loading configuration for active cycle"""
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
def test_load_configuration_replace_when_no_batches(test_schema):
    """Test that configuration can be replaced when no batches exist (even if ACTIVE)"""
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
def test_load_configuration_fails_when_batches_exist(test_schema):
    """Test that configuration cannot be replaced when batches exist"""
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
        'Staging ETL'
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

    assert len(BATCH_TYPE_TRANSFORMERS) == 14, "Should have 12 business + 2 test batch types (total 14)"


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
    """Test Grouping transformer"""
    config = {
        'Metadata': {'Current Date Value': '202503'},
        'Groupings': [
            {'Group_Name': 'G1', 'items': ['P1', 'P2']},
            {'Group_Name': 'G2', 'items': ['A1', 'A2']}
        ]
    }

    result = create_job_configurations('Grouping', config)

    assert len(result) == 2, "Should create one job per group"
    assert result[0]['Metadata'] == config['Metadata']
    assert result[0]['Group_Name'] == 'G1'
    assert result[0]['items'] == ['P1', 'P2']


@pytest.mark.unit
def test_transform_export_to_rdm():
    """Test Export to RDM transformer - single job when <=100 items"""
    config = {
        'Metadata': {'Current Date Value': '202503', 'Export RDM Name': 'TestRDM'},
        'Analysis Table': [
            {'Analysis Name': 'A1'},
            {'Analysis Name': 'A2'}
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


@pytest.mark.unit
def test_transform_export_to_rdm_chunking():
    """Test Export to RDM transformer - chunking when >100 items"""
    # Create config with 150 analyses (requires seed job + 2 chunks)
    # seed (1) + remaining 149 split into [100, 49] = 3 jobs total
    analysis_names = [{'Analysis Name': f'Analysis_{i}'} for i in range(150)]
    config = {
        'Metadata': {'Current Date Value': '202503', 'Export RDM Name': 'LargeRDM'},
        'Analysis Table': analysis_names,
        'Groupings': []
    }

    result = create_job_configurations('Export to RDM', config)

    # Should create seed job (1 analysis) + 2 chunks (100 + 49)
    assert len(result) == 3, "Should create seed + 2 chunks for 150 items"

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

    # Third job has remaining 49
    assert result[2]['is_seed_job'] is False
    assert len(result[2]['analysis_names']) == 49
    assert result[2]['analysis_names'][0] == 'Analysis_101'


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
def test_validate_configuration_file(test_schema):
    """Test validate_configuration_file (validation only, no DB insert)"""
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