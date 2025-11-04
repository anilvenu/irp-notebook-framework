"""
Test suite for Assurant Excel configuration validation

This test file validates the new Assurant Excel configuration format with 9 sheets:
- Metadata (key-value structure)
- Databases (table)
- Portfolios (table with foreign keys)
- Reinsurance Treaties (complex table)
- GeoHaz Thresholds (table with percentages)
- Analysis Table (large table with multiple foreign keys)
- Groupings (special dict-of-lists with 50 item columns)
- Products and Perils (sparse table)
- Moody's Reference Data (dict-of-lists)

Run these tests:
    pytest workspace/tests/test_assurant_configuration.py
    pytest workspace/tests/test_assurant_configuration.py -v
    pytest workspace/tests/test_assurant_configuration.py::test_load_assurant_config_success
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

from helpers.configuration import (
    load_configuration_file,
    validate_configuration_file,
    read_configuration,
    ConfigurationError,
    _validate_key_value,
    _validate_table,
    _validate_groupings,
    _validate_dict_of_lists,
    _validate_foreign_keys,
    _validate_special_references,
    _validate_groupings_references,
    _validate_business_rules
)
from helpers.constants import (
    ConfigurationStatus,
    METADATA_SCHEMA,
    DATABASES_SCHEMA,
    PORTFOLIOS_SCHEMA,
    EXCEL_VALIDATION_SCHEMAS
)
from helpers.database import execute_insert, execute_command


# Test file paths
VALID_EXCEL_PATH = str(Path(__file__).parent / 'files/valid_excel_configuration.xlsx')
INVALID_EXCEL_PATH = str(Path(__file__).parent / 'files/invalid_excel_configuration.xlsx')


# ============================================================================
# Helper Functions
# ============================================================================

def create_test_cycle(test_schema, cycle_name='test_cycle'):
    """Helper to create a test cycle"""
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
# Tests - Individual Sheet Validators
# ============================================================================

@pytest.mark.unit
def test_validate_metadata_success():
    """Test _validate_key_value with valid Metadata structure"""
    # Create DataFrame without header (Metadata format) - ALL 11 required keys
    data = {
        0: [
            'Current Date Value', 'EDM Data Version', 'Geocode Version', 'Hazard Version',
            'DLM Model Version', 'Validate DLM Model Versions?', 'Wildfire HD Model Version',
            'SCS HD Model Version', 'Inland Flood HD Model Version', 'Validate HD Model Versions?',
            'Export RDM Name'
        ],
        1: [
            '202503', '23.0.0', '23.0', '23',
            23, 'Y', 2,
            1, 1.2, 'Y',
            'RMS_RDM_202503_TEST'
        ]
    }
    df = pd.DataFrame(data)

    is_valid, errors, warnings, parsed_data = _validate_key_value(df, METADATA_SCHEMA, 'Metadata')

    assert is_valid == True, f"Validation failed with errors: {errors}"
    assert len(errors) == 0
    assert parsed_data is not None
    assert parsed_data['Current Date Value'] == '202503'
    assert parsed_data['EDM Data Version'] == '23.0.0'
    assert parsed_data['DLM Model Version'] == 23


@pytest.mark.unit
def test_validate_metadata_missing_key():
    """Test _validate_key_value with missing required key"""
    # Missing 'Export RDM Name'
    data = {
        0: ['Current Date Value', 'EDM Data Version'],
        1: ['202503', '23.0.0']
    }
    df = pd.DataFrame(data)

    is_valid, errors, warnings, parsed_data = _validate_key_value(df, METADATA_SCHEMA, 'Metadata')

    assert is_valid == False
    assert len(errors) > 0
    assert any('Export RDM Name' in err for err in errors)


@pytest.mark.unit
def test_validate_metadata_invalid_pattern():
    """Test _validate_key_value with invalid version pattern"""
    data = {
        0: ['EDM Data Version', 'Validate DLM Model Versions?'],
        1: ['invalid_version', 'MAYBE']  # Invalid version and Y/N flag
    }
    df = pd.DataFrame(data)

    # Use minimal schema for testing
    schema = {
        'required_keys': ['EDM Data Version'],
        'value_patterns': {
            'EDM Data Version': r'\d+(\.\d+)?(\.\d+)?',
            'Validate DLM Model Versions?': r'^[YN]$'
        }
    }

    is_valid, errors, warnings, parsed_data = _validate_key_value(df, schema, 'Metadata')

    assert is_valid == False
    assert len(errors) >= 1


@pytest.mark.unit
def test_validate_databases_success():
    """Test _validate_table with valid Databases structure"""
    df = pd.DataFrame({
        'Database': ['RM_EDM_202503_USAP', 'RM_EDM_202503_TEST'],
        'Store in Data Bridge?': ['Y', 'N']
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, DATABASES_SCHEMA, 'Databases')

    assert is_valid == True
    assert len(errors) == 0
    assert len(parsed_data) == 2
    assert parsed_data[0]['Database'] == 'RM_EDM_202503_USAP'


@pytest.mark.unit
def test_validate_databases_missing_columns():
    """Test _validate_table with missing required columns"""
    df = pd.DataFrame({
        'Database': ['RM_EDM_202503_USAP']
        # Missing 'Store in Data Bridge?' column
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, DATABASES_SCHEMA, 'Databases')

    assert is_valid == False
    assert len(errors) > 0
    assert any('Store in Data Bridge?' in err for err in errors)


@pytest.mark.unit
def test_validate_databases_invalid_pattern():
    """Test _validate_table with invalid database name pattern"""
    df = pd.DataFrame({
        'Database': ['INVALID_NAME', 'RM_EDM_202503_USAP'],
        'Store in Data Bridge?': ['Y', 'Y']
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, DATABASES_SCHEMA, 'Databases')

    assert is_valid == False
    assert len(errors) > 0
    assert any('INVALID_NAME' in err for err in errors)


@pytest.mark.unit
def test_validate_databases_null_value():
    """Test _validate_table with null in non-nullable column"""
    df = pd.DataFrame({
        'Database': ['RM_EDM_202503_USAP', None],
        'Store in Data Bridge?': ['Y', 'N']
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, DATABASES_SCHEMA, 'Databases')

    assert is_valid == False
    assert len(errors) > 0
    assert any('null' in err.lower() for err in errors)


@pytest.mark.unit
def test_validate_databases_duplicate():
    """Test _validate_table with duplicate values in unique column"""
    df = pd.DataFrame({
        'Database': ['RM_EDM_202503_USAP', 'RM_EDM_202503_USAP'],
        'Store in Data Bridge?': ['Y', 'N']
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, DATABASES_SCHEMA, 'Databases')

    assert is_valid == False
    assert len(errors) > 0
    assert any('duplicate' in err.lower() for err in errors)


@pytest.mark.unit
def test_validate_portfolios_success():
    """Test _validate_table with valid Portfolios structure"""
    df = pd.DataFrame({
        'Portfolio': ['Portfolio_A', 'Portfolio_B'],
        'Database': ['RM_EDM_202503_USAP', 'RM_EDM_202503_USAP'],
        'Import File': ['import_a.csv', 'import_b.csv'],
        'Base Portfolio?': ['Y', 'N']
    })

    is_valid, errors, warnings, parsed_data = _validate_table(df, PORTFOLIOS_SCHEMA, 'Portfolios')

    assert is_valid == True
    assert len(errors) == 0
    assert len(parsed_data) == 2


@pytest.mark.unit
def test_validate_groupings_success():
    """Test _validate_groupings with valid structure"""
    df = pd.DataFrame({
        'Group_Name': ['Group1', 'Group2'],
        'Item1': ['Item_A', 'Item_X'],
        'Item2': ['Item_B', 'Item_Y'],
        'Item3': [None, 'Item_Z']
    })

    from helpers.constants import GROUPINGS_SCHEMA
    is_valid, errors, warnings, parsed_data = _validate_groupings(df, GROUPINGS_SCHEMA, 'Groupings')

    assert is_valid == True
    assert len(errors) == 0
    assert len(parsed_data) == 2
    assert parsed_data[0]['Group_Name'] == 'Group1'
    assert parsed_data[0]['items'] == ['Item_A', 'Item_B']
    assert parsed_data[1]['items'] == ['Item_X', 'Item_Y', 'Item_Z']


@pytest.mark.unit
def test_validate_groupings_duplicate_name():
    """Test _validate_groupings with duplicate group names"""
    df = pd.DataFrame({
        'Group_Name': ['Group1', 'Group1'],
        'Item1': ['Item_A', 'Item_B']
    })

    from helpers.constants import GROUPINGS_SCHEMA
    is_valid, errors, warnings, parsed_data = _validate_groupings(df, GROUPINGS_SCHEMA, 'Groupings')

    assert is_valid == False
    assert len(errors) > 0
    assert any('duplicate' in err.lower() for err in errors)


@pytest.mark.unit
def test_validate_dict_of_lists_success():
    """Test _validate_dict_of_lists with valid Moody's Reference Data"""
    df = pd.DataFrame({
        'Model Profiles': ['Profile1', 'Profile2', None],
        'Output Profiles': ['Output1', 'Output2', 'Output3'],
        'Event Rate Schemes': ['Scheme1', None, None]
    })

    from helpers.constants import MOODYS_REFERENCE_SCHEMA
    is_valid, errors, warnings, parsed_data = _validate_dict_of_lists(df, MOODYS_REFERENCE_SCHEMA, "Moody's Reference Data")

    assert is_valid == True
    assert len(errors) == 0
    assert 'Model Profiles' in parsed_data
    assert len(parsed_data['Model Profiles']) == 2  # Nulls dropped
    assert len(parsed_data['Output Profiles']) == 3


@pytest.mark.unit
def test_validate_dict_of_lists_missing_columns():
    """Test _validate_dict_of_lists with missing required columns"""
    df = pd.DataFrame({
        'Model Profiles': ['Profile1', 'Profile2']
        # Missing 'Output Profiles' and 'Event Rate Schemes'
    })

    from helpers.constants import MOODYS_REFERENCE_SCHEMA
    is_valid, errors, warnings, parsed_data = _validate_dict_of_lists(df, MOODYS_REFERENCE_SCHEMA, "Moody's Reference Data")

    assert is_valid == False
    assert len(errors) > 0


@pytest.mark.unit
def test_validate_dict_of_lists_duplicates():
    """Test _validate_dict_of_lists with duplicate values in column"""
    df = pd.DataFrame({
        'Model Profiles': ['Profile1', 'Profile1', 'Profile2'],
        'Output Profiles': ['Output1', 'Output2', 'Output3'],
        'Event Rate Schemes': ['Scheme1', 'Scheme2', 'Scheme3']
    })

    from helpers.constants import MOODYS_REFERENCE_SCHEMA
    is_valid, errors, warnings, parsed_data = _validate_dict_of_lists(df, MOODYS_REFERENCE_SCHEMA, "Moody's Reference Data")

    assert is_valid == False
    assert len(errors) > 0
    assert any('duplicate' in err.lower() for err in errors)


# ============================================================================
# Tests - Cross-Sheet Validators
# ============================================================================

@pytest.mark.unit
def test_validate_foreign_keys_success():
    """Test _validate_foreign_keys with valid references"""
    config_data = {
        'Databases': [
            {'Database': 'RM_EDM_202503_USAP', 'Store in Data Bridge?': 'Y'}
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'RM_EDM_202503_USAP', 'Import File': 'file1', 'Base Portfolio?': 'Y'}
        ]
    }

    errors = _validate_foreign_keys(config_data, EXCEL_VALIDATION_SCHEMAS)

    assert len(errors) == 0


@pytest.mark.unit
def test_validate_foreign_keys_invalid_reference():
    """Test _validate_foreign_keys with invalid foreign key"""
    config_data = {
        'Databases': [
            {'Database': 'RM_EDM_202503_USAP', 'Store in Data Bridge?': 'Y'}
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'INVALID_DB', 'Import File': 'file1', 'Base Portfolio?': 'Y'}
        ]
    }

    errors = _validate_foreign_keys(config_data, EXCEL_VALIDATION_SCHEMAS)

    assert len(errors) > 0
    assert any('INVALID_DB' in err for err in errors)


@pytest.mark.unit
def test_validate_special_references_success():
    """Test _validate_special_references with valid references"""
    config_data = {
        'Analysis Table': [
            {
                'Analysis Name': 'Analysis1',
                'Analysis Profile': 'Profile1',
                'Output Profile': 'Output1',
                'Event Rate': 'Scheme1',
                'Reinsurance Treaty 1': 'Treaty1'
            }
        ],
        "Moody's Reference Data": {
            'Model Profiles': ['Profile1', 'Profile2'],
            'Output Profiles': ['Output1', 'Output2'],
            'Event Rate Schemes': ['Scheme1', 'Scheme2']
        },
        'Reinsurance Treaties': [
            {'Treaty Name': 'Treaty1', 'Treaty Number': '001'}
        ],
        'Products and Perils': [
            {'Analysis Name': 'Analysis1', 'Peril': 'EQ', 'Product Group': 'PG1'}
        ],
        'Groupings': []
    }

    errors = _validate_special_references(config_data)

    assert len(errors) == 0


@pytest.mark.unit
def test_validate_special_references_invalid_profile():
    """Test _validate_special_references with invalid profile"""
    config_data = {
        'Analysis Table': [
            {
                'Analysis Name': 'Analysis1',
                'Analysis Profile': 'INVALID_PROFILE',
                'Output Profile': 'Output1'
            }
        ],
        "Moody's Reference Data": {
            'Model Profiles': ['Profile1'],
            'Output Profiles': ['Output1'],
            'Event Rate Schemes': []
        },
        'Reinsurance Treaties': [],
        'Products and Perils': [],
        'Groupings': []
    }

    errors = _validate_special_references(config_data)

    assert len(errors) > 0
    assert any('INVALID_PROFILE' in err for err in errors)


@pytest.mark.unit
def test_validate_special_references_products_perils_group_name():
    """Test _validate_special_references allows Group Names in Products and Perils"""
    config_data = {
        'Analysis Table': [
            {'Analysis Name': 'Analysis1'}
        ],
        "Moody's Reference Data": {
            'Model Profiles': [],
            'Output Profiles': [],
            'Event Rate Schemes': []
        },
        'Reinsurance Treaties': [],
        'Products and Perils': [
            {'Analysis Name': 'Analysis1', 'Peril': 'EQ', 'Product Group': 'PG1'},
            {'Analysis Name': 'Group1', 'Peril': 'HU', 'Product Group': 'PG2'}  # Group name
        ],
        'Groupings': [
            {'Group_Name': 'Group1', 'items': ['Analysis1']}
        ]
    }

    errors = _validate_special_references(config_data)

    assert len(errors) == 0


@pytest.mark.unit
def test_validate_groupings_references_success():
    """Test _validate_groupings_references with valid references"""
    config_data = {
        'Groupings': [
            {'Group_Name': 'Group1', 'items': ['Port1', 'Analysis1']},
            {'Group_Name': 'Group2', 'items': ['Group1']}  # Can reference other groups
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'DB1', 'Import File': 'f1', 'Base Portfolio?': 'Y'}
        ],
        'Analysis Table': [
            {'Analysis Name': 'Analysis1'}
        ]
    }

    errors = _validate_groupings_references(config_data)

    assert len(errors) == 0


@pytest.mark.unit
def test_validate_groupings_references_invalid():
    """Test _validate_groupings_references with invalid reference (STRICT)"""
    config_data = {
        'Groupings': [
            {'Group_Name': 'Group1', 'items': ['INVALID_ITEM', 'Port1']}
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'DB1', 'Import File': 'f1', 'Base Portfolio?': 'Y'}
        ],
        'Analysis Table': []
    }

    errors = _validate_groupings_references(config_data)

    assert len(errors) > 0
    assert any('INVALID_ITEM' in err for err in errors)


@pytest.mark.unit
def test_validate_business_rules_success():
    """Test _validate_business_rules - at least one base portfolio per database"""
    config_data = {
        'Databases': [
            {'Database': 'RM_EDM_202503_USAP'}
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'RM_EDM_202503_USAP', 'Base Portfolio?': 'Y'},
            {'Portfolio': 'Port2', 'Database': 'RM_EDM_202503_USAP', 'Base Portfolio?': 'N'}
        ]
    }

    errors = _validate_business_rules(config_data)

    assert len(errors) == 0


@pytest.mark.unit
def test_validate_business_rules_no_base_portfolio():
    """Test _validate_business_rules - database without base portfolio"""
    config_data = {
        'Databases': [
            {'Database': 'RM_EDM_202503_USAP'}
        ],
        'Portfolios': [
            {'Portfolio': 'Port1', 'Database': 'RM_EDM_202503_USAP', 'Base Portfolio?': 'N'}
        ]
    }

    errors = _validate_business_rules(config_data)

    assert len(errors) > 0
    assert any('RM_EDM_202503_USAP' in err for err in errors)
    assert any('Base Portfolio' in err for err in errors)


# ============================================================================
# Tests - Integration with Actual Excel File
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Assurant Excel file not found")
def test_load_assurant_config_success(test_schema):
    """Test loading the actual Assurant Excel configuration file"""
    cycle_id = create_test_cycle(test_schema, 'test-assurant-load')

    # Load configuration
    config_id = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Verify configuration was created
    assert isinstance(config_id, int)
    assert config_id > 0

    # Read back the configuration
    config = read_configuration(config_id, schema=test_schema)

    assert config['id'] == config_id
    assert config['cycle_id'] == cycle_id
    assert config['status'] == ConfigurationStatus.VALID

    # Verify all required sheets are present
    config_data = config['configuration_data']
    required_sheets = [
        'Metadata', 'Databases', 'Portfolios', 'Reinsurance Treaties',
        'GeoHaz Thresholds', 'Analysis Table', 'Groupings',
        'Products and Perils', "Moody's Reference Data"
    ]

    for sheet in required_sheets:
        assert sheet in config_data, f"Missing sheet: {sheet}"

    # Verify validation results
    assert '_validation' in config_data
    validation = config_data['_validation']

    for sheet in required_sheets:
        assert sheet in validation
        assert validation[sheet]['status'] == 'SUCCESS', f"Sheet {sheet} validation failed: {validation[sheet].get('errors', [])}"


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Assurant Excel file not found")
def test_load_assurant_config_data_structure(test_schema):
    """Test that loaded configuration has correct data structure"""
    cycle_id = create_test_cycle(test_schema, 'test-assurant-structure')

    config_id = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    config = read_configuration(config_id, schema=test_schema)
    config_data = config['configuration_data']

    # Check Metadata is a dict
    assert isinstance(config_data['Metadata'], dict)
    assert 'EDM Data Version' in config_data['Metadata']

    # Check Databases is a list of dicts
    assert isinstance(config_data['Databases'], list)
    assert len(config_data['Databases']) > 0
    assert 'Database' in config_data['Databases'][0]

    # Check Groupings has items arrays
    assert isinstance(config_data['Groupings'], list)
    if len(config_data['Groupings']) > 0:
        assert 'Group_Name' in config_data['Groupings'][0]
        assert 'items' in config_data['Groupings'][0]
        assert isinstance(config_data['Groupings'][0]['items'], list)

    # Check Moody's Reference Data is dict of lists
    assert isinstance(config_data["Moody's Reference Data"], dict)
    assert 'Model Profiles' in config_data["Moody's Reference Data"]
    assert isinstance(config_data["Moody's Reference Data"]['Model Profiles'], list)


@pytest.mark.database
@pytest.mark.integration
def test_load_config_missing_file(test_schema):
    """Test loading configuration with non-existent file"""
    cycle_id = create_test_cycle(test_schema, 'test-missing-file')

    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path='/non/existent/path.xlsx',
                schema=test_schema
        )


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Assurant Excel file not found")
def test_load_config_archived_cycle_fails(test_schema):
    """Test that loading configuration for archived cycle fails"""
    # Create archived cycle
    archived_cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('archived_cycle', 'ARCHIVED'),
        schema=test_schema
    )

    with pytest.raises(ConfigurationError, match="does not match active cycle"):
        load_configuration_file(
            cycle_id=archived_cycle_id,
            excel_config_path=VALID_EXCEL_PATH,
                schema=test_schema
        )


@pytest.mark.database
@pytest.mark.integration
@pytest.mark.skipif(not Path(VALID_EXCEL_PATH).exists(), reason="Assurant Excel file not found")
def test_load_config_duplicate_active_fails(test_schema):
    """Test that duplicate ACTIVE configurations are prevented"""
    cycle_id = create_test_cycle(test_schema, 'test-duplicate-active')

    # Load first configuration
    config_id_1 = load_configuration_file(
        cycle_id=cycle_id,
        excel_config_path=VALID_EXCEL_PATH,
        schema=test_schema
    )

    # Update to ACTIVE status
    from helpers.configuration import update_configuration_status
    update_configuration_status(config_id_1, ConfigurationStatus.ACTIVE, schema=test_schema)

    # Try to load second configuration - should fail
    with pytest.raises(ConfigurationError, match="ACTIVE configuration exists"):
        load_configuration_file(
            cycle_id=cycle_id,
            excel_config_path=VALID_EXCEL_PATH,
                schema=test_schema
        )

# ============================================================================
# Tests - Error Scenarios
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_load_config_invalid_excel(test_schema):
    """Test loading configuration with invalid/corrupt Excel file"""
    cycle_id = create_test_cycle(test_schema, 'test-invalid-excel')

    # Create a text file with .xlsx extension
    fake_excel_path = Path(__file__).parent / 'files/fake_excel.xlsx'
    fake_excel_path.parent.mkdir(exist_ok=True)

    with open(fake_excel_path, 'w') as f:
        f.write("This is not an Excel file!")

    try:
        with pytest.raises(ConfigurationError, match="Failed to read Excel file"):
            load_configuration_file(
                cycle_id=cycle_id,
                excel_config_path=str(fake_excel_path),
                        schema=test_schema
            )
    finally:
        if fake_excel_path.exists():
            fake_excel_path.unlink()


if __name__ == '__main__':
    # Run tests with pytest
    pytest.main([__file__, '-v'])
