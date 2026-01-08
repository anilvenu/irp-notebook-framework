"""
Tests for CSV Export Helper Module

Tests the csv_export module functionality including:
- Working files path resolution using WorkContext
- Building import filenames with standardized naming conventions
- Saving DataFrames to CSV files
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import sys

from helpers.csv_export import (
    get_working_files_path,
    build_import_filename,
    build_import_filenames,
    save_dataframes_to_csv,
    save_sql_results_to_csv
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test output files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'LocationID': ['LOC001', 'LOC002', 'LOC003'],
        'PolicyNumber': ['POL001', 'POL002', 'POL003'],
        'Premium': [1000.00, 1500.00, 2000.00]
    })


@pytest.fixture
def sample_dataframes():
    """Create multiple sample DataFrames for testing."""
    df_account = pd.DataFrame({
        'ACCNTNUM': ['ACC001', 'ACC002'],
        'ACCNTNAME': ['Account 1', 'Account 2'],
        'CEDANTID': ['ASST', 'ASST']
    })

    df_location = pd.DataFrame({
        'ACCNTNUM': ['ACC001', 'ACC002'],
        'LOCNUM': ['LOC001', 'LOC002'],
        'CITY': ['New York', 'Los Angeles']
    })

    return [df_account, df_location]


# ============================================================================
# Tests for get_working_files_path()
# ============================================================================

def test_get_working_files_path_with_explicit_path_stage_structure(temp_output_dir):
    """Test path resolution for notebooks in Stage folder structure."""
    # Create mock Stage folder structure
    stage_folder = temp_output_dir / 'Active_Q1_2024' / 'notebooks' / 'Stage_01_Extract'
    notebook_path = stage_folder / 'Step_01_Extract_Data.ipynb'
    stage_folder.mkdir(parents=True)

    # Call function
    result = get_working_files_path(notebook_path)

    # Expected: Active_Q1_2024/files/working_files
    expected = temp_output_dir / 'Active_Q1_2024' / 'files' / 'working_files'

    assert result == expected
    assert result.exists()


def test_get_working_files_path_with_explicit_path_tools_structure(temp_output_dir):
    """Test path resolution for notebooks in _Tools folder structure."""
    # Create mock _Tools folder structure
    tools_folder = temp_output_dir / '_Tools' / 'IRP Integration'
    notebook_path = tools_folder / 'IRP_Integration_Demo.ipynb'
    tools_folder.mkdir(parents=True)

    # Call function
    result = get_working_files_path(notebook_path)

    # Expected: _Tools/files/working_files
    expected = temp_output_dir / '_Tools' / 'files' / 'working_files'

    assert result == expected
    assert result.exists()


def test_get_working_files_path_creates_directory_if_not_exists(temp_output_dir):
    """Test that the function creates the directory if it doesn't exist."""
    stage_folder = temp_output_dir / 'Active_Test' / 'notebooks' / 'Stage_01_Setup'
    notebook_path = stage_folder / 'Step_01.ipynb'
    stage_folder.mkdir(parents=True)

    working_files = temp_output_dir / 'Active_Test' / 'files' / 'working_files'

    # Ensure directory doesn't exist yet
    assert not working_files.exists()

    # Call function
    result = get_working_files_path(notebook_path)

    # Directory should now exist
    assert result.exists()
    assert result.is_dir()


def test_get_working_files_path_without_explicit_path_raises_error():
    """Test that function raises error when WorkContext fails and no path provided."""
    with patch('helpers.csv_export.WorkContext') as mock_context:
        mock_context.side_effect = Exception("No notebook context")

        with pytest.raises(ValueError, match="Could not determine current notebook path"):
            get_working_files_path()


# ============================================================================
# Tests for build_import_filename()
# ============================================================================

def test_build_import_filename_basic():
    """Test building basic filename without cycle_type."""
    result = build_import_filename('202503', 'CBHU', 'Account')
    assert result == 'Modeling_202503_Moodys_CBHU_Account'


def test_build_import_filename_with_cycle_type():
    """Test building filename with cycle_type."""
    result = build_import_filename(
        '202511', 'USEQ', 'Location', cycle_type='Quarterly'
    )
    assert result == 'Modeling_202511_Moodys_Quarterly_USEQ_Location'


def test_build_import_filename_with_modifier():
    """Test building filename with modifier but no cycle_type."""
    result = build_import_filename(
        '202509', 'USHU', 'Account', modifier='Full'
    )
    assert result == 'Modeling_202509_Moodys_USHU_Full_Account'


def test_build_import_filename_with_cycle_type_and_modifier():
    """Test building filename with both cycle_type and modifier."""
    result = build_import_filename(
        '202509', 'USHU', 'Location',
        cycle_type='Quarterly',
        modifier='Leak'
    )
    assert result == 'Modeling_202509_Moodys_Quarterly_USHU_Leak_Location'


def test_build_import_filename_complex_modifier():
    """Test building filename with complex multi-word modifier."""
    result = build_import_filename(
        '202511', 'USFL', 'Account',
        cycle_type='Quarterly',
        modifier='Commercial_Excess'
    )
    assert result == 'Modeling_202511_Moodys_Quarterly_USFL_Commercial_Excess_Account'


# ============================================================================
# Tests for build_import_filenames()
# ============================================================================

def test_build_import_filenames_simple_pair():
    """Test building standard Account + Location pair."""
    result = build_import_filenames(
        '202511', 'USEQ', ['Account', 'Location'], cycle_type='Quarterly'
    )

    assert len(result) == 2
    assert result[0] == 'Modeling_202511_Moodys_Quarterly_USEQ_Account'
    assert result[1] == 'Modeling_202511_Moodys_Quarterly_USEQ_Location'


def test_build_import_filenames_with_modifiers():
    """Test building filenames with different modifiers for each file."""
    result = build_import_filenames(
        '202509', 'USHU',
        ['Account', 'Location', 'Account', 'Location'],
        cycle_type='Quarterly',
        modifiers=['Full', 'Full', 'Leak', 'Leak']
    )

    assert len(result) == 4
    assert result[0] == 'Modeling_202509_Moodys_Quarterly_USHU_Full_Account'
    assert result[1] == 'Modeling_202509_Moodys_Quarterly_USHU_Full_Location'
    assert result[2] == 'Modeling_202509_Moodys_Quarterly_USHU_Leak_Account'
    assert result[3] == 'Modeling_202509_Moodys_Quarterly_USHU_Leak_Location'


def test_build_import_filenames_without_cycle_type():
    """Test building filenames without cycle_type."""
    result = build_import_filenames(
        '202503', 'CBHU', ['Account', 'Location']
    )

    assert len(result) == 2
    assert result[0] == 'Modeling_202503_Moodys_CBHU_Account'
    assert result[1] == 'Modeling_202503_Moodys_CBHU_Location'


def test_build_import_filenames_mismatched_modifier_length():
    """Test that error is raised when modifier length doesn't match file_types."""
    with pytest.raises(ValueError, match="Length of modifiers.*must match"):
        build_import_filenames(
            '202511', 'USEQ',
            ['Account', 'Location'],
            modifiers=['Full']  # Only 1 modifier for 2 file types
        )


# ============================================================================
# Tests for save_dataframes_to_csv()
# ============================================================================

def test_save_dataframes_to_csv_single_dataframe(sample_dataframe, temp_output_dir):
    """Test saving a single DataFrame to CSV."""
    filename = 'Modeling_202511_Moodys_Quarterly_USEQ_Account'

    result = save_dataframes_to_csv(
        sample_dataframe,
        filename,
        output_dir=temp_output_dir
    )

    assert len(result) == 1
    assert result[0] == temp_output_dir / f'{filename}.csv'
    assert result[0].exists()

    # Verify CSV contents (tab-delimited)
    df_loaded = pd.read_csv(result[0], sep='\t')
    assert len(df_loaded) == 3
    assert list(df_loaded.columns) == ['LocationID', 'PolicyNumber', 'Premium']


def test_save_dataframes_to_csv_multiple_dataframes(sample_dataframes, temp_output_dir):
    """Test saving multiple DataFrames to CSV."""
    filenames = build_import_filenames(
        '202511', 'USEQ', ['Account', 'Location'], cycle_type='Quarterly'
    )

    result = save_dataframes_to_csv(
        sample_dataframes,
        filenames,
        output_dir=temp_output_dir
    )

    assert len(result) == 2
    assert result[0].name == 'Modeling_202511_Moodys_Quarterly_USEQ_Account.csv'
    assert result[1].name == 'Modeling_202511_Moodys_Quarterly_USEQ_Location.csv'

    # Verify both files exist
    for file_path in result:
        assert file_path.exists()

    # Verify first CSV (Account, tab-delimited)
    df_account = pd.read_csv(result[0], sep='\t')
    assert len(df_account) == 2
    assert 'ACCNTNUM' in df_account.columns


def test_save_dataframes_to_csv_adds_csv_extension(sample_dataframe, temp_output_dir):
    """Test that .csv extension is added if not present."""
    result = save_dataframes_to_csv(
        sample_dataframe,
        'test_file',  # No .csv extension
        output_dir=temp_output_dir
    )

    assert result[0].name == 'test_file.csv'
    assert result[0].exists()


def test_save_dataframes_to_csv_with_index(sample_dataframe, temp_output_dir):
    """Test saving DataFrame with index included."""
    result = save_dataframes_to_csv(
        sample_dataframe,
        'test_with_index',
        output_dir=temp_output_dir,
        index=True
    )

    # Read and verify index was saved (tab-delimited)
    df_loaded = pd.read_csv(result[0], sep='\t')
    assert 'Unnamed: 0' in df_loaded.columns  # pandas default index column name


def test_save_dataframes_to_csv_type_error_invalid_dataframes():
    """Test that TypeError is raised for invalid dataframes input."""
    with pytest.raises(TypeError, match="dataframes must be a DataFrame"):
        save_dataframes_to_csv("not a dataframe", "filename")


def test_save_dataframes_to_csv_type_error_invalid_item_in_list(sample_dataframe):
    """Test that TypeError is raised when list contains non-DataFrame."""
    with pytest.raises(TypeError, match="Item 1.*is not a DataFrame"):
        save_dataframes_to_csv([sample_dataframe, "not a dataframe"], ["f1", "f2"])


def test_save_dataframes_to_csv_value_error_mismatched_lengths(sample_dataframes):
    """Test that ValueError is raised when lengths don't match."""
    with pytest.raises(ValueError, match="Length of filenames.*must match"):
        save_dataframes_to_csv(
            sample_dataframes,  # 2 DataFrames
            ["only_one_filename"]  # 1 filename
        )


def test_save_dataframes_to_csv_without_output_dir(sample_dataframe):
    """Test that function uses get_working_files_path when output_dir is None."""
    with patch('helpers.csv_export.get_working_files_path') as mock_get_path:
        mock_path = Path(tempfile.mkdtemp())
        mock_get_path.return_value = mock_path

        try:
            result = save_dataframes_to_csv(
                sample_dataframe,
                'test_auto_path'
            )

            # Verify get_working_files_path was called
            mock_get_path.assert_called_once()

            # Verify file was created in the mocked path
            assert result[0].parent == mock_path
            assert result[0].exists()

        finally:
            shutil.rmtree(mock_path)


# ============================================================================
# Tests for save_sql_results_to_csv()
# ============================================================================

def test_save_sql_results_to_csv(sample_dataframes, temp_output_dir):
    """Test the convenience function that executes SQL and saves to CSV."""
    # Create a mock sqlserver module to avoid importing pyodbc
    mock_sqlserver = MagicMock()
    mock_execute = Mock(return_value=sample_dataframes)
    mock_sqlserver.execute_query_from_file = mock_execute

    with patch.dict(sys.modules, {'helpers.sqlserver': mock_sqlserver}):
        # Now when save_sql_results_to_csv imports from helpers.sqlserver, it gets our mock
        filenames = build_import_filenames(
            '202511', 'USEQ', ['Account', 'Location'], cycle_type='Quarterly'
        )

        result = save_sql_results_to_csv(
            sql_file='import_files/2_Create_USEQ_Moodys_ImportFile.sql',
            filenames=filenames,
            params={'DATE_VALUE': '202511', 'CYCLE_TYPE': 'Quarterly'},
            connection='DATABRIDGE',
            database='DW_EXP_MGMT_USER',
            output_dir=temp_output_dir
        )

        # Verify execute_query_from_file was called with correct parameters
        mock_execute.assert_called_once_with(
            file_path='import_files/2_Create_USEQ_Moodys_ImportFile.sql',
            params={'DATE_VALUE': '202511', 'CYCLE_TYPE': 'Quarterly'},
            connection='DATABRIDGE',
            database='DW_EXP_MGMT_USER'
        )

        # Verify CSV files were created
        assert len(result) == 2
        assert all(f.exists() for f in result)


def test_save_sql_results_to_csv_passes_all_parameters(sample_dataframe, temp_output_dir):
    """Test that all parameters are properly passed through."""
    # Create a mock sqlserver module to avoid importing pyodbc
    mock_sqlserver = MagicMock()
    mock_execute = Mock(return_value=[sample_dataframe])
    mock_sqlserver.execute_query_from_file = mock_execute

    with patch.dict(sys.modules, {'helpers.sqlserver': mock_sqlserver}):
        # Now when save_sql_results_to_csv imports from helpers.sqlserver, it gets our mock
        result = save_sql_results_to_csv(
            sql_file='test.sql',
            filenames='test_file',
            params={'param1': 'value1'},
            connection='TEST_CONN',
            database='TEST_DB',
            output_dir=temp_output_dir,
            index=True
        )

        # Verify parameters passed to execute_query_from_file
        mock_execute.assert_called_once()
        call_kwargs = mock_execute.call_args[1]
        assert call_kwargs['file_path'] == 'test.sql'
        assert call_kwargs['params'] == {'param1': 'value1'}
        assert call_kwargs['connection'] == 'TEST_CONN'
        assert call_kwargs['database'] == 'TEST_DB'

        # Verify CSV was created
        assert result[0].exists()

        # Verify index was saved (check by reading the CSV, tab-delimited)
        df_loaded = pd.read_csv(result[0], sep='\t')
        assert 'Unnamed: 0' in df_loaded.columns


# ============================================================================
# Integration Tests
# ============================================================================

def test_full_workflow_useq_example(temp_output_dir):
    """Integration test simulating the full USEQ export workflow."""
    # Create mock DataFrames
    df_account = pd.DataFrame({
        'ACCNTNUM': ['LOC001', 'LOC002'],
        'ACCNTNAME': ['Account1', 'Account2'],
        'POLICYNUM': ['POL001', 'POL002']
    })

    df_location = pd.DataFrame({
        'ACCNTNUM': ['LOC001', 'LOC002'],
        'LOCNUM': ['LOC001', 'LOC002'],
        'CITY': ['Miami', 'Tampa']
    })

    # Build filenames using naming convention
    filenames = build_import_filenames(
        date_value='202511',
        portfolio='USEQ',
        file_types=['Account', 'Location'],
        cycle_type='Quarterly'
    )

    # Save to CSV
    result = save_dataframes_to_csv(
        [df_account, df_location],
        filenames,
        output_dir=temp_output_dir
    )

    # Verify results
    assert len(result) == 2
    assert result[0].name == 'Modeling_202511_Moodys_Quarterly_USEQ_Account.csv'
    assert result[1].name == 'Modeling_202511_Moodys_Quarterly_USEQ_Location.csv'

    # Verify file contents (tab-delimited)
    df_account_loaded = pd.read_csv(result[0], sep='\t')
    assert len(df_account_loaded) == 2
    assert list(df_account_loaded['ACCNTNUM']) == ['LOC001', 'LOC002']


def test_full_workflow_ushu_with_modifiers(temp_output_dir):
    """Integration test simulating USHU export with Full/Leak modifiers."""
    # Create 4 mock DataFrames (Full and Leak, Account and Location)
    dataframes = [
        pd.DataFrame({'ACCNTNUM': ['A1'], 'TYPE': ['Full Account']}),
        pd.DataFrame({'LOCNUM': ['L1'], 'TYPE': ['Full Location']}),
        pd.DataFrame({'ACCNTNUM': ['A2'], 'TYPE': ['Leak Account']}),
        pd.DataFrame({'LOCNUM': ['L2'], 'TYPE': ['Leak Location']})
    ]

    # Build filenames
    filenames = build_import_filenames(
        date_value='202509',
        portfolio='USHU',
        file_types=['Account', 'Location', 'Account', 'Location'],
        cycle_type='Quarterly',
        modifiers=['Full', 'Full', 'Leak', 'Leak']
    )

    # Save to CSV
    result = save_dataframes_to_csv(
        dataframes,
        filenames,
        output_dir=temp_output_dir
    )

    # Verify results
    assert len(result) == 4
    expected_names = [
        'Modeling_202509_Moodys_Quarterly_USHU_Full_Account.csv',
        'Modeling_202509_Moodys_Quarterly_USHU_Full_Location.csv',
        'Modeling_202509_Moodys_Quarterly_USHU_Leak_Account.csv',
        'Modeling_202509_Moodys_Quarterly_USHU_Leak_Location.csv'
    ]

    for csv_path, expected_name in zip(result, expected_names):
        assert csv_path.name == expected_name
        assert csv_path.exists()
