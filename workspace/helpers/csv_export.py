"""
CSV Export Helper Module

Provides functionality to export pandas DataFrames to CSV files in the appropriate
working files directory, with automatic path resolution using WorkContext.

This module is designed to work with the output of execute_query_from_file() from
the sqlserver module, which returns a list of DataFrames.
"""

from pathlib import Path
from typing import List, Optional, Dict, Any, Union
import pandas as pd
from helpers.context import WorkContext


def get_working_files_path(notebook_path: Optional[Path] = None) -> Path:
    """
    Get the path to the files/working_files directory relative to the current notebook.

    This function uses WorkContext to automatically determine the current notebook's
    location and navigates to the appropriate files/working_files directory.

    For Active cycle workflows:
        .../workflows/Active_{cycle}/notebooks/Stage_XX_*/Step_XX_*.ipynb
        → .../workflows/Active_{cycle}/files/working_files/

    For _Tools workflows:
        .../workflows/_Tools/{folder}/Notebook.ipynb
        → .../workflows/_Tools/files/working_files/

    Args:
        notebook_path: Optional explicit notebook path. If None, uses WorkContext
                      to auto-detect the current notebook.

    Returns:
        Path object pointing to the files/working_files directory

    Raises:
        ValueError: If the working_files directory cannot be determined
    """
    if notebook_path is None:
        try:
            context = WorkContext()
            notebook_path = context.notebook_path
        except Exception as e:
            raise ValueError(
                f"Could not determine current notebook path via WorkContext: {e}"
            )

    if not isinstance(notebook_path, Path):
        notebook_path = Path(notebook_path)

    # Navigate from notebook to the workflow root
    # For notebooks in Stage folders: notebook → Stage_XX_* → notebooks → workflow_root
    # For notebooks in _Tools: notebook → tool_folder → _Tools

    current = notebook_path.parent

    # Check if we're in a Stage folder (part of Active cycle structure)
    if current.name.startswith('Stage_'):
        # Go up two levels: Stage_XX_* → notebooks → workflow_root
        workflow_root = current.parent.parent
    else:
        # We're in _Tools or similar - go up one level to the workflow folder
        workflow_root = current.parent

    # Construct path to working_files
    working_files_path = workflow_root / 'files' / 'working_files'

    # Create the directory if it doesn't exist
    working_files_path.mkdir(parents=True, exist_ok=True)

    return working_files_path


def build_import_filename(
    date_value: str,
    portfolio: str,
    file_type: str,
    cycle_type: Optional[str] = None,
    modifier: Optional[str] = None
) -> str:
    """
    Build a standardized Moody's import filename following the naming convention.

    Standard pattern: Modeling_{date_value}_Moodys_{cycle_type}_{portfolio}_{modifier}_{file_type}

    The cycle_type and modifier components are optional and will be omitted if not provided.

    Args:
        date_value: Date value (e.g., '202511', '202503')
        portfolio: Portfolio code (e.g., 'USEQ', 'USHU', 'CBHU', 'USFL')
        file_type: File type, typically 'Account' or 'Location'
        cycle_type: Optional cycle type (e.g., 'Quarterly', 'Annual')
        modifier: Optional modifier (e.g., 'Full', 'Leak', 'Commercial_Excess', 'Other')

    Returns:
        Filename string (without .csv extension)

    Examples:
        # Basic format (no cycle_type)
        build_import_filename('202503', 'CBHU', 'Account')
        # Returns: 'Modeling_202503_Moodys_CBHU_Account'

        # With cycle_type
        build_import_filename('202511', 'USEQ', 'Location', cycle_type='Quarterly')
        # Returns: 'Modeling_202511_Moodys_Quarterly_USEQ_Location'

        # With cycle_type and modifier
        build_import_filename('202509', 'USHU', 'Account', cycle_type='Quarterly', modifier='Full')
        # Returns: 'Modeling_202509_Moodys_Quarterly_USHU_Full_Account'

        # With modifier but no cycle_type
        build_import_filename('202509', 'USHU', 'Location', modifier='Leak')
        # Returns: 'Modeling_202509_Moodys_USHU_Leak_Location'
    """
    parts = ['Modeling', date_value, 'Moodys']

    if cycle_type:
        parts.append(cycle_type)

    parts.append(portfolio)

    if modifier:
        parts.append(modifier)

    parts.append(file_type)

    return '_'.join(parts)


def build_import_filenames(
    date_value: str,
    portfolio: str,
    file_types: List[str],
    cycle_type: Optional[str] = None,
    modifiers: Optional[List[Optional[str]]] = None
) -> List[str]:
    """
    Build multiple standardized Moody's import filenames at once.

    This is a convenience function for building multiple related filenames
    (e.g., Account and Location files for the same portfolio).

    Args:
        date_value: Date value (e.g., '202511', '202503')
        portfolio: Portfolio code (e.g., 'USEQ', 'USHU', 'CBHU')
        file_types: List of file types (e.g., ['Account', 'Location'])
        cycle_type: Optional cycle type (e.g., 'Quarterly', 'Annual')
        modifiers: Optional list of modifiers, one per file_type. If provided,
                  must be same length as file_types. Use None for files without modifiers.

    Returns:
        List of filename strings (without .csv extensions)

    Examples:
        # Simple Account + Location pair
        build_import_filenames('202511', 'USEQ', ['Account', 'Location'], cycle_type='Quarterly')
        # Returns: [
        #   'Modeling_202511_Moodys_Quarterly_USEQ_Account',
        #   'Modeling_202511_Moodys_Quarterly_USEQ_Location'
        # ]

        # With different modifiers for each file
        build_import_filenames(
            '202509', 'USHU',
            ['Account', 'Location', 'Account', 'Location'],
            cycle_type='Quarterly',
            modifiers=['Full', 'Full', 'Leak', 'Leak']
        )
        # Returns: [
        #   'Modeling_202509_Moodys_Quarterly_USHU_Full_Account',
        #   'Modeling_202509_Moodys_Quarterly_USHU_Full_Location',
        #   'Modeling_202509_Moodys_Quarterly_USHU_Leak_Account',
        #   'Modeling_202509_Moodys_Quarterly_USHU_Leak_Location'
        # ]

    Raises:
        ValueError: If modifiers list length doesn't match file_types length
    """
    if modifiers is not None and len(modifiers) != len(file_types):
        raise ValueError(
            f"Length of modifiers ({len(modifiers)}) must match "
            f"length of file_types ({len(file_types)})"
        )

    if modifiers is None:
        modifiers = [None] * len(file_types)

    return [
        build_import_filename(date_value, portfolio, file_type, cycle_type, modifier)
        for file_type, modifier in zip(file_types, modifiers)
    ]


def save_dataframes_to_csv(
    dataframes: Union[pd.DataFrame, List[pd.DataFrame]],
    filenames: Union[str, List[str]],
    output_dir: Optional[Union[str, Path]] = None,
    index: bool = False
) -> List[Path]:
    """
    Save one or more pandas DataFrames to CSV files in the working files directory.

    This function is designed to work with the output of execute_query_from_file(),
    which returns a list of DataFrames (one per SELECT statement in the SQL script).

    The filenames should match the table names being queried (without .csv extension).
    Use build_import_filename() or build_import_filenames() to generate standardized
    filenames following the Moody's import file naming convention.

    Args:
        dataframes: Single DataFrame or list of DataFrames to save
        filenames: Single filename or list of filenames (without .csv extension).
                  Use build_import_filename() to generate standardized names.
        output_dir: Optional explicit output directory. If None, uses get_working_files_path()
                   to auto-detect based on current notebook location
        index: Whether to include DataFrame index in CSV (default: False)

    Returns:
        List of Path objects for the created CSV files

    Examples:
        # Single DataFrame
        filename = build_import_filename('202511', 'USEQ', 'Account', cycle_type='Quarterly')
        save_dataframes_to_csv(df, filename)
        # → Modeling_202511_Moodys_Quarterly_USEQ_Account.csv

        # Multiple DataFrames
        filenames = build_import_filenames(
            '202511', 'USEQ', ['Account', 'Location'], cycle_type='Quarterly'
        )
        save_dataframes_to_csv([df_account, df_location], filenames)
        # → Modeling_202511_Moodys_Quarterly_USEQ_Account.csv
        # → Modeling_202511_Moodys_Quarterly_USEQ_Location.csv

        # Complex example with modifiers
        filenames = build_import_filenames(
            '202509', 'USHU',
            ['Account', 'Location', 'Account', 'Location'],
            cycle_type='Quarterly',
            modifiers=['Full', 'Full', 'Leak', 'Leak']
        )
        save_dataframes_to_csv(
            [df_full_acct, df_full_loc, df_leak_acct, df_leak_loc],
            filenames
        )

    Raises:
        ValueError: If filenames list length doesn't match number of DataFrames
        TypeError: If dataframes is not a DataFrame or list of DataFrames
    """
    # Normalize to lists
    if isinstance(dataframes, pd.DataFrame):
        dataframes = [dataframes]
    elif not isinstance(dataframes, list):
        raise TypeError(
            f"dataframes must be a DataFrame or list of DataFrames, got {type(dataframes)}"
        )

    if isinstance(filenames, str):
        filenames = [filenames]
    elif not isinstance(filenames, list):
        raise TypeError(
            f"filenames must be a string or list of strings, got {type(filenames)}"
        )

    # Validate all items are DataFrames
    for i, df in enumerate(dataframes):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Item {i} in dataframes list is not a DataFrame, got {type(df)}"
            )

    # Validate lengths match
    if len(filenames) != len(dataframes):
        raise ValueError(
            f"Length of filenames ({len(filenames)}) must match "
            f"number of DataFrames ({len(dataframes)})"
        )

    # Determine output directory
    if output_dir is None:
        output_path = get_working_files_path()
    else:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

    # Save each DataFrame
    created_files = []

    for df, filename in zip(dataframes, filenames):
        # Add .csv extension if not present
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"

        # Full path
        file_path = output_path / filename

        # Save as tab-delimited to avoid issues with commas in data
        # Moody's accepts delimiter="TAB" for import files
        df.to_csv(file_path, index=index, sep='\t')

        created_files.append(file_path)

    return created_files


def save_sql_results_to_csv(
    sql_file: Union[str, Path],
    filenames: Union[str, List[str]],
    params: Dict[str, Any],
    connection: str,
    database: Optional[str] = None,
    output_dir: Optional[Union[str, Path]] = None,
    index: bool = False
) -> List[Path]:
    """
    Convenience function to execute a SQL file and save results to CSV in one call.

    This combines execute_query_from_file() and save_dataframes_to_csv() into a
    single operation.

    Use build_import_filename() or build_import_filenames() to generate standardized
    filenames following the Moody's import file naming convention.

    Args:
        sql_file: Path to SQL file (relative to workspace/sql/ or absolute)
        filenames: Filename(s) for CSV files (without .csv extension).
                  Use build_import_filename() or build_import_filenames() to create.
        params: Parameters for SQL substitution ({{ param_name }} syntax)
        connection: SQL Server connection name (from environment variables)
        database: Optional database name to use
        output_dir: Optional output directory (auto-detected if None)
        index: Whether to include DataFrame index in CSV

    Returns:
        List of Path objects for created CSV files

    Example:
        # Generate filenames using naming convention
        filenames = build_import_filenames(
            date_value='202511',
            portfolio='USEQ',
            file_types=['Account', 'Location'],
            cycle_type='Quarterly'
        )

        # Execute SQL and save results
        csv_files = save_sql_results_to_csv(
            sql_file='import_files/2_Create_USEQ_Moodys_ImportFile.sql',
            filenames=filenames,
            params={'DATE_VALUE': '202511', 'CYCLE_TYPE': 'Quarterly'},
            connection='DATABRIDGE',
            database='DW_EXP_MGMT_USER'
        )
    """
    from helpers.sqlserver import execute_query_from_file

    # Execute SQL and get DataFrames
    dataframes = execute_query_from_file(
        file_path=sql_file,
        params=params,
        connection=connection,
        database=database
    )

    # Save to CSV
    return save_dataframes_to_csv(
        dataframes=dataframes,
        filenames=filenames,
        output_dir=output_dir,
        index=index
    )