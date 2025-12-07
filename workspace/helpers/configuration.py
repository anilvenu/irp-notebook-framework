"""
IRP Notebook Framework - Configuration Management

ARCHITECTURE:
-------------
Layer 2 (CRUD): read_configuration, create_configuration, update_configuration_status
Layer 3 (Workflow): load_configuration_file (reads Excel + creates config)

TRANSACTION BEHAVIOR:
--------------------
- All CRUD functions (Layer 2) never manage transactions
- They are safe to call within or outside transaction_context()
- load_configuration_file (Layer 3) performs multiple operations but does NOT use transaction
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

# Database imports are lazy to avoid connection on module import
# Functions that need database will import these locally
# from helpers.database import execute_query, execute_command, execute_insert, DatabaseError
from helpers.constants import (
    ConfigurationStatus, CONFIGURATION_TAB_LIST,
    EXCEL_VALIDATION_SCHEMAS, VALIDATION_ERROR_CODES, BatchType
)
from helpers.irp_integration import IRPClient
from helpers.irp_integration.exceptions import IRPAPIError


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


# ============================================================================
# TRANSFORMERS - Helper Functions
# ============================================================================

def _extract_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata from configuration.

    Args:
        config: Configuration dictionary

    Returns:
        Metadata dictionary
    """
    return config.get('Metadata', {})


def get_base_portfolios(portfolios: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter base portfolios from a list of portfolio dictionaries.

    Args:
        portfolios: List of portfolio dictionaries with keys:
                   - "Portfolio": Portfolio name
                   - "Database": Database name
                   - "Base Portfolio?": "Y" (base) or "N" (not base)

    Returns:
        List of portfolio dictionaries where "Base Portfolio?" == "Y"
    """
    return [p for p in portfolios if p.get('Base Portfolio?') == 'Y']


# ============================================================================
# TRANSFORMERS - Batch Type Functions
# ============================================================================

def transform_edm_creation(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for EDM Creation batch type.
    Creates one job configuration per database row.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per database)
    """
    metadata = _extract_metadata(config)
    databases = config.get('Databases', [])

    job_configs = []
    for db_row in databases:
        job_config = {
            'Metadata': metadata,
            **db_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_portfolio_creation(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Portfolio Creation batch type.
    Creates one job configuration per portfolio row.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per portfolio)
    """
    metadata = _extract_metadata(config)
    portfolios = config.get('Portfolios', [])
    base_portfolios = get_base_portfolios(portfolios)

    job_configs = []
    for portfolio_row in base_portfolios:
        job_config = {
            'Metadata': metadata,
            **portfolio_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_mri_import(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for MRI Import batch type.
    Creates one job configuration per portfolio row.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per portfolio)
    """
    metadata = _extract_metadata(config)
    portfolios = config.get('Portfolios', [])
    base_portfolios = get_base_portfolios(portfolios)

    # Extract date_value from Metadata
    date_value = metadata.get('Current Date Value', '')

    job_configs = []
    for portfolio_row in base_portfolios:
        # Extract import_file_value from portfolio row
        import_file_value = portfolio_row.get('Import File', '')

        # Build CSV filenames
        accounts_import_file = f"Modeling_{date_value}_Moodys_{import_file_value}_Account.csv"
        locations_import_file = f"Modeling_{date_value}_Moodys_{import_file_value}_Location.csv"

        job_config = {
            'Metadata': metadata,
            **portfolio_row,
            'accounts_import_file': accounts_import_file,
            'locations_import_file': locations_import_file
        }
        job_configs.append(job_config)

    return job_configs


def transform_create_reinsurance_treaties(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Create Reinsurance Treaties batch type.
    Creates one job configuration per unique treaty-EDM combination.

    The treaty definitions come from the "Reinsurance Treaties" sheet, but the
    EDM associations are inferred from the "Analysis Table" sheet which has
    columns: Database, Reinsurance Treaty 1-5.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per unique treaty-EDM combination)
    """
    metadata = _extract_metadata(config)
    treaties = config.get('Reinsurance Treaties', [])
    analyses = config.get('Analysis Table', [])

    # Build a lookup of treaty data by name
    treaty_by_name = {t.get('Treaty Name'): t for t in treaties if t.get('Treaty Name')}

    # Collect unique treaty-EDM combinations from Analysis Table
    treaty_edm_combinations = set()
    treaty_columns = ['Reinsurance Treaty 1', 'Reinsurance Treaty 2', 'Reinsurance Treaty 3',
                      'Reinsurance Treaty 4', 'Reinsurance Treaty 5']

    for analysis in analyses:
        edm = analysis.get('Database')
        if not edm:
            continue
        for col in treaty_columns:
            treaty_name = analysis.get(col)
            if treaty_name and treaty_name in treaty_by_name:
                treaty_edm_combinations.add((treaty_name, edm))

    # Create job configs for each unique treaty-EDM combination
    job_configs = []
    for treaty_name, edm in sorted(treaty_edm_combinations):
        treaty_data = treaty_by_name[treaty_name]
        job_config = {
            'Metadata': metadata,
            'Database': edm,
            **treaty_data
        }
        job_configs.append(job_config)

    return job_configs


def transform_edm_db_upgrade(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for EDM DB Upgrade batch type.
    Creates one job configuration per database row.

    The target EDM version is extracted from Metadata['EDM Data Version'].
    The version format "22.0.0" is converted to "22" for the API.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per database)
    """
    metadata = _extract_metadata(config)
    databases = config.get('Databases', [])

    # Extract target version from metadata (e.g., "22.0.0" -> "22")
    edm_version_full = metadata.get('EDM Data Version', '')
    target_version = edm_version_full.split('.')[0] if '.' in edm_version_full else edm_version_full

    job_configs = []
    for db_row in databases:
        job_config = {
            'Metadata': metadata,
            'target_edm_version': target_version,
            **db_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_geohaz(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for GeoHaz batch type.
    Creates one job configuration per base portfolio.

    The geocode version is extracted from Metadata['Geocode Version'].

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per base portfolio)
    """
    metadata = _extract_metadata(config)
    portfolios = config.get('Portfolios', [])

    # Filter to base portfolios only
    base_portfolios = get_base_portfolios(portfolios)

    # Extract geocode version from metadata (convert "22.0.0" to "22.0")
    geocode_version_full = metadata.get('Geocode Version', '22.0')
    version_parts = geocode_version_full.split('.')
    geocode_version = '.'.join(version_parts[:2]) if len(version_parts) >= 2 else geocode_version_full

    job_configs = []
    for portfolio_row in base_portfolios:
        job_config = {
            'Metadata': metadata,
            'geocode_version': geocode_version,
            **portfolio_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_portfolio_mapping(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Portfolio Mapping batch type.
    Creates one job configuration per base portfolio.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per base portfolio)
    """
    metadata = _extract_metadata(config)
    portfolios = config.get('Portfolios', [])

    # Filter to base portfolios only
    base_portfolios = get_base_portfolios(portfolios)

    job_configs = []
    for portfolio_row in base_portfolios:
        job_config = {
            'Metadata': metadata,
            **portfolio_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_analysis(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Analysis batch type.
    Creates one job configuration per analysis table row.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per analysis)
    """
    metadata = _extract_metadata(config)
    analysis_table = config.get('Analysis Table', [])

    job_configs = []
    for analysis_row in analysis_table:
        job_config = {
            'Metadata': metadata,
            **analysis_row
        }
        job_configs.append(job_config)

    return job_configs


def _build_analysis_edm_map(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build a mapping of analysis names to their EDM (database) names.

    The mapping is built from the Analysis Table sheet, which contains
    'Analysis Name' and 'Database' columns. This mapping is used when
    looking up analyses in Moody's API, since analysis names are only
    unique within an EDM.

    Args:
        config: Configuration dictionary containing 'Analysis Table'

    Returns:
        Dict mapping analysis names to EDM names
    """
    analysis_table = config.get('Analysis Table', [])
    analysis_edm_map = {}

    for row in analysis_table:
        analysis_name = row.get('Analysis Name')
        edm_name = row.get('Database')
        if analysis_name and pd.notna(analysis_name) and edm_name and pd.notna(edm_name):
            analysis_edm_map[analysis_name] = edm_name

    return analysis_edm_map


def _get_group_names(config: Dict[str, Any]) -> set:
    """
    Get the set of all group names from the Groupings sheet.

    Args:
        config: Configuration dictionary containing 'Groupings'

    Returns:
        Set of group names
    """
    groupings = config.get('Groupings', [])
    return {
        g.get('Group_Name') for g in groupings
        if g.get('Group_Name') and pd.notna(g.get('Group_Name'))
    }


def classify_groupings(config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Classify groupings into analysis-only groups and rollup groups.

    Analysis-only groups: All items reference analysis names (from Analysis Table)
    Rollup groups: At least one item references another group name (from Groupings)

    Args:
        config: Configuration dictionary

    Returns:
        Tuple of (analysis_only_groups, rollup_groups)
    """
    groupings = config.get('Groupings', [])
    analysis_table = config.get('Analysis Table', [])

    # Build set of valid analysis names
    analysis_names = {
        a.get('Analysis Name') for a in analysis_table
        if a.get('Analysis Name') and pd.notna(a.get('Analysis Name'))
    }

    # Build set of group names
    group_names = _get_group_names(config)

    analysis_only_groups = []
    rollup_groups = []

    for group in groupings:
        items = group.get('items', [])
        if not items:
            continue

        # Check if any item is a group name
        has_group_reference = any(item in group_names for item in items)

        if has_group_reference:
            rollup_groups.append(group)
        else:
            # All items must be analysis names (or portfolios, but we treat as analysis-only)
            analysis_only_groups.append(group)

    return analysis_only_groups, rollup_groups


def transform_grouping(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Grouping batch type.
    Creates one job configuration per ANALYSIS-ONLY group.

    Analysis-only groups are groups where ALL items reference analysis names
    (not other group names). These can be created immediately since analyses
    already exist.

    Each job config includes:
    - analysis_edm_map: Mapping of analysis names to EDM names (for API lookup)
    - group_names: Set of all group names (to distinguish groups from analyses)

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per analysis-only group)
    """
    metadata = _extract_metadata(config)
    analysis_only_groups, _ = classify_groupings(config)

    # Build lookup maps for API calls
    analysis_edm_map = _build_analysis_edm_map(config)
    group_names = list(_get_group_names(config))  # Convert to list for JSON serialization

    job_configs = []
    for group_row in analysis_only_groups:
        job_config = {
            'Metadata': metadata,
            'analysis_edm_map': analysis_edm_map,
            'group_names': group_names,
            **group_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_grouping_rollup(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Grouping Rollup batch type.
    Creates one job configuration per ROLLUP group.

    Rollup groups are groups that contain references to other groups.
    These must be created AFTER the child groups exist (requires chaining).

    Each job config includes:
    - analysis_edm_map: Mapping of analysis names to EDM names (for API lookup)
    - group_names: Set of all group names (to distinguish groups from analyses)

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per rollup group)
    """
    metadata = _extract_metadata(config)
    _, rollup_groups = classify_groupings(config)

    # Build lookup maps for API calls
    analysis_edm_map = _build_analysis_edm_map(config)
    group_names = list(_get_group_names(config))  # Convert to list for JSON serialization

    job_configs = []
    for group_row in rollup_groups:
        job_config = {
            'Metadata': metadata,
            'analysis_edm_map': analysis_edm_map,
            'group_names': group_names,
            **group_row
        }
        job_configs.append(job_config)

    return job_configs


def transform_export_to_rdm(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Export to RDM batch type.

    The Moody's RDM export API has a limit of 100 analyses per request.
    When there are more than 100 items to export, this creates:
    - A "seed job" with 1 analysis (creates the RDM)
    - Remaining jobs with up to 100 analyses each (append to RDM using database_id)

    The job exports to the RDM specified in Metadata['Export RDM Name'].
    Analysis names come from the Analysis Table sheet.
    Group names come from the Groupings sheet.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (1 if <=100 items, multiple if >100)
    """
    CHUNK_SIZE = 100

    metadata = _extract_metadata(config)
    analysis_table = config.get('Analysis Table', [])
    groupings = config.get('Groupings', [])

    # Collect all analysis names
    analysis_names = [
        a.get('Analysis Name') for a in analysis_table
        if a.get('Analysis Name') and pd.notna(a.get('Analysis Name'))
    ]

    # Collect all group names
    group_names = [
        g.get('Group_Name') for g in groupings
        if g.get('Group_Name') and pd.notna(g.get('Group_Name'))
    ]

    # Combine into single export list (analyses + groups)
    all_export_names = analysis_names + group_names

    rdm_name = metadata.get('Export RDM Name')
    server_name = 'databridge-1'

    # If within limit, create single job (no seed needed)
    if len(all_export_names) <= CHUNK_SIZE:
        return [{
            'Metadata': metadata,
            'rdm_name': rdm_name,
            'server_name': server_name,
            'analysis_names': all_export_names,
            'analysis_count': len(analysis_names),
            'group_count': len(group_names),
            'is_seed_job': False,
            'database_id': None
        }]

    # Chunking needed: seed job (1 analysis) + remaining chunks
    job_configs = []

    # Seed job: first analysis only (creates the RDM)
    job_configs.append({
        'Metadata': metadata,
        'rdm_name': rdm_name,
        'server_name': server_name,
        'analysis_names': [all_export_names[0]],
        'analysis_count': 1 if analysis_names else 0,
        'group_count': 1 if not analysis_names and group_names else 0,
        'is_seed_job': True,
        'database_id': None
    })

    # Remaining items in chunks of up to 100
    remaining = all_export_names[1:]
    chunks = [remaining[i:i+CHUNK_SIZE] for i in range(0, len(remaining), CHUNK_SIZE)]

    for chunk in chunks:
        job_configs.append({
            'Metadata': metadata,
            'rdm_name': rdm_name,
            'server_name': server_name,
            'analysis_names': chunk,
            'analysis_count': len([n for n in chunk if n in analysis_names]),
            'group_count': len([n for n in chunk if n in group_names]),
            'is_seed_job': False,
            'database_id': None  # Will be populated after seed job completes
        })

    return job_configs


def transform_staging_etl(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform configuration for Staging ETL batch type.
    Creates one job configuration per database row.

    Args:
        config: Configuration dictionary

    Returns:
        List of job configurations (one per database)
    """
    metadata = _extract_metadata(config)
    databases = config.get('Databases', [])

    job_configs = []
    for db_row in databases:
        job_config = {
            'Metadata': metadata,
            **db_row
        }
        job_configs.append(job_config)

    return job_configs


# ============================================================================
# TRANSFORMERS - Test-Only Functions
# ============================================================================

def transform_test_default(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Test-only transformer: creates single job with config as-is.

    Used for testing batch/job workflows without coupling to business logic.

    Args:
        config: Configuration dictionary

    Returns:
        List containing a single job configuration (copy of input)
    """
    return [config.copy()]


def transform_test_multi_job(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Test-only transformer: creates multiple jobs from 'jobs' list.

    Used for testing multi-job batch handling. If config has a 'jobs' key
    with a list, returns that list. Otherwise, returns single job.

    Args:
        config: Configuration dictionary with optional 'jobs' key

    Returns:
        List of job configurations
    """
    if 'jobs' in config and isinstance(config['jobs'], list):
        return config['jobs']
    return [config.copy()]


# ============================================================================
# TRANSFORMERS - Batch Type Registry
# ============================================================================

BATCH_TYPE_TRANSFORMERS = {
    # Business transformers (12)
    BatchType.EDM_CREATION: transform_edm_creation,
    BatchType.PORTFOLIO_CREATION: transform_portfolio_creation,
    BatchType.MRI_IMPORT: transform_mri_import,
    BatchType.CREATE_REINSURANCE_TREATIES: transform_create_reinsurance_treaties,
    BatchType.EDM_DB_UPGRADE: transform_edm_db_upgrade,
    BatchType.GEOHAZ: transform_geohaz,
    BatchType.PORTFOLIO_MAPPING: transform_portfolio_mapping,
    BatchType.ANALYSIS: transform_analysis,
    BatchType.GROUPING: transform_grouping,
    BatchType.GROUPING_ROLLUP: transform_grouping_rollup,
    BatchType.EXPORT_TO_RDM: transform_export_to_rdm,
    BatchType.STAGING_ETL: transform_staging_etl,
    # Test-only transformers (2)
    BatchType.TEST_DEFAULT: transform_test_default,
    BatchType.TEST_MULTI_JOB: transform_test_multi_job,
}


def create_job_configurations(
    batch_type: str,
    configuration: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Transform configuration into job configurations based on batch type.

    Args:
        batch_type: The type of batch (e.g., 'EDM Creation', 'Portfolio Creation', etc.)
        configuration: The configuration dictionary to transform

    Returns:
        List of job configuration dictionaries

    Raises:
        ConfigurationError: If batch type is not recognized
    """
    if batch_type not in BATCH_TYPE_TRANSFORMERS:
        available_types = list(BATCH_TYPE_TRANSFORMERS.keys())
        raise ConfigurationError(
            f"Unknown batch type '{batch_type}'. Available types: {available_types}"
        )

    transformer_func = BATCH_TYPE_TRANSFORMERS[batch_type]
    return transformer_func(configuration)


# ============================================================================
# VALIDATION HELPER FUNCTIONS
# ============================================================================

def _check_type(value: Any, expected_type: str) -> bool:
    """
    Check if value matches expected type.

    Args:
        value: Value to check
        expected_type: Expected type ('string', 'integer', 'float', 'date')

    Returns:
        True if type matches, False otherwise
    """
    if pd.isna(value):
        return True  # Null handling is separate

    if expected_type == 'string':
        return isinstance(value, str)
    elif expected_type == 'integer':
        return isinstance(value, (int, np.integer)) and not isinstance(value, bool)
    elif expected_type == 'float':
        return isinstance(value, (float, np.floating, int, np.integer)) and not isinstance(value, bool)
    elif expected_type == 'date':
        return isinstance(value, (datetime, pd.Timestamp))

    return False


def _format_error(code: str, **kwargs) -> str:
    """
    Format error message using error code and parameters.

    Args:
        code: Error code (e.g., 'STRUCT-001')
        **kwargs: Parameters to format into error message

    Returns:
        Formatted error message
    """
    template = VALIDATION_ERROR_CODES.get(code, f"{code}: {{error}}")
    try:
        return template.format(**kwargs)
    except KeyError:
        return f"{code}: {kwargs}"


def _convert_pandas_types(data: Any) -> Any:
    """
    Convert pandas and numpy types to JSON-serializable Python types.

    Converts:
    - pd.Timestamp -> ISO 8601 string format "2025-10-15T00:00:00.000Z"
    - np.integer, np.floating -> Python int/float
    - pd.NaT, np.nan, pd.NA -> None

    Args:
        data: Data to convert (can be dict, list, or primitive)

    Returns:
        Converted data with all pandas/numpy types replaced
    """
    if isinstance(data, pd.Timestamp):
        # Convert to ISO format: 2025-10-15T00:00:00.000Z
        # Use strftime for formatting, then add milliseconds (.000) and Z suffix
        return data.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    elif isinstance(data, (np.integer, np.floating)):
        # Convert numpy types to Python native types
        return data.item()
    elif isinstance(data, dict):
        # Recursively convert dictionary values
        return {k: _convert_pandas_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        # Recursively convert list items
        return [_convert_pandas_types(item) for item in data]
    elif pd.isna(data):
        # Convert NaN, NaT, NA to None
        return None
    else:
        # Return primitive types as-is
        return data


# ============================================================================
# SHEET VALIDATORS
# ============================================================================

def _validate_key_value(df: pd.DataFrame, schema: Dict[str, Any], sheet_name: str) -> Tuple[bool, List[str], List[str], Optional[Dict]]:
    """
    Validate key-value pair structure (e.g., Metadata sheet).

    Args:
        df: DataFrame containing sheet data (no header)
        schema: Validation schema dictionary
        sheet_name: Name of the sheet being validated

    Returns:
        Tuple of (is_valid, errors, warnings, parsed_data)
    """
    errors = []
    warnings = []

    # Convert to dict (no header row, first column is keys, second is values)
    data = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))

    # Check required keys
    for key in schema.get('required_keys', []):
        if key not in data:
            errors.append(_format_error('STRUCT-003', key=key, sheet_name=sheet_name))

    # Check value types
    for key, expected_type in schema.get('value_types', {}).items():
        if key in data:
            if not _check_type(data[key], expected_type):
                errors.append(_format_error('TYPE-002', key=key, sheet_name=sheet_name, expected=expected_type))

    # Check patterns
    for key, pattern in schema.get('value_patterns', {}).items():
        if key in data and pd.notna(data[key]):
            if not re.match(pattern, str(data[key])):
                errors.append(_format_error('FMT-001', value=data[key], pattern=pattern, field=f"{sheet_name}.{key}"))

    is_valid = len(errors) == 0
    result_data = data if is_valid else None

    # Convert pandas types to JSON-serializable Python types
    if result_data is not None:
        result_data = _convert_pandas_types(result_data)

    return (is_valid, errors, warnings, result_data)


def _validate_table(df: pd.DataFrame, schema: Dict[str, Any], sheet_name: str) -> Tuple[bool, List[str], List[str], Optional[List[Dict]]]:
    """
    Validate table structure (e.g., Databases, Portfolios, Analysis Table).

    Args:
        df: DataFrame containing sheet data
        schema: Validation schema dictionary
        sheet_name: Name of the sheet being validated

    Returns:
        Tuple of (is_valid, errors, warnings, parsed_data)
    """
    errors = []
    warnings = []

    # Check required columns
    missing_cols = [col for col in schema.get('required_columns', []) if col not in df.columns]
    if missing_cols:
        errors.append(_format_error('STRUCT-002', columns=missing_cols, sheet_name=sheet_name))
        return (False, errors, warnings, None)

    # Check nullable constraints
    nullable_config = schema.get('nullable', {})
    for col in schema.get('required_columns', []):
        is_nullable = nullable_config.get(col, True)
        if not is_nullable and df[col].isnull().any():
            errors.append(_format_error('NULL-001', column=col, sheet_name=sheet_name))

    # Check uniqueness
    for col in schema.get('unique_columns', []):
        if col in df.columns and df[col].duplicated().any():
            duplicates = df[col][df[col].duplicated()].unique().tolist()
            errors.append(f"{sheet_name}: Duplicate values in '{col}': {duplicates}")

    # Check value patterns
    for col, pattern in schema.get('value_patterns', {}).items():
        if col in df.columns:
            for idx, val in df[col].items():
                if pd.notna(val) and not re.match(pattern, str(val)):
                    errors.append(_format_error('FMT-001', value=val, pattern=pattern, field=f"{sheet_name}.{col} row {idx+2}"))

    # Check range constraints
    for col, (min_val, max_val) in schema.get('range_constraints', {}).items():
        if col in df.columns:
            for idx, val in df[col].items():
                if pd.notna(val):
                    try:
                        numeric_val = float(val)
                        if not (min_val <= numeric_val <= max_val):
                            errors.append(_format_error('RANGE-001', value=val, field=f"{sheet_name}.{col}", min=min_val, max=max_val))
                    except (ValueError, TypeError):
                        errors.append(f"{sheet_name}: Non-numeric value '{val}' in column '{col}' (expected numeric)")

    # Check date ordering (for Reinsurance Treaties)
    if sheet_name == 'Reinsurance Treaties':
        for idx, row in df.iterrows():
            if pd.notna(row.get('Inception Date')) and pd.notna(row.get('Expiration Date')):
                if row['Expiration Date'] <= row['Inception Date']:
                    errors.append(_format_error('BUS-002', treaty_name=row.get('Treaty Name', f'Row {idx+2}')))

    is_valid = len(errors) == 0
    data = df.to_dict('records') if is_valid or len(errors) < 5 else None

    # Convert pandas types to JSON-serializable Python types
    if data is not None:
        data = _convert_pandas_types(data)

    return (is_valid, errors, warnings, data)


def _validate_groupings(df: pd.DataFrame, schema: Dict[str, Any], sheet_name: str) -> Tuple[bool, List[str], List[str], Optional[List[Dict]]]:
    """
    Validate Groupings structure (special dict-of-lists with items).

    Args:
        df: DataFrame containing sheet data
        schema: Validation schema dictionary
        sheet_name: Name of the sheet being validated

    Returns:
        Tuple of (is_valid, errors, warnings, parsed_data)
    """
    errors = []
    warnings = []

    key_col = schema['key_column']

    # Check key column exists
    if key_col not in df.columns:
        errors.append(_format_error('STRUCT-002', columns=[key_col], sheet_name=sheet_name))
        return (False, errors, warnings, None)

    # Check for nulls in key column
    if df[key_col].isnull().any():
        errors.append(_format_error('NULL-001', column=key_col, sheet_name=sheet_name))

    # Check for duplicates
    if df[key_col].duplicated().any():
        duplicates = df[key_col][df[key_col].duplicated()].tolist()
        errors.append(f"{sheet_name}: Duplicate group names: {duplicates}")

    # Convert to list of dicts with 'items' array
    data = []
    for _, row in df.iterrows():
        group_name = row[key_col]
        # Collect all non-null items from Item1 to Item50
        items = []
        for i in range(1, schema['max_items'] + 1):
            item_col = f'Item{i}'
            if item_col in df.columns and pd.notna(row.get(item_col)):
                items.append(row[item_col])

        if not items:
            warnings.append(f"{sheet_name}: Group '{group_name}' has no items")

        data.append({
            'Group_Name': group_name,
            'items': items
        })

    is_valid = len(errors) == 0
    result_data = data if is_valid else None

    # Convert pandas types to JSON-serializable Python types
    if result_data is not None:
        result_data = _convert_pandas_types(result_data)

    return (is_valid, errors, warnings, result_data)


def _validate_dict_of_lists(df: pd.DataFrame, schema: Dict[str, Any], sheet_name: str) -> Tuple[bool, List[str], List[str], Optional[Dict]]:
    """
    Validate dict-of-lists structure (e.g., Moody's Reference Data).

    Args:
        df: DataFrame containing sheet data
        schema: Validation schema dictionary
        sheet_name: Name of the sheet being validated

    Returns:
        Tuple of (is_valid, errors, warnings, parsed_data)
    """
    errors = []
    warnings = []

    # Check required columns
    missing_cols = [col for col in schema.get('required_columns', []) if col not in df.columns]
    if missing_cols:
        errors.append(_format_error('STRUCT-002', columns=missing_cols, sheet_name=sheet_name))
        return (False, errors, warnings, None)

    # Convert to dict of lists (drop nulls)
    data = {}
    for col in df.columns:
        values = df[col].dropna().tolist()

        # Check uniqueness within column if required
        if schema.get('unique_within_column', False):
            if len(values) != len(set(values)):
                duplicates = [v for v in values if values.count(v) > 1]
                errors.append(f"{sheet_name}: Duplicate values in column '{col}': {set(duplicates)}")

        data[col] = values

    is_valid = len(errors) == 0
    result_data = data if is_valid else None

    # Convert pandas types to JSON-serializable Python types
    if result_data is not None:
        result_data = _convert_pandas_types(result_data)

    return (is_valid, errors, warnings, result_data)


def _validate_sheet(df: pd.DataFrame, schema: Dict[str, Any], sheet_name: str) -> Tuple[bool, List[str], List[str], Any]:
    """
    Generic validator dispatcher based on structure type.

    Args:
        df: DataFrame containing sheet data
        schema: Validation schema dictionary
        sheet_name: Name of the sheet being validated

    Returns:
        Tuple of (is_valid, errors, warnings, parsed_data)
    """
    structure_type = schema.get('structure_type')

    if structure_type == 'key_value':
        return _validate_key_value(df, schema, sheet_name)
    elif structure_type == 'table':
        return _validate_table(df, schema, sheet_name)
    elif structure_type == 'groupings':
        return _validate_groupings(df, schema, sheet_name)
    elif structure_type == 'dict_of_lists':
        return _validate_dict_of_lists(df, schema, sheet_name)
    else:
        errors = [_format_error('STRUCT-004', structure_type=structure_type, sheet_name=sheet_name)]
        return (False, errors, [], None)


# ============================================================================
# CROSS-SHEET VALIDATORS
# ============================================================================

def _validate_foreign_keys(config_data: Dict[str, Any], schemas: Dict[str, Any]) -> List[str]:
    """
    Validate foreign key references across sheets.

    Args:
        config_data: Parsed configuration data
        schemas: Validation schemas

    Returns:
        List of error messages
    """
    errors = []

    for sheet_name, schema in schemas.items():
        if 'foreign_keys' not in schema:
            continue

        sheet_data = config_data.get(sheet_name)
        if not sheet_data:
            continue

        # Handle different data structures
        if isinstance(sheet_data, dict):
            # Skip Metadata and Moody's Reference Data (dict structures)
            continue

        for col, (ref_sheet, ref_col) in schema['foreign_keys'].items():
            ref_data = config_data.get(ref_sheet, [])

            # Extract valid reference values
            if isinstance(ref_data, dict):
                # For Metadata or dict_of_lists structures
                valid_values = list(ref_data.values()) if ref_sheet == 'Metadata' else []
            else:
                # For table structures
                valid_values = [row[ref_col] for row in ref_data if ref_col in row and pd.notna(row[ref_col])]

            # Check each row's foreign key
            for row in sheet_data:
                if col in row and pd.notna(row[col]) and row[col] not in valid_values:
                    errors.append(_format_error('REF-001',
                                                column=col,
                                                value=row[col],
                                                ref_sheet=ref_sheet,
                                                ref_column=ref_col))

    return errors


def _validate_special_references(config_data: Dict[str, Any]) -> List[str]:
    """
    Special validation for cross-sheet references (Analysis Table and Products/Perils).

    Validates:
    - Analysis Table references to Moody's Reference Data and Reinsurance Treaties
    - Products and Perils "Analysis Name" can be EITHER Analysis Name OR Group Name

    Args:
        config_data: Parsed configuration data

    Returns:
        List of error messages
    """
    errors = []

    # 1. Validate Analysis Table references to Moody's Reference Data and Treaties
    analysis_data = config_data.get('Analysis Table', [])
    moodys_data = config_data.get("Moody's Reference Data", {})
    treaties_data = config_data.get('Reinsurance Treaties', [])

    # Extract valid values
    model_profiles = moodys_data.get('Model Profiles', [])
    output_profiles = moodys_data.get('Output Profiles', [])
    event_rate_schemes = moodys_data.get('Event Rate Schemes', [])
    treaty_names = [t['Treaty Name'] for t in treaties_data if 'Treaty Name' in t and pd.notna(t['Treaty Name'])]

    for row in analysis_data:
        # Check Analysis Profile
        if pd.notna(row.get('Analysis Profile')) and row['Analysis Profile'] not in model_profiles:
            errors.append(_format_error('REF-004', value=row['Analysis Profile']))

        # Check Output Profile
        if pd.notna(row.get('Output Profile')) and row['Output Profile'] not in output_profiles:
            errors.append(_format_error('REF-005', value=row['Output Profile']))

        # Check Event Rate (nullable)
        if pd.notna(row.get('Event Rate')) and row['Event Rate'] not in event_rate_schemes:
            errors.append(_format_error('REF-006', value=row['Event Rate']))

        # Check Reinsurance Treaties (5 optional columns)
        for i in range(1, 6):
            treaty_col = f'Reinsurance Treaty {i}'
            if pd.notna(row.get(treaty_col)) and row[treaty_col] not in treaty_names:
                errors.append(_format_error('REF-007', value=row[treaty_col]))

    # 2. Validate Products and Perils - Analysis Name can be EITHER Analysis Name OR Group Name
    products_perils = config_data.get('Products and Perils', [])
    groupings_data = config_data.get('Groupings', [])

    valid_analyses = {a['Analysis Name'] for a in analysis_data if 'Analysis Name' in a and pd.notna(a['Analysis Name'])}
    valid_groups = {g['Group_Name'] for g in groupings_data if 'Group_Name' in g and pd.notna(g['Group_Name'])}
    valid_analysis_or_group = valid_analyses | valid_groups

    for idx, row in enumerate(products_perils):
        analysis_name = row.get('Analysis Name')
        if pd.notna(analysis_name) and analysis_name not in valid_analysis_or_group:
            errors.append(
                f"Products and Perils row {idx+2}: Analysis Name '{analysis_name}' not found "
                f"in Analysis Table or Groupings"
            )

    return errors


def _validate_reference_data_api(config_data: Dict[str, Any]) -> List[str]:
    """
    Validate reference data exists in Moody's API.

    Validates that all Analysis Profiles, Output Profiles, and Event Rate Schemes
    used in the Analysis Table actually exist in Moody's system.

    Args:
        config_data: Parsed configuration data

    Returns:
        List of error messages (empty if all valid)

    Raises:
        ConfigurationError: If API connection fails entirely
    """
    errors = []
    analysis_data = config_data.get('Analysis Table', [])

    # Extract unique values from Analysis Table columns
    model_profiles = set()
    output_profiles = set()
    event_rate_schemes = set()

    for row in analysis_data:
        if pd.notna(row.get('Analysis Profile')):
            model_profiles.add(row['Analysis Profile'])
        if pd.notna(row.get('Output Profile')):
            output_profiles.add(row['Output Profile'])
        if pd.notna(row.get('Event Rate')):  # Event Rate is nullable
            event_rate_schemes.add(row['Event Rate'])

    try:
        irp_client = IRPClient()

        # Validate Model Profiles (Analysis Profiles)
        for profile_name in model_profiles:
            if not profile_name:
                continue
            try:
                result = irp_client.reference_data.get_model_profile_by_name(profile_name)
                if result.get('count', 0) == 0:
                    errors.append(_format_error('API-REF-001', value=profile_name))
            except IRPAPIError as e:
                errors.append(_format_error('API-REF-004', error=str(e)))

        # Validate Output Profiles
        for profile_name in output_profiles:
            if not profile_name:
                continue
            try:
                result = irp_client.reference_data.get_output_profile_by_name(profile_name)
                if not result:  # Empty list = not found
                    errors.append(_format_error('API-REF-002', value=profile_name))
            except IRPAPIError as e:
                errors.append(_format_error('API-REF-004', error=str(e)))

        # Validate Event Rate Schemes
        for scheme_name in event_rate_schemes:
            if not scheme_name:
                continue
            try:
                result = irp_client.reference_data.get_event_rate_scheme_by_name(scheme_name)
                if result.get('count', 0) == 0:
                    errors.append(_format_error('API-REF-003', value=scheme_name))
            except IRPAPIError as e:
                errors.append(_format_error('API-REF-004', error=str(e)))

    except Exception as e:
        raise ConfigurationError(
            f"Failed to connect to Moody's API for reference data validation: {str(e)}"
        )

    return errors


def _validate_groupings_references(config_data: Dict[str, Any]) -> List[str]:
    """
    Validate Groupings items reference valid entities (STRICT, order-dependent validation).

    Items can reference:
    - Portfolio names (from Portfolios sheet)
    - Analysis names (from Analysis Table sheet)
    - Group names (from Groupings sheet) - BUT must be defined BEFORE reference

    Args:
        config_data: Parsed configuration data

    Returns:
        List of error messages
    """
    errors = []

    groupings_data = config_data.get('Groupings', [])
    portfolios_data = config_data.get('Portfolios', [])
    analysis_data = config_data.get('Analysis Table', [])

    # Build static reference sets (Portfolios and Analysis Names don't depend on order)
    valid_portfolios = {p['Portfolio'] for p in portfolios_data if 'Portfolio' in p and pd.notna(p['Portfolio'])}
    valid_analyses = {a['Analysis Name'] for a in analysis_data if 'Analysis Name' in a and pd.notna(a['Analysis Name'])}

    # Build group names incrementally (order matters - groups must be defined before reference)
    defined_groups = set()

    # Process each group in order
    for idx, group in enumerate(groupings_data):
        group_name = group.get('Group_Name')

        # All valid references AT THIS POINT include groups defined so far
        all_valid_refs = valid_portfolios | valid_analyses | defined_groups

        # Validate this group's items
        for item in group.get('items', []):
            if pd.notna(item) and item not in all_valid_refs:
                errors.append(
                    f"Groupings row {idx+2}: Item '{item}' not found in Portfolios, Analysis Table, "
                    f"or previously defined Groups (groups must be defined before reference)"
                )

        # Add this group to the set of defined groups for subsequent validation
        if pd.notna(group_name):
            defined_groups.add(group_name)

    return errors


def _validate_business_rules(config_data: Dict[str, Any]) -> List[str]:
    """
    Validate business rules.

    Args:
        config_data: Parsed configuration data

    Returns:
        List of error messages
    """
    errors = []

    portfolios = config_data.get('Portfolios', [])
    databases = config_data.get('Databases', [])

    # Rule: At least one Base Portfolio per Database
    for db_row in databases:
        db_name = db_row.get('Database')
        if not db_name:
            continue

        has_base = any(
            p.get('Database') == db_name and p.get('Base Portfolio?') == 'Y'
            for p in portfolios
        )
        if not has_base:
            errors.append(_format_error('BUS-001', database=db_name))

    return errors


# ============================================================================
# CONFIGURATION CRUD OPERATIONS (Layer 2)
# ============================================================================

def create_configuration(
    cycle_id: int,
    configuration_file_name: str,
    configuration_data: Dict[str, Any],
    status: str = ConfigurationStatus.NEW,
    file_last_updated_ts: Optional[datetime] = None,
    schema: str = 'public'
) -> int:
    """
    Create a new configuration record.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_id: Cycle ID to associate with this configuration
        configuration_file_name: Path/name of configuration file
        configuration_data: Configuration data dictionary
        status: Initial status (default: NEW)
        file_last_updated_ts: File last modified timestamp (default: now)
        schema: Database schema to use (default: 'public')

    Returns:
        Configuration ID

    Raises:
        ConfigurationError: If creation fails
    """
    # Lazy import of database functions
    from helpers.database import execute_insert, DatabaseError

    # Validate status
    if status not in ConfigurationStatus.all():
        raise ConfigurationError(f"Invalid status: {status}. Must be one of {ConfigurationStatus.all()}")

    # Validate inputs
    if not isinstance(cycle_id, int) or cycle_id <= 0:
        raise ConfigurationError(f"Invalid cycle_id: {cycle_id}")

    if not isinstance(configuration_file_name, str) or not configuration_file_name.strip():
        raise ConfigurationError(f"Invalid configuration_file_name: {configuration_file_name}")

    # Use current timestamp if not provided
    if file_last_updated_ts is None:
        raise ConfigurationError("file_last_updated_ts must be provided")

    # Insert configuration record
    try:
        query = """
            INSERT INTO irp_configuration
            (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
            VALUES (%s, %s, %s, %s, %s)
        """
        config_id = execute_insert(
            query,
            (
                cycle_id,
                configuration_file_name,
                json.dumps(configuration_data),
                status,
                file_last_updated_ts
            ),
            schema=schema
        )
        return config_id
    except DatabaseError as e: # pragma: no cover
        raise ConfigurationError(f"Failed to create configuration: {str(e)}") # pragma: no cover


def read_configuration(config_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Read configuration by ID.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        config_id: Configuration ID
        schema: Database schema to use (default: 'public')

    Returns:
        Dictionary containing configuration details

    Raises:
        ConfigurationError: If configuration not found
    """
    # Lazy import of database functions
    from helpers.database import execute_query

    query = """
        SELECT id, cycle_id, configuration_file_name, configuration_data,
               status, file_last_updated_ts, created_ts, updated_ts
        FROM irp_configuration
        WHERE id = %s
    """

    df = execute_query(query, (config_id,), schema=schema)

    if df.empty:
        raise ConfigurationError(f"Configuration with id {config_id} not found")

    config = df.iloc[0].to_dict()

    # Parse JSON data if it's a string
    if isinstance(config['configuration_data'], str):
        config['configuration_data'] = json.loads(config['configuration_data']) # pragma: no cover

    return config


def update_configuration_status(config_id: int, status: str, schema: str = 'public') -> bool:
    """
    Update configuration status.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        config_id: Configuration ID
        status: New status (NEW, VALID, ACTIVE, ERROR)
        schema: Database schema to use (default: 'public')

    Returns:
        True if status was updated, False otherwise

    Raises:
        ConfigurationError: If configuration not found or invalid status
    """
    # Lazy import of database functions
    from helpers.database import execute_command

    # Validate status
    if status not in ConfigurationStatus.all():
        raise ConfigurationError(f"Invalid status: {status}. Must be one of {ConfigurationStatus.all()}")

    # Read current configuration
    current_config = read_configuration(config_id, schema=schema)

    # If status is the same, no update needed
    if current_config['status'] == status:
        return False

    # Update status and timestamp
    query = """
        UPDATE irp_configuration
        SET status = %s, updated_ts = NOW()
        WHERE id = %s
    """

    rows = execute_command(query, (status, config_id), schema=schema)
    return rows > 0


def validate_configuration(config_data: Dict[str, Any]) -> bool:
    """
    Validate configuration data (updated implementation).

    Checks if _validation exists and all sheets have SUCCESS status.

    Args:
        config_data: Configuration data dictionary

    Returns:
        True if valid, False otherwise
    """
    # Check if _validation exists and all sheets are SUCCESS
    validation = config_data.get('_validation', {})

    for sheet_name, status_info in validation.items():
        if status_info.get('status') != 'SUCCESS':
            return False

    return True


def _validate_excel_file(excel_config_path: str):
    """
    Internal helper to validate Excel configuration file.

    Args:
        excel_config_path: Path to Excel configuration file

    Returns:
        tuple: (config_data, validation_results, all_valid, file_mtime)

    Raises:
        ConfigurationError: If validation fails or file issues
    """
    # Check if configuration file exists
    config_path = Path(excel_config_path)
    if not config_path.exists():
        raise ConfigurationError(f"Configuration file not found: {excel_config_path}")

    # Get file last modified timestamp
    file_mtime = datetime.fromtimestamp(config_path.stat().st_mtime)

    # Read Excel file and perform all validation within context manager
    try:
        with pd.ExcelFile(excel_config_path) as excel_file:
            available_tabs = excel_file.sheet_names

            # Check for required sheets
            missing_tabs = [tab for tab in CONFIGURATION_TAB_LIST if tab not in available_tabs]
            if missing_tabs:
                raise ConfigurationError(
                    f"Missing required sheets: {missing_tabs}. "
                    f"Required: {CONFIGURATION_TAB_LIST}, Found: {available_tabs}"
                )

            # Validate each sheet
            config_data = {}
            validation_results = {}
            all_valid = True

            for sheet_name in CONFIGURATION_TAB_LIST:
                schema_def = EXCEL_VALIDATION_SCHEMAS.get(sheet_name)
                if not schema_def:
                    continue  # Skip sheets without schema (e.g., Validations)

                # Parse sheet (with or without header based on schema)
                try:
                    if schema_def.get('has_header', True):
                        df = excel_file.parse(sheet_name)
                    else:
                        df = excel_file.parse(sheet_name, header=None)
                except Exception as e:
                    validation_results[sheet_name] = {
                        'status': 'ERROR',
                        'errors': [f"Failed to parse sheet: {str(e)}"],
                        'warnings': [],
                        'row_count': 0,
                        'column_count': 0,
                        'validated_at': datetime.now().isoformat()
                    }
                    all_valid = False
                    continue

                # Validate sheet
                is_valid, errors, warnings, parsed_data = _validate_sheet(df, schema_def, sheet_name)

                # Store validation results
                validation_results[sheet_name] = {
                    'status': 'SUCCESS' if is_valid else 'ERROR',
                    'errors': errors,
                    'warnings': warnings,
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'validated_at': datetime.now().isoformat()
                }

                if is_valid and parsed_data is not None:
                    config_data[sheet_name] = parsed_data
                else:
                    all_valid = False

            # If any sheet validation failed, stop here
            if not all_valid:
                config_data['_validation'] = validation_results
                error_summary = []
                for sheet, result in validation_results.items():
                    if result['status'] == 'ERROR':
                        error_summary.extend([f"{sheet}: {err}" for err in result['errors']])
                raise ConfigurationError(f"Validation errors:\n" + "\n".join(error_summary[:10]))

            # Cross-sheet validation
            cross_errors = []
            cross_errors.extend(_validate_foreign_keys(config_data, EXCEL_VALIDATION_SCHEMAS))
            cross_errors.extend(_validate_special_references(config_data))
            cross_errors.extend(_validate_groupings_references(config_data))
            cross_errors.extend(_validate_business_rules(config_data))

            # Validate reference data against Moody's API
            # Only run if no cross-sheet errors (avoid unnecessary API calls)
            if not cross_errors:
                api_errors = _validate_reference_data_api(config_data)
                cross_errors.extend(api_errors)

            if cross_errors:
                validation_results['_cross_sheet'] = {
                    'status': 'ERROR',
                    'errors': cross_errors,
                    'warnings': [],
                    'validated_at': datetime.now().isoformat()
                }
                config_data['_validation'] = validation_results
                raise ConfigurationError(f"Cross-sheet validation errors:\n" + "\n".join(cross_errors[:10]))

            # Add validation results to config
            config_data['_validation'] = validation_results
    except ConfigurationError:
        # Re-raise ConfigurationErrors as-is
        raise
    except Exception as e:
        raise ConfigurationError(f"Failed to read Excel file: {str(e)}")

    return config_data, validation_results, all_valid, file_mtime


def load_configuration_file(
    cycle_id: int,
    excel_config_path: str,
    schema: str = 'public'
) -> int:
    """
    Load configuration from Excel file into the database.

    This function validates and loads an Excel configuration file.
    It will delete any existing configurations for the cycle before loading.

    Args:
        cycle_id: Cycle ID to associate with this configuration
        excel_config_path: Path to Excel configuration file
        schema: Database schema to use (default: 'public')

    Returns:
        Configuration ID

    Raises:
        ConfigurationError: If validation fails or file issues
    """
    # Lazy import of database functions
    from helpers.database import execute_query, execute_command, execute_insert, schema_context
    from helpers.cycle import get_active_cycle_id

    # Validate that the provided cycle_id matches the active cycle
    with schema_context(schema):
        active_cycle_id = get_active_cycle_id()
    if active_cycle_id != cycle_id:
        raise ConfigurationError(
            _format_error('BUS-004', cycle_id=cycle_id, active_cycle_id=active_cycle_id)
        )

    # Check if there's an existing configuration with batches created from it
    # If batches exist, we cannot replace the configuration (data integrity)
    # If no batches exist, we can safely replace the configuration
    config_query = """
        SELECT id FROM irp_configuration
        WHERE cycle_id = %s
    """
    config_df = execute_query(config_query, (cycle_id,), schema=schema)

    if not config_df.empty:
        config_id = int(config_df.iloc[0]['id'])
        # Check if any batches have been created from this configuration
        batch_query = """
            SELECT COUNT(*) as batch_count FROM irp_batch
            WHERE configuration_id = %s
        """
        batch_df = execute_query(batch_query, (config_id,), schema=schema)
        batch_count = int(batch_df.iloc[0]['batch_count'])

        if batch_count > 0:
            # Batches exist - cannot replace configuration
            raise ConfigurationError(
                _format_error('BUS-003', batch_count=batch_count, config_id=config_id)
            )
        # No batches exist - safe to replace (will be deleted below)

    # Validate Excel file using helper function
    config_data, validation_results, all_valid, file_mtime = _validate_excel_file(excel_config_path)

    # Delete existing configs and insert new configuration
    delete_query = "DELETE FROM irp_configuration WHERE cycle_id = %s"
    execute_command(delete_query, (cycle_id,), schema=schema)

    insert_query = """
        INSERT INTO irp_configuration
        (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
        VALUES (%s, %s, %s, %s, %s)
    """
    config_id = execute_insert(
        insert_query,
        (cycle_id, excel_config_path, json.dumps(config_data), ConfigurationStatus.NEW, file_mtime),
        schema=schema
    )

    # Update status to VALID (since validation passed)
    update_configuration_status(config_id, ConfigurationStatus.VALID, schema=schema)

    return config_id


def validate_configuration_file(excel_config_path: str, cycle_id: Optional[int] = None) -> dict:
    """
    Validate Excel configuration file without loading to database.

    This function performs all validation checks but does not insert data
    into the database. Useful for preview/validation before committing.

    Args:
        excel_config_path: Path to Excel configuration file
        cycle_id: Optional cycle ID to validate against (checks if active).
                  If None, skips cycle validation (useful for pure file validation)

    Returns:
        dict: {
            'validation_passed': bool,
            'configuration_data': dict (includes _validation section),
            'file_info': {
                'path': str,
                'last_modified': str,
                'size_bytes': int
            }
        }

    Raises:
        ConfigurationError: If validation fails or file issues
    """
    # Optionally validate that the provided cycle_id matches the active cycle
    if cycle_id is not None:
        from helpers.cycle import get_active_cycle_id
        active_cycle_id = get_active_cycle_id()
        if active_cycle_id != cycle_id:
            raise ConfigurationError(
                _format_error('BUS-004', cycle_id=cycle_id, active_cycle_id=active_cycle_id)
            )

    # Validate Excel file using helper function
    config_data, validation_results, all_valid, file_mtime = _validate_excel_file(excel_config_path)

    # Get file info
    config_path = Path(excel_config_path)
    file_info = {
        'path': str(config_path.absolute()),
        'last_modified': file_mtime.isoformat(),
        'size_bytes': config_path.stat().st_size
    }

    return {
        'validation_passed': all_valid,
        'configuration_data': config_data,
        'file_info': file_info
    }


def get_transformer_list(include_test: bool = False) -> List[str]:
    """
    Get list of available transformer batch types.

    Args:
        include_test: If True, include test transformers (default: False)

    Returns:
        List of batch type names
    """
    transformers = []
    for batch_type in BATCH_TYPE_TRANSFORMERS.keys():
        if include_test or not batch_type.startswith('test_'):
            transformers.append(batch_type)
    return transformers


def preview_transformer_jobs(
    batch_type: str,
    configuration_data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Preview job configurations that would be created by a transformer.

    This function runs the transformer on configuration data without
    creating any batch or job records in the database.

    Args:
        batch_type: The type of batch transformer to run
        configuration_data: Configuration dictionary (from validate_configuration_file)

    Returns:
        List of job configuration dictionaries

    Raises:
        ConfigurationError: If batch type is not recognized
    """
    # Use existing create_job_configurations function which doesn't touch DB
    return create_job_configurations(batch_type, configuration_data)
