"""
IRP Notebook Framework - Configuration Management
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd

from helpers.database import execute_query, execute_command, execute_insert, DatabaseError
from helpers.constants import ConfigurationStatus, CONFIGURATION_TAB_LIST, CONFIGURATION_COLUMNS
from helpers.cycle import get_active_cycle_id


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


class ConfigurationTransformer:
    """
    Transform configuration data into job configurations based on batch type.

    Uses a registry pattern to map batch types to transformation functions.
    Each transformation function takes a configuration dict and returns a list
    of job configuration dicts.
    """

    # Registry mapping batch type to transformation function
    _transformers = {}

    @classmethod
    def register(cls, batch_type: str):
        """
        Decorator to register a transformation function for a batch type.

        Usage:
            @ConfigurationTransformer.register('my_batch_type')
            def transform_my_type(config: Dict[str, Any]) -> List[Dict[str, Any]]:
                # Transform logic here
                return [job_config1, job_config2, ...]

        Args:
            batch_type: The batch type identifier
        """
        def decorator(func):
            cls._transformers[batch_type] = func
            return func
        return decorator

    @classmethod
    def get_job_configurations(
        cls,
        batch_type: str,
        configuration: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Transform configuration into job configurations based on batch type.

        Args:
            batch_type: The type of batch (e.g., 'default', 'portfolio', etc.)
            configuration: The configuration dictionary to transform

        Returns:
            List of job configuration dictionaries

        Raises:
            ConfigurationError: If batch type is not registered
        """
        if batch_type not in cls._transformers:
            raise ConfigurationError(
                f"No transformer registered for batch type '{batch_type}'. "
                f"Available types: {list(cls._transformers.keys())}"
            )

        transformer_func = cls._transformers[batch_type]
        return transformer_func(configuration)

    @classmethod
    def list_types(cls) -> List[str]:
        """
        Get list of registered batch types.

        Returns:
            List of registered batch type names
        """
        return list(cls._transformers.keys())


# ============================================================================
# TRANSFORMERS
# ============================================================================

@ConfigurationTransformer.register('default')
def transform_default(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Default transformer - creates a single job configuration by copying config as-is.

    Args:
        config: Configuration dictionary

    Returns:
        List containing a single job configuration
    """
    return [config.copy()]


@ConfigurationTransformer.register('passthrough')
def transform_passthrough(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Passthrough transformer - returns config unchanged (no copy).

    Args:
        config: Configuration dictionary

    Returns:
        List containing the original configuration
    """
    return [config]


@ConfigurationTransformer.register('multi_job')
def transform_multi_job(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Multi-job transformer - creates multiple job configurations from a list.

    Expects config to have a 'jobs' key with a list of job configurations.
    If 'jobs' key doesn't exist, falls back to single job.

    Args:
        config: Configuration dictionary with optional 'jobs' key

    Returns:
        List of job configurations
    """
    if 'jobs' in config and isinstance(config['jobs'], list):
        return config['jobs']
    return [config.copy()]



def read_configuration(config_id: int, schema: str = 'public') -> Dict[str, Any]:
    """
    Read configuration by ID

    Args:
        config_id: Configuration ID
        schema: Database schema to use (default: 'public')

    Returns:
        Dictionary containing configuration details

    Raises:
        ConfigurationError: If configuration not found
    """
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
        config['configuration_data'] = json.loads(config['configuration_data'])

    return config


def update_configuration_status(config_id: int, status: str, schema: str = 'public') -> bool:
    """
    Update configuration status

    Args:
        config_id: Configuration ID
        status: New status (NEW, VALID, ACTIVE, ERROR)
        schema: Database schema to use (default: 'public')

    Returns:
        True if status was updated, False otherwise

    Raises:
        ConfigurationError: If configuration not found or invalid status
    """
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
    Validate configuration data

    Args:
        config_data: Configuration data dictionary

    Returns:
        True if valid (placeholder for now)
    """
    # Placeholder validation - returns True for now
    # Can be extended with actual validation logic
    return True


def load_configuration_file(
    cycle_id: int,
    excel_config_path: str,
    register: bool = False,
    schema: str = 'public'
) -> int:
    """
    Load configuration from Excel file

    Args:
        cycle_id: Cycle ID to associate with this configuration
        excel_config_path: Path to Excel configuration file
        register: If True, delete existing configurations and register this as NEW
        schema: Database schema to use (default: 'public')

    Returns:
        Configuration ID

    Raises:
        ConfigurationError: If validation fails or file issues
    """
    # Validate that the provided cycle_id matches the active cycle
    # Note: get_active_cycle_id() now uses schema context
    from helpers.database import schema_context
    with schema_context(schema):
        active_cycle_id = get_active_cycle_id()
    if active_cycle_id != cycle_id:
        raise ConfigurationError(
            f"Provided cycle_id {cycle_id} does not match active cycle {active_cycle_id}"
        )

    # Check if there's already an ACTIVE configuration for this cycle
    query = """
        SELECT id FROM irp_configuration
        WHERE cycle_id = %s AND status = %s
    """
    df = execute_query(query, (cycle_id, ConfigurationStatus.ACTIVE), schema=schema)

    if not df.empty:
        raise ConfigurationError(
            f"An ACTIVE configuration already exists for cycle {cycle_id}. "
            f"Configuration ID: {df.iloc[0]['id']}"
        )

    # Check if configuration file exists
    config_path = Path(excel_config_path)
    if not config_path.exists():
        raise ConfigurationError(f"Configuration file not found: {excel_config_path}")

    # Get file last modified timestamp
    file_mtime = datetime.fromtimestamp(config_path.stat().st_mtime)

    # Read Excel file
    try:
        excel_file = pd.ExcelFile(excel_config_path)
        available_tabs = excel_file.sheet_names
    except Exception as e:
        raise ConfigurationError(f"Failed to read Excel file: {str(e)}")

    # Validate tabs against CONFIGURATION_TAB_LIST
    missing_tabs = [tab for tab in CONFIGURATION_TAB_LIST if tab not in available_tabs]
    if missing_tabs:
        raise ConfigurationError(
            f"Missing required tabs: {missing_tabs}. "
            f"Required: {CONFIGURATION_TAB_LIST}, Found: {available_tabs}"
        )

    # Read required tabs into dict of dataframes
    tab_dataframes = {}
    tab_status = {}

    for tab_name in CONFIGURATION_TAB_LIST:
        try:
            df = excel_file.parse(tab_name)
            tab_dataframes[tab_name] = df

            # Validate columns
            expected_cols = CONFIGURATION_COLUMNS.get(tab_name, [])
            missing_cols = [col for col in expected_cols if col not in df.columns]

            if missing_cols:
                tab_status[tab_name] = {
                    'status': 'ERROR',
                    'error': f"Missing required columns: {missing_cols}"
                }
            else:
                tab_status[tab_name] = {'status': 'SUCCESS', 'error': None}

        except Exception as e:
            tab_status[tab_name] = {
                'status': 'ERROR',
                'error': f"Failed to read tab: {str(e)}"
            }

    # Check if any tabs had errors
    errors = [f"{tab}: {info['error']}" for tab, info in tab_status.items()
              if info['status'] == 'ERROR']
    if errors:
        raise ConfigurationError(f"Tab validation errors: {'; '.join(errors)}")

    # Convert dataframes to list of dicts
    config_data = {}
    for tab_name, df in tab_dataframes.items():
        config_data[tab_name] = df.to_dict(orient='records')

    # Add validation status to config data
    config_data['_validation'] = tab_status

    # If register=True, delete all existing configurations for this cycle
    if register:
        delete_query = """
            DELETE FROM irp_configuration
            WHERE cycle_id = %s
        """
        execute_command(delete_query, (cycle_id,), schema=schema)

    # Insert new configuration record
    if register:
        insert_query = """
            INSERT INTO irp_configuration
            (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts)
            VALUES (%s, %s, %s, %s, %s)
        """
        config_id = execute_insert(
            insert_query,
            (
                cycle_id,
                excel_config_path,
                json.dumps(config_data),
                ConfigurationStatus.NEW,
                file_mtime
            ),
            schema=schema
        )

        # Validate configuration
        is_valid = validate_configuration(config_data)

        # Update status based on validation
        if is_valid:
            update_configuration_status(config_id, ConfigurationStatus.VALID, schema=schema)
        else:
            update_configuration_status(config_id, ConfigurationStatus.ERROR, schema=schema)

        return config_id
    else:
        # If not registering, just return the validation info (no insertion)
        # This is a dry-run mode
        raise ConfigurationError(
            "Configuration file validated successfully but not registered (register=False). "
            f"Tab status: {tab_status}"
        )
