"""
Post-processing operations for writing cycle data to SQL Server.

This module handles the final step of the IRP workflow cycle, writing
summary data to the Risk_Modeler_PremiumIQ_Variable table in SQL Server.
"""
import calendar
import json
from datetime import date
from typing import Any, Dict, List

from helpers.sqlserver import execute_command
from helpers.irp_integration import IRPClient


def convert_date_value_to_inforce_date(date_value: str) -> str:
    """
    Convert date value from YYYYMM format to MM/DD/YYYY (last day of month).

    Args:
        date_value: Date in YYYYMM format (e.g., "202503")

    Returns:
        Date in MM/DD/YYYY format (e.g., "03/31/2025")
    """
    year = int(date_value[:4])
    month = int(date_value[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day).strftime('%m/%d/%Y')


def extract_peril_code(edm_name: str) -> str:
    """
    Extract peril code from EDM name (last segment after underscore).

    Args:
        edm_name: EDM name (e.g., "RM_EDM_202503_Portfolio_USEQ")

    Returns:
        Peril code (e.g., "USEQ")
    """
    return edm_name.rsplit('_', 1)[-1]


def format_model_version(dlm_model_version: str) -> str:
    """
    Format DLM Model Version to "RM vXX" format.

    Args:
        dlm_model_version: Version string (e.g., "23.0.0")

    Returns:
        Formatted version string (e.g., "RM v23")
    """
    major_version = dlm_model_version.split('.')[0]
    return f'RM v{major_version}'


def build_post_processing_rows(
    configuration_data: Dict[str, Any],
    irp_client: IRPClient
) -> List[Dict[str, str]]:
    """
    Build all rows to be written to Risk_Modeler_PremiumIQ_Variable.

    Args:
        configuration_data: Configuration data dict (from read_configuration)
        irp_client: IRPClient instance for API lookups

    Returns:
        List of dicts with keys: inforce_date, variable_name, variable_value
    """
    rows = []

    # Extract data from configuration
    metadata = configuration_data.get('Metadata', {})
    databases = configuration_data.get('Databases', [])
    products_perils = configuration_data.get('Products and Perils', [])

    current_date_value = metadata.get('Current Date Value')
    rdm_raw_name = metadata.get('Export RDM Name')
    edm_raw_names = [db['Database'] for db in databases]

    # Convert date
    inforce_date = convert_date_value_to_inforce_date(current_date_value)

    # Build EDM rows (DB_{peril} and srcedm_{peril})
    for edm_raw_name in edm_raw_names:
        peril_code = extract_peril_code(edm_raw_name)

        # Look up full EDM name from Moody's API
        edms = irp_client.edm.search_edms(filter=f'exposureName="{edm_raw_name}"')
        if edms:
            edm_full_name = edms[0]['databaseName']
        else:
            edm_full_name = edm_raw_name  # Fallback

        # DB_{peril} row
        rows.append({
            'inforce_date': inforce_date,
            'variable_name': f'DB_{peril_code}',
            'variable_value': edm_full_name
        })

        # srcedm_{peril} row
        rows.append({
            'inforce_date': inforce_date,
            'variable_name': f'srcedm_{peril_code}',
            'variable_value': edm_raw_name
        })

    # Products and Perils row
    rows.append({
        'inforce_date': inforce_date,
        'variable_name': 'products_and_perils',
        'variable_value': json.dumps(products_perils)
    })

    # Current date value row
    rows.append({
        'inforce_date': inforce_date,
        'variable_name': 'current_date_value',
        'variable_value': current_date_value
    })

    # DB_USAP row (RDM full name)
    rdm_full_name = irp_client.rdm.get_rdm_database_full_name(rdm_raw_name)
    rows.append({
        'inforce_date': inforce_date,
        'variable_name': 'DB_USAP',
        'variable_value': rdm_full_name
    })

    # Model version row
    dlm_model_version = metadata.get('DLM Model Version')
    if dlm_model_version:
        rows.append({
            'inforce_date': inforce_date,
            'variable_name': 'model_version',
            'variable_value': format_model_version(dlm_model_version)
        })

    return rows


def delete_existing_rows_for_inforce_date(
    inforce_date: str,
    connection: str = 'ASSURANT',
    database: str = 'DW_EXP_MGMT_USER'
) -> int:
    """
    Delete existing rows for a given InforceDate before inserting new data.

    Args:
        inforce_date: The InforceDate value to delete (e.g., "03/31/2025")
        connection: SQL Server connection name
        database: Database name

    Returns:
        Number of rows deleted
    """
    return execute_command(
        """
        DELETE FROM Risk_Modeler_PremiumIQ_Variable
        WHERE InforceDate = {{ inforce_date }}
        """,
        params={'inforce_date': inforce_date},
        connection=connection,
        database=database
    )


def write_post_processing_data(
    rows: List[Dict[str, str]],
    connection: str = 'ASSURANT',
    database: str = 'DW_EXP_MGMT_USER'
) -> int:
    """
    Write rows to Risk_Modeler_PremiumIQ_Variable table.

    Deletes existing rows for the InforceDate first, then inserts new data.

    Args:
        rows: List of dicts with inforce_date, variable_name, variable_value
        connection: SQL Server connection name
        database: Database name

    Returns:
        Number of rows inserted
    """
    if not rows:
        return 0

    # Delete existing rows for this inforce_date first
    inforce_date = rows[0]['inforce_date']
    delete_existing_rows_for_inforce_date(inforce_date, connection, database)

    # Insert new rows
    inserted = 0
    for row in rows:
        execute_command(
            """
            INSERT INTO Risk_Modeler_PremiumIQ_Variable (InforceDate, VariableName, VariableValue)
            VALUES ({{ inforce_date }}, {{ var_name }}, {{ var_value }})
            """,
            params={
                'inforce_date': row['inforce_date'],
                'var_name': row['variable_name'],
                'var_value': row['variable_value']
            },
            connection=connection,
            database=database
        )
        inserted += 1
    return inserted
