"""
IRP Notebook Framework - Batch Preview Utilities

This module provides display utilities for previewing batch types and job
configurations in notebooks. It handles the presentation logic separately
from the core batch functionality in batch.py.

Two types of previews:
1. Pre-creation previews: Show what jobs WILL be created from configuration data
2. Post-creation previews: Show job configurations AFTER batches are created
"""

import json
from typing import Any, Dict, List, Callable, Union

from helpers.database import execute_query
from helpers.constants import BatchType


# ============================================================================
# PRE-CREATION PREVIEW UTILITIES
# ============================================================================

def preview_batch_type(
    batch_type: str,
    batch_types_to_create: List[str],
    data: List[Any],
    headers: List[str],
    fields: List[Union[str, Callable]],
    notes: List[str],
    not_needed_msg: str,
    ux_module,
    step=None,
    extra_info: str = None,
    footer: str = None,
    warning: str = None,
    limit: int = None
) -> None:
    """
    Display preview for a single batch type before jobs are created.

    Args:
        batch_type: BatchType constant (e.g., BatchType.EDM_CREATION)
        batch_types_to_create: List of batch types that will be created
        data: List of configuration items to preview
        headers: Column headers for the table
        fields: List of field names or callables to extract values from items
        notes: List of notes about job configuration contents
        not_needed_msg: Message to show if batch type is not being created
        ux_module: UX module with header(), info(), table(), warning() methods
        step: Optional step object for logging
        extra_info: Optional extra info line to display
        footer: Optional footer message
        warning: Optional warning message
        limit: Optional limit on rows to display
    """
    display_name = batch_type.replace('_', ' ').title()
    ux_module.header(f"{display_name} Batch Preview")

    if batch_type not in batch_types_to_create:
        ux_module.info(not_needed_msg)
        return

    # Handle empty data
    if not data:
        ux_module.info("This batch will be created with 0 jobs (empty batch for workflow continuity)")
        if step:
            step.log(f"Previewed {display_name} batch: 0 jobs")
        return

    # Display intro
    ux_module.info(f"This batch will create {len(data)} job(s):")
    if extra_info:
        ux_module.info(extra_info)
    if warning:
        ux_module.warning(f"⚠ {warning}")
    ux_module.info("")

    # Build and display table
    rows = []
    display_data = list(data)[:limit] if limit else list(data)
    for item in display_data:
        row = []
        for field in fields:
            if callable(field):
                row.append(field(item))
            elif isinstance(item, dict):
                row.append(item.get(field, 'N/A'))
            elif isinstance(item, (list, tuple)):
                idx = fields.index(field) if field in fields else 0
                row.append(item[idx] if idx < len(item) else 'N/A')
            else:
                row.append('N/A')
        rows.append(row)

    ux_module.table(rows, headers=headers)

    if limit and len(data) > limit:
        ux_module.info(f"... and {len(data) - limit} more job(s)")

    # Display notes
    ux_module.info("")
    ux_module.info("Each job configuration will contain:")
    for note in notes:
        ux_module.info(f"  - {note}")

    if footer:
        ux_module.info("")
        ux_module.info(footer)

    if step:
        step.log(f"Previewed {display_name} batch: {len(data)} jobs")


def trunc(val: Any, max_len: int = 30) -> str:
    """Truncate a string value with ellipsis if too long."""
    if val is None:
        return 'N/A'
    val_str = str(val)
    if len(val_str) > max_len:
        return val_str[:max_len] + '...'
    return val_str


def list_preview(items: List[Any], show: int = 3) -> str:
    """Create a preview string from a list with '... (+N more)' suffix."""
    if not items:
        return 'N/A'
    preview = ', '.join(str(i) for i in items[:show])
    if len(items) > show:
        preview += f', ... (+{len(items) - show} more)'
    return preview


def preview_export_to_rdm(
    batch_type: str,
    batch_types_to_create: List[str],
    rdm_name: str,
    analyses: List[Dict],
    groupings: List[Dict],
    ux_module,
    step=None
) -> None:
    """
    Display preview for Export to RDM batch (one job per analysis/group).

    Args:
        batch_type: BatchType.EXPORT_TO_RDM
        batch_types_to_create: List of batch types that will be created
        rdm_name: Target RDM database name
        analyses: List of analysis configurations
        groupings: List of grouping configurations
        ux_module: UX module
        step: Optional step object for logging
    """
    ux_module.header("Export to RDM Batch Preview")

    if batch_type not in batch_types_to_create:
        ux_module.info("Export to RDM batch not needed (Export RDM Name not specified or no analyses/groups)")
        return

    analysis_names = [a.get('Analysis Name') for a in analyses if a.get('Analysis Name')]
    group_names = [g.get('Group_Name') for g in groupings if g.get('Group_Name')]
    total_items = len(analysis_names) + len(group_names)

    ux_module.info(f"This batch will create {total_items} job(s) - one per analysis/group.")
    ux_module.info(f"Target RDM: {rdm_name}")
    ux_module.info("Server: databridge-1")

    if total_items > 1:
        ux_module.info("")
        ux_module.info("The first job (seed job) creates the RDM.")
        ux_module.info("Subsequent jobs append to the same RDM.")

    ux_module.info("")

    ux_module.table(
        [["Analyses to export", len(analysis_names)],
         ["Groups to export", len(group_names)],
         ["Total jobs", total_items]],
        headers=["Item Type", "Count / Jobs"]
    )

    ux_module.info("")
    ux_module.info("Each job configuration will contain:")
    ux_module.info("  - Metadata from configuration file")
    ux_module.info("  - rdm_name: Target RDM database name")
    ux_module.info("  - server_name: databridge-1")
    ux_module.info("  - analysis_names: Single analysis or group name")
    ux_module.info("")
    ux_module.info("exportHdLossesAs setting (determined at submission time):")
    ux_module.info("  - PLT items: 'exportHdLossesAs': 'PLT' included")
    ux_module.info("  - ELT items: setting omitted")
    ux_module.warning("⚠ IMPORTANT: Export to RDM can only run AFTER all Grouping batches complete.")

    if step:
        step.log(f"Previewed Export to RDM batch: {len(analysis_names)} analyses + {len(group_names)} groups = {total_items} jobs")


# ============================================================================
# POST-CREATION JOB PREVIEW UTILITIES
# ============================================================================

# Configuration for job preview display by batch type
# Each entry defines: display_name, headers, field extractors, and optional notes
JOB_PREVIEW_CONFIG = {
    BatchType.EDM_CREATION: {
        'display_name': 'EDM Creation',
        'headers': ['Job ID', 'Database', 'Status'],
        'fields': ['job_id', 'Database', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Database-specific fields from Databases sheet',
            'Additional fields: Description, Connection details, etc.'
        ]
    },
    BatchType.PORTFOLIO_CREATION: {
        'display_name': 'Portfolio Creation',
        'headers': ['Job ID', 'Portfolio', 'EDM', 'Status'],
        'fields': ['job_id', 'Portfolio', 'Database', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Portfolio-specific fields from Portfolios sheet',
            'Additional fields: Portfolio Name, Database, Base Portfolio flag, etc.'
        ]
    },
    BatchType.MRI_IMPORT: {
        'display_name': 'MRI Import',
        'headers': ['Job ID', 'Portfolio', 'EDM', 'Accounts File', 'Locations File', 'Status'],
        'fields': ['job_id', 'Portfolio', 'Database', 'accounts_import_file', 'locations_import_file', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Portfolio-specific fields from Portfolios sheet',
            'CSV import filenames: accounts_import_file, locations_import_file',
            'Additional fields: Portfolio Name, Database, Import File, etc.'
        ]
    },
    BatchType.CREATE_REINSURANCE_TREATIES: {
        'display_name': 'Create Reinsurance Treaties',
        'headers': ['Job ID', 'Treaty Name', 'EDM', 'Status'],
        'fields': ['job_id', 'Treaty Name', 'Database', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Database (EDM) where the treaty will be created',
            'Treaty-specific fields from Reinsurance Treaties sheet'
        ]
    },
    BatchType.EDM_DB_UPGRADE: {
        'display_name': 'EDM DB Upgrade',
        'headers': ['Job ID', 'Database', 'Target Version', 'Status'],
        'fields': ['job_id', 'Database', 'target_edm_version', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Database-specific fields from Databases sheet',
            'target_edm_version: The version to upgrade to'
        ]
    },
    BatchType.GEOHAZ: {
        'display_name': 'GeoHaz',
        'headers': ['Job ID', 'Portfolio', 'EDM', 'Geocode Version', 'Status'],
        'fields': ['job_id', 'Portfolio', 'Database', 'geocode_version', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Portfolio-specific fields from Portfolios sheet',
            'geocode_version: The geocode version to use'
        ]
    },
    BatchType.PORTFOLIO_MAPPING: {
        'display_name': 'Portfolio Mapping',
        'headers': ['Job ID', 'Portfolio', 'EDM', 'Import File', 'Status'],
        'fields': ['job_id', 'Portfolio', 'Database', 'Import File', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Portfolio-specific fields from Portfolios sheet',
            'SQL script path based on Import File'
        ],
        'footer': 'Note: Portfolio Mapping executes SQL scripts locally (not submitted to Moody\'s)'
    },
    BatchType.ANALYSIS: {
        'display_name': 'Analysis',
        'headers': ['Job ID', 'Analysis Name', 'Portfolio', 'EDM', 'Analysis Profile', 'Status'],
        'fields': ['job_id', 'Analysis Name', 'Portfolio', 'Database', 'Analysis Profile:25', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Analysis-specific fields from Analysis Table sheet',
            'Analysis Profile, Output Profile, Event Rate, Treaties, Tags'
        ]
    },
    BatchType.GROUPING: {
        'display_name': 'Grouping (Analysis-only)',
        'headers': ['Job ID', 'Group Name', '# Analyses', 'Analyses (Preview)', 'Status'],
        'fields': ['job_id', 'Group_Name', 'items:count', 'items:preview:2', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Group_Name: Name of the group',
            'items: List of analysis names to group together'
        ]
    },
    BatchType.GROUPING_ROLLUP: {
        'display_name': 'Grouping Rollup (Groups of Groups)',
        'headers': ['Job ID', 'Group Name', '# Items', 'Items (Preview)', 'Status'],
        'fields': ['job_id', 'Group_Name', 'items:count', 'items:preview:2', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Group_Name: Name of the rollup group',
            'items: List of group names AND/OR analysis names'
        ],
        'warning': 'These jobs require Grouping batch to complete first'
    },
    BatchType.EXPORT_TO_RDM: {
        'display_name': 'Export to RDM',
        'headers': ['Job ID', 'RDM Name', 'Item Name', 'Seed?', 'Status'],
        'fields': ['job_id', 'rdm_name', 'analysis_names:preview:1', 'is_seed_job', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'rdm_name: Target RDM database name',
            'server_name: Database server (databridge-1)',
            'analysis_names: Single analysis or group name per job',
            'exportHdLossesAs: Conditionally set based on analysisFramework (PLT only)'
        ],
        'warning': 'This batch requires all Grouping batches to complete first'
    },
    BatchType.DATA_EXTRACTION: {
        'display_name': 'Data Extraction',
        'headers': ['Job ID', 'Portfolio', 'Import File', 'Account File', 'Location File', 'Status'],
        'fields': ['job_id', 'Portfolio', 'Import File', 'accounts_import_file', 'locations_import_file', 'status'],
        'notes': [
            'Full metadata from configuration file',
            'Portfolio-specific fields from Portfolios sheet',
            'SQL script path: import_files/{cycle_type}/2_Create_{Import File}_Moodys_ImportFile.sql',
            'CSV output files: Account and Location files for Moody\'s import'
        ],
        'footer': 'Note: Data Extraction executes SQL scripts locally and exports CSVs to files/data/'
    }
}


def _extract_field_value(config_data: Dict[str, Any], field_spec: str, job_id: int, status: str) -> Any:
    """
    Extract a field value from job configuration data based on field specification.

    Field specifications:
    - 'job_id': Returns the job_id parameter
    - 'status': Returns the status parameter
    - 'field_name': Returns config_data.get('field_name', 'N/A')
    - 'field_name:N': Returns field truncated to N chars with '...' suffix
    - 'field_name:count': Returns len(field_value) if list, else 0
    - 'field_name:preview:N': Returns first N items joined, with '... (+X more)' suffix

    Args:
        config_data: Job configuration data dict
        field_spec: Field specification string
        job_id: Job ID (for 'job_id' field)
        status: Job status (for 'status' field)

    Returns:
        Extracted and formatted field value
    """
    # Handle special fields
    if field_spec == 'job_id':
        return job_id
    if field_spec == 'status':
        return status

    # Parse field specification
    parts = field_spec.split(':')
    field_name = parts[0]

    # Get raw value
    value = config_data.get(field_name, 'N/A')

    # Handle modifiers
    if len(parts) == 1:
        return value

    modifier = parts[1]

    # Truncate string to N characters
    if modifier.isdigit():
        max_len = int(modifier)
        if isinstance(value, str) and len(value) > max_len:
            return value[:max_len] + '...'
        return value

    # Count items in list
    if modifier == 'count':
        if isinstance(value, list):
            return len(value)
        return 0

    # Preview first N items from list
    if modifier == 'preview' and len(parts) >= 3:
        preview_count = int(parts[2])
        if isinstance(value, list):
            preview = ', '.join(str(v) for v in value[:preview_count])
            if len(value) > preview_count:
                preview += f', ... (+{len(value) - preview_count} more)'
            return preview
        return str(value) if value != 'N/A' else 'N/A'

    return value


def get_job_preview_data(
    batch_id: int,
    batch_type: str,
    limit: int = 5,
    schema: str = 'public'
) -> Dict[str, Any]:
    """
    Get job configuration preview data for display.

    This function retrieves job configurations for a batch and formats them
    for display based on the batch type's preview configuration.

    Args:
        batch_id: Batch ID to preview
        batch_type: Type of batch (must be in JOB_PREVIEW_CONFIG)
        limit: Maximum number of jobs to preview (default 5)
        schema: Database schema

    Returns:
        Dictionary containing:
        - 'display_name': Human-readable batch type name
        - 'headers': List of column headers
        - 'rows': List of row data (list of values)
        - 'notes': List of info notes about job contents
        - 'warning': Optional warning message
        - 'footer': Optional footer message
        - 'total_jobs': Total number of jobs in batch
        - 'shown_jobs': Number of jobs shown in preview

    Raises:
        ValueError: If batch_type not in JOB_PREVIEW_CONFIG
    """
    if batch_type not in JOB_PREVIEW_CONFIG:
        raise ValueError(f"No preview configuration for batch type: {batch_type}")

    config = JOB_PREVIEW_CONFIG[batch_type]

    # Query job configurations with job status
    query = """
        SELECT
            jc.id,
            jc.job_configuration_data,
            j.id as job_id,
            j.status
        FROM irp_job_configuration jc
        INNER JOIN irp_job j ON jc.id = j.job_configuration_id
        WHERE jc.batch_id = %s
        ORDER BY j.id
    """

    df = execute_query(query, (batch_id,), schema=schema)
    total_jobs = len(df)

    if df.empty:
        return {
            'display_name': config['display_name'],
            'headers': config['headers'],
            'rows': [],
            'notes': config.get('notes', []),
            'warning': config.get('warning'),
            'footer': config.get('footer'),
            'total_jobs': 0,
            'shown_jobs': 0
        }

    # Limit rows for preview
    df_limited = df.head(limit)

    # Extract rows
    rows = []
    for _, row in df_limited.iterrows():
        config_data = row['job_configuration_data']
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        row_values = []
        for field_spec in config['fields']:
            value = _extract_field_value(
                config_data,
                field_spec,
                row['job_id'],
                row['status']
            )
            row_values.append(value)
        rows.append(row_values)

    return {
        'display_name': config['display_name'],
        'headers': config['headers'],
        'rows': rows,
        'notes': config.get('notes', []),
        'warning': config.get('warning'),
        'footer': config.get('footer'),
        'total_jobs': total_jobs,
        'shown_jobs': len(rows)
    }


def display_job_preview(
    batch_id: int,
    batch_type: str,
    ux_module,
    limit: int = 5,
    schema: str = 'public'
) -> None:
    """
    Display a formatted preview of job configurations for a batch.

    This is a convenience function that gets preview data and displays it
    using the provided UX module.

    Args:
        batch_id: Batch ID to preview
        batch_type: Type of batch (must be in JOB_PREVIEW_CONFIG)
        ux_module: UX module with table(), info(), warning() methods
        limit: Maximum number of jobs to preview (default 5)
        schema: Database schema
    """
    preview = get_job_preview_data(batch_id, batch_type, limit, schema)

    ux_module.subheader(f"{preview['display_name']} Jobs (first {limit})")

    if not preview['rows']:
        ux_module.warning("No job configurations found")
        return

    # Display table
    ux_module.table(preview['rows'], headers=preview['headers'])

    # Show "and X more" if there are additional jobs
    if preview['total_jobs'] > preview['shown_jobs']:
        remaining = preview['total_jobs'] - preview['shown_jobs']
        ux_module.info(f"... and {remaining} more job(s)")

    # Display notes
    if preview['notes']:
        ux_module.info("\nEach job configuration contains:")
        for note in preview['notes']:
            ux_module.info(f"  - {note}")

    # Display footer if present
    if preview.get('footer'):
        ux_module.info(preview['footer'])

    # Display warning if present
    if preview.get('warning'):
        ux_module.warning(f"⚠ {preview['warning']}")
