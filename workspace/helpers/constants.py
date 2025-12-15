"""
IRP Notebook Framework - Configuration and Constants
"""

import os
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# Determine workspace path - works in both Docker and local environments
if os.getenv("NOTEBOOK_HOME_DIR"):
    # Running in Docker/Jupyter container
    WORKSPACE_PATH = Path(f'{os.getenv("NOTEBOOK_HOME_DIR")}/workspace')
else:
    # Running locally (tests, direct Python execution)
    # Workspace is relative to this constants.py file location
    WORKSPACE_PATH = Path(__file__).parent.parent.resolve()

HELPERS_PATH = WORKSPACE_PATH / 'helpers'
WORKFLOWS_PATH = WORKSPACE_PATH / 'workflows'
TEMPLATE_PATH = WORKFLOWS_PATH / '_Template'
TOOLS_PATH = WORKFLOWS_PATH / '_Tools'
ARCHIVE_PATH = WORKFLOWS_PATH / '_Archive'
SQL_SCRIPTS_PATH = WORKSPACE_PATH / 'sql'

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

# Database configuration - NO DEFAULTS for security
# Environment variables MUST be set explicitly:
# - Production: Set via docker-compose.yml (reads from .env)
# - Test: Set via run-tests.sh script

DB_CONFIG = {
    'host': os.getenv('DB_SERVER'),
    'port': int(os.getenv('DB_PORT', '5432')),  # Port defaults to 5432 (standard PostgreSQL)
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Validate database configuration - fail fast if incomplete
_missing_config = [k for k, v in DB_CONFIG.items() if v is None and k != 'port']
if _missing_config:
    raise ValueError(
        f"Database configuration incomplete! Missing environment variables: {', '.join(_missing_config).upper()}\n"
        f"Required: DB_NAME, DB_USER, DB_PASSWORD, DB_SERVER\n"
        f"Ensure these are set in docker-compose.yml (production) or run-tests.sh (test)"
    )

# ============================================================================
# SYSTEM CONFIGURATION
# ============================================================================

SYSTEM_USER = os.getenv('SYSTEM_USER', 'notebook_user')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# ============================================================================
# MOODY'S RISK MODELER CONFIGURATION
# ============================================================================

DEFAULT_DATABASE_SERVER = 'databridge-1'

# ============================================================================
# STATUS ENUMS
# ============================================================================

class CycleStatus:
    ACTIVE = 'ACTIVE'
    ARCHIVED = 'ARCHIVED'
    
    @classmethod
    def all(cls):
        return [cls.ACTIVE, cls.ARCHIVED]


class StepStatus:
    ACTIVE = 'ACTIVE'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'
    
    @classmethod
    def all(cls):
        return [cls.ACTIVE, cls.COMPLETED, cls.FAILED, cls.SKIPPED]
    
    @classmethod
    def terminal(cls):
        """Terminal statuses - step cannot continue from these"""
        return [cls.COMPLETED, cls.FAILED, cls.SKIPPED]


class BatchStatus:
    INITIATED = 'INITIATED'
    ACTIVE = 'ACTIVE'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    ERROR = 'ERROR'

    @classmethod
    def all(cls):
        return [cls.INITIATED, cls.ACTIVE, cls.COMPLETED, cls.FAILED, cls.CANCELLED, cls.ERROR]


class ConfigurationStatus:
    NEW = 'NEW'
    VALID = 'VALID'
    ACTIVE = 'ACTIVE'   # TOTO - Remove if not used
    ERROR = 'ERROR'

    @classmethod
    def all(cls):
        return [cls.NEW, cls.VALID, cls.ACTIVE, cls.ERROR]


class JobStatus:
    INITIATED = 'INITIATED'
    SUBMITTED = 'SUBMITTED'
    QUEUED = 'QUEUED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
    FAILED = 'FAILED'
    CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    CANCELLING = 'CANCELLING'
    CANCELLED = 'CANCELLED'
    ERROR = 'ERROR'


    @classmethod
    def all(cls):
        return [
            cls.INITIATED, cls.SUBMITTED, cls.QUEUED, cls.PENDING,
            cls.RUNNING, cls.FINISHED, cls.FAILED, cls.CANCEL_REQUESTED,
            cls.CANCELLING, cls.CANCELLED, cls.ERROR
        ]

    @classmethod
    def ready_for_submit(cls):
        """Statuses that are ready for submission (including retry of failed submissions)"""
        return [cls.INITIATED, cls.FAILED, cls.ERROR]

    @classmethod
    def terminal(cls):
        """Terminal statuses - job has finished processing (success or failure)"""
        return [cls.FINISHED, cls.FAILED, cls.CANCELLED, cls.ERROR]

    @classmethod
    def failed(cls):
        """Failed terminal statuses - job ended unsuccessfully"""
        return [cls.FAILED, cls.CANCELLED, cls.ERROR]

    def __str__(self):
        return self.value

# ============================================================================
# BATCH TYPES
# ============================================================================

class BatchType:
    """
    Batch type definitions with execution pattern metadata.

    Execution Patterns:
    - SYNCHRONOUS: Jobs complete quickly, can wait for immediate results
    - ASYNCHRONOUS: Jobs take time, require polling/monitoring
    """
    # Batch type constants
    EDM_CREATION = 'EDM Creation'
    PORTFOLIO_CREATION = 'Portfolio Creation'
    MRI_IMPORT = 'MRI Import'
    CREATE_REINSURANCE_TREATIES = 'Create Reinsurance Treaties'
    EDM_DB_UPGRADE = 'EDM DB Upgrade'
    GEOHAZ = 'GeoHaz'
    PORTFOLIO_MAPPING = 'Portfolio Mapping'
    ANALYSIS = 'Analysis'
    GROUPING = 'Grouping'
    GROUPING_ROLLUP = 'Grouping Rollup'
    EXPORT_TO_RDM = 'Export to RDM'
    STAGING_ETL = 'Staging ETL'
    # Test-only batch types
    TEST_DEFAULT = 'test_default'
    TEST_MULTI_JOB = 'test_multi_job'

    # Execution pattern constants
    SYNCHRONOUS = 'synchronous'
    ASYNCHRONOUS = 'asynchronous'

    # Batch type to execution pattern mapping
    _PATTERNS = {
        EDM_CREATION: ASYNCHRONOUS,
        PORTFOLIO_CREATION: SYNCHRONOUS,
        MRI_IMPORT: ASYNCHRONOUS,
        CREATE_REINSURANCE_TREATIES: SYNCHRONOUS,
        EDM_DB_UPGRADE: ASYNCHRONOUS,
        GEOHAZ: ASYNCHRONOUS,
        PORTFOLIO_MAPPING: SYNCHRONOUS,
        ANALYSIS: ASYNCHRONOUS,
        GROUPING: ASYNCHRONOUS,
        GROUPING_ROLLUP: ASYNCHRONOUS,
        EXPORT_TO_RDM: ASYNCHRONOUS,
        STAGING_ETL: ASYNCHRONOUS,
        # Test-only
        TEST_DEFAULT: ASYNCHRONOUS,
        TEST_MULTI_JOB: ASYNCHRONOUS,
    }

    # Batch type to display name field mapping
    # Specifies which field from job_configuration_data to show as "Name" in UI
    _DISPLAY_NAME_FIELDS = {
        EDM_CREATION: 'Database',
        PORTFOLIO_CREATION: 'Portfolio',
        MRI_IMPORT: 'Portfolio',
        CREATE_REINSURANCE_TREATIES: 'Treaty Name',
        EDM_DB_UPGRADE: 'Database',
        GEOHAZ: 'Portfolio',
        PORTFOLIO_MAPPING: 'Portfolio',
        ANALYSIS: 'Analysis Name',
        GROUPING: 'Group_Name',
        GROUPING_ROLLUP: 'Group_Name',
        EXPORT_TO_RDM: 'Group_Name',
        STAGING_ETL: 'Database',
        # Test-only
        TEST_DEFAULT: 'name',
        TEST_MULTI_JOB: 'name',
    }

    @classmethod
    def all(cls):
        """Return all defined batch types"""
        return list(cls._PATTERNS.keys())

    @classmethod
    def get_pattern(cls, batch_type: str) -> str:
        """
        Get the execution pattern for a batch type.

        Args:
            batch_type: The batch type name

        Returns:
            'synchronous' or 'asynchronous'

        Raises:
            ValueError: If batch_type is not recognized
        """
        if batch_type not in cls._PATTERNS:
            raise ValueError(
                f"Unknown batch type: {batch_type}. "
                f"Valid types: {', '.join(cls.all())}"
            )
        return cls._PATTERNS[batch_type]

    @classmethod
    def is_synchronous(cls, batch_type: str) -> bool:
        """Check if a batch type uses synchronous execution"""
        return cls.get_pattern(batch_type) == cls.SYNCHRONOUS

    @classmethod
    def is_asynchronous(cls, batch_type: str) -> bool:
        """Check if a batch type uses asynchronous execution"""
        return cls.get_pattern(batch_type) == cls.ASYNCHRONOUS

    @classmethod
    def get_by_pattern(cls, pattern: str) -> list:
        """
        Get all batch types that use a specific execution pattern.

        Args:
            pattern: Either 'synchronous' or 'asynchronous'

        Returns:
            List of batch type names matching the pattern
        """
        if pattern not in [cls.SYNCHRONOUS, cls.ASYNCHRONOUS]:
            raise ValueError(f"Invalid pattern: {pattern}. Use 'synchronous' or 'asynchronous'")
        return [bt for bt, p in cls._PATTERNS.items() if p == pattern]

    @classmethod
    def get_display_name_field(cls, batch_type: str) -> str:
        """
        Get the field name to display as 'Name' for a batch type in the UI.

        Args:
            batch_type: The batch type name

        Returns:
            Field name from job_configuration_data to use as display name,
            or None if not configured
        """
        return cls._DISPLAY_NAME_FIELDS.get(batch_type)

# ============================================================================
# DISPLAY SETTINGS
# ============================================================================

DISPLAY_CONFIG = {
    'max_table_rows': 20,
    'max_column_width': 50,
    'progress_bar_width': 40,
    'date_format': '%Y-%m-%d %H:%M:%S'
}

# ============================================================================
# NOTEBOOK PATTERNS
# ============================================================================

# Pattern for extracting stage/step from notebook filename
# Example: "Step_01_Initialize.ipynb" -> (stage=1, step=1, name="Initialize")
NOTEBOOK_PATTERN = r'Step_(\d+)_(.+)\.ipynb'
STAGE_PATTERN = r'Stage_(\d+)_(.+)'

# ============================================================================
# VALIDATION RULES
# ============================================================================

CYCLE_NAME_RULES = {
    'min_length': 3,
    'max_length': 255,
    'example': 'Analysis-2025-Q4 OR Analysis-2025-Q4-v1',
}

# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================

# List of required tabs in configuration Excel file (Assurant format)
CONFIGURATION_TAB_LIST = [
    'Metadata',
    'Databases',
    'Portfolios',
    'Reinsurance Treaties',
    'GeoHaz Thresholds',
    'Analysis Table',
    'Groupings',
    'Products and Perils',
    "Moody's Reference Data"
]

# ============================================================================
# EXCEL CONFIGURATION VALIDATION SCHEMAS
# ============================================================================

# Metadata Sheet (Key-Value Structure, No Header Row)
METADATA_SCHEMA = {
    'structure_type': 'key_value',
    'has_header': False,
    'required_keys': [
        'Current Date Value',
        'EDM Data Version',
        'Geocode Version',
        'Hazard Version',
        'DLM Model Version',
        'Validate DLM Model Versions?',
        'Wildfire HD Model Version',
        'SCS HD Model Version',
        'Inland Flood HD Model Version',
        'Validate HD Model Versions?',
        'Export RDM Name',
        'Cycle Type'
    ],
    'value_types': {
        'Current Date Value': 'string',
        'EDM Data Version': 'string',
        'Geocode Version': 'string',
        'Hazard Version': 'string',
        'DLM Model Version': 'integer',
        'Validate DLM Model Versions?': 'string',
        'Wildfire HD Model Version': 'integer',
        'SCS HD Model Version': 'integer',
        'Inland Flood HD Model Version': 'float',
        'Validate HD Model Versions?': 'string',
        'Export RDM Name': 'string',
        'Cycle Type': 'string'
    },
    'value_patterns': {
        'EDM Data Version': r'\d+(\.\d+)?(\.\d+)?',
        'Geocode Version': r'\d+(\.\d+)?(\.\d+)?',
        'Hazard Version': r'\d+(\.\d+)?(\.\d+)?',
        'Validate DLM Model Versions?': r'^[YN]$',
        'Validate HD Model Versions?': r'^[YN]$',
        'Export RDM Name': r'^(RM_|RMS_|TEST_)RDM_'
    }
}

# Databases Sheet
DATABASES_SCHEMA = {
    'structure_type': 'table',
    'required_columns': ['Database', 'Store in Data Bridge?'],
    'column_types': {
        'Database': 'string',
        'Store in Data Bridge?': 'string'
    },
    'nullable': {
        'Database': False,
        'Store in Data Bridge?': False
    },
    'unique_columns': ['Database'],
    'value_patterns': {
        'Database': r'(RM_|RMS_|TEST_)EDM_\d{6}_.*',  # Accepts RM_EDM, RMS_EDM, and TEST_EDM
        'Store in Data Bridge?': r'^[YN]$'
    }
}

# Portfolios Sheet
PORTFOLIOS_SCHEMA = {
    'structure_type': 'table',
    'required_columns': ['Portfolio', 'Database', 'Import File', 'Base Portfolio?'],
    'column_types': {
        'Portfolio': 'string',
        'Database': 'string',
        'Import File': 'string',
        'Base Portfolio?': 'string'
    },
    'nullable': {
        'Portfolio': False,
        'Database': False,
        'Import File': False,
        'Base Portfolio?': False
    },
    'unique_columns': ['Portfolio'],
    'value_patterns': {
        'Base Portfolio?': r'^[YN]$'
    },
    'foreign_keys': {
        'Database': ('Databases', 'Database')
    }
}

# Reinsurance Treaties Sheet
REINSURANCE_TREATIES_SCHEMA = {
    'structure_type': 'table',
    'required_columns': [
        'Treaty Name', 'Treaty Number', 'Type', 'Per-Risk Limit', 'Occurrence Limit',
        'Attachment Point', 'Inception Date', 'Expiration Date', 'Currency',
        'Attachment Basis', 'Exposure Level', '% Covered', '% Place', '% Share',
        '% Retention', 'Premium', 'Reinstatements', '% Reinstatement Charge',
        'Aggregate Limit', 'Aggregate Deductible Amount', 'Inuring Priority',
        'Producer', 'Tags'
    ],
    'column_types': {
        'Treaty Name': 'string',
        'Treaty Number': 'string',
        'Type': 'string',
        'Per-Risk Limit': 'integer',
        'Occurrence Limit': 'integer',
        'Attachment Point': 'integer',
        'Inception Date': 'date',
        'Expiration Date': 'date',
        'Currency': 'string',
        'Attachment Basis': 'string',
        'Exposure Level': 'string',
        '% Covered': 'integer',
        '% Place': 'integer',
        '% Share': 'integer',
        '% Retention': 'integer',
        'Premium': 'integer',
        'Reinstatements': 'integer',
        '% Reinstatement Charge': 'integer',
        'Aggregate Limit': 'integer',
        'Aggregate Deductible Amount': 'integer',
        'Inuring Priority': 'integer',
        'Producer': 'float',
        'Tags': 'float'
    },
    'nullable': {
        'Treaty Name': False,
        'Treaty Number': False,
        'Type': False,
        'Per-Risk Limit': False,
        'Occurrence Limit': False,
        'Attachment Point': False,
        'Inception Date': False,
        'Expiration Date': False,
        'Currency': False,
        'Attachment Basis': False,
        'Exposure Level': False,
        '% Covered': False,
        '% Place': False,
        '% Share': False,
        '% Retention': False,
        'Premium': False,
        'Reinstatements': False,
        '% Reinstatement Charge': False,
        'Aggregate Limit': False,
        'Aggregate Deductible Amount': False,
        'Inuring Priority': False,
        'Producer': True,
        'Tags': True
    },
    'unique_columns': ['Treaty Name', 'Treaty Number'],
    'value_patterns': {
        'Type': r'^(Working Excess|Quota Share)$'
    },
    'range_constraints': {
        '% Covered': (0, 100),
        '% Place': (0, 100),
        '% Share': (0, 100),
        '% Retention': (0, 100),
        '% Reinstatement Charge': (0, 100)
    }
}

# GeoHaz Thresholds Sheet
GEOHAZ_THRESHOLDS_SCHEMA = {
    'structure_type': 'table',
    'required_columns': ['Geocode Level', 'Import File', '% of Grand Total', 'Threshold %'],
    'column_types': {
        'Geocode Level': 'string',
        'Import File': 'string',
        '% of Grand Total': 'float',
        'Threshold %': 'float'
    },
    'nullable': {
        'Geocode Level': False,
        'Import File': False,
        '% of Grand Total': False,
        'Threshold %': False
    },
    'foreign_keys': {
        'Import File': ('Portfolios', 'Import File')
    },
    'range_constraints': {
        '% of Grand Total': (0, 100),
        'Threshold %': (0, 100)
    }
}

# Analysis Table Sheet
ANALYSIS_TABLE_SCHEMA = {
    'structure_type': 'table',
    'required_columns': [
        'Database', 'Portfolio', 'Analysis Name', 'Analysis Profile', 'Output Profile',
        'Event Rate', 'Reinsurance Treaty 1', 'Reinsurance Treaty 2', 'Reinsurance Treaty 3',
        'Reinsurance Treaty 4', 'Reinsurance Treaty 5', 'Tag 1', 'Tag 2', 'Tag 3', 'Tag 4', 'Tag 5'
    ],
    'column_types': {
        'Database': 'string',
        'Portfolio': 'string',
        'Analysis Name': 'string',
        'Analysis Profile': 'string',
        'Output Profile': 'string',
        'Event Rate': 'string',
        'Reinsurance Treaty 1': 'string',
        'Reinsurance Treaty 2': 'string',
        'Reinsurance Treaty 3': 'string',
        'Reinsurance Treaty 4': 'string',
        'Reinsurance Treaty 5': 'string',
        'Tag 1': 'string',
        'Tag 2': 'string',
        'Tag 3': 'string',
        'Tag 4': 'string',
        'Tag 5': 'string'
    },
    'nullable': {
        'Database': False,
        'Portfolio': False,
        'Analysis Name': False,
        'Analysis Profile': False,
        'Output Profile': False,
        'Event Rate': True,
        'Reinsurance Treaty 1': True,
        'Reinsurance Treaty 2': True,
        'Reinsurance Treaty 3': True,
        'Reinsurance Treaty 4': True,
        'Reinsurance Treaty 5': True,
        'Tag 1': False,
        'Tag 2': True,
        'Tag 3': True,
        'Tag 4': True,
        'Tag 5': True
    },
    'unique_columns': ['Analysis Name'],
    'foreign_keys': {
        'Database': ('Databases', 'Database'),
        'Portfolio': ('Portfolios', 'Portfolio')
    }
}

# Groupings Sheet (Special structure with up to 50 item columns)
GROUPINGS_SCHEMA = {
    'structure_type': 'groupings',
    'key_column': 'Group_Name',
    'max_items': 50,
    'nullable': {
        'Group_Name': False
    },
    'unique_columns': ['Group_Name']
}

# Products and Perils Sheet
PRODUCTS_PERILS_SCHEMA = {
    'structure_type': 'table',
    'required_columns': ['Analysis Name', 'Peril', 'Product Group'],
    'column_types': {
        'Analysis Name': 'string',
        'Peril': 'string',
        'Product Group': 'string'
    },
    'nullable': {
        'Analysis Name': False,
        'Peril': True,
        'Product Group': True
    }
    # foreign_keys removed - Analysis Name can be EITHER Analysis Name OR Group Name
    # Validated separately in _validate_special_references()
}

# Moody's Reference Data Sheet (Dictionary of Lists)
MOODYS_REFERENCE_SCHEMA = {
    'structure_type': 'dict_of_lists',
    'required_columns': ['Model Profiles', 'Output Profiles', 'Event Rate Schemes'],
    'unique_within_column': True
}

# Master schema registry
EXCEL_VALIDATION_SCHEMAS = {
    'Metadata': METADATA_SCHEMA,
    'Databases': DATABASES_SCHEMA,
    'Portfolios': PORTFOLIOS_SCHEMA,
    'Reinsurance Treaties': REINSURANCE_TREATIES_SCHEMA,
    'GeoHaz Thresholds': GEOHAZ_THRESHOLDS_SCHEMA,
    'Analysis Table': ANALYSIS_TABLE_SCHEMA,
    'Groupings': GROUPINGS_SCHEMA,
    'Products and Perils': PRODUCTS_PERILS_SCHEMA,
    "Moody's Reference Data": MOODYS_REFERENCE_SCHEMA
}

# ============================================================================
# VALIDATION ERROR CODES
# ============================================================================

VALIDATION_ERROR_CODES = {
    # Structure errors
    'STRUCT-001': 'Missing required sheet: {sheet_name}',
    'STRUCT-002': 'Missing required columns: {columns} in sheet {sheet_name}',
    'STRUCT-003': 'Missing required key: {key} in sheet {sheet_name}',
    'STRUCT-004': 'Invalid structure type: {structure_type} for sheet {sheet_name}',

    # Type errors
    'TYPE-001': 'Column {column} has wrong type in sheet {sheet_name} (expected {expected}, got {actual})',
    'TYPE-002': 'Key {key} has wrong type in sheet {sheet_name} (expected {expected})',
    'TYPE-003': 'Cannot convert value {value} to type {expected_type} in {location}',

    # Nullability errors
    'NULL-001': 'Column {column} has null values in sheet {sheet_name} (not nullable)',
    'NULL-002': 'Key {key} is null in sheet {sheet_name} (required)',
    'NULL-003': 'Row {row} has null value in required column {column}',

    # Reference errors (Foreign Keys)
    'REF-001': 'Broken reference: {column} value "{value}" not found in {ref_sheet}.{ref_column}',
    'REF-002': 'Circular reference detected in Groupings: {group_chain}',
    'REF-003': 'Invalid reference in Groupings: {item} not found in any valid sheet',
    'REF-004': 'Analysis Profile "{value}" not found in Moody\'s Reference Data',
    'REF-005': 'Output Profile "{value}" not found in Moody\'s Reference Data',
    'REF-006': 'Event Rate Scheme "{value}" not found in Moody\'s Reference Data',
    'REF-007': 'Reinsurance Treaty "{value}" not found in Reinsurance Treaties',

    # API Reference Data Validation errors
    'API-REF-001': 'Model Profile "{value}" not found in Moody\'s system',
    'API-REF-002': 'Output Profile "{value}" not found in Moody\'s system',
    'API-REF-003': 'Event Rate Scheme "{value}" not found in Moody\'s system',
    'API-REF-004': 'Failed to validate reference data against Moody\'s API: {error}',

    # Entity Existence Validation errors (entities already exist in Moody's)
    'ENT-EDM-001': 'EDMs already exist: {names}',
    'ENT-EDM-002': 'Required EDMs not found: {names}',
    'ENT-PORT-001': 'Portfolios already exist: {names}',
    'ENT-PORT-002': 'Required portfolios not found: {names}',
    'ENT-ACCT-001': 'Portfolios already have accounts (must be empty for import): {names}',
    'ENT-TREATY-001': 'Treaties already exist: {names}',
    'ENT-ANALYSIS-001': 'Analyses already exist: {names}',
    'ENT-GROUP-001': 'Groups already exist: {names}',
    'ENT-RDM-001': 'RDM already exists: {name}',
    'ENT-SERVER-001': 'Database server not found: {name}',
    'ENT-FILE-001': 'Required files not found: {names}',
    'ENT-API-001': 'Entity validation API error: {error}',

    # Business rule errors
    'BUS-001': 'Database "{database}" has no Base Portfolio (at least one required)',
    'BUS-002': 'Expiration Date must be after Inception Date for treaty {treaty_name}',
    'BUS-003': 'Cannot replace configuration: {batch_count} batch(es) have been created from the current configuration (config_id={config_id}). To start fresh, archive this cycle and create a new one.',
    'BUS-004': 'Provided cycle_id {cycle_id} does not match active cycle {active_cycle_id}',

    # Format/Pattern errors
    'FMT-001': 'Value "{value}" does not match required pattern {pattern} for {field}',
    'FMT-002': 'Invalid version format: {value} (expected format: X or X.Y or X.Y.Z)',
    'FMT-003': 'Invalid date format: {value} in column {column}',
    'FMT-004': 'Database name "{value}" must match pattern RM_EDM_YYYYMM_*',

    # Range errors
    'RANGE-001': 'Value {value} out of range for {field} (expected {min}-{max})',
    'RANGE-002': 'Percentage value {value} must be between 0 and 100 in {field}',
    'RANGE-003': 'Monetary value {value} must be >= 0 in {field}',
    'RANGE-004': 'Integer value {value} must be > 0 in {field}'
}