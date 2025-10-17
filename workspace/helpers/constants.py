"""
IRP Notebook Framework - Configuration and Constants
"""

import os
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

WORKSPACE_PATH = Path(f'{os.getenv("NOTEBOOK_HOME_DIR", "/home/jovyan")}/workspace')
HELPERS_PATH = WORKSPACE_PATH / 'helpers'
WORKFLOWS_PATH = WORKSPACE_PATH / 'workflows'
TEMPLATE_PATH = WORKFLOWS_PATH / '_Template'
TOOLS_PATH = WORKFLOWS_PATH / '_Tools'
ARCHIVE_PATH = WORKFLOWS_PATH / '_Archive'

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
        """Statuses that are ready for submission"""
        return [cls.INITIATED]

    def __str__(self):
        return self.value

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
    'max_length': 100,
    'allowed_chars': r'^[a-zA-Z0-9_\-]+$',
    'valid_pattern': r'^Analysis-20\d{2}-Q[1-4](-[\w-]+)?$',
    'example': 'Analysis-2025-Q4 OR Analysis-2025-Q4-v1',
    'forbidden_prefixes': ['Active_']
}

# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================

# List of required tabs in configuration Excel file
CONFIGURATION_TAB_LIST = ['TAB-A', 'TAB-B']

# Required columns for each configuration tab
CONFIGURATION_COLUMNS = {
    'TAB-A': ['A-1', 'A-2', 'A-3'],
    'TAB-B': ['B-1', 'B-2']
}