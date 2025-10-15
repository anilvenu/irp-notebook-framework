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

DB_CONFIG = {
    'host': os.getenv('DB_SERVER', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'irp_db'),
    'user': os.getenv('DB_USER', 'irp_user'),
    'password': os.getenv('DB_PASSWORD', 'irp_pass')
}

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

    @classmethod
    def all(cls):
        return [cls.INITIATED, cls.ACTIVE, cls.COMPLETED, cls.FAILED, cls.CANCELLED]


class ConfigurationStatus:
    NEW = 'NEW'
    VALID = 'VALID'
    ACTIVE = 'ACTIVE'
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
    FORCED_OK = 'FORCED_OK'


    @classmethod
    def all(cls):
        return [
            cls.INITIATED, cls.SUBMITTED, cls.QUEUED, cls.PENDING,
            cls.RUNNING, cls.FINISHED, cls.FAILED, cls.CANCEL_REQUESTED,
            cls.CANCELLING, cls.CANCELLED, cls.FORCED_OK
        ]
    @classmethod
    def terminal(cls):
        """Terminal statuses - job cannot continue from these"""
        return [cls.FINISHED, cls.FAILED, cls.CANCELLED, cls.FORCED_OK]

    @classmethod
    def active(cls):
        """Active statuses - job is still in progress"""
        return [cls.PENDING, cls.RUNNING, cls.CANCEL_REQUESTED, cls.CANCELLING]

    @classmethod
    def completed(cls):
        """Completed statuses - job has finished one way or another"""
        return [cls.FINISHED, cls.FORCED_OK]

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