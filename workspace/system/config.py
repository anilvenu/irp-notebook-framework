"""
IRP Notebook Framework - Configuration and Constants
"""

import os
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

WORKSPACE_PATH = Path(f'{os.getenv("NOTEBOOK_HOME_DIR", "/home/jovyan")}/workspace')
SYSTEM_PATH = WORKSPACE_PATH / 'system'
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
    ACTIVE = 'active'
    ARCHIVED = 'archived'
    FAILED = 'failed'
    
    @classmethod
    def all(cls):
        return [cls.ACTIVE, cls.ARCHIVED, cls.FAILED]


class StepStatus:
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    
    @classmethod
    def all(cls):
        return [cls.RUNNING, cls.COMPLETED, cls.FAILED, cls.SKIPPED]
    
    @classmethod
    def terminal(cls):
        """Terminal statuses - step cannot continue from these"""
        return [cls.COMPLETED, cls.FAILED, cls.SKIPPED]


class BatchStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    
    @classmethod
    def all(cls):
        return [cls.PENDING, cls.RUNNING, cls.COMPLETED, cls.FAILED, cls.CANCELLED]


class JobStatus:
    PENDING = 'pending'
    SUBMITTED = 'submitted'
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    
    @classmethod
    def all(cls):
        return [cls.PENDING, cls.SUBMITTED, cls.QUEUED, cls.RUNNING, 
                cls.COMPLETED, cls.FAILED, cls.CANCELLED]

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
    'forbidden_prefixes': ['Active_', 'Archive_', '_']
}