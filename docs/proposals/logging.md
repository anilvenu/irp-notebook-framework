# IRP Notebook Framework - Logging System Proposal

**Status**: Proposal
**Author**: System Architecture
**Date**: 2025-11-24
**Version**: 1.0

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Product Requirements Document](#product-requirements-document)
3. [Current State Analysis](#current-state-analysis)
4. [Solution Architecture](#solution-architecture)
5. [Context Fallback Strategy](#context-fallback-strategy)
6. [Implementation Plan](#implementation-plan)
7. [Configuration Reference](#configuration-reference)
8. [Testing Strategy](#testing-strategy)
9. [Migration Path](#migration-path)
10. [Open Questions & Decisions](#open-questions--decisions)

---

## Executive Summary

### Problem Statement

The IRP Notebook Framework currently lacks a comprehensive logging system:
- Only 4 of 20+ helper modules have logging
- No centralized configuration
- No file-based logging (only stderr)
- No context-aware logging (cycle/stage/step information missing)
- Errors don't appear inline in Jupyter notebooks
- No support for scheduled/automated notebook execution

### Proposed Solution

Implement a **context-aware, dual-output logging framework** that provides:
- **File-based logging** with time-based rotation (configurable, default: 7 days)
- **Inline notebook output** for errors (configurable verbosity)
- **Automatic context injection** (cycle/stage/step info in every log entry)
- **Comprehensive fallback strategy** for edge cases (no active cycle, _Tools, scheduled runs)
- **Zero breaking changes** to existing code
- **Performance-optimized** (<5ms overhead per log entry)

### Key Features

1. **Context-Aware Logging**: Every log entry includes cycle, stage, and step information
2. **Dual Output Channels**:
   - Detailed logs to rotating files
   - Error-level logs inline in notebook cells (via `ux` module styling)
3. **Intelligent Fallback**: Works seamlessly across:
   - Active cycles
   - _Tools notebooks (no active cycle)
   - System operations (pre-cycle)
   - Scheduled/automated execution
   - Test environments
4. **Flexible Configuration**: Global defaults with per-module and per-notebook overrides
5. **Backward Compatible**: Existing code works without changes

### Success Metrics

- ✅ 100% of helper modules instrumented with logging
- ✅ <5ms P95 logging overhead
- ✅ Zero breaking changes to existing notebooks
- ✅ >90% test coverage for logging code
- ✅ All edge cases handled gracefully

---

## Product Requirements Document

### 1. Functional Requirements

#### FR-1: File-Based Logging

**Requirement**: All logs must write to files under appropriate directories based on execution context.

**Specifications**:
- **Log location hierarchy**:
  - Active cycle: `workflows/Active_{cycle}/logs/`
  - _Tools: `workflows/_Tools/logs/`
  - System (no cycle): `workflows/logs/`
  - Archived cycles: `workflows/_Archive/{cycle}/logs/`
  - Tests: `workspace/tests/logs/{test_schema}/`
- **Rotation**: Time-based, daily by default (configurable via `rotation_days`)
- **Retention**: Keep last N rotations (configurable via `retention_count`, default: 30)
- **Filename pattern**: `irp_framework_{YYYY-MM-DD}.log`
- **Scheduled runs**: Optional separate file `scheduled_{YYYY-MM-DD}.log`

**Priority**: P0 (Critical)

#### FR-2: Log Levels

**Requirement**: Support standard Python logging levels with sensible defaults.

**Specifications**:
- **Levels**: `DEBUG` < `INFO` < `WARNING` < `ERROR` < `CRITICAL`
- **Default**: `INFO` (configurable globally and per-module)
- **Configuration methods**:
  1. Environment variable: `LOG_LEVEL=DEBUG`
  2. Configuration file: `workspace/config/logging.yaml`
  3. Runtime (notebook): `logging_config.set_global_log_level('DEBUG')`
  4. Per-module: `get_logger(__name__, level='DEBUG')`

**Priority**: P0 (Critical)

#### FR-3: Context-Aware Logging

**Requirement**: Every log entry must include execution context information.

**Specifications**:
- **Required fields**:
  - Timestamp (ISO 8601 format: `YYYY-MM-DD HH:MM:SS`)
  - Log level
  - Logger name (module.function)
  - **Cycle name** (e.g., `Analysis-2025-Q4`, `TOOLS`, `SYSTEM`)
  - **Stage** (e.g., `Stage_03_Data_Import`, `N/A`)
  - **Step** (e.g., `Step_01_Create_EDM_Batch`, `N/A`)
  - Message
  - Exception traceback (for ERROR/CRITICAL levels)

- **Context detection hierarchy**:
  1. Explicit thread-local context (set by `notebook_setup.initialize_notebook_context()`)
  2. Inferred from current working directory (parse path)
  3. Environment variables (`CYCLE_NAME`, `STAGE_NAME`, `STEP_NAME`)
  4. Fallback to `SYSTEM` context

**Example log entry**:
```
2025-11-24 14:32:15 | INFO     | helpers.batch | [Cycle:Analysis-2025-Q4 Stage:Stage_03_Data_Import Step:Step_01_Create_EDM_Batch] | Created batch 123 for configuration 456
```

**Priority**: P0 (Critical)

#### FR-4: Inline Notebook Output

**Requirement**: Critical logs must appear inline in Jupyter notebook cells.

**Specifications**:
- **Default behavior**: `ERROR` and `CRITICAL` logs appear inline (in addition to file)
- **Configurable per notebook**:
  ```python
  logging_config.set_notebook_output_level('INFO')  # Show INFO and above
  logging_config.set_notebook_output_level('WARNING')  # Show WARNING and above
  logging_config.disable_notebook_output()  # Disable inline output
  ```
- **Styling**: Use existing `ux` module for visual consistency
  - `ERROR` → `ux.error()` (red box)
  - `WARNING` → `ux.warning()` (yellow box)
  - `INFO` → `ux.info()` (blue box)
- **Format**: Simple, user-friendly messages (not full technical details)

**Priority**: P0 (Critical)

#### FR-5: Performance

**Requirement**: Logging must have minimal performance overhead.

**Specifications**:
- **Latency**: <5ms per log entry at P95
- **Async writes**: For high-throughput operations (optional, configurable)
- **Lazy initialization**: No overhead if logging not used in a module
- **Configurable buffering**: Batch writes to disk (configurable)
- **Hot path optimization**: DEBUG logs skipped when level > DEBUG (no string formatting)

**Priority**: P1 (High)

#### FR-6: Configuration System

**Requirement**: Centralized configuration with flexible overrides.

**Specifications**:
- **Global config**: `workspace/config/logging.yaml`
- **Override hierarchy** (highest to lowest priority):
  1. Runtime API calls (`set_global_log_level()`)
  2. Environment variables (`LOG_LEVEL`, `LOG_DIR`, etc.)
  3. Configuration file (`logging.yaml`)
  4. Code defaults
- **Per-module overrides**: Supported in config file
- **Hot reload**: Configuration changes apply to new loggers (existing loggers retain config)

**Priority**: P1 (High)

#### FR-7: Scheduled/Automated Execution Support

**Requirement**: Support for automated notebook execution without file logging.

**Specifications**:
- **Disable file logging**: `export DISABLE_FILE_LOGGING=true`
- **Custom log directory**: `export LOG_DIR=/var/log/irp/scheduled`
- **Separate log files**: `export NOTEBOOK_EXECUTION_MODE=scheduled` → `scheduled_{date}.log`
- **Context override**: `export CYCLE_NAME=Analysis-2025-Q4` (for scheduled jobs)

**Priority**: P1 (High)

### 2. Non-Functional Requirements

#### NFR-1: Backward Compatibility

**Requirement**: Existing code must work without modifications.

**Specifications**:
- Existing `step.log()` database logging preserved and unchanged
- Existing `ux` module output unchanged
- No changes required to existing notebooks
- No changes required to existing helper function signatures

**Priority**: P0 (Critical)

#### NFR-2: Testing

**Requirement**: Comprehensive test coverage with isolated test logging.

**Specifications**:
- Test logs isolated per test file (e.g., `test_batch.log` → `workspace/tests/logs/test_batch/`)
- Ability to capture and assert on log messages (pytest `caplog` fixture)
- No file writes in CI unless explicitly enabled (`--preserve-logs` flag)
- Test schema logs separate from production logs

**Priority**: P0 (Critical)

#### NFR-3: Observability

**Requirement**: Tools for viewing and analyzing logs.

**Specifications**:
- Log viewer notebook in `_Tools/notebooks/View_Logs.ipynb`
- Search and filter capabilities (by cycle, module, level, date range)
- Log statistics (counts by level, module, cycle)
- Optional structured logging (JSON format) for advanced analysis

**Priority**: P2 (Medium)

#### NFR-4: Security

**Requirement**: Prevent sensitive data leakage in logs.

**Specifications**:
- No passwords, API keys, or tokens in logs
- SQL queries sanitized (parameter values masked or excluded)
- PII detection and masking (optional, configurable)
- File permissions: 644 (owner read/write, group/other read-only)
- Log rotation preserves permissions

**Priority**: P1 (High)

---

## Current State Analysis

### Modules with Logging (4/20+)

| Module | Logging Pattern | Log Destinations | Notes |
|--------|----------------|------------------|-------|
| `step_chain.py` | `logger = logging.getLogger(__name__)` | stderr only | Step chaining logic |
| `notebook_executor.py` | `logger = logging.getLogger(__name__)` | stderr only | Notebook execution engine |
| `teams_notification.py` | `logger = logging.getLogger(__name__)` | stderr only | Teams webhook notifications |
| `sqlserver.py` | `logger = logging.getLogger(__name__)` | stderr only | SQL Server operations |

### Modules WITHOUT Logging (16+)

**Critical (workflow orchestration)**:
- `database.py` - All PostgreSQL operations (most critical)
- `batch.py` - Batch creation, submission, reconciliation
- `job.py` - Job management, submission, tracking
- `cycle.py` - Cycle operations
- `step.py` - Step execution tracking
- `configuration.py` - Configuration loading and validation

**Supporting modules**:
- `context.py` - WorkContext parsing
- `stage.py` - Stage operations
- `csv_export.py` - CSV export utilities
- `ux.py` - Display utilities
- `notebook_setup.py` - Notebook initialization
- `constants.py` - Configuration and enums
- `irp_integration/*` - External integrations (Moody's API)

### Current Output Mechanisms

1. **Database logging**: `step.log()` → `irp_step_run_log` table
   - High-level step events only
   - Stored in database for reporting
   - NOT detailed technical logs

2. **Visual output**: `ux.error()`, `ux.success()`, `ux.info()` → Jupyter cell output
   - User-facing messages only
   - No persistence
   - No technical details

3. **Python logging**: 4 modules → stderr only
   - No file output
   - No context information
   - Lost after notebook execution completes

### Current Error Handling

**Pattern**: Custom exception classes per module
```python
class BatchError(Exception):
    """Custom exception for batch operation errors"""
    pass

# Usage
try:
    result = execute_query(...)
except DatabaseError as e:
    raise BatchError(f"Failed to create batch: {str(e)}")
```

**Issues**:
- Exception messages not logged to file
- Stack traces lost if not captured in notebook
- No context (cycle/stage/step) in error messages
- Difficult to debug issues after the fact

### Directory Structure

```
workspace/
├── workflows/
│   ├── _Template/
│   │   └── logs/                  # Empty (just .gitkeep)
│   ├── _Tools/
│   │   └── logs/                  # Empty (just .gitkeep)
│   └── Active_Analysis-2025-Q4/
│       └── logs/                  # Empty (just .gitkeep)
└── tests/
    └── logs/                      # Does not exist
```

**Key Insight**: Log directories exist but are unused. Infrastructure ready for file logging.

---

## Solution Architecture

### 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Notebook Cell                            │
│  from helpers.batch import create_batch                          │
│  batch_id = create_batch('edm_create', config_id, step_id)       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Helper Module (batch.py)                      │
│  logger = get_logger(__name__)                                   │
│  logger.info("Created batch %s", batch_id)                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              CycleAwareLogger (Custom Logger Class)              │
│  - Injects context (cycle/stage/step) into LogRecord            │
│  - Routes to multiple handlers                                  │
└────────────┬────────────────────────────────────┬───────────────┘
             │                                    │
             ▼                                    ▼
┌────────────────────────────┐      ┌────────────────────────────┐
│ TimedRotatingFileHandler   │      │  NotebookOutputHandler     │
│ - Writes to disk           │      │  - Writes to notebook cell │
│ - Rotates daily            │      │  - Uses ux module          │
│ - Detailed format          │      │  - ERROR+ only (default)   │
└────────────┬───────────────┘      └────────────┬───────────────┘
             │                                    │
             ▼                                    ▼
┌────────────────────────────┐      ┌────────────────────────────┐
│ Log File                   │      │ Jupyter Cell Output        │
│ Active_Q4/logs/            │      │ (Inline, formatted)        │
│ irp_framework_2025-11-24   │      │                            │
│ .log                       │      │ ✗ Error: Batch creation    │
└────────────────────────────┘      │   failed: Invalid config   │
                                     └────────────────────────────┘
```

### 2. Component Specifications

#### 2.1 Core Components

**A. `logging_config.py` - Central Configuration Module**

Location: `workspace/helpers/logging_config.py`

**Public API**:
```python
# Logger factory
def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get or create a context-aware logger."""

# Initialization (called by notebook_setup.py)
def initialize_logging(context: Optional[WorkContext] = None) -> None:
    """Initialize logging system with configuration."""

# Runtime configuration
def set_global_log_level(level: str) -> None:
    """Change log level for all loggers."""

def set_notebook_output_level(level: str) -> None:
    """Set minimum level for inline notebook output."""

def disable_notebook_output() -> None:
    """Disable inline notebook output (file only)."""

def enable_notebook_output() -> None:
    """Enable inline notebook output."""

# Context management
def set_context(context: WorkContext) -> None:
    """Explicitly set execution context for logging."""

def clear_context() -> None:
    """Clear execution context (useful for testing)."""

# Utilities
def get_log_file_path(context: Optional[WorkContext] = None) -> Optional[Path]:
    """Get current log file path based on context."""

def get_current_context() -> WorkContext:
    """Get current execution context."""
```

**B. `CycleAwareLogger` - Custom Logger Class**

Extends `logging.Logger` to automatically inject context into every log record.

```python
class CycleAwareLogger(logging.Logger):
    """Logger that automatically injects cycle/stage/step context."""

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        """Override to inject context into every log record."""
        record = super().makeRecord(name, level, fn, lno, msg, args, exc_info,
                                    func, extra, sinfo)

        # Inject context
        context = _detect_context()
        record.cycle_name = context.cycle_name
        record.stage = context.stage
        record.step = context.step

        return record
```

**C. `NotebookOutputHandler` - Custom Handler for Jupyter**

Writes logs to Jupyter notebook cell output using `ux` module.

```python
class NotebookOutputHandler(logging.Handler):
    """Handler that writes to Jupyter notebook cell output."""

    def __init__(self, level=logging.ERROR):
        super().__init__(level)
        self.enabled = True

    def emit(self, record):
        """Emit log record to notebook cell output."""
        if not self.enabled:
            return

        try:
            from helpers import ux

            msg = self.format(record)

            if record.levelno >= logging.ERROR:
                ux.error(f"✗ {msg}")
            elif record.levelno >= logging.WARNING:
                ux.warning(f"⚠ {msg}")
            elif record.levelno >= logging.INFO:
                ux.info(f"ℹ {msg}")
        except Exception:
            self.handleError(record)
```

**D. `ContextFilter` - Logging Filter**

Injects cycle/stage/step into log records (alternative to custom logger).

```python
class ContextFilter(logging.Filter):
    """Filter that injects cycle/stage/step into log records."""

    def filter(self, record):
        """Add context fields to log record."""
        context = _detect_context()
        record.cycle_name = context.cycle_name
        record.stage = context.stage
        record.step = context.step
        return True
```

### 3. Log Formats

#### File Log Format (Human-Readable)

**Default format**:
```
%(asctime)s | %(levelname)-8s | %(name)s | [Cycle:%(cycle_name)s Stage:%(stage)s Step:%(step)s] | %(message)s
```

**Example output**:
```
2025-11-24 14:32:15 | INFO     | helpers.batch | [Cycle:Analysis-2025-Q4 Stage:Stage_03_Data_Import Step:Step_01_Create_EDM_Batch] | Created batch 123 for configuration 456
2025-11-24 14:32:18 | DEBUG    | helpers.database | [Cycle:Analysis-2025-Q4 Stage:Stage_03_Data_Import Step:Step_01_Create_EDM_Batch] | Executing query: INSERT INTO irp_batch...
2025-11-24 14:32:22 | ERROR    | helpers.job | [Cycle:Analysis-2025-Q4 Stage:Stage_03_Data_Import Step:Step_01_Create_EDM_Batch] | Failed to submit job 789: Connection timeout
Traceback (most recent call last):
  File "/workspace/helpers/job.py", line 234, in submit_job
    response = api_client.submit(job_data)
  ConnectionError: Connection timeout after 30s
```

#### Notebook Output Format (User-Friendly)

**Format**: `%(levelname)s: %(message)s`

**Example**:
```
ERROR: Failed to submit job 789: Connection timeout
```

Rendered with `ux.error()` styling (red box in Jupyter).

#### Structured Log Format (JSON - Optional)

**Format**: JSON lines (one JSON object per line)

**Example**:
```json
{
  "timestamp": "2025-11-24T14:32:15.123456Z",
  "level": "ERROR",
  "logger": "helpers.batch",
  "cycle": "Analysis-2025-Q4",
  "stage": "Stage_03_Data_Import",
  "step": "Step_01_Create_EDM_Batch",
  "message": "Failed to create batch",
  "exception": {
    "type": "BatchError",
    "message": "Invalid configuration",
    "traceback": "Traceback (most recent call last):\n  File ..."
  },
  "extra": {
    "batch_type": "edm_create",
    "config_id": 456
  }
}
```

**Use case**: Advanced log analysis, aggregation, external log management systems.

---

## Context Fallback Strategy

### 1. Context Detection Hierarchy

The logging system detects execution context in this priority order:

```
┌─────────────────────────────────────────────────────────┐
│  Priority 1: Explicit Thread-Local Context              │
│  - Set by initialize_notebook_context()                 │
│  - Most reliable, always use if available               │
└─────────────────────┬───────────────────────────────────┘
                      │ Not available
                      ▼
┌─────────────────────────────────────────────────────────┐
│  Priority 2: Infer from Current Working Directory       │
│  - Parse cwd path for Active_, _Tools, _Archive         │
│  - Use WorkContext.from_path()                          │
└─────────────────────┬───────────────────────────────────┘
                      │ Path parsing fails
                      ▼
┌─────────────────────────────────────────────────────────┐
│  Priority 3: Environment Variables                      │
│  - CYCLE_NAME, STAGE_NAME, STEP_NAME                    │
│  - Used for scheduled/automated execution               │
└─────────────────────┬───────────────────────────────────┘
                      │ Not set
                      ▼
┌─────────────────────────────────────────────────────────┐
│  Priority 4: Fallback to SYSTEM Context                 │
│  - cycle_name='SYSTEM', stage='N/A', step='N/A'         │
│  - Always works, safe fallback                          │
└─────────────────────────────────────────────────────────┘
```

### 2. Implementation

```python
def _detect_context() -> WorkContext:
    """
    Detect execution context with comprehensive fallback strategy.

    Returns:
        WorkContext with cycle/stage/step info, or fallback SYSTEM context
    """
    # Priority 1: Explicit thread-local context
    if hasattr(_thread_local, 'context') and _thread_local.context:
        return _thread_local.context

    # Priority 2: Infer from current working directory
    try:
        cwd = Path.cwd()

        # Check if in Active cycle
        if 'Active_' in str(cwd):
            return WorkContext.from_path(cwd)

        # Check if in _Tools
        if '_Tools' in str(cwd):
            return WorkContext(cycle_name='TOOLS', stage='N/A', step='N/A')

        # Check if in _Archive
        if '_Archive' in str(cwd):
            parts = str(cwd).split('_Archive/')
            if len(parts) > 1:
                cycle_name = parts[1].split('/')[0]
                return WorkContext(
                    cycle_name=f"ARCHIVE:{cycle_name}",
                    stage='N/A',
                    step='N/A'
                )
    except (WorkContextError, Exception):
        pass

    # Priority 3: Environment variable override
    if os.getenv('CYCLE_NAME'):
        return WorkContext(
            cycle_name=os.getenv('CYCLE_NAME'),
            stage=os.getenv('STAGE_NAME', 'N/A'),
            step=os.getenv('STEP_NAME', 'N/A')
        )

    # Priority 4: Fallback to SYSTEM context
    return WorkContext(cycle_name='SYSTEM', stage='N/A', step='N/A')
```

### 3. Log Location Mapping

| Scenario | Context Detection | Log File Path | Context Label |
|----------|------------------|---------------|---------------|
| **Active Cycle Notebook** | Thread-local from `initialize_notebook_context()` | `workflows/Active_Analysis-2025-Q4/logs/irp_framework_2025-11-24.log` | `[Cycle:Analysis-2025-Q4 Stage:Stage_03 Step:Step_01]` |
| **_Tools Notebook** | CWD contains `_Tools` | `workflows/_Tools/logs/irp_framework_2025-11-24.log` | `[Cycle:TOOLS Stage:N/A Step:N/A]` |
| **Archived Cycle (read)** | CWD contains `_Archive` | `workflows/_Archive/Analysis-2025-Q3/logs/irp_framework_2025-11-24.log` | `[Cycle:ARCHIVE:Analysis-2025-Q3 Stage:N/A Step:N/A]` |
| **No Active Cycle** | Fallback to SYSTEM | `workflows/logs/irp_framework_2025-11-24.log` | `[Cycle:SYSTEM Stage:N/A Step:N/A]` |
| **Python Script** | Fallback to SYSTEM | `workflows/logs/irp_framework_2025-11-24.log` | `[Cycle:SYSTEM Stage:N/A Step:N/A]` |
| **Scheduled Notebook** | Environment: `CYCLE_NAME=Q4` | `${LOG_DIR}/irp_framework_2025-11-24.log` | `[Cycle:Q4 Stage:N/A Step:N/A]` |
| **Test Execution** | Environment: `DB_SCHEMA=test_batch` | `workspace/tests/logs/test_batch/irp_framework_2025-11-24.log` | `[Cycle:TEST Stage:N/A Step:N/A]` |

### 4. Log Directory Resolution

```python
def _get_log_directory(context: WorkContext) -> Optional[Path]:
    """
    Determine log directory based on context and environment.

    Args:
        context: WorkContext with cycle/stage/step info

    Returns:
        Path to log directory (creates if doesn't exist), or None if disabled
    """
    # Check for environment override first
    if os.getenv('LOG_DIR'):
        log_dir = Path(os.getenv('LOG_DIR'))
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    # Check if file logging is disabled
    if os.getenv('DISABLE_FILE_LOGGING', '').lower() == 'true':
        return None  # Signal to skip file handler

    # Check if in test environment
    if os.getenv('DB_SCHEMA', '').startswith('test_'):
        test_schema = os.getenv('DB_SCHEMA')
        log_dir = Path('workspace/tests/logs') / test_schema
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    # Determine directory based on context
    if context.cycle_name == 'TOOLS':
        log_dir = WORKFLOWS_PATH / '_Tools' / 'logs'

    elif context.cycle_name == 'SYSTEM':
        log_dir = WORKFLOWS_PATH / 'logs'

    elif context.cycle_name.startswith('ARCHIVE:'):
        cycle_name = context.cycle_name.replace('ARCHIVE:', '')
        log_dir = WORKFLOWS_PATH / '_Archive' / cycle_name / 'logs'

    else:
        # Active cycle - find matching Active_* directory
        active_dirs = list(WORKFLOWS_PATH.glob(f'Active_*{context.cycle_name}*'))
        if active_dirs:
            log_dir = active_dirs[0] / 'logs'
        else:
            # Fallback if no active cycle found
            log_dir = WORKFLOWS_PATH / 'logs'

    # Create directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir
```

### 5. Scheduled Notebook Configuration

#### Scenario A: Disable File Logging

```bash
#!/bin/bash
# Run notebook without file logging (console/stderr only)

export DISABLE_FILE_LOGGING=true
export CYCLE_NAME=Analysis-2025-Q4

cd /workspace/workflows/Active_Analysis-2025-Q4/notebooks/Stage_03_Data_Import
jupyter nbconvert --execute --to notebook --inplace Step_Monitor_Jobs.ipynb

# Result: No log file created, stderr only
```

#### Scenario B: Redirect to Central Log Directory

```bash
#!/bin/bash
# Centralized logs for scheduled jobs

export LOG_DIR=/var/log/irp/scheduled
export CYCLE_NAME=Analysis-2025-Q4-Automated

jupyter nbconvert --execute Step_Submit_Batches.ipynb

# Result: Logs to /var/log/irp/scheduled/irp_framework_2025-11-24.log
```

#### Scenario C: Separate Scheduled Log File

```bash
#!/bin/bash
# Use separate log file for scheduled runs

export NOTEBOOK_EXECUTION_MODE=scheduled
export CYCLE_NAME=Analysis-2025-Q4

jupyter nbconvert --execute Step_Daily_Report.ipynb

# Result: Logs to Active_Analysis-2025-Q4/logs/scheduled_2025-11-24.log
```

### 6. Updated Directory Structure

```
workspace/
├── workflows/
│   ├── logs/                          # NEW: System-level logs (no active cycle)
│   │   ├── irp_framework_2025-11-24.log
│   │   ├── irp_framework_2025-11-23.log
│   │   └── .gitkeep
│   │
│   ├── _Tools/
│   │   ├── notebooks/
│   │   └── logs/                      # Tools-specific logs
│   │       ├── irp_framework_2025-11-24.log
│   │       └── .gitkeep
│   │
│   ├── _Archive/
│   │   └── Analysis-2025-Q3/
│   │       └── logs/                  # Archived cycle logs (retained)
│   │           ├── irp_framework_2025-09-*.log
│   │           └── .gitkeep
│   │
│   └── Active_Analysis-2025-Q4/
│       └── logs/                      # Active cycle logs
│           ├── irp_framework_2025-11-24.log
│           ├── irp_framework_2025-11-23.log
│           ├── scheduled_2025-11-24.log  # Optional: scheduled runs
│           └── .gitkeep
│
└── tests/
    └── logs/                          # Test logs (created dynamically)
        ├── test_batch/
        │   └── irp_framework_2025-11-24.log
        └── test_job/
            └── irp_framework_2025-11-24.log
```

---

## Implementation Plan

### Phase 1: POC (Proof of Concept)

**Goal**: Validate architecture with minimal implementation

**Duration**: 2-3 days

**Scope**:

1. **Create core logging infrastructure** (Day 1)
   - `workspace/helpers/logging_config.py` (new module)
   - `workspace/config/logging.yaml` (new configuration)
   - Implement: `get_logger()`, `initialize_logging()`, context detection
   - Implement: `NotebookOutputHandler`, `ContextFilter`
   - Implement: File rotation logic

2. **Add logging to 2 critical modules** (Day 1-2)
   - Update `workspace/helpers/database.py`
     - Add logging to all CRUD operations
     - Log query execution, row counts, errors
     - Level: DEBUG for query text, INFO for summaries
   - Update `workspace/helpers/batch.py`
     - Add logging to batch lifecycle (create, submit, recon)
     - Log batch status changes, job counts
     - Level: INFO for operations, ERROR for failures

3. **Update notebook initialization** (Day 2)
   - Modify `workspace/helpers/notebook_setup.py`
   - Call `initialize_logging()` in `initialize_notebook_context()`
   - Set thread-local context for logging

4. **Create demo notebook** (Day 2)
   - `workspace/workflows/_Tools/notebooks/Demo_Logging.ipynb`
   - Demonstrate all features:
     - File logging (show log file contents)
     - Inline output (trigger errors to show inline)
     - Context injection (show cycle/stage/step in logs)
     - Level control (change log level dynamically)
     - Fallback scenarios (run from _Tools, no cycle)

5. **Write tests** (Day 3)
   - `workspace/tests/test_logging_config.py` (unit tests)
     - Test context detection hierarchy
     - Test log directory resolution
     - Test file rotation
     - Test notebook handler
   - `workspace/tests/test_logging_integration.py` (integration tests)
     - Test logging in database.py operations
     - Test logging in batch.py operations
     - Test log file creation in correct locations
     - Test fallback scenarios

**Success Criteria**:
- ✅ Logs written to correct directories based on context
- ✅ Errors appear inline in notebook cells
- ✅ Context (cycle/stage/step) included in all log entries
- ✅ Fallback strategy works (TOOLS, SYSTEM, scheduled)
- ✅ No performance degradation (<5ms overhead)
- ✅ All tests pass (>90% coverage)

**Deliverables**:
1. `workspace/helpers/logging_config.py` (new)
2. `workspace/config/logging.yaml` (new)
3. `workspace/helpers/database.py` (modified)
4. `workspace/helpers/batch.py` (modified)
5. `workspace/helpers/notebook_setup.py` (modified)
6. `workspace/workflows/_Tools/notebooks/Demo_Logging.ipynb` (new)
7. `workspace/tests/test_logging_config.py` (new)
8. `workspace/tests/test_logging_integration.py` (new)
9. `workspace/tests/logs/` directory (new)
10. `workspace/workflows/logs/` directory (new)

### Phase 2: Core Rollout

**Goal**: Add logging to all critical helper modules

**Duration**: 3-4 days

**Scope**:

1. **Add logging to workflow modules** (Day 1-2)
   - `job.py` - Job lifecycle, submission, tracking, resubmission
   - `step.py` - Step execution, lifecycle management
   - `cycle.py` - Cycle operations, validation, archival
   - `stage.py` - Stage creation and lookup
   - `configuration.py` - Configuration loading, validation, transformation

2. **Add logging to integration modules** (Day 2-3)
   - `csv_export.py` - CSV export operations, file creation
   - `notebook_executor.py` - Update existing logging to use new system
   - `step_chain.py` - Update existing logging to use new system
   - `teams_notification.py` - Update existing logging to use new system
   - `sqlserver.py` - Update existing logging to use new system

3. **Calibrate log levels** (Day 3)
   - Review all log statements
   - Ensure appropriate levels:
     - DEBUG: Detailed technical info (SQL queries, API payloads)
     - INFO: Normal operations (batch created, job submitted)
     - WARNING: Unusual but handled (retries, resubmissions)
     - ERROR: Failures (exceptions, API errors)
     - CRITICAL: System failures (database down, data corruption)
   - Update `logging.yaml` with per-module overrides

4. **Enhance configuration** (Day 3-4)
   - Add per-module log level overrides
   - Add log rotation and retention settings
   - Add performance tuning options (buffering, async writes)
   - Add structured logging (JSON format) option

5. **Create log viewer notebook** (Day 4)
   - `workspace/workflows/_Tools/notebooks/View_Logs.ipynb`
   - Features:
     - Select cycle (active or archived)
     - Filter by date range
     - Filter by log level
     - Filter by module
     - Search by keyword
     - Display in pandas DataFrame
     - Export filtered logs to CSV

6. **Update documentation** (Day 4)
   - Update `CLAUDE.md` with logging usage
   - Create `docs/LOGGING.md` with detailed guide
   - Update notebook templates with logging examples

**Success Criteria**:
- ✅ All 20+ helper modules have appropriate logging
- ✅ Log levels calibrated (no noise at INFO level)
- ✅ Log viewer notebook functional
- ✅ Log rotation working correctly
- ✅ Test coverage >90% for logging code
- ✅ Documentation complete

**Deliverables**:
1. Updated: All helper modules with logging
2. `workspace/config/logging.yaml` (enhanced)
3. `workspace/workflows/_Tools/notebooks/View_Logs.ipynb` (new)
4. `docs/LOGGING.md` (new documentation)
5. Updated: `CLAUDE.md`, notebook templates

### Phase 3: Advanced Features

**Goal**: Add observability and debugging tools

**Duration**: 2-3 days

**Scope**:

1. **Structured logging (JSON format)** (Day 1)
   - Add JSON formatter option
   - Support extra context fields
   - Add log parsing utilities

2. **Log aggregation across cycles** (Day 1-2)
   - `_Tools/notebooks/Aggregate_Logs.ipynb`
   - Search across all cycles (active + archived)
   - Time-series analysis
   - Error trends and patterns

3. **Log statistics and analytics** (Day 2)
   - Log dashboard in `_Tools/notebooks/Log_Analytics.ipynb`
   - Metrics:
     - Log volume by level/module/cycle
     - Error rates over time
     - Most active modules
     - Average execution times (if instrumented)

4. **Performance profiling integration** (Day 2-3)
   - Add execution time logging
   - Add database query timing
   - Add API call timing
   - Performance reports in log analytics

5. **Alerting integration** (Day 3)
   - Teams notifications for critical errors
   - Email alerts for error threshold breaches
   - Configurable alert rules

6. **Documentation and training** (Day 3)
   - Create logging best practices guide
   - Add code examples for common patterns
   - Create troubleshooting guide
   - Add FAQ section

**Success Criteria**:
- ✅ JSON logging functional and tested
- ✅ Log search across all cycles working
- ✅ Log analytics dashboard useful
- ✅ Performance metrics collected and visible
- ✅ Alerting functional and configurable
- ✅ Documentation comprehensive

**Deliverables**:
1. JSON formatter implementation
2. `_Tools/notebooks/Aggregate_Logs.ipynb` (new)
3. `_Tools/notebooks/Log_Analytics.ipynb` (new)
4. Performance profiling decorators
5. Alerting integration
6. `docs/LOGGING_BEST_PRACTICES.md` (new)
7. `docs/LOGGING_TROUBLESHOOTING.md` (new)

---

## Configuration Reference

### Complete `logging.yaml` Schema

```yaml
# workspace/config/logging.yaml

logging:
  # Version for future compatibility
  version: 1

  # ============================================
  # Global Settings
  # ============================================

  # Default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  default_level: INFO

  # Time-based rotation (days)
  rotation_days: 7

  # Number of rotated files to keep (0 = unlimited)
  retention_count: 30

  # ============================================
  # Context Detection
  # ============================================

  context:
    # Enable fallback to SYSTEM context if detection fails
    fallback_enabled: true

    # Try to infer context from current working directory
    detect_from_cwd: true

    # Require explicit context (raise error if not found)
    # WARNING: Set to false for production
    require_explicit_context: false

  # ============================================
  # File Output Configuration
  # ============================================

  file:
    # Enable file logging
    enabled: true

    # Log filename pattern ({date} replaced with YYYY-MM-DD)
    filename_pattern: "irp_framework_{date}.log"

    # Filename for scheduled execution mode
    scheduled_filename_pattern: "scheduled_{date}.log"

    # Log format (human-readable)
    format: "%(asctime)s | %(levelname)-8s | %(name)-30s | [Cycle:%(cycle_name)s Stage:%(stage)s Step:%(step)s] | %(message)s"

    # Date format
    datefmt: "%Y-%m-%d %H:%M:%S"

    # File permissions (octal)
    permissions: 0o644

    # Buffering (0=unbuffered, >0=buffer size in bytes, -1=system default)
    buffering: -1

    # Async file writes (for high throughput)
    async_enabled: false

  # ============================================
  # Notebook Output Configuration
  # ============================================

  notebook:
    # Enable inline notebook output
    enabled: true

    # Minimum level for inline output (ERROR means only ERROR and CRITICAL)
    level: ERROR

    # Simple format for notebook (user-friendly)
    format: "%(levelname)s: %(message)s"

    # Include timestamp in notebook output
    include_timestamp: false

    # Include module name in notebook output
    include_module: false

  # ============================================
  # Structured Logging (JSON)
  # ============================================

  json:
    # Enable JSON format logging (in addition to or instead of text)
    enabled: false

    # JSON log filename pattern
    filename_pattern: "irp_framework_{date}.json"

    # Pretty print JSON (easier to read, larger files)
    pretty_print: false

    # Include exception details
    include_exception: true

    # Include stack trace
    include_stack: true

  # ============================================
  # Log Locations
  # ============================================

  locations:
    # System logs (no active cycle)
    system_logs: "workflows/logs"

    # Tools logs
    tools_logs: "workflows/_Tools/logs"

    # Active cycle logs ({cycle} replaced with cycle name)
    active_cycle_logs: "workflows/Active_{cycle}/logs"

    # Archived cycle logs
    archived_cycle_logs: "workflows/_Archive/{cycle}/logs"

    # Test logs ({test_schema} replaced with test schema name)
    test_logs: "workspace/tests/logs/{test_schema}"

  # ============================================
  # Environment Variable Overrides
  # ============================================

  env_overrides:
    # Custom log directory (overrides all location settings)
    - LOG_DIR

    # Disable file logging entirely
    - DISABLE_FILE_LOGGING

    # Override context
    - CYCLE_NAME
    - STAGE_NAME
    - STEP_NAME

    # Override log level
    - LOG_LEVEL

    # Execution mode ('scheduled' or 'interactive')
    - NOTEBOOK_EXECUTION_MODE

  # ============================================
  # Per-Module Log Levels
  # ============================================

  loggers:
    # Database operations (very detailed)
    helpers.database:
      level: INFO  # Change to DEBUG for query debugging

    # SQL Server operations
    helpers.sqlserver:
      level: INFO

    # Batch operations
    helpers.batch:
      level: INFO

    # Job operations
    helpers.job:
      level: INFO

    # Cycle operations
    helpers.cycle:
      level: INFO

    # Step operations
    helpers.step:
      level: INFO

    # Configuration operations
    helpers.configuration:
      level: INFO

    # CSV export
    helpers.csv_export:
      level: INFO

    # Context detection
    helpers.context:
      level: WARNING  # Only log issues

    # Notebook execution
    helpers.notebook_executor:
      level: INFO

    # Step chaining
    helpers.step_chain:
      level: INFO

    # Teams notifications
    helpers.teams_notification:
      level: INFO

    # IRP Integration (Moody's API)
    helpers.irp_integration:
      level: INFO

  # ============================================
  # Testing Configuration
  # ============================================

  testing:
    # Enable logging in tests
    enabled: true

    # Write log files in tests (usually disabled)
    file_enabled: false

    # Enable log capture for assertions (pytest caplog)
    capture_enabled: true

    # Test log level
    level: DEBUG

  # ============================================
  # Performance Configuration
  # ============================================

  performance:
    # Enable performance logging (execution time tracking)
    enabled: false

    # Log slow operations (threshold in seconds)
    slow_operation_threshold: 5.0

    # Include SQL query timing
    log_query_timing: true

    # Include API call timing
    log_api_timing: true

  # ============================================
  # Security Configuration
  # ============================================

  security:
    # Mask sensitive data in logs
    mask_sensitive: true

    # Patterns to mask (regex)
    sensitive_patterns:
      - "password=.*"
      - "api_key=.*"
      - "token=.*"
      - "secret=.*"
      - "Authorization: Bearer .*"

    # Mask SQL parameter values
    mask_sql_params: true

    # Maximum log message length (prevent log injection)
    max_message_length: 10000
```

### Environment Variables Reference

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `LOG_LEVEL` | String | `INFO` | Global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `LOG_DIR` | Path | (auto) | Override log directory (absolute path) |
| `DISABLE_FILE_LOGGING` | Boolean | `false` | Disable file logging entirely (true/false) |
| `CYCLE_NAME` | String | (auto) | Override cycle name for context |
| `STAGE_NAME` | String | `N/A` | Override stage name for context |
| `STEP_NAME` | String | `N/A` | Override step name for context |
| `NOTEBOOK_EXECUTION_MODE` | String | `interactive` | Execution mode (interactive/scheduled) |

### Usage Examples

#### Example 1: Enable DEBUG Logging for Database Module

```bash
# In shell
export LOG_LEVEL=DEBUG  # Global override

# Or in logging.yaml
loggers:
  helpers.database:
    level: DEBUG
```

#### Example 2: Scheduled Notebook with Custom Log Location

```bash
#!/bin/bash
export LOG_DIR=/var/log/irp/scheduled
export NOTEBOOK_EXECUTION_MODE=scheduled
export CYCLE_NAME=Analysis-2025-Q4

jupyter nbconvert --execute Step_Daily_Report.ipynb
```

#### Example 3: Disable File Logging in CI/CD

```bash
# In CI/CD pipeline
export DISABLE_FILE_LOGGING=true
pytest workspace/tests/
```

#### Example 4: Runtime Log Level Change (Notebook)

```python
# In notebook cell
from helpers import logging_config

# Show INFO and above inline
logging_config.set_notebook_output_level('INFO')

# Change global level to DEBUG
logging_config.set_global_log_level('DEBUG')

# Execute operations (will see DEBUG logs in file, INFO+ in notebook)
from helpers.batch import create_batch
batch_id = create_batch('edm_create', config_id, step_id)
```

---

## Testing Strategy

### 1. Unit Tests (`test_logging_config.py`)

**Test cases**:

```python
def test_context_detection_from_thread_local(test_schema):
    """Test Priority 1: Explicit thread-local context"""
    from helpers.logging_config import set_context, _detect_context
    from helpers.context import WorkContext

    context = WorkContext(cycle_name='Test-Q4', stage='Stage_01', step='Step_01')
    set_context(context)

    detected = _detect_context()
    assert detected.cycle_name == 'Test-Q4'
    assert detected.stage == 'Stage_01'
    assert detected.step == 'Step_01'


def test_context_detection_from_cwd(test_schema, tmp_path, monkeypatch):
    """Test Priority 2: Infer from current working directory"""
    from helpers.logging_config import _detect_context

    # Create mock directory structure
    active_dir = tmp_path / 'workflows' / 'Active_Analysis-2025-Q4' / 'notebooks'
    active_dir.mkdir(parents=True)

    # Change to that directory
    monkeypatch.chdir(active_dir)

    detected = _detect_context()
    assert 'Analysis-2025-Q4' in detected.cycle_name


def test_context_detection_from_env(test_schema, monkeypatch):
    """Test Priority 3: Environment variables"""
    from helpers.logging_config import _detect_context, clear_context

    clear_context()  # Clear thread-local

    monkeypatch.setenv('CYCLE_NAME', 'Env-Q4')
    monkeypatch.setenv('STAGE_NAME', 'Stage_02')

    detected = _detect_context()
    assert detected.cycle_name == 'Env-Q4'
    assert detected.stage == 'Stage_02'


def test_context_detection_fallback_to_system(test_schema):
    """Test Priority 4: Fallback to SYSTEM"""
    from helpers.logging_config import _detect_context, clear_context

    clear_context()  # Clear all context

    detected = _detect_context()
    assert detected.cycle_name == 'SYSTEM'
    assert detected.stage == 'N/A'
    assert detected.step == 'N/A'


def test_log_directory_resolution_active_cycle(test_schema):
    """Test log directory for active cycle"""
    from helpers.logging_config import _get_log_directory
    from helpers.context import WorkContext

    context = WorkContext(cycle_name='Analysis-2025-Q4', stage='N/A', step='N/A')
    log_dir = _get_log_directory(context)

    assert 'Active_Analysis-2025-Q4/logs' in str(log_dir)


def test_log_directory_resolution_tools(test_schema):
    """Test log directory for _Tools"""
    from helpers.logging_config import _get_log_directory
    from helpers.context import WorkContext

    context = WorkContext(cycle_name='TOOLS', stage='N/A', step='N/A')
    log_dir = _get_log_directory(context)

    assert '_Tools/logs' in str(log_dir)


def test_log_directory_resolution_system(test_schema):
    """Test log directory for SYSTEM (no cycle)"""
    from helpers.logging_config import _get_log_directory
    from helpers.context import WorkContext

    context = WorkContext(cycle_name='SYSTEM', stage='N/A', step='N/A')
    log_dir = _get_log_directory(context)

    assert str(log_dir).endswith('workflows/logs')


def test_disable_file_logging_env(test_schema, monkeypatch):
    """Test DISABLE_FILE_LOGGING environment variable"""
    from helpers.logging_config import _get_log_directory
    from helpers.context import WorkContext

    monkeypatch.setenv('DISABLE_FILE_LOGGING', 'true')

    context = WorkContext(cycle_name='Test', stage='N/A', step='N/A')
    log_dir = _get_log_directory(context)

    assert log_dir is None  # File logging disabled


def test_custom_log_dir_env(test_schema, monkeypatch, tmp_path):
    """Test LOG_DIR environment variable override"""
    from helpers.logging_config import _get_log_directory
    from helpers.context import WorkContext

    custom_dir = tmp_path / 'custom_logs'
    monkeypatch.setenv('LOG_DIR', str(custom_dir))

    context = WorkContext(cycle_name='Test', stage='N/A', step='N/A')
    log_dir = _get_log_directory(context)

    assert log_dir == custom_dir
    assert log_dir.exists()  # Should be created


def test_notebook_output_handler(test_schema, capfd):
    """Test NotebookOutputHandler writes to stdout"""
    import logging
    from helpers.logging_config import NotebookOutputHandler

    handler = NotebookOutputHandler(level=logging.ERROR)
    logger = logging.getLogger('test_notebook_handler')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # ERROR should appear
    logger.error("Test error message")

    # INFO should not appear (below handler level)
    logger.info("Test info message")

    # Check output (simplified - actual test would check ux module calls)
    # In real implementation, would mock ux.error()


def test_log_file_rotation(test_schema):
    """Test log file rotation creates new files"""
    # This is a placeholder - actual test would manipulate time
    # and verify rotation occurs
    pass


def test_context_injection_in_log_record(test_schema):
    """Test that context is injected into log records"""
    import logging
    from helpers.logging_config import get_logger, set_context
    from helpers.context import WorkContext

    context = WorkContext(cycle_name='Test-Q4', stage='Stage_01', step='Step_01')
    set_context(context)

    logger = get_logger('test_logger')

    # Capture log record
    with logging.LoggerAdapter(logger, {}) as adapter:
        # Would need custom handler to capture LogRecord
        # and verify cycle_name, stage, step attributes
        pass
```

### 2. Integration Tests (`test_logging_integration.py`)

**Test cases**:

```python
def test_database_logging_creates_file(test_schema):
    """Test that database operations create log file"""
    from helpers.database import execute_query
    from helpers.logging_config import get_log_file_path

    # Execute database operation
    df = execute_query("SELECT 1 as test", schema=test_schema)

    # Verify log file created
    log_file = get_log_file_path()
    assert log_file.exists()

    # Verify log contains database operation
    log_content = log_file.read_text()
    assert 'SELECT 1 as test' in log_content or 'Executing query' in log_content


def test_batch_logging_includes_context(test_schema):
    """Test that batch operations log with context"""
    from helpers.batch import create_batch
    from helpers.logging_config import get_log_file_path, set_context
    from helpers.context import WorkContext
    from helpers.cycle import register_cycle
    from helpers.configuration import load_configuration
    from helpers.step import Step

    # Set up test data
    cycle_id = register_cycle('Test-Batch-Logging')
    config_id = load_configuration(cycle_id, 'test_config.xlsx')

    context = WorkContext(cycle_name='Test-Batch-Logging', stage='Stage_03', step='Step_01')
    set_context(context)

    # Execute batch operation
    batch_id = create_batch('test_batch', config_id, None)

    # Verify log file contains context
    log_file = get_log_file_path()
    log_content = log_file.read_text()

    assert '[Cycle:Test-Batch-Logging' in log_content
    assert 'Stage:Stage_03' in log_content
    assert 'Step:Step_01' in log_content


def test_logging_in_tools_context(test_schema):
    """Test logging from _Tools notebooks (no active cycle)"""
    from helpers.logging_config import set_context, get_log_file_path
    from helpers.context import WorkContext
    from helpers.cycle import get_all_cycles

    # Set TOOLS context
    context = WorkContext(cycle_name='TOOLS', stage='N/A', step='N/A')
    set_context(context)

    # Execute operation
    cycles = get_all_cycles()

    # Verify log file in _Tools directory
    log_file = get_log_file_path()
    assert '_Tools/logs' in str(log_file)

    # Verify context in logs
    log_content = log_file.read_text()
    assert '[Cycle:TOOLS' in log_content


def test_logging_with_no_context_fallback(test_schema):
    """Test logging falls back to SYSTEM when no context"""
    from helpers.logging_config import clear_context, get_log_file_path
    from helpers.database import execute_query

    # Clear all context
    clear_context()

    # Execute operation
    df = execute_query("SELECT 1", schema=test_schema)

    # Verify log file in workflows/logs (SYSTEM)
    log_file = get_log_file_path()
    assert str(log_file).endswith('workflows/logs/irp_framework_2025-11-24.log')

    # Verify SYSTEM context in logs
    log_content = log_file.read_text()
    assert '[Cycle:SYSTEM' in log_content


def test_error_appears_inline_in_notebook(test_schema, capsys):
    """Test that ERROR level logs appear inline"""
    # This test would need to mock IPython/Jupyter environment
    # and verify ux.error() is called
    pass


def test_scheduled_execution_mode(test_schema, monkeypatch):
    """Test scheduled execution uses separate log file"""
    from helpers.logging_config import get_log_file_path, set_context
    from helpers.context import WorkContext

    monkeypatch.setenv('NOTEBOOK_EXECUTION_MODE', 'scheduled')

    context = WorkContext(cycle_name='Test-Scheduled', stage='N/A', step='N/A')
    set_context(context)

    log_file = get_log_file_path()

    # Verify scheduled log file name
    assert 'scheduled_' in log_file.name


def test_log_performance_overhead(test_schema, benchmark):
    """Test that logging overhead is <5ms"""
    from helpers.logging_config import get_logger

    logger = get_logger('performance_test')

    # Benchmark log call
    def log_operation():
        logger.info("Test message with %s", "parameter")

    result = benchmark(log_operation)

    # Verify <5ms (5000 microseconds)
    assert result.stats.mean < 0.005  # 5ms
```

### 3. Test Coverage Goals

| Component | Target Coverage | Priority |
|-----------|----------------|----------|
| `logging_config.py` | >95% | P0 |
| Database logging (`database.py`) | >90% | P0 |
| Batch logging (`batch.py`) | >90% | P0 |
| Context detection | 100% | P0 |
| Log directory resolution | 100% | P0 |
| Notebook handler | >85% | P1 |
| File rotation | >85% | P1 |

### 4. Performance Testing

**Benchmark tests**:

```python
def test_logging_performance_at_scale(test_schema, benchmark):
    """Test logging performance with 1000 log entries"""
    from helpers.logging_config import get_logger

    logger = get_logger('scale_test')

    def log_1000_entries():
        for i in range(1000):
            logger.info("Log entry %d with data %s", i, {'key': 'value'})

    result = benchmark(log_1000_entries)

    # Average per-log overhead should be <5ms
    avg_per_log = result.stats.mean / 1000
    assert avg_per_log < 0.005


def test_logging_does_not_slow_down_database_operations(test_schema, benchmark):
    """Test that logging doesn't significantly slow down database ops"""
    from helpers.database import execute_query

    # Baseline: execute query without logging
    # (This would require a way to disable logging temporarily)

    # With logging: execute query with logging
    def query_with_logging():
        execute_query("SELECT 1", schema=test_schema)

    result = benchmark(query_with_logging)

    # Should be dominated by database time, not logging time
    # (Specific threshold depends on query complexity)
```

---

## Migration Path

### Phase 1: POC (No Breaking Changes)

**Changes required**: None for existing code

**What works**:
- All existing notebooks work unchanged
- All existing helper functions work unchanged
- Logging happens automatically in instrumented modules (database.py, batch.py)

**What analysts see**:
- Errors now appear inline (new behavior, additive)
- Log files created in logs/ directories (new files, no impact)

### Phase 2: Core Rollout (Optional Enhancements)

**Changes required**: None (optional)

**Optional enhancements** analysts can adopt:
```python
# Before (still works)
from helpers.batch import create_batch
batch_id = create_batch('edm_create', config_id, step_id)

# After (optional - more control)
from helpers import logging_config
logging_config.set_notebook_output_level('INFO')  # See more inline

from helpers.batch import create_batch
batch_id = create_batch('edm_create', config_id, step_id)
```

### Phase 3: Advanced Features (Optional Tools)

**Changes required**: None

**New tools available**:
- Log viewer notebook (`_Tools/notebooks/View_Logs.ipynb`)
- Log analytics notebook (`_Tools/notebooks/Log_Analytics.ipynb`)
- Aggregate logs notebook (`_Tools/notebooks/Aggregate_Logs.ipynb`)

### Rollback Strategy

If issues arise, rollback is simple:

1. **Disable file logging**:
   ```bash
   export DISABLE_FILE_LOGGING=true
   ```

2. **Revert code changes**:
   - Remove logging statements from helper modules
   - Revert `notebook_setup.py` changes
   - Delete `logging_config.py`

3. **No data loss**: Existing functionality unchanged, only logging is new

### Communication Plan

**Before POC**:
- Share this proposal with stakeholders
- Gather feedback on design decisions
- Finalize configuration

**After POC**:
- Demo logging system to analysts
- Share demo notebook
- Collect feedback

**Before Core Rollout**:
- Announce rollout timeline
- Share updated documentation
- Provide training on log viewer tools

**After Core Rollout**:
- Monitor for issues
- Collect usage feedback
- Iterate on log levels and configuration

---

## Open Questions & Decisions

### Critical Decisions (Required for POC)

#### Q1: Log File Organization

**Decision**: Single rotating log file per cycle (Option A)

**Rationale**:
- Simpler to implement and maintain
- Easier to tail (`tail -f irp_framework_2025-11-24.log`)
- Easier to correlate events across modules
- Can switch to per-module logs later if needed (config change only)

**Alternative**: Per-module log files (can be added later via config)

#### Q2: Rotation Strategy

**Decision**: Time-based, daily rotation (Option A)

**Rationale**:
- Predictable (new file each day)
- Aligns with daily operations
- Easier to find logs for specific dates
- File size less critical (cycles typically short-lived)

**Configuration**: `rotation_days: 1` (daily)

#### Q3: Inline Notebook Output Level

**Decision**: ERROR and CRITICAL only by default

**Rationale**:
- Minimizes clutter in notebook output
- Errors are most important for analysts
- Analysts can increase verbosity if needed
- All logs still in file for debugging

**Configuration**: `notebook.level: ERROR`

#### Q4: Context Injection Method

**Decision**: Thread-local storage with fallback hierarchy

**Rationale**:
- No API changes required
- Works with existing notebook_setup pattern
- Handles edge cases gracefully
- Can be overridden when needed

**Implementation**: See "Context Fallback Strategy" section

#### Q5: Testing Log Files

**Decision**: No file writes in tests by default (Option A)

**Rationale**:
- Faster test execution
- No cleanup required
- Use pytest `caplog` for assertions
- File writes can be enabled for debugging (`--preserve-logs`)

**Configuration**: `testing.file_enabled: false`

### Medium Priority Decisions (Phase 2)

#### Q6: Archived Cycles - Keep Log Files?

**Decision**: Yes, include logs in cycle archival

**Rationale**:
- Critical for historical debugging
- Audit trail for compliance
- Disk space not a concern (logs compress well)
- Can be purged separately if needed

**Implementation**: Logs stay in `_Archive/{cycle}/logs/`

#### Q7: Log Viewing in JupyterLab?

**Decision**: Implement in Phase 3

**Rationale**:
- Phase 2 provides log viewer notebook (sufficient)
- JupyterLab extension more complex
- Can add if analysts request it

**Alternative**: Basic log viewer in Phase 2, enhanced in Phase 3

#### Q8: Sanitize Sensitive Data?

**Decision**: Yes, implement `SensitiveDataFilter`

**Rationale**:
- Security best practice
- Prevent accidental exposure
- Regex-based masking (configurable patterns)
- Performance impact minimal

**Implementation**: Phase 2

#### Q9: Database Operations - DEBUG or INFO?

**Decision**: INFO for summaries, DEBUG for query text

**Rationale**:
- INFO: Row counts, operation success (useful at default level)
- DEBUG: Full query text (only for debugging)
- Keeps INFO level clean

**Example**:
```python
logger.debug(f"Executing query: {query}")  # Full query
df = execute_query(query)
logger.info(f"Query returned {len(df)} rows")  # Summary
```

#### Q10: Database Logging vs File Logging?

**Decision**: Keep both, different purposes

**Rationale**:
- **Database logging** (`step.log()`): High-level workflow events, reporting
- **File logging**: Detailed technical logs, debugging
- No overlap, complementary

**Implementation**: No changes to `step.log()`

#### Q11: Log Format - Human vs JSON?

**Decision**: Human-readable for POC, JSON optional in Phase 2

**Rationale**:
- Analysts prefer readable logs
- JSON useful for advanced analysis (Phase 3)
- Both can coexist (separate files)

**Configuration**: `json.enabled: false` (default)

### Low Priority Decisions (Phase 3)

#### Q12: Log Aggregation Service?

**Decision**: Defer to Phase 3

**Rationale**:
- Not critical for POC or core rollout
- File-based logging sufficient initially
- Can integrate external tools later (ELK, Splunk, etc.)

#### Q13: Real-Time Log Monitoring?

**Decision**: Defer to Phase 3

**Rationale**:
- `tail -f` sufficient for now
- Real-time dashboard requires infrastructure
- Low priority for batch workflows

#### Q14: Log Compression?

**Decision**: Not implemented (rely on OS/backup)

**Rationale**:
- Log files relatively small
- OS-level compression available (gzip, backup tools)
- Not critical for functionality

### Risks & Mitigation

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Performance degradation | High | Low | Benchmark tests, async writes, lazy initialization |
| Log files fill disk | Medium | Low | Retention policy, monitoring, compression |
| Sensitive data in logs | High | Medium | SensitiveDataFilter, code review, testing |
| Context detection fails | Medium | Low | Comprehensive fallback strategy, tests |
| Breaking changes | High | Low | Extensive testing, backward compatibility focus |
| Adoption resistance | Low | Medium | Clear documentation, training, demo notebooks |

---

## Appendix A: Code Examples

### Example 1: Basic Logging in Helper Module

```python
# workspace/helpers/batch.py

from helpers.logging_config import get_logger

logger = get_logger(__name__)

def create_batch(batch_type: str, configuration_id: int, step_id: Optional[int]) -> int:
    """Create a new batch for job submission."""

    logger.info(f"Creating batch: type={batch_type}, config_id={configuration_id}")

    try:
        # Validate inputs
        if not batch_type:
            raise BatchError("batch_type is required")

        logger.debug(f"Validating configuration {configuration_id}")
        config = get_configuration(configuration_id)

        if config['status'] != 'ACTIVE':
            logger.warning(f"Configuration {configuration_id} is not ACTIVE (status={config['status']})")
            raise BatchError(f"Configuration must be ACTIVE, got {config['status']}")

        # Create batch
        query = """
            INSERT INTO irp_batch (step_id, configuration_id, batch_type, status)
            VALUES (%s, %s, %s, %s)
        """
        batch_id = execute_insert(query, (step_id, configuration_id, batch_type, 'INITIATED'))

        logger.info(f"Batch {batch_id} created successfully")
        return batch_id

    except DatabaseError as e:
        logger.error(f"Failed to create batch: {e}", exc_info=True)
        raise BatchError(f"Failed to create batch: {e}")
    except Exception as e:
        logger.critical(f"Unexpected error creating batch: {e}", exc_info=True)
        raise
```

### Example 2: Logging in Notebook

```python
# Active_Analysis-2025-Q4/notebooks/Stage_03_Data_Import/Step_01_Create_EDM_Batch.ipynb

from helpers.notebook_setup import initialize_notebook_context
from helpers import ux, logging_config
from helpers.batch import create_batch
from helpers.configuration import get_active_configuration

# Initialize context and logging
context, step = initialize_notebook_context('Step_01_Create_EDM_Batch.ipynb')

# Optional: Show INFO logs inline (default is ERROR only)
logging_config.set_notebook_output_level('INFO')

# Display header
ux.header("Create EDM Batch")
ux.info(f"Cycle: {context.cycle_name}")

# Get configuration
config = get_active_configuration(context.cycle_name)
ux.success(f"✓ Loaded configuration: {config['name']}")

# Create batch
try:
    batch_id = create_batch('edm_create', config['id'], step.id)
    ux.success(f"✓ Batch {batch_id} created successfully")

    # Log to step run log (database)
    step.log(f"Created batch {batch_id}")

except Exception as e:
    ux.error(f"✗ Failed to create batch: {e}")
    step.fail(str(e))
    raise

# Complete step
step.complete({'batch_id': batch_id})
```

### Example 3: Scheduled Notebook Script

```bash
#!/bin/bash
# daily_batch_monitor.sh

# Scheduled job to monitor batch status

set -e

# Configuration
export LOG_DIR=/var/log/irp/scheduled
export NOTEBOOK_EXECUTION_MODE=scheduled
export CYCLE_NAME=Analysis-2025-Q4
export DISABLE_FILE_LOGGING=false  # Enable logging

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Run notebook
cd /workspace/workflows/Active_Analysis-2025-Q4/notebooks/Stage_04_Analysis_Execution

jupyter nbconvert \
    --execute \
    --to notebook \
    --inplace \
    --ExecutePreprocessor.timeout=3600 \
    Monitor_Batch_Status.ipynb

# Check exit code
if [ $? -eq 0 ]; then
    echo "Batch monitoring completed successfully"
else
    echo "Batch monitoring failed"
    # Send alert (Teams, email, etc.)
    exit 1
fi
```

### Example 4: Test with Log Capture

```python
# workspace/tests/test_batch.py

import logging
import pytest
from helpers.batch import create_batch
from helpers.logging_config import get_logger, set_context, clear_context
from helpers.context import WorkContext


def test_create_batch_logs_correctly(test_schema, caplog):
    """Test that batch creation logs expected messages."""

    # Set up test context
    context = WorkContext(cycle_name='Test-Batch', stage='Stage_03', step='Step_01')
    set_context(context)

    # Capture logs at INFO level
    caplog.set_level(logging.INFO)

    # Set up test data
    cycle_id = create_test_cycle(test_schema, 'Test-Batch')
    config_id = create_test_configuration(test_schema, cycle_id)

    # Execute test
    batch_id = create_batch('test_batch', config_id, None)

    # Assert on logs
    assert "Creating batch" in caplog.text
    assert f"type=test_batch" in caplog.text
    assert f"Batch {batch_id} created successfully" in caplog.text

    # Assert on log levels
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info_records) >= 2  # At least "Creating" and "created successfully"

    # Assert context in logs
    for record in caplog.records:
        assert record.cycle_name == 'Test-Batch'
        assert record.stage == 'Stage_03'
        assert record.step == 'Step_01'

    # Cleanup
    clear_context()
```

---

## Appendix B: File Structure

```
workspace/
├── config/
│   └── logging.yaml                          # NEW: Logging configuration
│
├── helpers/
│   ├── logging_config.py                     # NEW: Core logging module
│   ├── database.py                           # MODIFIED: Add logging
│   ├── batch.py                              # MODIFIED: Add logging
│   ├── job.py                                # MODIFIED: Add logging (Phase 2)
│   ├── cycle.py                              # MODIFIED: Add logging (Phase 2)
│   ├── step.py                               # MODIFIED: Add logging (Phase 2)
│   ├── configuration.py                      # MODIFIED: Add logging (Phase 2)
│   ├── notebook_setup.py                     # MODIFIED: Initialize logging
│   └── ... (other helpers)
│
├── workflows/
│   ├── logs/                                 # NEW: System-level logs
│   │   ├── irp_framework_2025-11-24.log
│   │   ├── irp_framework_2025-11-23.log
│   │   └── .gitkeep
│   │
│   ├── _Tools/
│   │   ├── notebooks/
│   │   │   ├── Demo_Logging.ipynb            # NEW: Logging demo
│   │   │   ├── View_Logs.ipynb               # NEW: Log viewer (Phase 2)
│   │   │   ├── Aggregate_Logs.ipynb          # NEW: Log aggregation (Phase 3)
│   │   │   └── Log_Analytics.ipynb           # NEW: Log analytics (Phase 3)
│   │   └── logs/
│   │       ├── irp_framework_2025-11-24.log
│   │       └── .gitkeep
│   │
│   ├── _Archive/
│   │   └── Analysis-2025-Q3/
│   │       └── logs/
│   │           ├── irp_framework_2025-09-*.log
│   │           └── .gitkeep
│   │
│   └── Active_Analysis-2025-Q4/
│       └── logs/
│           ├── irp_framework_2025-11-24.log
│           ├── irp_framework_2025-11-23.log
│           └── .gitkeep
│
└── tests/
    ├── test_logging_config.py                # NEW: Unit tests
    ├── test_logging_integration.py           # NEW: Integration tests
    └── logs/                                 # NEW: Test logs
        ├── test_batch/
        │   └── irp_framework_2025-11-24.log
        └── test_job/
            └── irp_framework_2025-11-24.log
```

---

## Appendix C: Success Metrics

### POC Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Log file creation | 100% | All test scenarios create expected log files |
| Context injection | 100% | All log entries include cycle/stage/step |
| Inline error display | 100% | ERROR logs appear in notebook cells |
| Fallback scenarios | 100% | All edge cases handled (TOOLS, SYSTEM, scheduled) |
| Performance overhead | <5ms | P95 latency benchmark |
| Test coverage | >90% | pytest coverage report |
| Breaking changes | 0 | All existing tests pass |

### Phase 2 Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Module coverage | 100% | All 20+ helper modules instrumented |
| Log noise (INFO level) | <10/min | Manual review of log files |
| Log viewer functionality | 100% | All features working |
| Documentation completeness | 100% | Peer review |
| Analyst adoption | >50% | Usage analytics (optional features) |

### Phase 3 Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| JSON logging accuracy | 100% | Validation against schema |
| Log search performance | <2s | Benchmark across 100K+ log entries |
| Analytics dashboard usefulness | >80% | User feedback survey |
| Alert accuracy | >95% | False positive rate <5% |

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-11-24 | System Architecture | Initial proposal |

---

**End of Proposal**
