# Refactoring Strategy: Cycle/Stage/Step Operations

**Date**: October 17, 2024
**Status**: Planning - Revised Approach
**Risk Level**: Low (Environment-based schema selection)

---

## Problem Statement

**Current Issues**:
1. ❌ Cycle, Stage, Step operations scattered in `database.py`
2. ❌ Schema parameter passed everywhere (verbose, error-prone)
3. ❌ No dedicated modules (`cycle.py`, `stage.py`, `step.py`)
4. ❌ Zero test coverage for these operations
5. ⚠️ **Risk**: Jupyter notebooks may break if we refactor incorrectly

**Functions Affected** (from database.py lines 313-502):
- 5 Cycle operations
- 1 Stage operation
- 2 Step operations
- 3 Step Run operations
- 2 Query helpers

---

## Revised Strategy: Environment-Based Schema Selection

### Core Concept

Instead of passing `schema=` parameter everywhere:
```python
# ❌ OLD: Verbose, error-prone
create_cycle('Q1-2024', schema='test_cycle')
create_batch(..., schema='test_cycle')
submit_job(..., schema='test_cycle')
```

Use environment variable or context manager:
```python
# ✅ NEW: Clean, consistent
set_database_schema('test_cycle')  # or context manager
create_cycle('Q1-2024')
create_batch(...)
submit_job(...)
```

---

## Implementation Design

### 1. Schema Context Manager (Preferred)

**New Module**: `workspace/helpers/db_context.py`

```python
"""
Database context management for schema selection
"""
import os
from contextlib import contextmanager
from threading import local

# Thread-local storage for schema
_context = local()

def get_current_schema() -> str:
    """
    Get the current schema for database operations.

    Returns 'public' if no schema is set.
    """
    return getattr(_context, 'schema', 'public')

def set_schema(schema: str):
    """
    Set the schema for subsequent database operations.

    Args:
        schema: Schema name to use

    Example:
        set_schema('test_cycle')
        create_cycle('Q1-2024')  # Uses 'test_cycle' schema
    """
    _context.schema = schema

@contextmanager
def schema_context(schema: str):
    """
    Context manager for temporary schema selection.

    Args:
        schema: Schema name to use within context

    Example:
        # Production code
        with schema_context('public'):
            cycle_id = create_cycle('Q1-2024')

        # Test code
        with schema_context('test_cycle'):
            cycle_id = create_cycle('test_cycle_1')
            assert cycle_id > 0
    """
    old_schema = get_current_schema()
    set_schema(schema)
    try:
        yield
    finally:
        set_schema(old_schema)

# Environment variable fallback
def get_schema_from_env() -> str:
    """
    Get schema from environment variable.

    Checks DB_SCHEMA environment variable, defaults to 'public'.
    """
    return os.environ.get('DB_SCHEMA', 'public')

def init_from_environment():
    """Initialize schema from environment variable"""
    env_schema = get_schema_from_env()
    if env_schema != 'public':
        set_schema(env_schema)
```

### 2. Update Core Database Functions

**Modify**: `workspace/helpers/database.py`

```python
# Add at top
from helpers.db_context import get_current_schema

# Update all database functions
def execute_query(query: str, params: tuple = None, schema: str = None) -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Override schema (optional, uses context if not provided)

    Returns:
        DataFrame with query results
    """
    # Use provided schema, or get from context
    active_schema = schema if schema is not None else get_current_schema()

    try:
        converted_query, param_dict = _convert_query_params(query, params)
        engine = get_engine()
        with engine.connect() as conn:
            _set_schema(conn, active_schema)
            df = pd.read_sql_query(text(converted_query), conn, params=param_dict)
        return df
    except Exception as e:
        raise DatabaseError(f"Query failed: {str(e)}")

# Similar updates for execute_insert, execute_command, execute_scalar, bulk_insert
```

### 3. Simplify Higher-Level Functions

**Create New Modules** without schema parameters:

**`workspace/helpers/cycle.py`**:
```python
"""Cycle management operations"""
from typing import Optional, Dict, Any
from helpers.database import execute_query, execute_insert, execute_command
from helpers.constants import CycleStatus

class CycleError(Exception):
    """Custom exception for cycle errors"""
    pass

def get_active_cycle() -> Optional[Dict[str, Any]]:
    """Get the currently active cycle"""
    query = """
        SELECT id, cycle_name, status, created_ts
        FROM irp_cycle
        WHERE status = 'ACTIVE'
        ORDER BY created_ts DESC
        LIMIT 1
    """
    df = execute_query(query)
    return df.iloc[0].to_dict() if not df.empty else None

def create_cycle(cycle_name: str) -> int:
    """Create new cycle"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """
    return execute_insert(query, (cycle_name, CycleStatus.ACTIVE))

def archive_cycle(cycle_id: int) -> bool:
    """Archive a cycle"""
    query = """
        UPDATE irp_cycle
        SET status = %s, archived_ts = NOW()
        WHERE id = %s
    """
    rows = execute_command(query, (CycleStatus.ARCHIVED, cycle_id))
    return rows > 0
```

---

## Usage Examples

### Production Notebooks
```python
# No changes needed - uses 'public' schema by default
from helpers.cycle import create_cycle, get_active_cycle

cycle_id = create_cycle('Q1-2024')
active = get_active_cycle()
```

### Test Files
```python
# Option 1: Context manager (preferred)
from helpers.db_context import schema_context
from helpers.cycle import create_cycle

def test_create_cycle():
    with schema_context('test_cycle'):
        cycle_id = create_cycle('test_cycle_1')
        assert cycle_id > 0

# Option 2: pytest fixture (even better!)
@pytest.fixture
def test_schema():
    schema = 'test_cycle'
    with schema_context(schema):
        init_database(schema=schema)
        yield schema
        drop_schema(schema)

def test_create_cycle(test_schema):
    # Automatically in test_schema context
    cycle_id = create_cycle('test_cycle_1')
    assert cycle_id > 0
```

### Update conftest.py Fixture
```python
@pytest.fixture(scope="module")
def test_schema(request):
    """
    Automatically create and manage test schema for each test module.
    Sets schema context for all operations within the module.
    """
    schema = Path(request.fspath).stem

    # Drop and create schema
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.commit()

    # Initialize schema
    init_database(schema=schema)

    # SET SCHEMA CONTEXT FOR ALL TESTS IN MODULE
    from helpers.db_context import set_schema
    set_schema(schema)

    yield schema

    # Cleanup
    if not request.config.getoption("--preserve-schema"):
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.commit()

    # Reset to public
    set_schema('public')
```

---

## 4-Phase Implementation Plan

### Phase 1: Add Schema Context System ✅

**Goal**: Implement context manager without breaking anything

**Steps**:
1. Create `workspace/helpers/db_context.py`
2. Update core database functions to check context
3. Keep existing `schema=` parameters for backward compatibility
4. Add tests for context manager

**Files to Create**:
- `workspace/helpers/db_context.py` (~100 lines)

**Files to Modify**:
- `workspace/helpers/database.py` (add context awareness)

**Validation**:
- ✅ Old code works (still uses schema=)
- ✅ New context manager works
- ✅ Tests pass

**Estimated Time**: 2 hours
**Risk**: Very Low (additive only)

---

### Phase 2: Create New Modules Without Schema Param ✅

**Goal**: Move functions to proper modules, clean API

**Create**:
- `workspace/helpers/cycle.py` - Cycle operations
- `workspace/helpers/stage.py` - Stage operations
- `workspace/helpers/step.py` - Step & Step Run operations

**Key Difference**: No `schema=` parameters anywhere!

```python
# Clean, simple APIs
def create_cycle(cycle_name: str) -> int:
    """Create new cycle - uses schema from context"""
    query = "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)"
    return execute_insert(query, (cycle_name, CycleStatus.ACTIVE))
```

**Files to Create**:
- `workspace/helpers/cycle.py` (~150 lines)
- `workspace/helpers/stage.py` (~80 lines)
- `workspace/helpers/step.py` (~200 lines)

**Files to Modify**:
- `workspace/helpers/database.py` (keep old functions, add imports)

**Validation**:
- ✅ New modules work with context
- ✅ Old database.py functions still work

**Estimated Time**: 2 hours
**Risk**: Low

---

### Phase 3: Update conftest.py and Create Tests ✅

**Goal**: Update fixture to set schema context, write comprehensive tests

**Update `conftest.py`**:
```python
@pytest.fixture(scope="module")
def test_schema(request):
    schema = Path(request.fspath).stem

    # Setup
    drop_and_create_schema(schema)
    init_database(schema=schema)

    # SET CONTEXT - this is the magic!
    from helpers.db_context import set_schema
    set_schema(schema)

    yield schema

    # Cleanup
    if not preserve:
        drop_schema(schema)
    set_schema('public')  # Reset
```

**Create Test File**: `workspace/tests/test_cycle_stage_step.py`

**Test Structure**:
```python
# No schema parameter anywhere!
def test_create_cycle(test_schema):
    # Schema context already set by fixture
    cycle_id = create_cycle('test_cycle_1')
    assert cycle_id > 0

    cycle = get_cycle_by_name('test_cycle_1')
    assert cycle is not None
    assert cycle['cycle_name'] == 'test_cycle_1'

def test_full_workflow(test_schema):
    # Create hierarchy - no schema params!
    cycle_id = create_cycle('workflow_test')
    stage_id = get_or_create_stage(cycle_id, 1, 'Stage 1')
    step_id = get_or_create_step(stage_id, 1, 'Step 1')
    run_id, run_num = create_step_run(step_id, 'tester')

    assert run_num == 1
    update_step_run(run_id, 'COMPLETED')

    last_run = get_last_step_run(step_id)
    assert last_run['status'] == 'COMPLETED'
```

**Files to Create**:
- `workspace/tests/test_cycle_stage_step.py` (~800 lines, 23 tests)

**Files to Modify**:
- `workspace/tests/conftest.py` (add schema context setting)

**Estimated Time**: 3 hours
**Risk**: None (only tests)

---

### Phase 4: Deprecate Old Functions (Optional) ✅

**Goal**: Gradually migrate to new modules

**Approach**:
1. Add deprecation warnings to `database.py` functions
2. Update notebooks to use new modules
3. Eventually remove from database.py

**Example**:
```python
# In database.py
import warnings

def create_cycle(cycle_name: str, schema: str = 'public') -> int:
    """
    DEPRECATED: Use helpers.cycle.create_cycle() instead
    """
    warnings.warn(
        "create_cycle from database module is deprecated. "
        "Use 'from helpers.cycle import create_cycle' instead.",
        DeprecationWarning,
        stacklevel=2
    )
    from helpers.cycle import create_cycle as new_create_cycle
    from helpers.db_context import schema_context
    with schema_context(schema):
        return new_create_cycle(cycle_name)
```

**Estimated Time**: 1 hour
**Risk**: Low (warnings only)

---

## Benefits of This Approach

### 1. Cleaner Code
```python
# ❌ OLD: Schema parameter everywhere
create_cycle('Q1', schema='test')
create_batch(..., schema='test')
submit_job(..., schema='test')

# ✅ NEW: Set once, use everywhere
with schema_context('test'):
    create_cycle('Q1')
    create_batch(...)
    submit_job(...)
```

### 2. Less Error-Prone
- No forgetting to pass schema
- No typos in schema names
- Consistent across entire operation

### 3. Better Testing
```python
# Tests automatically use correct schema
def test_workflow(test_schema):
    # test_schema fixture sets context
    # All operations use test schema automatically
    cycle_id = create_cycle('test')
    batch_id = create_batch(...)
    # No schema= anywhere!
```

### 4. Backward Compatible
- Old code works (uses `schema='public'`)
- New code cleaner (uses context)
- Gradual migration path

### 5. Thread-Safe
- Using thread-local storage
- Each thread has own schema context
- Safe for concurrent operations

---

## Migration Path for Existing Code

### Notebooks (No Changes Needed)
```python
# Works as-is - uses 'public' by default
from helpers.database import create_cycle
cycle_id = create_cycle('Q1-2024')
```

### Tests (Two Options)

**Option 1: Update fixture (recommended)**
```python
# conftest.py sets context
# All tests automatically use test schema

def test_something(test_schema):
    # No schema= needed!
    cycle_id = create_cycle('test')
```

**Option 2: Explicit context**
```python
def test_something():
    with schema_context('test_something'):
        cycle_id = create_cycle('test')
```

---

## Comparison: Old vs New Approach

| Aspect | Schema Parameter | Context Manager |
|--------|------------------|-----------------|
| **Verbosity** | High (pass everywhere) | Low (set once) |
| **Error-prone** | Yes (can forget) | No (automatic) |
| **Test clarity** | Cluttered | Clean |
| **Thread-safe** | Yes | Yes |
| **Backward compat** | N/A | ✅ Yes |
| **Migration effort** | Low | Very Low |

---

## Timeline

| Phase | Task | Time | Risk |
|-------|------|------|------|
| 1 | Add context system | 2 hours | Very Low |
| 2 | Create new modules | 2 hours | Low |
| 3 | Update tests | 3 hours | None |
| 4 | Deprecate old (optional) | 1 hour | Low |
| **Total** | | **8 hours** | **Low** |

---

## Decision

### Recommended Approach: ✅ **Context Manager**

**Reasoning**:
1. ✅ **Cleaner code**: No schema= everywhere
2. ✅ **Less error-prone**: Set once, use many
3. ✅ **Better testing**: Automatic schema from fixture
4. ✅ **Backward compatible**: Old code still works
5. ✅ **Thread-safe**: Uses thread-local storage
6. ✅ **Industry standard**: Similar to Flask's request context, SQLAlchemy sessions

---

## Next Steps

1. ✅ **Approve this revised strategy**
2. ✅ **Create git branch**: `refactor/context-based-schema`
3. ✅ **Implement Phase 1**: Context manager system
4. ✅ **Implement Phase 2**: New clean modules
5. ✅ **Implement Phase 3**: Update tests
6. ✅ **Validate**: Run all tests + notebooks
7. ✅ **Merge**: When everything passes

---

**Status**: Awaiting approval
**Revised Approach**: Context manager > Schema parameters
**Recommendation**: Proceed with this cleaner approach
