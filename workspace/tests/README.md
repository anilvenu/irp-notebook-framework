# IRP Notebook Framework - Test Suite Summary


## Test Infrastructure & Execution Strategy

The IRP Notebook Framework uses a sophisticated test infrastructure built on pytest with Docker-based PostgreSQL, per-file schema isolation, and careful handling of the "one active cycle" business constraint. This section explains how tests are executed, why they're structured this way, and how to safely run them in parallel.

---

### Test Execution Infrastructure (test.sh)

The `test.sh` script orchestrates the entire test execution process:

**Database Setup (Docker-based)**:
- Verifies PostgreSQL container `irp-postgres` is running
- Auto-creates test database (`test_db`) and test user (`test_user`) if they don't exist
- Sets environment variables for database connection:
  ```bash
  DB_SERVER=localhost
  DB_PORT=5432
  DB_NAME=test_db
  DB_USER=test_user
  DB_PASSWORD=test_pass
  ```

**Environment Preparation**:
- Detects and activates virtual environment (`venv` or `.venv`)
- Installs test requirements from `requirements-test.txt`
- Sets up Python import path via `workspace.pth` for helper module imports

**Test Execution**:
- Runs all tests with single pytest command: `pytest workspace/tests/ -v "$@"`
- Passes through all arguments (supports `-m`, `-k`, `--preserve-schema`, etc.)

---

### Schema Isolation Strategy (conftest.py - test_schema fixture)

**Key Concept**: Each test file gets its own isolated PostgreSQL schema

**Schema Naming Convention**:
- `test_batch.py` → schema: `test_batch`
- `test_job.py` → schema: `test_job`
- `test_configuration.py` → schema: `test_configuration`
- etc.

**Module-Level Fixture Lifecycle** (`@pytest.fixture(scope="module")`):

1. **Setup Phase**:
   - Drop existing schema (cleanup from previous run or `--preserve-schema`)
   - Create new schema with full database structure via `init_database(schema=schema)`
   - Set schema context globally: `set_schema(schema)` - allows tests to omit `schema=` parameter
   - Print setup status for debugging

2. **Execution Phase**:
   - Yield schema name to all tests in the module
   - All tests in file share this schema
   - Tests can use domain functions without explicit `schema=` parameter

3. **Cleanup Phase**:
   - Drop schema after tests complete (unless `--preserve-schema` flag set)
   - Reset schema context to 'public'
   - Print cleanup status

**Benefits**:
- **Complete isolation** between test files - no data pollution
- **Parallel test file execution** - each file has independent schema
- **Clean slate** for each test run - no leftover data
- **Debugging support** - use `--preserve-schema` to inspect post-test state
- **Realistic testing** - full database structure in each schema

**Example**:
```python
def test_something(test_schema):
    # test_schema = "test_batch" (derived from test_batch.py)
    # Schema context already set, can call without schema= parameter
    cycle_id = register_cycle('my_cycle')  # Creates in test_batch schema
```

---

### "One Active Cycle" Rule

**Critical Production Constraint**: The IRP system enforces a business rule that **only ONE cycle can have status='ACTIVE' at any given time**. This mirrors real-world usage where only one analysis cycle is actively being worked on.

This has significant impact on test design.

**Problem Scenario**:
```python
# Test 1 creates an active cycle
test_1_cycle = register_cycle('Cycle1')  # status=ACTIVE

# Test 2 tries to create another active cycle
test_2_cycle = register_cycle('Cycle2')  # status=ACTIVE

# WorkContextError: Active cycle 'Cycle1' exists, but notebook is in 'Cycle2'
# get_active_cycle will produce unexpected results at this point.
```

**Solution**:
```python
# Tests MUST archive cycles after use
def test_example(test_schema):
    cycle_id = register_cycle('my_cycle')

    # ... test logic ...

    # Cleanup: Archive cycle so it doesn't interfere with other tests
    archive_cycle(cycle_id)  # Changes status to ARCHIVED
```

**Helper Function** (from test_configuration.py):
```python
def create_test_cycle(test_schema, cycle_name='test_cycle'):
    """Archives all existing ACTIVE cycles before creating new one"""
    # Archive all existing active cycles
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE status = 'ACTIVE'",
        schema=test_schema
    )

    # Now safe to create new active cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, 'ACTIVE'),
        schema=test_schema
    )
    return cycle_id
```

**Why This Matters**:
- Prevents false negatives from business rule violations
- Ensures tests clean up properly

---

**Parallel Execution at FILE Level**

Each test file has its own schema → can run in parallel safely:

```bash
# Safe: Run test files in parallel with pytest-xdist
pytest workspace/tests/ -n auto

# Each file has isolated schema:
# - test_batch.py    → test_batch schema
# - test_job.py      → test_job schema
# - test_cycle.py    → test_cycle schema
# No conflicts!
```

**Avoid Parallel Execution Within SAME FILE**

Tests within same file share the same schema → "one active cycle" conflicts:

```python
# In test_batch.py (all share test_batch schema)

@pytest.mark.parametrize("cycle_name", ["cycle1", "cycle2", "cycle3"])
def test_something(test_schema, cycle_name):
    cycle_id = register_cycle(cycle_name)  # ❌ If run in parallel, conflicts!
    # Only ONE can be ACTIVE at a time in test_batch schema
```

**Recommendation**:
1. **Use file-level parallelization**: `pytest -n auto workspace/tests/`
2. **Avoid test-level parallelization** within same file (conflicts likely)
3. **Serial execution within file**: Default pytest behavior (safe)
4. **Clean up cycles**: Always call `archive_cycle()` or use helper that archives
5. **Debug with `--preserve-schema`**: Inspect test schema after failures
---

### Test Configuration Components (conftest.py)

**Session-Level Fixtures** (run once per entire test session):
- `verify_database_connection()`: Fails fast if PostgreSQL unavailable (prevents cryptic errors)

**Module-Level Fixtures** (run once per test file):
- `test_schema(request)`: Creates isolated schema for test file (see Section 2)

**Function-Level Fixtures** (run for each test that uses them):
- `sample_cycle(test_schema)`: Creates a test cycle
- `sample_hierarchy(test_schema)`: Creates cycle → stage → step → configuration
- `sample_batch(sample_hierarchy, test_schema)`: Creates complete batch with hierarchy

**Custom pytest Options**:
```bash
--preserve-schema  # Keep test schemas after tests (for debugging)
```

**Custom pytest Markers**:
```python
@pytest.mark.unit          # Unit tests for individual functions
@pytest.mark.integration   # Integration tests for component interactions
@pytest.mark.e2e           # End-to-end tests for complete workflows
@pytest.mark.database      # Tests requiring database connection
@pytest.mark.slow          # Tests taking >5 seconds
```

**Helper Functions** (defined in conftest.py, usable in tests):
```python
create_test_hierarchy(cycle_name, schema)  # Creates full cycle→stage→step→config
```

---

### 6. Test Execution Examples

**Run All Tests**:
```bash
./test.sh workspace/tests/                      # All tests, default verbosity
./test.sh workspace/tests/ -v                   # Verbose output
./test.sh workspace/tests/ -s                   # Show print statements
```

**Run by Test Type (Marker)**:
```bash
./test.sh workspace/tests/ -m unit              # Only unit tests (fast)
./test.sh workspace/tests/ -m integration       # Only integration tests
./test.sh workspace/tests/ -m e2e               # Only end-to-end tests
./test.sh workspace/tests/ -m database          # Only database tests
./test.sh workspace/tests/ -m "unit and not slow"  # Fast unit tests
```

**Run Specific Test Files**:
```bash
./test.sh workspace/tests/test_batch.py -v                   # Single file
./test.sh workspace/tests/test_batch.py::test_read_batch -v  # Single test
./test.sh workspace/tests/ -k "numpy" -v                     # Pattern match
```

**Debugging with Preserved Schemas**:
```bash
./test.sh workspace/tests/test_batch.py --preserve-schema -s -v

# After test completes, inspect schema:
psql -U test_user test_db

test_db=> \dn                          # List schemas
test_db=> SET search_path TO test_batch, public;
test_db=> SELECT * FROM irp_cycle;     # Inspect test data
```

**Parallel Execution (File-Level Only)**:
```bash
# Requires: pip install pytest-xdist
./test.sh workspace/tests/ -n auto     # Auto-detect CPU cores
./test.sh workspace/tests/ -n 4        # Use 4 workers
```

---

### Schema Management Details

**PostgreSQL Schemas** (not to be confused with "schema" as in table structure):
- Schemas are namespaces for database objects (tables, functions, etc.)
- Like directories for organizing database objects
- Syntax: `test_batch.irp_cycle` vs `test_job.irp_cycle` - same table name, different schemas

**Full Database Structure in Each Schema**:
Each test schema contains complete copy of:
- All tables: `irp_cycle`, `irp_stage`, `irp_step`, `irp_batch`, `irp_job`, `irp_configuration`, etc.
- All enums: `cycle_status_enum`, `step_status_enum`, `batch_status_enum`, etc.
- All constraints: foreign keys, unique constraints, check constraints

**Schema Context System**:
```python
# Explicit schema parameter (works always)
register_cycle('my_cycle', schema='test_batch')

# Implicit via context (requires set_schema() first)
set_schema('test_batch')  # Set once
register_cycle('my_cycle')  # Uses test_batch schema automatically
```

**Schema Context Precedence** (see database.py documentation):
1. **Explicit parameter**: `schema='test_batch'` → Always wins
2. **Thread-local context**: `set_schema('test_batch')` → Second priority
3. **Environment variable**: `DB_SCHEMA=test_batch` → Third priority
4. **Default**: `'public'` → Fallback

**Inspecting Test Schemas**:
```bash
# List all schemas
psql -U test_user test_db -c "\dn"

# Inspect specific test schema
psql -U test_user test_db <<EOF
SET search_path TO test_batch, public;
\dt                                    -- List tables
SELECT * FROM irp_cycle;               -- View data
\d irp_cycle                           -- Describe table structure
EOF
```

---

### Common Test Patterns

**Pattern 1: Single-Cycle Test (with cleanup)**:
```python
def test_something(test_schema):
    cycle_id = register_cycle('test_cycle')

    # Test logic here

    archive_cycle(cycle_id)  # Cleanup
```

**Pattern 2: Multiple-Cycle Test (archive between)**:
```python
def test_multiple_cycles(test_schema):
    cycle_1 = register_cycle('cycle_1')
    archive_cycle(cycle_1)  # Archive before creating next

    cycle_2 = register_cycle('cycle_2')
    archive_cycle(cycle_2)
```

**Pattern 3: Using Fixtures**:
```python
def test_with_hierarchy(sample_hierarchy, test_schema):
    cycle_id, stage_id, step_id, config_id = sample_hierarchy
    # Hierarchy pre-created, ready to use
```

**Pattern 4: Schema Context Usage**:
```python
def test_with_context(test_schema):
    # test_schema fixture already called set_schema(test_schema)
    # Can omit schema= parameter
    cycle_id = register_cycle('my_cycle')  # Implicitly uses test_schema
```

---

## Test Count Summary


### Test Count by Type

| Test Type | Count | Percentage |
|-----------|-------|------------|
| Unit Tests (`@pytest.mark.unit`) | 90 | 38.3% |
| Integration Tests (`@pytest.mark.integration`) | 95 | 40.4% |
| E2E Tests (`@pytest.mark.e2e`) | 6 | 2.6% |
| Database Tests (`@pytest.mark.database`) | 201 | 85.5% |
| Tests without marks | 44 | 18.7% |

### Test Files Overview

| Test File | Test Count | Primary Module Tested | Uses cycle.py | Uses database.py |
|-----------|-----------|----------------------|--------------|-----------------|
| test_batch.py | 12 | batch.py | N | Y |
| test_job.py | 17 | job.py | N | Y |
| test_batch_job_integration.py | 6 | batch.py, job.py | N | Y |
| test_stage.py | 9 | stage.py | N | Y |
| test_database_crud.py | 16 | database.py (via cycle ops) | N | Y |
| test_step.py | 12 | step.py | N | Y |
| test_configuration.py | 15 | configuration.py | N | Y |
| test_database_schema.py | 20 | database.py (schema context) | N | Y |
| test_cycle.py | 17 | cycle.py | Y | Y |
| test_constants.py | 29 | constants.py | N | Y (5 tests) |
| test_context.py | 30 | context.py | N | Y |
| test_database.py | 17 | database.py (bulk ops, numpy) | N | Y |

---

## Detailed Test Tables

### test_batch.py (12 tests)
Tests batch management operations

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_read_batch | batch.py | read_batch | N | Y | Test reading batch by ID | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_batch_status | batch.py | update_batch_status, read_batch | N | Y | Test updating batch status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_create_batch_default | batch.py | create_batch, submit_batch, read_batch, get_batch_jobs | N | Y | Test creating batch with default transformer | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_batch_multi_job | batch.py | create_batch, submit_batch, get_batch_jobs | N | Y | Test creating batch with multi_job transformer | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_get_batch_jobs_with_filters | batch.py, job.py | get_batch_jobs, create_job, update_job_status, skip_job | N | Y | Test getting batch jobs with various filters | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_recon_batch_all_completed | batch.py | recon_batch, create_batch, submit_batch, get_batch_jobs | N | Y | Test reconciling batch with all jobs completed | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_recon_batch_with_failures | batch.py | recon_batch, create_batch, submit_batch | N | Y | Test reconciling batch with failed jobs | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_recon_batch_all_cancelled | batch.py | recon_batch, create_batch, submit_batch | N | Y | Test reconciling batch with all jobs cancelled | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_batch_error_invalid_id | batch.py | read_batch (error handling) | N | Y | Test error handling for invalid batch ID | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_batch_error_not_found | batch.py | read_batch (error handling) | N | Y | Test error handling for batch not found | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_batch_error_invalid_status | batch.py | update_batch_status (error handling) | N | Y | Test error handling for invalid status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_batch_error_unknown_type | batch.py | create_batch (error handling) | N | Y | Test error handling for unknown batch type | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |

---

### test_job.py (17 tests)
Tests job management operations

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_read_job | job.py | read_job | N | Y | Test reading job by ID | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_job_status | job.py | update_job_status, create_job, read_job | N | Y | Test updating job status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_create_job_with_new_config | job.py | create_job, read_job, get_job_config | N | Y | Test creating job with new configuration | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_job_with_existing_config | job.py | create_job, read_job | N | Y | Test creating job with existing configuration | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_job_validation_neither_param | job.py | create_job (error handling) | N | Y | Test create job validation - neither parameter provided | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_create_job_validation_both_params | job.py | create_job (error handling) | N | Y | Test create job validation - both parameters provided | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_skip_job | job.py | skip_job, create_job, read_job | N | Y | Test skipping a job | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_submit_job | job.py | submit_job, create_job, read_job | N | Y | Test submitting a job | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_submit_job_force_resubmit | job.py | submit_job, create_job, read_job | N | Y | Test force resubmitting a job | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_track_job_status | job.py | track_job_status, submit_job, create_job | N | Y | Test tracking job status | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_resubmit_job_without_override | job.py | resubmit_job, create_job, submit_job, read_job | N | Y | Test resubmitting job without configuration override | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_resubmit_job_with_override | job.py | resubmit_job, create_job, get_job_config, read_job | N | Y | Test resubmitting job with configuration override | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_job_error_invalid_id | job.py | read_job (error handling) | N | Y | Test error handling for invalid job ID | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_job_error_not_found | job.py | read_job (error handling) | N | Y | Test error handling for job not found | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_job_error_invalid_status | job.py | update_job_status (error handling) | N | Y | Test error handling for invalid status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_job_error_track_without_submission | job.py | track_job_status (error handling) | N | Y | Test error handling for tracking unsubmitted job | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_job_error_resubmit_override_no_reason | job.py | resubmit_job (error handling) | N | Y | Test error handling for resubmit with override but no reason | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |

---

### test_batch_job_integration.py (6 tests)
Tests end-to-end batch and job workflows

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_end_to_end_batch_workflow | batch.py, job.py | create_batch, submit_batch, read_batch, get_batch_jobs, track_job_status, recon_batch | N | Y | Test complete end-to-end batch workflow | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |
| test_multi_job_batch_workflow | batch.py | create_batch, submit_batch, get_batch_jobs | N | Y | Test multi-job batch workflow | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |
| test_job_resubmission_workflow | batch.py, job.py | create_batch, submit_batch, get_batch_jobs, resubmit_job, read_job, submit_job, track_job_status, recon_batch | N | Y | Test job resubmission workflow | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |
| test_configuration_override_workflow | batch.py, job.py | create_batch, submit_batch, get_batch_jobs, resubmit_job, get_job_config, read_job | N | Y | Test configuration override workflow | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |
| test_mixed_job_states_recon | batch.py, job.py | create_batch, submit_batch, get_batch_jobs, update_job_status, skip_job, recon_batch | N | Y | Test batch reconciliation with mixed job states | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |
| test_parent_child_job_chain | batch.py, job.py | create_batch, submit_batch, get_batch_jobs, resubmit_job, update_job_status, read_job | N | Y | Test parent-child job chain | @pytest.mark.database, @pytest.mark.e2e | ✓ Appropriate |

---

### test_stage.py (9 tests)
Tests stage.py module

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_get_or_create_stage_new | stage.py | get_or_create_stage, get_stage_by_id | N | Y | Test creating a new stage using stage.py module | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_or_create_stage_existing | stage.py | get_or_create_stage | N | Y | Test get_or_create_stage returns existing stage (idempotent) | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_or_create_stage_different_numbers | stage.py | get_or_create_stage | N | Y | Test creating stages with different stage numbers | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_stage_by_id_found | stage.py | get_stage_by_id, get_or_create_stage | N | Y | Test retrieving stage by ID | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_stage_by_id_not_found | stage.py | get_stage_by_id | N | Y | Test retrieving non-existent stage returns None | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_list_stages_for_cycle_empty | stage.py | list_stages_for_cycle | N | Y | Test listing stages for cycle with no stages | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_list_stages_for_cycle_multiple | stage.py | list_stages_for_cycle, get_or_create_stage | N | Y | Test listing multiple stages for a cycle | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_list_stages_for_cycle_isolation | stage.py | list_stages_for_cycle, get_or_create_stage | N | Y | Test that stages are properly isolated by cycle | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_stage_module_uses_context | stage.py | get_or_create_stage, get_stage_by_id, list_stages_for_cycle | N | Y | Test that stage.py functions use schema context correctly | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |

---

### test_database_crud.py (16 tests)
Tests cycle/stage/step hierarchy and database operations

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_create_cycle | database.py | register_cycle, get_cycle_by_name | N | Y | Test creating a new cycle | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_cycle_by_name | database.py | register_cycle, get_cycle_by_name | N | Y | Test retrieving cycle by name | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_cycle_by_name_not_found | database.py | get_cycle_by_name | N | Y | Test retrieving non-existent cycle returns None | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_active_cycle | database.py | register_cycle, get_active_cycle | N | Y | Test retrieving the active cycle | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_archive_cycle | database.py | register_cycle, get_cycle_by_name, archive_cycle | N | Y | Test archiving a cycle | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_multiple_cycles | database.py | register_cycle, archive_cycle, get_active_cycle, get_cycle_by_name | N | Y | Test creating multiple cycles (only one can be active) | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_step | database.py | register_cycle, get_or_create_stage, get_or_create_step, get_step_info | N | Y | Test creating a step | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_or_create_step_existing | database.py | get_or_create_step | N | Y | Test get_or_create_step returns existing step | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_step_info | database.py | get_step_info, get_or_create_step | N | Y | Test getting complete step information | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_create_step_run | database.py | create_step_run | N | Y | Test creating a step run | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_multiple_step_runs | database.py | create_step_run | N | Y | Test creating multiple runs for same step | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_step_run_completed | database.py | create_step_run, update_step_run, get_last_step_run | N | Y | Test updating step run to completed | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_step_run_failed | database.py | create_step_run, update_step_run, get_last_step_run | N | Y | Test updating step run to failed | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_last_step_run | database.py | get_last_step_run, create_step_run, update_step_run | N | Y | Test getting the most recent step run | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_complete_hierarchy | database.py, stage.py | register_cycle, get_or_create_stage, get_or_create_step, create_step_run, update_step_run, get_active_cycle, list_stages_for_cycle, get_step_info, get_last_step_run | N | Y | Test creating complete cycle → stage → step → run hierarchy | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_schema_isolation | database.py | register_cycle, get_cycle_by_name | N | Y | Test that schema context works correctly | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |

---

### test_step.py (12 tests)
Tests Step class in step.py module

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_step_initialization | step.py | Step class | N | Y | Test Step class initialization | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_already_executed | step.py | Step class | N | Y | Test Step detects already executed steps | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_log | step.py | Step.log() | N | Y | Test Step logging functionality | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_checkpoint | step.py | Step.checkpoint() | N | Y | Test Step checkpoint functionality | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_complete | step.py | Step.complete() | N | Y | Test Step completion | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_fail | step.py | Step.fail() | N | Y | Test Step failure | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_skip | step.py | Step.skip() | N | Y | Test Step skipping | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_get_last_output | step.py | Step.get_last_output() | N | Y | Test getting last output from completed step | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_context_manager | step.py | Step (context manager) | N | Y | Test Step as context manager | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_context_manager_exception | step.py | Step (context manager) | N | Y | Test Step context manager with exception - auto fails | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_force_reexecution | step.py | Step.start() | N | Y | Test forcing re-execution of already executed step | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_step_module_uses_context | step.py | Step class | N | Y | Test that Step class uses schema context correctly | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |

---

### test_configuration.py (15 tests)
Tests configuration management operations

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_read_configuration | configuration.py | read_configuration | N | Y | Test reading configuration from database | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_configuration_status | configuration.py | update_configuration_status, read_configuration | N | Y | Test updating configuration status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_update_configuration_status_to_error | configuration.py | update_configuration_status, read_configuration | N | Y | Test updating configuration status to ERROR | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_load_configuration_file_success | configuration.py | load_configuration_file, read_configuration | N | Y | Test loading valid configuration file | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_load_configuration_file_validation_errors | configuration.py | load_configuration_file (error handling) | N | Y | Test loading configuration with validation errors | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_load_configuration_active_cycle | configuration.py | load_configuration_file | N | Y | Test loading configuration for active cycle | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_load_configuration_archived_cycle_fails | configuration.py | load_configuration_file (error handling) | N | Y | Test that loading configuration for archived cycle fails | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_load_configuration_duplicate_active_fails | configuration.py | load_configuration_file, update_configuration_status (error handling) | N | Y | Test that duplicate ACTIVE configurations are prevented | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_configuration_transformer_default | configuration.py | ConfigurationTransformer.get_job_configurations() | N | N | Test ConfigurationTransformer default type | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_passthrough | configuration.py | ConfigurationTransformer.get_job_configurations() | N | N | Test ConfigurationTransformer passthrough type | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_multi_job_with_jobs | configuration.py | ConfigurationTransformer.get_job_configurations() | N | N | Test ConfigurationTransformer multi_job type with jobs list | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_multi_job_fallback | configuration.py | ConfigurationTransformer.get_job_configurations() | N | N | Test ConfigurationTransformer multi_job type without jobs list (fallback) | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_unknown_type | configuration.py | ConfigurationTransformer.get_job_configurations() (error handling) | N | N | Test ConfigurationTransformer unknown type error | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_list_types | configuration.py | ConfigurationTransformer.list_types() | N | N | Test listing registered transformer types | @pytest.mark.unit | ✓ Appropriate |
| test_configuration_transformer_custom_registration | configuration.py | ConfigurationTransformer.register() | N | N | Test custom transformer registration | @pytest.mark.unit | ✓ Appropriate |

---

### test_database_schema.py (20 tests)
Tests database schema context management

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_default_schema_is_public | database.py | get_current_schema, reset_schema | N | Y | Test that default schema is 'public' when nothing is set | None | Add @pytest.mark.unit |
| test_set_schema_changes_context | database.py | get_current_schema, set_schema | N | Y | Test that set_schema() changes the current schema context | None | Add @pytest.mark.unit |
| test_reset_schema_returns_to_public | database.py | set_schema, get_current_schema, reset_schema | N | Y | Test that reset_schema() always returns to 'public' | None | Add @pytest.mark.unit |
| test_set_schema_rejects_empty_string | database.py | set_schema (error handling) | N | Y | Test that set_schema() rejects empty string | None | Add @pytest.mark.unit |
| test_set_schema_rejects_none | database.py | set_schema (error handling) | N | Y | Test that set_schema() rejects None | None | Add @pytest.mark.unit |
| test_schema_context_temporary_change | database.py | schema_context, get_current_schema | N | Y | Test that schema_context() temporarily changes schema | None | Add @pytest.mark.unit |
| test_schema_context_nested | database.py | schema_context, get_current_schema | N | Y | Test nested schema contexts | None | Add @pytest.mark.unit |
| test_schema_context_restores_on_exception | database.py | schema_context, get_current_schema | N | Y | Test that schema_context() restores schema even when exception occurs | None | Add @pytest.mark.unit |
| test_schema_context_rejects_empty_string | database.py | schema_context (error handling) | N | Y | Test that schema_context() rejects empty string | None | Add @pytest.mark.unit |
| test_schema_context_rejects_none | database.py | schema_context (error handling) | N | Y | Test that schema_context() rejects None | None | Add @pytest.mark.unit |
| test_schema_isolation_between_operations | database.py | init_database, set_schema, register_cycle, get_cycle_by_name | N | Y | Test that operations in different schemas are isolated | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_explicit_schema_parameter_overrides_context | database.py | init_database, set_schema, register_cycle, get_cycle_by_name, execute_query | N | Y | Test that explicit schema= parameter overrides context on execute_ functions | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_schema_context_with_explicit_override | database.py | init_database, set_schema, register_cycle, schema_context, execute_query | N | Y | Test that explicit schema parameter works within schema_context on execute_ functions | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_get_schema_from_env_default | database.py | get_schema_from_env | N | Y | Test that get_schema_from_env() returns 'public' when not set | None | Add @pytest.mark.unit |
| test_get_schema_from_env_reads_variable | database.py | get_schema_from_env | N | Y | Test that get_schema_from_env() reads DB_SCHEMA environment variable | None | Add @pytest.mark.unit |
| test_complete_schema_precedence_flow | database.py | init_database, set_schema, register_cycle, get_cycle_by_name, init_from_environment, execute_query | N | Y | Test complete schema precedence: explicit > context > env > default | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_schema_context_in_practical_use_case | database.py | init_database, schema_context, register_cycle, get_cycle_by_name, set_schema | N | Y | Test a practical use case: Running tests in isolated schema | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_multiple_set_schema_calls | database.py | set_schema, get_current_schema | N | Y | Test multiple consecutive set_schema calls | None | Add @pytest.mark.unit |
| test_schema_context_same_schema | database.py | set_schema, get_current_schema, schema_context | N | Y | Test schema_context with same schema as current | None | Add @pytest.mark.unit |
| test_schema_special_characters | database.py | set_schema, get_current_schema, schema_context | N | Y | Test schema names with underscores and numbers | None | Add @pytest.mark.unit |

---

### test_cycle.py (17 tests)
Tests cycle.py module for high-level cycle management

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_delete_archived_cycles | cycle.py | delete_archived_cycles | Y | Y | Test deleting archived cycles | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_validate_cycle_name_too_short | cycle.py | validate_cycle_name | Y | N | Test cycle name validation - too short | @pytest.mark.database, @pytest.mark.unit | Remove @pytest.mark.database (pure validation) |
| test_validate_cycle_name_too_long | cycle.py | validate_cycle_name | Y | N | Test cycle name validation - too long | @pytest.mark.database, @pytest.mark.unit | Remove @pytest.mark.database (pure validation) |
| test_validate_cycle_name_invalid_pattern | cycle.py | validate_cycle_name | Y | N | Test cycle name validation - invalid characters | @pytest.mark.database, @pytest.mark.unit | Remove @pytest.mark.database (pure validation) |
| test_validate_cycle_name_forbidden_prefix | cycle.py | validate_cycle_name | Y | N | Test cycle name validation - forbidden prefix | @pytest.mark.database, @pytest.mark.unit | Remove @pytest.mark.database (pure validation) |
| test_validate_cycle_name_already_exists | cycle.py | validate_cycle_name | Y | Y | Test cycle name validation - name already exists | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_validate_cycle_name_valid | cycle.py | validate_cycle_name | Y | N | Test cycle name validation - valid name | @pytest.mark.database, @pytest.mark.unit | Remove @pytest.mark.database (pure validation) |
| test_get_cycle_status | cycle.py | get_cycle_status | Y | Y | Test getting cycle status | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_get_cycle_progress | cycle.py | get_cycle_progress | Y | Y | Test getting cycle progress | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_get_step_history | cycle.py | get_step_history | Y | Y | Test getting step history | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_get_step_history_filtered_by_stage | cycle.py | get_step_history | Y | Y | Test getting step history filtered by stage number | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_cycle_module_uses_context | cycle.py | get_active_cycle_id | Y | Y | Test that cycle.py functions use schema context correctly | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_cycle_with_directories | cycle.py | create_cycle | Y | Y | Test cycle.create_cycle() creates directories and database records | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_cycle_archives_previous | cycle.py | create_cycle | Y | Y | Test cycle.create_cycle() archives previous active cycle | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_archive_cycle_by_name | cycle.py | archive_cycle_by_name | Y | Y | Test cycle.archive_cycle_by_name() moves directory and updates database | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_create_cycle_duplicate_run | cycle.py | create_cycle (error handling) | Y | N | Test cycle.create_cycle() back to back mimicing accidental rerun | @pytest.mark.database, @pytest.mark.integration | Remove @pytest.mark.database (error logic test) |
| test_create_cycle_registers_stages_and_steps | cycle.py | create_cycle | Y | Y | Test cycle.create_cycle() registers stages and steps from template | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |

---

### test_constants.py (29 tests)
Tests constants module configuration and validation

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_db_config_from_environment | constants.py | DB_CONFIG | N | N | Test that DB_CONFIG correctly loads from environment variables set in test.sh | None | Add @pytest.mark.unit |
| test_db_config_port_defaults_to_5432 | constants.py | DB_CONFIG | N | N | Test that DB_PORT defaults to 5432 if not set | None | Add @pytest.mark.unit |
| test_db_config_types | constants.py | DB_CONFIG | N | N | Test that DB_CONFIG values have correct types | None | Add @pytest.mark.unit |
| test_db_config_no_none_values | constants.py | DB_CONFIG | N | N | Test that no DB_CONFIG values are None in test environment | None | Add @pytest.mark.unit |
| test_missing_config_validation_would_catch_missing_vars | constants.py | _missing_config validation | N | N | Test that _missing_config validation logic catches missing variables | None | Add @pytest.mark.unit |
| test_missing_config_allows_port_to_be_none | constants.py | _missing_config validation | N | N | Test that port is excluded from required variables (it has a default) | None | Add @pytest.mark.unit |
| test_cycle_status_all_includes_all_values | constants.py | CycleStatus.all() | N | N | Test CycleStatus.all() includes all status values | None | Add @pytest.mark.unit |
| test_step_status_all_includes_all_values | constants.py | StepStatus.all() | N | N | Test StepStatus.all() includes all status values | None | Add @pytest.mark.unit |
| test_batch_status_all_includes_all_values | constants.py | BatchStatus.all() | N | N | Test BatchStatus.all() includes all status values | None | Add @pytest.mark.unit |
| test_configuration_status_all_includes_all_values | constants.py | ConfigurationStatus.all() | N | N | Test ConfigurationStatus.all() includes all status values | None | Add @pytest.mark.unit |
| test_job_status_all_includes_all_values | constants.py | JobStatus.all() | N | N | Test JobStatus.all() includes all status values | None | Add @pytest.mark.unit |
| test_cycle_status_matches_database | constants.py | CycleStatus.all() | N | Y | Test CycleStatus values match cycle_status_enum in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_step_status_matches_database | constants.py | StepStatus.all() | N | Y | Test StepStatus values match step_status_enum in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_batch_status_matches_database | constants.py | BatchStatus.all() | N | Y | Test BatchStatus values match batch_status_enum in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_configuration_status_matches_database | constants.py | ConfigurationStatus.all() | N | Y | Test ConfigurationStatus values match configuration_status_enum in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_job_status_matches_database | constants.py | JobStatus.all() | N | Y | Test JobStatus values match job_status_enum in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_notebook_pattern_valid_filenames | constants.py | NOTEBOOK_PATTERN regex | N | N | Test NOTEBOOK_PATTERN matches valid notebook filenames | None | Add @pytest.mark.unit |
| test_notebook_pattern_invalid_filenames | constants.py | NOTEBOOK_PATTERN regex | N | N | Test NOTEBOOK_PATTERN rejects invalid filenames | None | Add @pytest.mark.unit |
| test_notebook_pattern_extracts_groups_correctly | constants.py | NOTEBOOK_PATTERN regex | N | N | Test that pattern extracts both capture groups | None | Add @pytest.mark.unit |
| test_stage_pattern_valid_names | constants.py | STAGE_PATTERN regex | N | N | Test STAGE_PATTERN matches valid stage names | None | Add @pytest.mark.unit |
| test_stage_pattern_invalid_names | constants.py | STAGE_PATTERN regex | N | N | Test STAGE_PATTERN rejects invalid stage names | None | Add @pytest.mark.unit |
| test_cycle_name_valid_pattern_matches_correct_format | constants.py | CYCLE_NAME_RULES['valid_pattern'] regex | N | N | Test valid_pattern matches correct cycle name format | None | Add @pytest.mark.unit |
| test_cycle_name_valid_pattern_rejects_invalid_format | constants.py | CYCLE_NAME_RULES['valid_pattern'] regex | N | N | Test valid_pattern rejects invalid cycle name format | None | Add @pytest.mark.unit |
| test_cycle_name_rules_min_length | constants.py | CYCLE_NAME_RULES['min_length'] | N | N | Test CYCLE_NAME_RULES min_length constraint | None | Add @pytest.mark.unit |
| test_cycle_name_rules_max_length | constants.py | CYCLE_NAME_RULES['max_length'] | N | N | Test CYCLE_NAME_RULES max_length constraint | None | Add @pytest.mark.unit |
| test_cycle_name_rules_forbidden_prefixes | constants.py | CYCLE_NAME_RULES['forbidden_prefixes'] | N | N | Test CYCLE_NAME_RULES forbidden_prefixes | None | Add @pytest.mark.unit |
| test_cycle_name_rules_example_is_valid | constants.py | CYCLE_NAME_RULES['example'] | N | N | Test that CYCLE_NAME_RULES example is actually valid | None | Add @pytest.mark.unit |
| test_step_status_terminal_subset_of_all | constants.py | StepStatus.terminal(), StepStatus.all() | N | N | Test that StepStatus.terminal() is a subset of all() | None | Add @pytest.mark.unit |
| test_job_status_ready_for_submit_subset_of_all | constants.py | JobStatus.ready_for_submit(), JobStatus.all() | N | N | Test that JobStatus.ready_for_submit() is a subset of all() | None | Add @pytest.mark.unit |
| test_notebook_and_stage_patterns_consistent | constants.py | NOTEBOOK_PATTERN, STAGE_PATTERN regex | N | N | Test that notebook and stage patterns use consistent format | None | Add @pytest.mark.unit |

---

### test_context.py (30 tests)
Tests WorkContext class and context management

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_parse_valid_path_basic | context.py | WorkContext | N | Y | Test parsing of a basic valid notebook path | None | Add @pytest.mark.unit |
| test_parse_valid_path_with_numbers | context.py | WorkContext | N | Y | Test parsing path with numbers in names | None | Add @pytest.mark.unit |
| test_parse_valid_path_with_underscores | context.py | WorkContext | N | Y | Test parsing path with underscores in names | None | Add @pytest.mark.unit |
| test_parse_extracts_cycle_from_active_prefix | context.py | WorkContext | N | Y | Test that cycle name is extracted from Active_ prefix | None | Add @pytest.mark.unit |
| test_parse_handles_double_digit_numbers | context.py | WorkContext | N | Y | Test parsing with double-digit stage and step numbers | None | Add @pytest.mark.unit |
| test_error_on_missing_active_prefix | context.py | WorkContext (error handling) | N | Y | Test error when path doesn't contain Active_ prefix | None | Add @pytest.mark.unit |
| test_error_on_missing_stage_directory | context.py | WorkContext (error handling) | N | Y | Test error when stage directory is missing or invalid | None | Add @pytest.mark.unit |
| test_error_on_invalid_stage_format | context.py | WorkContext (error handling) | N | Y | Test error when stage directory has invalid format | None | Add @pytest.mark.unit |
| test_error_on_invalid_notebook_filename | context.py | WorkContext (error handling) | N | Y | Test error when notebook filename doesn't match pattern | None | Add @pytest.mark.unit |
| test_error_on_non_ipynb_extension | context.py | WorkContext (error handling) | N | Y | Test error when file is not .ipynb | None | Add @pytest.mark.unit |
| test_error_on_archived_cycle | context.py | WorkContext (error handling) | N | Y | Test error when cycle exists but is archived | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_error_on_active_cycle_mismatch | context.py | WorkContext (error handling) | N | Y | Test error when different active cycle exists | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_creates_database_entries_for_new_cycle | context.py | WorkContext | N | Y | Test that WorkContext creates cycle, stage, and step in database | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_uses_existing_cycle | context.py | WorkContext | N | Y | Test that WorkContext uses existing cycle if available | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_creates_multiple_stages_in_same_cycle | context.py | WorkContext | N | Y | Test creating multiple stages in the same cycle | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_creates_multiple_steps_in_same_stage | context.py | WorkContext | N | Y | Test creating multiple steps in the same stage | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_idempotent_context_creation | context.py | WorkContext | N | Y | Test that creating context multiple times for same path is idempotent | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_get_info_returns_all_fields | context.py | WorkContext.get_info() | N | Y | Test that get_info() returns complete dictionary | None | Add @pytest.mark.unit |
| test_repr_contains_key_info | context.py | WorkContext.__repr__() | N | Y | Test __repr__ contains cycle, stage, and step info | None | Add @pytest.mark.unit |
| test_str_human_readable_format | context.py | WorkContext.__str__() | N | Y | Test __str__ returns human-readable format | None | Add @pytest.mark.unit |
| test_get_context_creates_work_context | context.py | get_context() | N | Y | Test that get_context() returns WorkContext instance | None | Add @pytest.mark.unit |
| test_get_context_with_path_parameter | context.py | get_context() | N | Y | Test get_context() accepts explicit path | None | Add @pytest.mark.unit |
| test_cycle_name_with_special_characters | context.py | WorkContext | N | Y | Test cycle names with underscores and hyphens | None | Add @pytest.mark.unit |
| test_stage_name_with_multiple_underscores | context.py | WorkContext | N | Y | Test stage names with multiple underscores | None | Add @pytest.mark.unit |
| test_step_name_with_multiple_underscores | context.py | WorkContext | N | Y | Test step names with multiple underscores | None | Add @pytest.mark.unit |
| test_path_with_absolute_linux_path | context.py | WorkContext | N | Y | Test with absolute Linux-style path | None | Add @pytest.mark.unit |
| test_path_stores_as_pathlib_path | context.py | WorkContext | N | Y | Test that notebook_path is stored as Path object | None | Add @pytest.mark.unit |
| test_single_digit_step_number | context.py | WorkContext | N | Y | Test that single digit step numbers work | None | Add @pytest.mark.unit |
| test_context_respects_schema_context | context.py | WorkContext | N | Y | Test that WorkContext works with schema_context | None | Add @pytest.mark.database, @pytest.mark.integration |
| test_contexts_isolated_between_schemas | context.py | WorkContext | N | Y | Test that contexts in different schemas are isolated | None | Add @pytest.mark.database, @pytest.mark.integration |

---

### test_database.py (17 tests)
Tests database operations (bulk_insert, error handling, numpy type conversion)

| Test Name | Code File | Code Function | Uses cycle.py | Uses database.py | Description | Marks | Recommended Marks |
|-----------|-----------|---------------|---------------|------------------|-------------|-------|-------------------|
| test_basic_bulk_insert | database.py | bulk_insert, execute_query | N | Y | Test basic bulk insert without JSONB fields | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_error_handling_rollback | database.py | bulk_insert, execute_query (error handling) | N | Y | Test error handling and transaction rollback on duplicate key | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_configuration_jsonb_insert | database.py | bulk_insert, execute_insert, execute_query | N | Y | Test bulk insert into irp_configuration table with JSONB config_data | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_bulk_insert_returns_correct_ids | database.py | bulk_insert, execute_query | N | Y | Test that bulk_insert returns IDs in insertion order | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_empty_bulk_insert | database.py | bulk_insert | N | Y | Test bulk_insert behavior with empty params list | @pytest.mark.database, @pytest.mark.unit | ✓ Appropriate |
| test_multiple_jsonb_columns | database.py | bulk_insert, execute_insert, execute_query | N | Y | Test bulk insert with multiple JSONB columns | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |
| test_convert_params_to_native_types_with_numpy_int64 | database.py | _convert_params_to_native_types | N | Y | Test conversion of numpy.int64 to Python int | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_numpy_float64 | database.py | _convert_params_to_native_types | N | Y | Test conversion of numpy.float64 to Python float | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_numpy_bool | database.py | _convert_params_to_native_types | N | Y | Test conversion of numpy.bool_ to Python bool | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_numpy_str | database.py | _convert_params_to_native_types | N | Y | Test conversion of numpy.str_ to Python str | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_mixed_types | database.py | _convert_params_to_native_types | N | Y | Test conversion with mixed numpy and Python types | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_empty_tuple | database.py | _convert_params_to_native_types | N | Y | Test conversion with empty tuple | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_none | database.py | _convert_params_to_native_types | N | Y | Test conversion with None params | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_preserves_complex_types | database.py | _convert_params_to_native_types | N | Y | Test that complex Python types (dict, list) pass through unchanged | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_various_numpy_int_types | database.py | _convert_params_to_native_types | N | Y | Test conversion of various numpy integer types | @pytest.mark.unit | ✓ Appropriate |
| test_convert_params_to_native_types_with_various_numpy_float_types | database.py | _convert_params_to_native_types | N | Y | Test conversion of various numpy float types | @pytest.mark.unit | ✓ Appropriate |
| test_numpy_int64_in_real_database_operation | database.py | execute_insert, execute_query, execute_command | N | Y | Integration test: Verify numpy.int64 values work in real database operations | @pytest.mark.database, @pytest.mark.integration | ✓ Appropriate |

---

## Test Coverage Matrix

### Code Files Tested

| Code File | Test Files | Test Count | Coverage Notes |
|-----------|-----------|-----------|----------------|
| batch.py | test_batch.py, test_batch_job_integration.py | 18 | ✓ Comprehensive (CRUD, transformers, reconciliation, errors) |
| job.py | test_job.py, test_batch_job_integration.py | 23 | ✓ Comprehensive (CRUD, submission, tracking, resubmission, errors) |
| stage.py | test_stage.py, test_database_crud.py | 10 | ✓ Good (creation, retrieval, listing, isolation) |
| step.py | test_step.py | 12 | ✓ Comprehensive (Step class, context manager, lifecycle) |
| configuration.py | test_configuration.py | 15 | ✓ Comprehensive (loading, validation, transformers, errors) |
| database.py | test_database.py, test_database_schema.py, test_database_crud.py, test_constants.py | 58 | ✓ Excellent (CRUD, schema context, bulk ops, numpy conversion) |
| cycle.py | test_cycle.py | 17 | ✓ Comprehensive (validation, creation, archival, progress, history) |
| constants.py | test_constants.py | 29 | ✓ Excellent (DB_CONFIG, Status classes, regex patterns, database alignment) |
| context.py | test_context.py | 30 | ✓ Excellent (path parsing, database integration, error handling, schema isolation) |

### Untested Code Files
Based on the test analysis, the following helper files may not have dedicated tests (or tests are minimal):
- `helpers/db_context.py` - **DEPRECATED** (functionality consolidated into database.py)
- Other utility/helper modules (if any) - further investigation needed

---

## Test Execution Guidelines

### Running All Tests
```bash
# From project root
source ./test.sh

# From project root, preserve the test schema after completion of tests 
source ./test.sh --preserve-schema

# From project root, show prints
source ./test.sh --preserve-schema -s -v
```

### Running Tests by Type

```bash
... -m unit
... -m integration
... -m database
... -m e2e
```

### Running Specific Test Files
```bash
# Run a specific test file
source ./test.sh workspace/tests/test_batch.py -v

# Run a specific test function
source ./test.sh workspace/tests/test_batch.py::test_read_batch -v

# Run tests matching a pattern
source ./test.sh workspace/tests/ -k "numpy" -v
```

### Running Tests by Module Under Test
```bash
# Test batch operations
source ./test.sh workspace/tests/test_batch.py workspace/tests/test_batch_job_integration.py

# Test configuration
source ./test.sh workspace/tests/test_configuration.py workspace/tests/test_constants.py

# Test database operations
source ./test.sh workspace/tests/test_database.py workspace/tests/test_database_schema.py workspace/tests/test_database_crud.py
```

---

## Test Maintenance Recommendations

### Missing Test Marks
The following test files have tests **without pytest marks** and should be updated:

1. **test_database_schema.py** (20 tests) - Add `@pytest.mark.unit` or `@pytest.mark.integration` and `@pytest.mark.database`
2. **test_constants.py** (29 tests) - Add `@pytest.mark.unit` or `@pytest.mark.integration` (5 tests need `@pytest.mark.database`)
3. **test_context.py** (30 tests) - Add `@pytest.mark.unit` or `@pytest.mark.integration` (some need `@pytest.mark.database`)

### Recommended Mark Additions

#### test_cycle.py
- Tests `test_validate_cycle_name_*` (lines testing pure validation logic without DB) should **remove** `@pytest.mark.database`
- Test `test_create_cycle_duplicate_run` may not need `@pytest.mark.database` if it's testing error logic only

#### test_constants.py
- All regex pattern tests: Add `@pytest.mark.unit`
- DB_CONFIG tests: Add `@pytest.mark.unit`
- Status class tests (database alignment): Add `@pytest.mark.database` and `@pytest.mark.integration`

#### test_context.py
- Path parsing tests: Add `@pytest.mark.unit`
- Database interaction tests: Add `@pytest.mark.database` and `@pytest.mark.integration`

#### test_database_schema.py
- Pure schema context tests: Add `@pytest.mark.unit`
- Database isolation tests: Add `@pytest.mark.database` and `@pytest.mark.integration`