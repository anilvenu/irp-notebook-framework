# Database Tests

This directory contains tests for the IRP Notebook Framework database operations.

## Test Files

- **test_database.py**: Comprehensive test suite for database operations including:
  - `bulk_insert()` function with JSONB support
  - Error handling and transaction rollback
  - Performance comparisons between bulk and individual inserts
  - Tests across multiple tables (cycles, step runs, configurations)

## Running Tests

### Quick Start

From the project root directory, run:

```bash
./run_tests.sh
```

This script automatically:
- Sets up environment variables for localhost database connection
- Activates virtual environment if available
- Runs all database tests
- Uses a separate 'test' schema to avoid affecting production data

### Manual Execution

If you need to run tests manually:

```bash
# Set environment variables for database connection
export DB_SERVER=localhost
export DB_PORT=5432
export DB_NAME=irp_db
export DB_USER=irp_user
export DB_PASSWORD=irp_pass

# Activate virtual environment (if using one)
source venv/bin/activate

# Run tests
python workspace/tests/test_database.py
```

### Test Schema

All tests run in a **separate 'test' schema** that is:
- Created automatically before tests run
- Isolated from production data
- Dropped automatically after tests complete

This ensures your production data remains untouched during testing.

## Database Connection

The tests expect a PostgreSQL database to be running and accessible.

### For Docker Users

If you're using the Docker setup with `DB_SERVER=postgres`, change it to `localhost` for testing:

```bash
export DB_SERVER=localhost
```

### Connection Verification

The test suite will:
1. Test database connectivity
2. Create a test schema
3. Run all tests
4. Clean up the test schema
5. Report results

## Test Coverage

### Test 1: Basic Bulk Insert
- Inserts multiple cycles without JSONB fields
- Verifies all records are created correctly
- Checks returned IDs match inserted records

### Test 2: JSONB Bulk Insert
- Inserts cycles with complex metadata (JSONB)
- Verifies JSONB data is stored and retrieved correctly
- Tests automatic dict-to-JSON conversion

### Test 3: Complex JSONB Inserts
- Creates complete hierarchy (cycle → stage → step → step_run)
- Bulk inserts step runs with complex output_data
- Tests nested JSONB structures

### Test 4: Error Handling
- Tests transaction rollback on duplicate key violations
- Verifies no partial inserts occur on error
- Confirms proper error messages

### Test 5: Performance Comparison
- Compares bulk vs individual insert performance
- Reports speedup and time savings
- Demonstrates efficiency of bulk operations

### Test 6: Configuration JSONB
- Tests bulk insert into configuration table
- Verifies complex config_data JSONB handling
- Tests skip flag and foreign keys

## Example Output

```
================================================================================
BULK INSERT TEST SUITE
================================================================================

Testing database connection...
✓ Database connection successful

================================================================================
SETUP: Initializing Test Schema
================================================================================
Creating and initializing schema 'test'...
Database initialized successfully (schema: test)
✓ Test schema 'test' initialized successfully

================================================================================
TEST 1: Basic Bulk Insert (Multiple Cycles)
================================================================================
Inserting 5 cycles...
✓ Successfully inserted 5 records
  Returned IDs: [1, 2, 3, 4, 5]
  Time elapsed: 0.0234 seconds
✓ Verified 5 records in database

...

================================================================================
TEST SUMMARY
================================================================================
✓ PASS: Basic Bulk Insert
✓ PASS: JSONB Bulk Insert
✓ PASS: Complex JSONB Inserts
✓ PASS: Error Handling
✓ PASS: Performance Comparison
✓ PASS: Configuration JSONB

6/6 tests passed

🎉 All tests passed!
```

## pytest (New Testing Framework)

### 🆕 pytest Support Added!

We've added pytest infrastructure alongside the existing test runner. Both methods work!

**To use pytest**:
```bash
# Install pytest dependencies
pip install -r requirements-test.txt

# Run all tests with pytest (schemas cleaned up after tests)
pytest

# Run with verbose output
pytest -v

# Run with verbose output AND show setup/teardown messages
pytest -v -s

# Run with coverage
pytest --cov=workspace/helpers --cov-report=html

# Run specific test
pytest workspace/tests/test_database.py::test_basic_bulk_insert

# Run in parallel (faster!)
pytest -n auto

# 🔍 PRESERVE schemas for debugging (schemas NOT dropped after tests)
pytest --preserve-schema

# Use convenience script (includes proper environment setup)
source ./run_pytest_test.sh
```

**Schema Management (Critical)**:

The `--preserve-schema` flag controls test schema lifecycle:

| Command | Behavior | When to Use |
|---------|----------|-------------|
| `pytest` | ✓ Drop existing schema at start<br>✓ Create fresh schema<br>✓ **Drop schema after tests** | Normal test runs |
| `pytest --preserve-schema` | ✓ Drop existing schema at start<br>✓ Create fresh schema<br>⚠️ **Keep schema after tests** | Debugging test failures, inspecting test data |

**Important Notes**:
- ✅ **Always drops previously preserved schemas** at the start of each run
- ✅ No manual cleanup needed between runs
- ✅ Multiple test files use different schemas (test_database, test_job, test_batch)
- ✅ Safe to run tests repeatedly

**Example Workflow**:
```bash
# 1. Run tests normally (auto-cleanup)
pytest workspace/tests/test_database.py

# 2. Test fails - want to inspect data
pytest workspace/tests/test_database.py --preserve-schema

# 3. Connect to database and query:
#    SELECT * FROM test_database.irp_cycle;

# 4. Fix code, run again (auto-drops old preserved schema, creates new)
pytest workspace/tests/test_database.py

# 5. Tests pass, schema is cleaned up automatically
```

**Benefits of pytest**:
- ✅ Automatic test discovery
- ✅ Rich assertion messages
- ✅ Fixtures for shared test data
- ✅ Coverage reporting
- ✅ Parallel execution
- ✅ Better CI/CD integration

**Migration Status**:
- Phase 1: ✅ Infrastructure added (pytest.ini, conftest.py, requirements-test.txt)
- Phase 2: 🔄 Converting test files to pytest style
- Phase 3: ⏳ Full migration

See [PYTEST_MIGRATION_GUIDE.md](../../docs/PYTEST_MIGRATION_GUIDE.md) for details.

---

## Troubleshooting

### Database Connection Failed

If you see "Database connection failed":
1. Verify PostgreSQL is running
2. Check database credentials in environment variables
3. Ensure the database exists (`test_db` for tests)
4. Verify the user has appropriate permissions

### Import Errors

If you see import errors:
1. Make sure you're in the project root directory
2. Activate the virtual environment if using one
3. Ensure all dependencies are installed: `pip install -r requirements.txt`

### Schema Already Exists

If tests fail due to existing test schema:
```sql
-- Manually drop the test schema
DROP SCHEMA IF EXISTS test CASCADE;
DROP SCHEMA IF EXISTS test_database CASCADE;
DROP SCHEMA IF EXISTS test_job CASCADE;
DROP SCHEMA IF EXISTS test_batch CASCADE;
```

Or use pytest which handles this automatically:
```bash
pytest  # Auto-cleanup
```

Then re-run the tests.
