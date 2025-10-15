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

## Troubleshooting

### Database Connection Failed

If you see "Database connection failed":
1. Verify PostgreSQL is running
2. Check database credentials in environment variables
3. Ensure the database exists (`irp_db`)
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
```

Then re-run the tests.
