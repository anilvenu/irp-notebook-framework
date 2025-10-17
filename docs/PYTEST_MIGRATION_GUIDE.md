# pytest Migration Guide

**Date:** October 2025
**Status:** Phase 1 Complete - Infrastructure Added

---

## Overview

This guide documents the migration from custom test runner to pytest framework for the IRP Notebook Framework. The migration is being done in phases to ensure no disruption to existing workflows.

### Why pytest?

1. **Industry Standard**: Well-maintained, extensive ecosystem
2. **Better DX**: Rich assertions, fixtures, parametrization
3. **Scalability**: Parallel execution, better test organization
4. **CI/CD Ready**: Easy integration with GitHub Actions
5. **Preserves Innovation**: Works perfectly with our schema-per-file pattern

---

## Migration Phases

### ✅ Phase 1: Infrastructure Setup (COMPLETE)

**Status**: ✅ Complete - No breaking changes

**Files Added**:
- `pytest.ini` - pytest configuration
- `requirements-test.txt` - test dependencies
- `workspace/tests/conftest.py` - shared fixtures

**What Changed**:
- Nothing! Old tests still work exactly the same
- New pytest infrastructure added alongside existing setup

**To Install**:
```bash
pip install -r requirements-test.txt
```

---

### 🔄 Phase 2: Proof of Concept (IN PROGRESS)

**Goal**: Convert one test file to pytest style

**Target**: `test_database.py` (simplest, no complex dependencies)

**Approach**:
1. Keep original as `test_database_legacy.py`
2. Create pytest version as `test_database.py`
3. Run both side-by-side
4. Compare outputs and validate

**Conversion Example**:

**Before** (Custom Runner):
```python
def setup_test_schema():
    print("SETUP: Initializing Test Schema")
    success = init_database(schema=TEST_SCHEMA)
    if not success:
        print("Failed to initialize test schema")
        return False
    return True

def cleanup_test_schema():
    print("CLEANUP: Dropping Test Schema")
    # Drop schema logic
    pass

def test_basic_bulk_insert():
    print("TEST 1: Basic Bulk Insert")
    try:
        query = "INSERT INTO irp_cycle ..."
        ids = bulk_insert(query, params, schema=TEST_SCHEMA)
        assert len(ids) == 5
        print("✓ Test passed")
        return True
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def run_all_tests(preserve=False):
    if not setup_test_schema():
        return

    tests = [
        ("Basic Bulk Insert", test_basic_bulk_insert),
        # ...
    ]

    results = []
    for name, func in tests:
        result = func()
        results.append((name, result))

    if not preserve:
        cleanup_test_schema()

    # Print summary
    passed = sum(1 for _, r in results if r)
    print(f"{passed}/{len(results)} tests passed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--preserve', action='store_true')
    args = parser.parse_args()
    run_all_tests(preserve=args.preserve)
```

**After** (pytest Style):
```python
def test_basic_bulk_insert(test_schema):
    """Test 1: Basic Bulk Insert"""
    query = "INSERT INTO irp_cycle ..."
    ids = bulk_insert(query, params, schema=test_schema)

    assert len(ids) == 5
    assert ids == [1, 2, 3, 4, 5]

# No need for:
# - setup_test_schema() → test_schema fixture handles it
# - cleanup_test_schema() → automatic cleanup
# - run_all_tests() → pytest discovers tests
# - argparse → pytest handles CLI args
# - try/except → pytest captures exceptions
# - return True/False → pytest uses assertions
```

**Key Differences**:
1. ✅ Setup/teardown automatic via `test_schema` fixture
2. ✅ No manual test registration - pytest discovers functions starting with `test_`
3. ✅ No try/except needed - pytest handles exceptions
4. ✅ Rich assertions instead of manual checking
5. ✅ CLI arguments handled by pytest (--preserve-schema, etc.)

---

### ⏳ Phase 3: Gradual Conversion (PENDING)

**Order of Conversion**:
1. ✅ `test_database.py` (simplest)
2. `test_configuration.py`
3. `test_job.py`
4. `test_batch.py`
5. `test_batch_job_integration.py`

**For Each File**:
1. Rename original to `*_legacy.py`
2. Create pytest version
3. Run both versions in parallel
4. Validate output matches
5. Once stable, archive legacy version

---

### ⏳ Phase 4: Enhanced Features (FUTURE)

**Parametrized Tests**:
```python
@pytest.mark.parametrize("job_status,expected_batch_status", [
    (JobStatus.FINISHED, BatchStatus.COMPLETED),
    (JobStatus.FAILED, BatchStatus.FAILED),
    (JobStatus.ERROR, BatchStatus.ERROR),
    (JobStatus.CANCELLED, BatchStatus.CANCELLED),
])
def test_batch_recon_status(test_schema, job_status, expected_batch_status):
    """Test batch reconciliation for different job statuses"""
    batch_id = create_test_batch(schema=test_schema)
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    update_job_status(jobs[0]['id'], job_status, schema=test_schema)

    result = recon_batch(batch_id, schema=test_schema)
    assert result == expected_batch_status
```

**Custom Fixtures**:
```python
@pytest.fixture
def batch_with_jobs(test_schema, sample_hierarchy):
    """Fixture providing a batch with 3 jobs in different states"""
    cycle_id, stage_id, step_id, config_id = sample_hierarchy
    batch_id = create_batch('multi_job', config_id, step_id, schema=test_schema)
    submit_batch(batch_id, schema=test_schema)
    return batch_id

def test_complex_scenario(batch_with_jobs, test_schema):
    """Use the fixture"""
    jobs = get_batch_jobs(batch_with_jobs, schema=test_schema)
    assert len(jobs) == 3
```

---

## Current Test Execution

### Old Way (Still Works)
```bash
# Run all tests
./test.sh

# Run with schema preservation
./test.sh --preserve

# Run specific test
python workspace/tests/test_database.py --preserve
```

### New Way (pytest)
```bash
# Install dependencies first
pip install -r requirements-test.txt

# Run all tests
pytest

# Run with schema preservation
pytest --preserve-schema

# Run specific test file
pytest workspace/tests/test_database.py

# Run specific test function
pytest workspace/tests/test_database.py::test_basic_bulk_insert

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=workspace/helpers --cov-report=html

# Run in parallel (faster!)
pytest -n auto

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Stop at first failure
pytest -x

# Drop into debugger on failure
pytest --pdb

# Show local variables on failure
pytest -l
```

---

## Key Features

### 1. Automatic Schema Management

**Our Innovation**: Schema-per-file for parallel execution

```python
# conftest.py automatically does this:
# test_database.py → schema: test_database
# test_job.py → schema: test_job
# test_batch.py → schema: test_batch
```

This allows safe parallel execution:
```bash
pytest -n auto  # Run all tests in parallel
```

### 2. Rich Fixtures

**Shared Test Data**:
```python
def test_something(sample_hierarchy, test_schema):
    """Automatically get cycle, stage, step, config"""
    cycle_id, stage_id, step_id, config_id = sample_hierarchy
    # Use in test
```

**Available Fixtures** (from conftest.py):
- `test_schema`: Auto-managed schema for test module
- `sample_cycle`: Single cycle
- `sample_hierarchy`: Complete cycle → stage → step → config
- `sample_batch`: Batch with hierarchy

### 3. Better Assertions

**pytest provides rich assertion output**:
```python
# Instead of:
if len(ids) != 5:
    print(f"✗ Expected 5 ids, got {len(ids)}")
    return False

# Write:
assert len(ids) == 5

# pytest shows:
# AssertionError: assert 3 == 5
#  +  where 3 = len([1, 2, 3])
```

### 4. Test Organization

**Use markers** to organize tests:
```python
@pytest.mark.unit
def test_read_job(test_schema):
    """Unit test"""
    pass

@pytest.mark.integration
@pytest.mark.slow
def test_complete_job_lifecycle(test_schema):
    """Integration test"""
    pass
```

**Run by marker**:
```bash
pytest -m unit           # Only unit tests
pytest -m integration    # Only integration tests
pytest -m "not slow"     # Skip slow tests
```

---

## Schema-Per-File Pattern

### Why It's Brilliant

Each test file gets its own schema:
- No conflicts between tests
- Can run tests in parallel
- Easy cleanup
- Scales infinitely

### How It Works with pytest

**conftest.py**:
```python
@pytest.fixture(scope="module")
def test_schema(request):
    # Derive schema from filename
    schema = Path(request.fspath).stem

    # Setup
    init_database(schema=schema)

    yield schema

    # Cleanup (unless --preserve-schema)
    if not request.config.getoption("--preserve-schema"):
        drop_schema(schema)
```

**Your test**:
```python
def test_something(test_schema):
    # test_schema = "test_database" (from filename)
    job_id = create_job(..., schema=test_schema)
```

---

## Coverage Tracking

### Generate Coverage Reports

```bash
# Terminal report
pytest --cov=workspace/helpers --cov-report=term-missing

# HTML report (browsable)
pytest --cov=workspace/helpers --cov-report=html
# Open: htmlcov/index.html

# Fail if coverage below threshold
pytest --cov=workspace/helpers --cov-fail-under=80
```

### Coverage Output Example
```
---------- coverage: platform linux, python 3.11 -----------
Name                            Stmts   Miss  Cover   Missing
-------------------------------------------------------------
workspace/helpers/__init__.py       0      0   100%
workspace/helpers/batch.py        150     12    92%   45-48, 67
workspace/helpers/database.py      85      5    94%   12, 34
workspace/helpers/job.py          200     25    88%   102-115, 234
-------------------------------------------------------------
TOTAL                             435     42    90%
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: test_password
        options: >-
          --health-cmd pg_isready
          --health-interval 10s

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-test.txt

      - name: Run tests with coverage
        run: |
          pytest --cov=workspace/helpers \
                 --cov-report=xml \
                 --cov-report=term \
                 --cov-fail-under=80

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.xml
```

---

## Troubleshooting

### "No tests collected"

**Cause**: pytest can't find tests

**Fix**: Ensure test functions start with `test_`:
```python
# ✅ Good
def test_something():
    pass

# ❌ Bad
def check_something():
    pass
```

### "fixture 'test_schema' not found"

**Cause**: conftest.py not loaded

**Fix**: Ensure you're running from project root:
```bash
cd /path/to/irp-notebook-framework
pytest
```

### Tests fail with "schema does not exist"

**Cause**: Schema setup failed

**Fix**: Check database connection and permissions:
```bash
# Verify database
python -c "from helpers.database import test_connection; print(test_connection())"
```

### Old tests still run with pytest

**Cause**: pytest discovers all test_*.py files

**Solution**: During migration, rename legacy files:
```bash
mv workspace/tests/test_database.py workspace/tests/test_database_legacy.py
```

---

## Next Steps

1. **Phase 2**: Convert `test_database.py` to pytest style
2. **Validate**: Run both old and new side-by-side
3. **Phase 3**: Convert remaining test files one at a time
4. **Phase 4**: Add enhanced features (parametrization, fixtures)
5. **CI/CD**: Integrate pytest into GitHub Actions

---

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [pytest Parametrize](https://docs.pytest.org/en/stable/parametrize.html)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)

---

## Questions?

Contact the development team or check:
- [Testing Strategy](./TESTING_STRATEGY.md)
- [Design Document](./DESIGN_DOCUMENT.md)
