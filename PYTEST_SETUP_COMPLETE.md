# pytest Infrastructure Setup - Complete ✅

**Date:** October 2025
**Phase:** 1 of 4
**Status:** ✅ Complete - Ready to Use

---

## What Was Done

### Phase 1: Infrastructure Setup ✅ COMPLETE

We've successfully added pytest infrastructure to your project **without breaking anything**. Your existing test.sh and test files continue to work exactly as before.

### Files Created

1. **`pytest.ini`** - pytest configuration
   - Test discovery settings
   - Markers for test organization (unit, integration, slow, etc.)
   - Output formatting options

2. **`requirements-test.txt`** - Test dependencies
   - pytest 7.4.3
   - pytest-cov (coverage reporting)
   - pytest-xdist (parallel execution)
   - pytest-timeout (timeout protection)
   - pytest-mock (mocking support)

3. **`workspace/tests/conftest.py`** - Shared fixtures and configuration
   - Automatic schema management (preserves your schema-per-file pattern)
   - Database connection verification
   - Shared fixtures: `test_schema`, `sample_cycle`, `sample_hierarchy`, `sample_batch`
   - Custom CLI option: `--preserve-schema`

4. **`docs/PYTEST_MIGRATION_GUIDE.md`** - Complete migration documentation
   - Phase-by-phase migration plan
   - Before/after code examples
   - pytest features and benefits
   - Troubleshooting guide

5. **Updated `workspace/tests/README.md`** - Added pytest instructions

---

## How to Use

### Step 1: Install pytest Dependencies

```bash
pip install -r requirements-test.txt
```

### Step 2: Verify Installation

```bash
# Check pytest is installed
pytest --version

# Should show: pytest 7.4.3
```

### Step 3: Run Tests with pytest

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest workspace/tests/test_database.py

# Run with coverage
pytest --cov=workspace/helpers --cov-report=html

# Run in parallel (faster!)
pytest -n auto

# Preserve schemas for debugging
pytest --preserve-schema
```

### Step 4: Verify Old Tests Still Work

```bash
# Old way still works!
./test.sh
./test.sh --preserve
```

---

## Key Features

### 1. Your Schema-Per-File Pattern is Preserved! 🎉

Your brilliant innovation is fully supported and enhanced:

```python
# conftest.py automatically:
# test_database.py → schema: test_database
# test_job.py → schema: test_job
# test_batch.py → schema: test_batch
```

This allows:
- ✅ Parallel test execution
- ✅ No conflicts between test files
- ✅ Easy cleanup
- ✅ Scales infinitely

### 2. Automatic Setup/Teardown

No more manual setup and cleanup code!

```python
# Just use the test_schema fixture
def test_something(test_schema):
    # schema is automatically created
    job_id = create_job(..., schema=test_schema)
    # schema is automatically cleaned up after test
```

### 3. Shared Fixtures

Reduce boilerplate with reusable test data:

```python
def test_with_hierarchy(sample_hierarchy, test_schema):
    """Automatically get cycle, stage, step, config"""
    cycle_id, stage_id, step_id, config_id = sample_hierarchy
    # Use in test
```

### 4. Rich Assertions

Better error messages:

```python
# Before
if len(ids) != 5:
    print(f"✗ Expected 5 ids, got {len(ids)}")
    return False

# After
assert len(ids) == 5
# pytest shows detailed assertion info on failure
```

---

## What Didn't Change

✅ Your existing test files work exactly as before
✅ `./test.sh` still works
✅ Test logic and validation unchanged
✅ Database schema pattern preserved
✅ No breaking changes

---

## Next Steps (Optional - When Ready)

### Phase 2: Convert One Test File

Convert `test_database.py` to pytest style as proof of concept:

1. Rename current: `mv test_database.py test_database_legacy.py`
2. Create pytest version as `test_database.py`
3. Run both and compare outputs
4. Once confident, proceed to Phase 3

See [docs/PYTEST_MIGRATION_GUIDE.md](docs/PYTEST_MIGRATION_GUIDE.md) for detailed instructions.

### Phase 3: Convert Remaining Files

Convert one file at a time:
1. test_configuration.py
2. test_job.py
3. test_batch.py
4. test_batch_job_integration.py

### Phase 4: Enhanced Features

Add advanced pytest features:
- Parametrized tests
- Custom fixtures
- Test markers and organization
- CI/CD integration

---

## Benefits You Get Now

Even without converting any test files, you can:

✅ **Run tests in parallel**: `pytest -n auto`
✅ **Get coverage reports**: `pytest --cov=workspace/helpers --cov-report=html`
✅ **Use test markers**: Add `@pytest.mark.slow` to your existing tests
✅ **Better CI/CD**: pytest integrates easily with GitHub Actions
✅ **Discover tests automatically**: No manual test registration needed

---

## Documentation

- **Migration Guide**: [docs/PYTEST_MIGRATION_GUIDE.md](docs/PYTEST_MIGRATION_GUIDE.md)
- **Testing Strategy**: [docs/TESTING_STRATEGY.md](docs/TESTING_STRATEGY.md)
- **Test README**: [workspace/tests/README.md](workspace/tests/README.md)

---

## Quick Reference

### Running Tests

```bash
# Old way (still works)
./test.sh
./test.sh --preserve

# New way (pytest)
pytest                           # Run all tests
pytest -v                        # Verbose
pytest -n auto                   # Parallel
pytest --preserve-schema         # Keep schemas
pytest -m unit                   # Only unit tests
pytest -m "not slow"             # Skip slow tests
pytest --cov=workspace/helpers   # With coverage
pytest -x                        # Stop at first failure
pytest --pdb                     # Debugger on failure
```

### pytest Configuration

- **Config file**: `pytest.ini`
- **Fixtures**: `workspace/tests/conftest.py`
- **Dependencies**: `requirements-test.txt`

### Custom Options

```bash
--preserve-schema    # Keep test schemas after completion (for debugging)
-m MARKER           # Run tests with specific marker (unit, integration, slow)
-n NUM              # Run tests in parallel (use 'auto' for CPU count)
--cov=PATH          # Generate coverage report for PATH
```

### ⚠️ CRITICAL: --preserve-schema Flag Behavior

**Your requirement**: "preserve option must keep the schema. omitting it should clean up the schema. there needs to be a drop schema at the beginning before creating it."

**✅ Implemented correctly in conftest.py**:

| Command | Drop at Start? | Drop at End? | Use Case |
|---------|----------------|--------------|----------|
| `pytest` | ✅ YES | ✅ YES | Normal testing |
| `pytest --preserve-schema` | ✅ YES | ❌ NO | Debug/inspect data |

**Behavior Details**:

1. **At Start (Always)**:
   ```python
   # conftest.py lines 127-143
   # Check if schema exists from previous --preserve run
   # DROP SCHEMA IF EXISTS {schema} CASCADE
   # Create fresh schema
   ```

2. **At End (Conditional)**:
   ```python
   # conftest.py lines 141-153
   if not request.config.getoption("--preserve-schema"):
       DROP SCHEMA  # Clean up
   else:
       print("Schema preserved for debugging")
   ```

**Example Flow**:
```bash
# Run 1: Normal (clean up)
$ pytest workspace/tests/test_database.py
# → Drop old 'test_database' if exists
# → Create new 'test_database'
# → Run tests
# → Drop 'test_database' ✓

# Run 2: Preserve for debugging
$ pytest workspace/tests/test_database.py --preserve-schema
# → Drop old 'test_database' (from Run 1's cleanup)
# → Create new 'test_database'
# → Run tests
# → Keep 'test_database' ⚠️

# Now inspect: SELECT * FROM test_database.irp_cycle;

# Run 3: Normal again
$ pytest workspace/tests/test_database.py
# → Drop preserved 'test_database' (from Run 2)
# → Create new 'test_database'
# → Run tests
# → Drop 'test_database' ✓
```

**Confirmation in Code** ([workspace/tests/conftest.py:111-153](workspace/tests/conftest.py#L111-L153))

---

## Troubleshooting

### "No tests collected"

**Fix**: Ensure test functions start with `test_`:
```python
def test_something():  # ✅ Good
    pass
```

### "fixture 'test_schema' not found"

**Fix**: Run from project root:
```bash
cd /path/to/irp-notebook-framework
pytest
```

### Old and new tests both run

**Expected during migration!** To run only new tests:
```bash
pytest workspace/tests/test_database.py  # Specific file
```

---

## Summary

🎉 **Phase 1 Complete!**

- ✅ pytest infrastructure added
- ✅ Zero breaking changes
- ✅ Old tests still work
- ✅ Schema-per-file pattern preserved
- ✅ Ready to use pytest features
- ✅ Documentation complete

You can now:
1. Continue using `./test.sh` as before
2. Start using `pytest` for new benefits
3. Convert test files when ready (optional)

**No urgency to migrate** - both methods work perfectly!

---

## Questions?

- Check [docs/PYTEST_MIGRATION_GUIDE.md](docs/PYTEST_MIGRATION_GUIDE.md)
- Review [pytest documentation](https://docs.pytest.org/)
- Contact development team
