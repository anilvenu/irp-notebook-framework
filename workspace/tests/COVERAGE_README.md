# Test Coverage Configuration

## Overview

The test suite now automatically generates coverage reports after every test run. This tracks which lines of code in `helpers/` are tested and which are missing coverage.

## Configuration Files

### `.coveragerc` (Project Root)
Controls what code is measured and how reports are generated:

**Includes:**
- All files in `workspace/helpers/`

**Excludes:**
- `workspace/helpers/irp_integration/*` (integration code)
- Test files themselves
- `__pycache__` and compiled Python files

**Settings:**
- Branch coverage enabled (tracks if/else branches)
- Shows missing line numbers
- Sorts by coverage percentage

### `pytest.ini` (Project Root)
Automatically runs coverage on every test execution with these flags:
```ini
--cov                      # Enable coverage
--cov-report=term-missing  # Show terminal report with missing lines
--cov-report=html          # Generate HTML report
--no-cov-on-fail          # Skip coverage if tests fail
```

## Generated Reports

### 1. Terminal Output (Automatic)
Every pytest run shows coverage summary:
```
Name                         Stmts   Miss  Cover   Missing
----------------------------------------------------------
workspace/helpers/cycle.py     149     11    93%   50-51, 61-62, 168, ...
workspace/helpers/job.py       170     19    89%   92-93, 137-138, ...
----------------------------------------------------------
TOTAL                          319     30    91%
```

### 2. HTML Report (Interactive)
- **Location**: `workspace/tests/htmlcov/index.html`
- **Features**:
  - Color-coded coverage visualization
  - Click on files to see line-by-line coverage
  - Red = not covered, Green = covered
  - Shows which tests covered which lines

**To view**: Open `workspace/tests/htmlcov/index.html` in a web browser

### 3. JSON Report (Programmatic)
- **Location**: `workspace/tests/coverage.json`
- **Use case**: Parse programmatically or integrate with CI/CD

### 4. XML Report (CI/CD)
- **Location**: `workspace/tests/coverage.xml`
- **Use case**: GitHub Actions, Jenkins, etc.

## Usage

### Run All Tests with Coverage (Default)
```bash
pytest
```

This automatically:
1. Runs all tests in `workspace/tests/`
2. Measures coverage for `helpers/` (excluding `helpers/irp_integration`)
3. Displays terminal report
4. Generates HTML report in `workspace/tests/htmlcov/`

### Run Specific Tests with Coverage
```bash
# Single test file
pytest workspace/tests/test_cycle.py

# Single test function
pytest workspace/tests/test_cycle.py::test_get_stages_and_steps_with_template_directory

# All tests matching pattern
pytest -k "test_create_job"
```

### Run Without Coverage (Faster)
```bash
pytest --no-cov
```

### Coverage for Specific Modules Only
```bash
pytest --cov=helpers.cycle --cov=helpers.job
```

### Generate Only HTML Report (No Terminal)
```bash
pytest --cov-report=html --cov-report=''
```

## Current Coverage Status

As of latest test run:

| Module | Coverage | Missing Lines |
|--------|----------|---------------|
| **cycle.py** | **93%** | 11 lines |
| **job.py** | **89%** | 19 lines |
| **Overall** | **91%** | 30 lines |

## Viewing Coverage Reports

### Option 1: Terminal (Quick Check)
Terminal report shows after every test run automatically.

### Option 2: HTML (Detailed Analysis)
```bash
# Run tests (generates HTML automatically)
pytest

# Open HTML report in browser
# On WSL/Linux with browser access:
xdg-open workspace/tests/htmlcov/index.html

# On Windows WSL, use:
explorer.exe workspace/tests/htmlcov/index.html

# Or manually open in browser:
# File -> Open -> /path/to/irp-notebook-framework/workspace/tests/htmlcov/index.html
```

## Coverage Goals

### Target Coverage: 95%+
- **cycle.py**: 93% → Need 2% more (cover 11 lines)
- **job.py**: 89% → Need 6% more (cover 19 lines)

### Remaining Uncovered Lines

**cycle.py (11 lines):**
- Lines 50-51: Error message for invalid pattern
- Lines 61-62: Error message for existing cycle
- Line 168: Default notebooks_dir assignment
- Line 181: Continue statement in loop
- Line 193: Continue statement in loop
- Line 236: Dry-run print statement
- Line 243: Continue statement
- Line 254: Dry-run print statement
- Line 260: Stage count increment

**job.py (19 lines):**
- Lines 92-93: Database error handling in _create_job_configuration
- Lines 137-138: Database error handling in _create_job
- Lines 227-228: Database error handling in _register_job_submission
- Lines 269-270: Database error handling in _insert_tracking_log
- Lines 313, 315: JSON parsing edge cases
- Line 341: Invalid status error path
- Lines 407, 413: Job config not found error path
- Line 484: Validation path (covered now)
- Lines 529-530: Database error in skip_job
- Lines 611-612: track_immediately with time.sleep
- Line 681: Track job without workflow_id

## Troubleshooting

### Coverage Not Running
Check if pytest-cov is installed:
```bash
pip list | grep pytest-cov
```

If missing:
```bash
pip install pytest-cov
```

### Wrong Files Measured
Check `.coveragerc` source and omit sections.

### HTML Report Not Generated
Ensure htmlcov directory is writable:
```bash
chmod -R 755 workspace/tests/htmlcov
```

## Integration with test.sh

The `test.sh` script uses pytest, so coverage runs automatically.

No changes needed to existing workflow!
