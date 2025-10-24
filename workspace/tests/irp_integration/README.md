# IRP Integration Tests

This directory contains tests for the `irp_integration` module, which provides integration with Moody's Risk Modeler API.

## Test Structure

```
irp_integration/
├── __init__.py                    # Package initialization
├── conftest.py                    # Shared fixtures for API tests
├── test_client.py                 # HTTP client unit tests (retry, timeout, polling)
├── test_irp_integration_e2e.py    # End-to-end workflow tests
└── fixtures/                      # Test data files
    ├── test_accounts.csv          # Sample account data (10 records)
    ├── test_locations.csv         # Sample location data (10 records)
    └── mapping.json               # MRI import mapping configuration
```

## Prerequisites

### Environment Variables

These tests automatically load environment variables from the `.env.test` file in the project root. The required variables are:

```bash
RISK_MODELER_BASE_URL=https://api-euw1.rms-ppe.com
RISK_MODELER_API_KEY=your_actual_api_key_here
RISK_MODELER_RESOURCE_GROUP_ID=your_resource_group_id
```

**Note:** The conftest automatically loads these from `.env.test` before running tests. If the `.env.test` file is missing or these variables are not set, the `Client` class will use default values which will cause authentication failures.

### Test Data

The test data files in the `fixtures/` directory are minimal versions (10 records each) of the full datasets used in the demo notebook. This keeps tests fast while still validating the complete workflow.

## Running Tests

### Run All IRP Integration Tests

```bash
pytest workspace/tests/irp_integration/ -v -s
```

### Run Specific Test Files

```bash
# Run HTTP client tests (fast, fully mocked, no API required)
pytest workspace/tests/irp_integration/test_client.py -v

# Run E2E workflow tests (slow, requires API credentials)
pytest workspace/tests/irp_integration/test_irp_integration_e2e.py -v -s
```

### Run with test.sh Script

```bash
# Run all integration tests
./test.sh workspace/tests/irp_integration/ -v

# Run only client tests
./test.sh workspace/tests/irp_integration/test_client.py -v
```

### Run with Marker Filters

```bash
# Run only tests marked with moody_api
pytest -m moody_api -v -s

# Run E2E and slow tests
pytest -m "e2e and slow" -v -s

# Skip slow tests
pytest workspace/tests/irp_integration/ -m "not slow" -v

# Run only unit tests (fast, no API)
pytest workspace/tests/irp_integration/ -m "unit" -v
```

### Collect Tests Without Running

```bash
pytest workspace/tests/irp_integration/ --collect-only
```

## Test Markers

Tests in this directory use the following pytest markers:

- **`@pytest.mark.unit`**: Unit tests with mocked dependencies (fast, no external APIs)
- **`@pytest.mark.integration`**: Integration tests that may use mocked APIs or real APIs
- **`@pytest.mark.moody_api`**: Tests that require Moody's API credentials
- **`@pytest.mark.e2e`**: End-to-end integration tests
- **`@pytest.mark.slow`**: Tests that take a long time to run (>1 minute)

## Client Unit Tests (test_client.py)

The `test_client.py` file contains comprehensive unit tests for the HTTP client that handles all API communication. These tests are **fully mocked** and do not require API credentials or network connectivity.

### Test Coverage: 100%

**60 tests covering:**

1. **Client Initialization** (4 tests)
   - Environment variable configuration
   - Headers setup
   - Session retry adapter configuration

2. **Retry Mechanism** (11 tests)
   - Retry on transient errors (429, 500, 502, 503, 504)
   - No retry on client errors (400, 404)
   - Max retry attempts (6 total: 1 original + 5 retries)
   - Retry across all HTTP methods

3. **Timeout Handling** (3 tests)
   - Default timeout (200s)
   - Custom timeout parameter
   - Timeout exception handling

4. **HTTP Request Methods** (10 tests)
   - GET, POST, PUT, PATCH, DELETE requests
   - Query parameters
   - JSON body handling
   - Custom headers merging
   - URL construction (full_url vs base_url + path)

5. **Error Handling** (6 tests)
   - HTTPError with JSON/text response enrichment
   - 401 Unauthorized, 403 Forbidden
   - Connection errors
   - raise_for_status validation

6. **Workflow Helper Methods** (6 tests)
   - Location header extraction
   - Workflow ID parsing
   - Case sensitivity handling

7. **Workflow Polling** (7 tests)
   - Single workflow polling
   - Status progression (QUEUED → PENDING → RUNNING → FINISHED)
   - FAILED, CANCELLED status handling
   - Timeout and invalid URL validation
   - Custom polling intervals

8. **Batch Workflow Polling** (6 tests)
   - Multi-workflow polling
   - Pagination handling (100+ workflows)
   - Mixed completion statuses
   - Batch timeout handling

9. **Execute Workflow** (7 tests)
   - Submit + poll flow for 201/202 responses
   - No polling for non-201/202 responses
   - Parameter and header passthrough
   - End-to-end execution

### Running Client Tests

```bash
# Fast execution (~21 seconds for all 60 tests)
pytest workspace/tests/irp_integration/test_client.py -v

# With coverage report
pytest workspace/tests/irp_integration/test_client.py --cov=helpers.irp_integration.client

# Run specific test
pytest workspace/tests/irp_integration/test_client.py::test_retry_on_429_rate_limit -v
```

### Key Features

- ✅ **No API Required** - All responses mocked using `responses` library
- ✅ **Fast** - Complete test suite runs in ~21 seconds
- ✅ **100% Coverage** - Every line of client.py tested
- ✅ **Comprehensive** - All error paths and edge cases covered

## E2E Workflow Test (test_irp_integration_e2e.py)

The `test_complete_workflow_edm_to_rdm` test validates the complete Moody's API integration workflow:

### Workflow Steps

1. **Create EDM** - Creates a new Exposure Data Manager database
2. **Create Portfolio** - Creates a portfolio within the EDM
3. **MRI Import** - Uploads CSV files and executes Model Ready Import
4. **Create Treaty** - Creates a reinsurance treaty with LOB assignments
5. **Upgrade EDM** - Upgrades the EDM to the latest data version
6. **GeoHaz** - Runs geocoding and hazard processing
7. **Execute Single Analysis** - Runs a single catastrophe analysis
8. **Execute Batch Analyses** - Submits and polls multiple analyses
9. **Create Analysis Group** - Groups analyses together
10. **Export to RDM** - Exports results to Risk Data Manager

### Expected Runtime

The E2E test typically takes **10-15 minutes** to complete, as it involves:
- Creating and configuring an EDM
- Uploading and importing data
- Running geocoding and hazard processing
- Executing multiple catastrophe analyses
- Exporting results

### Resource Cleanup

The test automatically cleans up resources after completion:
- EDMs are deleted via the `cleanup_edms` fixture
- Cleanup happens even if the test fails
- Cleanup errors are logged but don't fail the test

### Unique Naming

Each test run uses unique names based on timestamp to avoid conflicts:
- Format: `test_YYYYMMDD_HHMMSS_microseconds_<resource>`
- Examples:
  - `test_20251022_193045_123456_EDM`
  - `test_20251022_193045_123456_Portfolio`
  - `test_20251022_193045_123456_Treaty`

## Troubleshooting

### Authentication Errors

```
requests.HTTPError: 401 Client Error: Unauthorized
```

**Solution:** Verify your `RISK_MODELER_API_KEY` is correct and has not expired.

### Timeout Errors

```
TimeoutError: Workflow did not complete within 600 seconds
```

**Solution:** This is expected for very large datasets. The test data is intentionally small to avoid this. If you're testing with larger data, you may need to adjust timeout values in `client.py`.

### Resource Conflicts

```
Error: EDM with name 'test_xyz' already exists
```

**Solution:** This shouldn't happen with the timestamp-based naming. If it does:
1. Check if a previous test run failed before cleanup
2. Manually delete the conflicting EDM
3. Re-run the test

## Adding New Tests

When adding new tests to this directory:

1. **Use the fixtures** from `conftest.py`:
   - `irp_client` - Provides configured IRPClient instance
   - `unique_name` - Generates unique resource names
   - `test_data_dir` - Path to fixtures directory
   - `cleanup_edms` - Tracks EDMs for automatic deletion

2. **Add appropriate markers**:
   ```python
   @pytest.mark.moody_api
   @pytest.mark.slow  # if runtime > 1 minute
   def test_my_feature(irp_client, unique_name):
       ...
   ```

3. **Clean up resources**:
   ```python
   def test_creates_edm(irp_client, unique_name, cleanup_edms):
       edm_name = f"{unique_name}_MyEDM"
       cleanup_edms.append(edm_name)  # Auto-cleanup
       irp_client.edm.create_edm(edm_name, "server")
       ...
   ```

4. **Use descriptive assertions**:
   ```python
   assert response['status'] == 'FINISHED', \
       f"Expected FINISHED but got {response['status']}"
   ```

## Test Summary

### Current Test Files

| File | Tests | Coverage | Runtime | API Required | Description |
|------|-------|----------|---------|--------------|-------------|
| `test_client.py` | 60 | 100% | ~21s | ❌ No | HTTP client retry, timeout, polling |
| `test_irp_integration_e2e.py` | 1 | N/A | 10-15min | ✅ Yes | Complete workflow validation |

### Total: 61 tests

## Future Tests

Planned additions to this test directory:

- **`test_managers.py`** - Individual manager tests with mocked client
  - EDMManager specific tests
  - PortfolioManager specific tests
  - AnalysisManager specific tests
  - TreatyManager specific tests
  - RDMManager specific tests

- **`test_error_handling.py`** - Error handling tests
  - Invalid credentials
  - Network failures
  - API errors (400, 404, 500, etc.)
  - Malformed responses
  - Retry exhaustion scenarios

## References

- Demo notebook: `workspace/workflows/_Tools/IRP_Integration_Demo.ipynb`
- Client implementation: `workspace/helpers/irp_integration/client.py`
- Manager implementations: `workspace/helpers/irp_integration/*.py`
- API documentation: [Moody's Risk Modeler API Docs](https://developer.rms.com/risk-modeler/)
