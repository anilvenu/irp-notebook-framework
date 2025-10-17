# IRP Notebook Framework - Testing Strategy

**Version:** 1.0
**Date:** October 2025

---

## Executive Summary

### Purpose
This testing strategy ensures the IRP Notebook Framework delivers reliable, production-ready risk analysis workflows through comprehensive automated and manual testing.

### Testing Scope

The application workspace comprises two main parts:

1. **Workflows** - Jupyter Notebooks for risk analysts to perform core analysis cycle activities, plus tool notebooks for administrative functions
2. **Helpers** - Python and SQL code supporting the core workflow and tool notebooks

### Testing Approach

- **Workflow and Tools Testing**:
  - Tested by the project team during development
  - Delivered to the risk modeling team for user acceptance testing (UAT)

- **Helper Testing**:
  - Tested as part of workflow testing
  - Tested using dedicated scripts that validate database operations and key scenarios:
    - Cycle creation
    - Batch creation
    - Batch reconciliation
    - Job resubmission

- **Moody's Integration Testing**:
  - Controlled manual workflow using Jupyter Notebooks
  - End-to-end functional flow with example portfolio covering complete lifecycle
  - Workflow tools serve as testing tools for idempotent operations (job status tracking, batch reconciliation)

- **Source Database Integration Testing**:
  - Workflow notebooks that extract and stage data from SQL Server to CSVs
  - Test validations reused as pipeline validations post-development

### Key Testing Phases

| Phase | Responsibility | Method | Deliverables |
|-------|---------------|--------|--------------|
| **Unit Testing** | Development Team | Automated (pytest) | 60-75% code coverage on helpers |
| **Integration Testing** | Development Team | Automated + Manual | Database operations, Moody's API scenarios |
| **E2E Testing** | Development Team | Manual workflow execution | Complete cycle validation |
| **UAT** | Risk Modeling Team | Manual execution | User sign-off on workflows and tools |
| **Production Validation** | Development Team | Automated scripts | Pipeline integrity checks |

### Quality Gates

- All unit tests must pass before code merge
- Integration tests validate database and external API interactions
- E2E tests confirm complete workflow execution
- UAT approval required before production deployment
- Automated data pipeline validations run post-deployment

---

## 1. Overview

The IRP Notebook Framework comprises two main components:
- **Helpers**: Python modules in `workspace/helpers/`
- **Workflows**: Jupyter notebooks in `workspace/workflows/`

Testing covers all components, database operations, and external integrations.

---

## 2. Testing Approach

### 2.1 Testing Pyramid

```
           ┌──────────────┐
           │  E2E Tests   │  5-10%  (Manual + Automated)
           └──────────────┘
        ┌──────────────────┐
        │ Integration Tests │  20-30% (Automated)
        └──────────────────┘
     ┌────────────────────────┐
     │     Unit Tests         │  60-75% (Automated)
     └────────────────────────┘
```

### 2.2 Test Categories

| Category | Purpose | Tools | Frequency |
|----------|---------|-------|-----------|
| Unit Tests | Test individual functions | pytest | Every commit |
| Integration Tests | Test component interactions | pytest + PostgreSQL | Before merge |
| E2E Tests | Test complete workflows | Notebooks + validation | Before release |
| UAT | User acceptance testing | Manual execution | Major releases |

---

## 3. Component Testing

### 3.1 Helper Modules

**Helpers to Test**: `database.py`, `job.py`, `batch.py`, `configuration.py`, `cycle.py`

#### Database Module (`database.py`)
- Connection and query execution
- Transaction management
- Error handling
- SQL injection prevention

**Example Tests**:
```python
def test_execute_query_success():
    result = execute_query("SELECT * FROM irp_cycle WHERE id = %s", (1,))
    assert isinstance(result, pd.DataFrame)

def test_execute_insert_returns_id():
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ("Test-2025-Q1", "ACTIVE")
    )
    assert cycle_id > 0
```

#### Job Module (`job.py`)

**Test Scenarios**:
1. Job creation (with new/existing configuration)
2. Job submission (success, failure, already submitted)
3. Job tracking (status transitions)
4. Job resubmission (with/without override)

**Example Tests**:
```python
def test_create_job_with_new_configuration():
    job_id = create_job(
        batch_id=1,
        configuration_id=1,
        job_configuration_data={'portfolio_id': 'PORT001'},
        schema='test'
    )
    job = read_job(job_id, schema='test')
    assert job['status'] == JobStatus.INITIATED

def test_submit_job_handles_api_failure():
    """CRITICAL: Test error handling in submission"""
    with patch('helpers.job._submit_job') as mock_submit:
        mock_submit.side_effect = Exception("API connection failed")

        with pytest.raises(JobError):
            submit_job(job_id, schema='test')

        job = read_job(job_id, schema='test')
        assert job['status'] == JobStatus.ERROR
        assert 'API connection failed' in job['last_error']

def test_resubmit_job_with_override():
    original_job_id = create_job(...)
    update_job_status(original_job_id, JobStatus.FAILED, schema='test')

    new_job_id = resubmit_job(
        original_job_id,
        job_configuration_data={'portfolio_id': 'PORT002'},
        override_reason="Fixed portfolio ID",
        schema='test'
    )

    new_job = read_job(new_job_id, schema='test')
    assert new_job['parent_job_id'] == original_job_id

    original_job = read_job(original_job_id, schema='test')
    assert original_job['skipped'] is True
```

#### Batch Module (`batch.py`)

**Test Scenarios**:
1. Batch creation (with transformer)
2. Batch submission (all jobs)
3. Batch reconciliation (all status paths)

**Example Tests**:
```python
def test_create_batch_generates_jobs():
    def test_transformer(master_config):
        return [
            {'portfolio_id': 'PORT001'},
            {'portfolio_id': 'PORT002'}
        ]
    ConfigurationTransformer.register('test_batch', test_transformer)

    batch_id = create_batch(
        batch_type='test_batch',
        configuration_id=1,
        step_id=1,
        schema='test'
    )

    jobs = get_batch_jobs(batch_id, schema='test')
    assert len(jobs) == 2

def test_recon_batch_completed():
    batch_id = create_test_batch_with_jobs(
        num_jobs=2,
        job_statuses=[JobStatus.FINISHED, JobStatus.FINISHED],
        schema='test'
    )

    status = recon_batch(batch_id, schema='test')
    assert status == BatchStatus.COMPLETED

def test_recon_batch_failed():
    batch_id = create_test_batch_with_jobs(
        num_jobs=3,
        job_statuses=[JobStatus.FINISHED, JobStatus.FAILED, JobStatus.RUNNING],
        schema='test'
    )

    status = recon_batch(batch_id, schema='test')
    assert status == BatchStatus.FAILED

def test_recon_batch_error():
    batch_id = create_test_batch_with_jobs(
        num_jobs=2,
        job_statuses=[JobStatus.ERROR, JobStatus.INITIATED],
        schema='test'
    )

    status = recon_batch(batch_id, schema='test')
    assert status == BatchStatus.ERROR
```

#### Configuration Module (`configuration.py`)

**Test Scenarios**:
1. Configuration upload (valid/invalid Excel)
2. Configuration validation (tabs, columns, data types)
3. Configuration transformation (job config generation)

**Example Tests**:
```python
def test_upload_configuration_valid():
    config_id = upload_configuration(
        cycle_id=1,
        file_path='test_data/valid_config.xlsx',
        schema='test'
    )

    config = read_configuration(config_id, schema='test')
    assert config['status'] == ConfigurationStatus.VALID
    assert 'TAB-A' in config['configuration_data']

def test_configuration_transformer():
    master_config = {
        'portfolios': [
            {'id': 'PORT001', 'name': 'Tech Portfolio'},
            {'id': 'PORT002', 'name': 'Healthcare Portfolio'}
        ],
        'analysis_date': '2025-10-17',
        'parameters': {'risk_model': 'model_v3'}
    }

    job_configs = ConfigurationTransformer.get_job_configurations(
        'portfolio_analysis',
        master_config
    )

    assert len(job_configs) == 2
    assert job_configs[0]['portfolio_id'] == 'PORT001'
```

### 3.2 Workflow Notebooks

**Testing Approach**:
- Manual execution during development
- Automated execution via `papermill` or `nbconvert`
- Output validation via step_run logs

**Example Test**:
```python
def test_execute_step_01_initialize():
    result = execute_notebook(
        notebook_path='workflows/_Template/notebooks/Stage_01_Setup/Step_01_Initialize.ipynb',
        parameters={'cycle_id': 1},
        kernel_name='python3'
    )

    assert result['metadata']['execution']['status'] == 'ok'

    step_runs = get_step_runs_for_cycle(cycle_id=1, schema='test')
    assert step_runs[0]['status'] == StepStatus.COMPLETED
```

---

## 4. Integration Testing

### 4.1 Job Lifecycle

**Test Complete Flow**: Create → Submit → Track → Complete

```python
def test_job_lifecycle_success():
    # Setup
    cycle_id = create_test_cycle(schema='test')
    config_id = create_test_configuration(cycle_id, schema='test')
    batch_id = create_test_batch(config_id, schema='test')

    # Create job
    job_id = create_job(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data={'portfolio_id': 'PORT001'},
        schema='test'
    )

    # Submit
    submit_job(job_id, schema='test')
    job = read_job(job_id, schema='test')
    assert job['status'] == JobStatus.SUBMITTED

    # Track through statuses
    for expected_status in [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.FINISHED]:
        with patch_moody_api_status(expected_status):
            track_job_status(job_id, schema='test')
            job = read_job(job_id, schema='test')
            assert job['status'] == expected_status

    # Verify tracking logs
    logs = get_job_tracking_logs(job_id, schema='test')
    assert len(logs) >= 3
```

### 4.2 Batch Lifecycle

**Test Complete Flow**: Create → Submit → Track → Reconcile

```python
def test_batch_lifecycle_with_failures():
    # Create batch with 5 jobs
    batch_id = create_batch(...)

    # Submit
    submit_batch(batch_id, schema='test')

    # Simulate 2 failures
    jobs = get_batch_jobs(batch_id, schema='test')
    update_job_status(jobs[0]['id'], JobStatus.FINISHED, schema='test')
    update_job_status(jobs[1]['id'], JobStatus.FAILED, schema='test')
    update_job_status(jobs[2]['id'], JobStatus.FINISHED, schema='test')
    update_job_status(jobs[3]['id'], JobStatus.FAILED, schema='test')
    update_job_status(jobs[4]['id'], JobStatus.FINISHED, schema='test')

    # Reconcile
    status = recon_batch(batch_id, schema='test')
    assert status == BatchStatus.FAILED

    # Verify recon log
    logs = get_recon_logs(batch_id, schema='test')
    assert len(logs) == 1
    assert logs[0]['recon_summary']['failed_job_ids'] == [jobs[1]['id'], jobs[3]['id']]
```

### 4.3 External Integrations

**Moody's API** (when implemented):
```python
@pytest.mark.integration
@pytest.mark.moody_api
def test_moody_api_submit_job():
    job_config = {
        'portfolio_id': 'PORT001',
        'analysis_date': '2025-10-17',
        'parameters': {'risk_model': 'model_v3'}
    }

    workflow_id, request, response = _submit_job(job_id=123, job_config=job_config)

    assert workflow_id is not None
    assert workflow_id.startswith('MW-')
    assert response['status'] in ['ACCEPTED', 'QUEUED']
```

**SQL Server**:
```python
@pytest.mark.integration
@pytest.mark.sql_server
def test_extract_portfolio_data():
    query = """
        SELECT portfolio_id, portfolio_name, risk_rating
        FROM dbo.Portfolios
        WHERE analysis_date = ?
    """

    df = execute_source_query(query, ('2025-10-17',))

    assert len(df) > 0
    assert 'portfolio_id' in df.columns
```

---

## 5. End-to-End Testing

### 5.1 Complete Cycle Workflow

**Test Scenario**:
```
1. Create cycle
2. Upload/validate configuration
3. Execute Stage 01: Setup
4. Execute Stage 02: Extract (from SQL Server)
5. Execute Stage 03: Process (Moody's batch)
6. Execute Stage 04: Validate results
7. Archive cycle
```

**Validation**:
```python
def validate_cycle_completion(cycle_id, schema='public'):
    # Verify cycle archived
    cycle = read_cycle(cycle_id, schema=schema)
    assert cycle['status'] == CycleStatus.ARCHIVED

    # Verify all steps completed
    steps = get_cycle_steps(cycle_id, schema=schema)
    for step in steps:
        step_runs = get_step_runs(step['id'], schema=schema)
        assert any(sr['status'] == StepStatus.COMPLETED for sr in step_runs)

    # Verify all batches completed
    batches = get_cycle_batches(cycle_id, schema=schema)
    for batch in batches:
        assert batch['status'] == BatchStatus.COMPLETED

    # Verify all jobs finished
    for batch in batches:
        jobs = get_batch_jobs(batch['id'], schema=schema)
        for job in jobs:
            assert job['status'] == JobStatus.FINISHED or job['skipped'] is True
```

### 5.2 Failure Recovery Workflow

**Test Scenario**:
```
1. Create batch with 5 jobs
2. Simulate 2 job failures
3. Reconcile (status = FAILED)
4. Resubmit failed jobs
5. Track to completion
6. Reconcile (status = COMPLETED)
```

---

## 6. Test Data Management

### 6.1 Test Database

```python
@pytest.fixture(scope='function')
def test_db():
    """Setup and teardown test database"""
    # Setup
    execute_sql_file('workspace/helpers/db/init_database.sql', schema='test')

    yield

    # Teardown
    execute_command("DROP SCHEMA test CASCADE", schema='postgres')
```

### 6.2 Test Configuration Files

**Location**: `test_data/`
- `valid_config.xlsx`: Valid configuration
- `invalid_config_missing_tab.xlsx`: Missing tab
- `invalid_config_missing_column.xlsx`: Missing column
- `portfolio_config.xlsx`: Portfolio analysis
- `scenario_config.xlsx`: Scenario analysis

---

## 7. Test Execution

### 7.1 Test Organization

```
tests/
├── unit/
│   ├── test_database.py
│   ├── test_job.py
│   ├── test_batch.py
│   ├── test_configuration.py
│   └── test_cycle.py
├── integration/
│   ├── test_job_lifecycle.py
│   ├── test_batch_lifecycle.py
│   └── test_external_apis.py
├── e2e/
│   ├── test_complete_cycle.py
│   └── test_failure_recovery.py
├── conftest.py
└── test_data/
```

### 7.2 Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit

# Integration tests (requires database)
./test.sh

# Specific test
pytest tests/unit/test_job.py::test_create_job_with_new_configuration

# With coverage
pytest --cov=workspace/helpers --cov-report=term-missing
```

---

## 8. Key Improvements Needed

### 8.1 Critical Error Handling Tests

**Priority 1**: Add tests for error scenarios identified in design document

1. **Job Submission Failure**:
```python
def test_submit_job_api_failure():
    """Test that job status = ERROR when API fails"""
    with patch('helpers.job._submit_job') as mock:
        mock.side_effect = Exception("API timeout")
        with pytest.raises(JobError):
            submit_job(job_id, schema='test')

        job = read_job(job_id, schema='test')
        assert job['status'] == JobStatus.ERROR
        assert 'API timeout' in job['last_error']
```

2. **Resubmit Submission Failure**:
```python
def test_resubmit_job_submission_failure():
    """Test resubmit handles submission failure"""
    original_job_id = create_failed_job(schema='test')

    with patch('helpers.job.submit_job') as mock:
        mock.side_effect = JobError("Submission failed")

        with pytest.raises(JobError):
            resubmit_job(original_job_id, schema='test')

        # Verify original job still skipped
        original = read_job(original_job_id, schema='test')
        assert original['skipped'] is True
```

3. **Batch Recon Edge Cases**:
```python
def test_recon_batch_mixed_states():
    """Test batch recon with complex job state mix"""
    batch_id = create_test_batch_with_jobs(
        job_statuses=[
            JobStatus.FINISHED,
            JobStatus.FAILED,
            JobStatus.ERROR,
            JobStatus.RUNNING,
            JobStatus.CANCELLED
        ],
        schema='test'
    )

    # ERROR takes priority
    status = recon_batch(batch_id, schema='test')
    assert status == BatchStatus.ERROR
```

### 8.2 Performance Tests

Add tests for high-load scenarios:

```python
def test_batch_creation_1000_jobs():
    """Test batch creation with 1000 job configs"""
    start = time.time()
    batch_id = create_batch_with_n_jobs(num_jobs=1000, schema='test')
    duration = time.time() - start

    assert duration < 10.0  # Should complete in <10s
    jobs = get_batch_jobs(batch_id, schema='test')
    assert len(jobs) == 1000
```

### 8.3 Idempotency Tests

Ensure operations can be safely repeated:

```python
def test_batch_reconciliation_idempotent():
    """Test that recon can be run multiple times"""
    batch_id = create_completed_batch(schema='test')

    status1 = recon_batch(batch_id, schema='test')
    status2 = recon_batch(batch_id, schema='test')

    assert status1 == status2 == BatchStatus.COMPLETED

    logs = get_recon_logs(batch_id, schema='test')
    assert len(logs) == 2
```

### 8.4 Audit Trail Validation

Verify complete audit trail for compliance:

```python
def test_job_audit_trail_complete():
    """Test job has complete audit trail"""
    job_id = create_and_complete_job(schema='test')

    job = read_job(job_id, schema='test')
    assert job['created_ts'] is not None
    assert job['submitted_ts'] is not None
    assert job['completed_ts'] is not None
    assert job['submitted_ts'] > job['created_ts']
    assert job['completed_ts'] > job['submitted_ts']

    logs = get_job_tracking_logs(job_id, schema='test')
    assert len(logs) > 0
```

---

## 9. CI/CD Integration

### 9.1 GitHub Actions

```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: test_password

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov pytest-mock

      - name: Run unit tests
        run: pytest tests/unit --cov=workspace/helpers

      - name: Run integration tests
        run: pytest tests/integration

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

---

## Summary

This testing strategy provides:

1. **Three-tier approach**: Unit (60-75%), Integration (20-30%), E2E (5-10%)
2. **Comprehensive coverage**: All helpers, workflows, database, integrations
3. **Critical scenarios**: Job lifecycle, batch reconciliation, error handling
4. **Test automation**: pytest with CI/CD integration
5. **Quality metrics**: Code coverage >80% target

**Priority Testing Areas**:
1. ✅ Job submission error handling (critical bug fix needed)
2. ✅ Job resubmission error handling (critical bug fix needed)
3. ✅ Batch reconciliation logic (bug fix needed)
4. ⚠️ Configuration validation
5. ⚠️ Performance under load
