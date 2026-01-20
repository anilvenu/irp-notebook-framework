# IRP Notebook Framework - Testing Strategy

The application workspace comprises two main parts:

1. **Workflows** - Jupyter Notebooks for risk analysts to perform core analysis cycle activities, plus tool notebooks for administrative functions
2. **Helpers** - Python and SQL code supporting the core workflow and tool notebooks

### Testing Approach

- **Workflow and Tools Testing**:
  - Tested by the project team during development
  - Delivered to the risk modeling team for user acceptance testing (UAT)

- **Helper Testing**:
  - Tested as part of workflow testing
  - Tested using automated test scripts that validate logic and database records for key scenarios:
    - Cycle creation
    - Stage recording
    - Step recording
    - Step run logging
    - Batch creation
    - Configuration parsing
    - Configuration validation
    - Job creation
    - Batch reconciliation
    - Job resubmission

⚠ Job submission and polling requires Moody's integration and these would be tested manually
```pytest``` framework will be used to develop and run automated tests for helpers.


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

- All ```pytest``` unit tests must pass before code merge
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
         │  E2E Tests   │  5-10%  Manual + Automated
         └──────────────┘
       ┌───────────────────┐
       │ Integration Tests │  20-30% Manual (Moody's and Notebook) + Automated (Metadata)
       └───────────────────┘
     ┌────────────────────────┐
     │     Unit Tests         │  60-75% (Automated)
     └────────────────────────┘
```

| Category | Purpose | Tools | Frequency |
|----------|---------|-------|-----------|
| Unit Tests | Test individual functions | pytest | Every commit |
| Integration Tests | Test component interactions | pytest + PostgreSQL | Before merge |
| E2E Tests | Test complete workflows | Notebooks + validation | Before release |
| UAT | User acceptance testing | Manual execution | Major releases |

---

## Component Testing

### Database Module (`database.py`)
- Connection and query execution
- Transaction management
- Error handling
- SQL injection prevention

### Job Module (`job.py`)
- Job creation (with new/existing configuration)
- Job submission (success, failure, already submitted)
- Job tracking (status transitions)
- Job resubmission (with/without override)

### Batch Module (`batch.py`)
- Batch creation (with transformer)
- Batch submission (all jobs)
- Batch reconciliation (all status paths)

### Configuration Module (`configuration.py`)
- Configuration upload (valid/invalid Excel)
- Configuration validation (tabs, columns, data types)
- Configuration transformation (job config generation)

### Workflow Notebooks
- Manual execution during development
- Automated execution via `papermill` or `nbconvert`
- Output validation via step_run logs

---

## Integration Testing

### Job Lifecycle

The creation of job and pre-resubmit activities will be tested with automation.
Submission and post submission activities will be tested manually.

### Batch Lifecycle

Creation will be tested with automation.
Submission and tracking will be tested manually.
Recon of jobs will be tested with automation by setting up job entries on database.

### External Integrations

**Moody's**

An end-to-end flow cycle with test data will be tested in integration with Moody's, using an all encompassing Notebook.

**SQL Server**

These would be tested manually from Jupyter Notebooks using SQL Scripts provided by the modeling team.