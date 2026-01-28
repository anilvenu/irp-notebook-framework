# IRP Integration Guide

## Overview

The IRP Integration module (`workspace/helpers/irp_integration/`) provides a Python client library for integrating with Moody's Risk Modeler platform. It abstracts the complexity of the Risk Modeler API through a clean, domain-specific interface organized around insurance risk analysis workflows.

### Key Features

- **Domain-Specific Managers**: Separate managers for EDM, Portfolio, Analysis, Treaty, and RDM operations
- **Automatic Polling**: Built-in support for polling asynchronous workflows to completion
- **Robust Error Handling**: Structured exception hierarchy for different failure scenarios
- **Input Validation**: Pre-request validation to catch errors early
- **Batch Operations**: Submit and track multiple jobs in parallel
- **Pagination Support**: Automatic pagination for large result sets

### Architecture

The module follows a **facade pattern** where `IRPClient` provides unified access to specialized manager classes:

```
IRPClient (main entry point)
├── EDMManager - Exposure Data Management
├── PortfolioManager - Portfolio operations and geocoding
├── AnalysisManager - Analysis submission and grouping
├── TreatyManager - Treaty creation and LOB assignment
├── ReferenceDataManager - Lookup tables and metadata
├── MRIImportManager - Data Imports
├── RDMManager - Analysis / Grouping results exports
└── JobManager - Job tracking and polling
```

### API Endpoints and External Documentation

#### Constants Module

All API endpoints used by the integration module are defined in [workspace/helpers/irp_integration/constants.py](../workspace/helpers/irp_integration/constants.py). This module contains:

- **Workflow/Job Endpoints**: Workflow submission and job tracking endpoints
- **EDM/Datasource Endpoints**: Exposure data management and database server operations
- **Portfolio Endpoints**: Portfolio creation and geocoding endpoints
- **Analysis Endpoints**: Analysis job submission, results retrieval, and grouping
- **RDM Export Endpoints**: Risk Data Model export and database operations
- **Reference Data Endpoints**: Model profiles, currencies, simulation sets
- **Status Constants**: Workflow status codes and perspective codes (GR, GU, RL)
- **Treaty Constants**: Treaty types, attachment bases, and attachment levels

The constants module provides a centralized, maintainable reference for all API endpoints and ensures consistency across the integration layer.

#### Moody's API Documentation

For detailed information about the underlying APIs, refer to the official Moody's RMS developer documentation:

- **Platform APIs** (Recommended): [https://developer.rms.com/platform/reference/risk-data-api-overview](https://developer.rms.com/platform/reference/risk-data-api-overview)
  - Modern REST API set that replaces legacy Risk Modeler APIs
  - Covers exposure data management, portfolios, analyses, and RDM operations
  - Includes endpoint specifications, request/response schemas, and authentication details
  - **Moody's strongly recommends using Platform APIs wherever possible**

- **Risk Modeler APIs** (Legacy): [https://developer.rms.com/risk-modeler/reference/risk-modeler-api](https://developer.rms.com/risk-modeler/reference/risk-modeler-api)
  - Older API set, gradually being replaced by Platform APIs
  - Still required for some operations not yet available in Platform APIs

This integration module prioritizes Platform APIs and only falls back to Risk Modeler APIs when necessary. These resources are essential for understanding the full capabilities of the platform and troubleshooting advanced integration scenarios.

## Setup and Configuration

### Environment Variables

The integration module requires the following environment variables:

```bash
# Required
export RISK_MODELER_BASE_URL="https://api-euw1.rms-ppe.com"  # API endpoint
export RISK_MODELER_API_KEY="your-api-key-here"              # Authentication key
export RISK_MODELER_RESOURCE_GROUP_ID="your-resource-group"  # Resource scoping

# Optional
export DATABRIDGE_GROUP_ID="group-id"  # For RDM access control
export DB_SCHEMA="public"              # Database schema for operations
```

### Basic Usage

```python
from helpers.irp_integration import IRPClient

# Initialize client (reads from environment variables)
irp_client = IRPClient()

# Access manager modules
irp_client.edm.search_edms()
irp_client.portfolio.create_portfolio(...)
irp_client.analysis.submit_portfolio_analysis_job(...)
```

## Common Workflows

### 1. Creating an Exposure Data Management (EDM) Instance

EDMs are the top-level containers for exposure data in Risk Modeler.

```python
from helpers.irp_integration import IRPClient

irp_client = IRPClient()

# Define EDM parameters
edm_name = "MyInsuranceBook-2025"
server_name = "RMSServer01"

# Submit asynchronous job to create EDM
job_id, request_body = irp_client.edm.submit_create_edm_job(
    edm_name=edm_name,
    server_name=server_name
)

print(f"EDM creation job submitted: {job_id}")

# Poll job to completion (blocks until finished/failed)
final_status = irp_client.job.poll_risk_data_job_to_completion(
    job_id=job_id,
    interval=10,      # Poll every 10 seconds
    timeout=600000    # Timeout after 10 minutes
)

if final_status == 'FINISHED':
    print(f"EDM '{edm_name}' created successfully")
else:
    print(f"EDM creation failed with status: {final_status}")
```

**Batch Creation:**

```python
# Create multiple EDMs in parallel
edm_data_list = [
    {"edm_name": "Book1-2025", "server_name": "RMSServer01"},
    {"edm_name": "Book2-2025", "server_name": "RMSServer02"},
    {"edm_name": "Book3-2025", "server_name": "RMSServer01"},
]

# Submit all jobs
job_ids = irp_client.edm.submit_create_edm_jobs(edm_data_list)

# Poll all jobs to completion
results = irp_client.job.poll_risk_data_job_batch_to_completion(
    job_ids=job_ids,
    interval=10,
    timeout=600000
)

# Check results
for job in results:
    print(f"Job {job['id']}: {job['status']}")
```

### 2. Creating a Portfolio and Running Geocoding

Portfolios contain exposure data and require geocoding before analysis.

```python
# Create portfolio within an EDM
edm_name = "MyInsuranceBook-2025"
portfolio_name = "Commercial Property Portfolio"
portfolio_number = "CPP-001"
description = "Q1 2025 commercial property exposures"

portfolio_id = irp_client.portfolio.create_portfolio(
    edm_name=edm_name,
    portfolio_name=portfolio_name,
    portfolio_number=portfolio_number,
    description=description
)

print(f"Portfolio created with ID: {portfolio_id}")

# Submit geohaz job (geocoding + hazard assignment)
job_id = irp_client.portfolio.submit_geohaz_job(
    edm_name=edm_name,
    portfolio_id=portfolio_id
)

# Poll geohaz job to completion
final_status = irp_client.portfolio.poll_geohaz_job_to_completion(
    job_id=job_id,
    interval=30,       # Poll every 30 seconds
    timeout=1800000    # 30 minute timeout
)

if final_status == 'FINISHED':
    print("Geocoding completed successfully")
```

### 3. Submitting a Portfolio Analysis

Submit DLM (ELT) or HD (PLT) analyses on portfolios.

```python
# Define analysis parameters
analysis_name = "CPP-Q1-2025-EQ-CA"
portfolio_name = "Commercial Property Portfolio"
edm_name = "MyInsuranceBook-2025"

# Get reference data IDs (model profile, output profile, etc.)
model_profiles = irp_client.reference_data.get_model_profiles()
model_profile = irp_client.reference_data.find_reference_data_by_name(
    model_profiles,
    "US Earthquake HD 2024"
)

output_profiles = irp_client.reference_data.get_output_profiles()
output_profile = irp_client.reference_data.find_reference_data_by_name(
    output_profiles,
    "Standard Output Profile"
)

# Submit analysis
result = irp_client.analysis.submit_portfolio_analysis_job(
    analysis_name=analysis_name,
    portfolio_name=portfolio_name,
    edm_name=edm_name,
    model_profile_id=model_profile['id'],
    output_profile_id=output_profile['id'],
    perspective_code="GR",  # Gross perspective
    create_tag=True,        # Auto-create tag if doesn't exist
    tag_name="Q1-2025"
)

job_id = result['jobId']
analysis_id = result['analysisId']

# Poll to completion
final_status = irp_client.job.poll_risk_data_job_to_completion(job_id)

if final_status == 'FINISHED':
    print(f"Analysis {analysis_id} completed successfully")

    # Retrieve results
    elt = irp_client.analysis.get_analysis_elt(analysis_id)
    ep = irp_client.analysis.get_analysis_ep(analysis_id)
    stats = irp_client.analysis.get_analysis_stats(analysis_id)
```

### 4. Grouping Multiple Analyses

Combine multiple analyses into a single grouped analysis for portfolio-level views.

```python
# List of analysis IDs to group
analysis_ids = [12345, 12346, 12347, 12348]

# Build region/peril simulation set automatically
region_peril_set = irp_client.analysis.build_region_peril_simulation_set(
    analysis_ids=analysis_ids
)

# Submit grouping job
group_name = "CPP-Q1-2025-AllPerils"
analysis_names = [
    "CPP-Q1-2025-EQ-CA",
    "CPP-Q1-2025-HU-FL",
    "CPP-Q1-2025-WS-TX",
    "CPP-Q1-2025-TO-OK"
]

result = irp_client.analysis.submit_analysis_grouping_job(
    group_name=group_name,
    analysis_names=analysis_names,
    region_peril_simulation_set=region_peril_set,
    perspective_code="GR",
    create_tag=True,
    tag_name="Q1-2025"
)

# Poll to completion
job_id = result['jobId']
final_status = irp_client.job.poll_risk_data_job_to_completion(job_id)
```

### 5. Importing MRI (Multi-Risk Insurance) Data

End-to-end workflow for importing exposure data via AWS S3.

```python
import os

# Step 1: Get S3 bucket and credentials
bucket_info = irp_client.mri_import.create_aws_bucket(
    edm_name="MyInsuranceBook-2025"
)

bucket_name = bucket_info['bucketName']
credentials = bucket_info['credentials']  # Auto-decoded from base64

# Step 2: Upload CSV files
csv_files = [
    "/path/to/locations.csv",
    "/path/to/accounts.csv",
    "/path/to/policies.csv"
]

for csv_file in csv_files:
    with open(csv_file, 'rb') as f:
        file_name = os.path.basename(csv_file)
        irp_client.mri_import.upload_csv_to_s3(
            bucket_name=bucket_name,
            file_name=file_name,
            file_content=f.read(),
            credentials=credentials
        )

# Step 3: Upload mapping configuration
mapping = {
    "location": {"file": "locations.csv", "mapping": {...}},
    "account": {"file": "accounts.csv", "mapping": {...}},
    "policy": {"file": "policies.csv", "mapping": {...}}
}

irp_client.mri_import.upload_mapping_json(
    bucket_name=bucket_name,
    mapping=mapping,
    credentials=credentials
)

# Step 4: Submit import job
job_id = irp_client.mri_import.submit_mri_import_job(
    edm_name="MyInsuranceBook-2025"
)

# Poll to completion
final_status = irp_client.job.poll_risk_data_job_to_completion(job_id)
```

### 6. Exporting Analyses to Risk Data Model (RDM)

Export analysis results to RDM for downstream reporting and analytics.

```python
# Export single analysis
analysis_id = 12345
rdm_name = "CPP-Q1-2025-Results"

result = irp_client.rdm.export_analysis_to_rdm(
    analysis_id=analysis_id,
    rdm_name=rdm_name,
    export_plt_loss=True,   # Include PLT loss data
    export_hd_loss=True     # Include HD loss data
)

job_id = result['jobId']

# Poll to completion
final_status = irp_client.job.poll_risk_data_job_to_completion(job_id)

if final_status == 'FINISHED':
    print(f"RDM '{rdm_name}' created successfully")
```

**Exporting Analysis Groups:**

```python
# Export grouped analysis
group_id = 67890
rdm_name = "CPP-Q1-2025-Portfolio"

result = irp_client.rdm.export_analysis_group_to_rdm(
    analysis_group_id=group_id,
    rdm_name=rdm_name,
    export_plt_loss=True,
    export_hd_loss=True
)
```

## Best Practices

### 1. Error Handling

Always wrap API calls in try-except blocks to handle different error types:

```python
from helpers.irp_integration import IRPClient
from helpers.irp_integration.exceptions import (
    IRPAPIError,
    IRPValidationError,
    IRPWorkflowError,
    IRPReferenceDataError
)

irp_client = IRPClient()

try:
    portfolio_id = irp_client.portfolio.create_portfolio(
        edm_name=edm_name,
        portfolio_name=portfolio_name,
        portfolio_number=portfolio_number
    )
except IRPValidationError as e:
    print(f"Invalid input: {e}")
except IRPAPIError as e:
    print(f"API error: {e}")
except IRPWorkflowError as e:
    print(f"Workflow failed: {e}")
```

### 2. Timeout Configuration

Adjust polling timeouts based on expected operation duration:

```python
# Short timeout for fast operations (geocoding small portfolios)
irp_client.portfolio.poll_geohaz_job_to_completion(
    job_id=job_id,
    interval=10,
    timeout=300000  # 5 minutes
)

# Long timeout for complex operations (large analysis grouping)
irp_client.job.poll_risk_data_job_to_completion(
    job_id=job_id,
    interval=30,
    timeout=3600000  # 1 hour
)
```

### 3. Batch Operations

Use batch operations for multiple similar tasks to improve efficiency:

```python
# Create multiple EDMs in parallel
edm_list = [{"edm_name": f"Book{i}", "server_name": server}
            for i in range(1, 11)]

job_ids = irp_client.edm.submit_create_edm_jobs(edm_list)
results = irp_client.job.poll_risk_data_job_batch_to_completion(job_ids)
```

### 4. Reference Data Caching

Cache reference data lookups to avoid repeated API calls:

```python
# Load reference data once
model_profiles = irp_client.reference_data.get_model_profiles()
output_profiles = irp_client.reference_data.get_output_profiles()
currencies = irp_client.reference_data.get_currencies()

# Reuse for multiple operations
for analysis_config in analysis_configs:
    model_profile = irp_client.reference_data.find_reference_data_by_name(
        model_profiles,
        analysis_config['model_profile_name']
    )
    # Submit analysis...
```

### 5. Pagination for Large Result Sets

Use paginated methods when dealing with large datasets:

```python
# Automatic pagination (fetches all pages)
all_edms = irp_client.edm.search_edms_paginated()

# Manual pagination (control page size and offset)
page_1 = irp_client.edm.search_edms(limit=100, offset=0)
page_2 = irp_client.edm.search_edms(limit=100, offset=100)
```

## Troubleshooting

### Common Issues

#### Issue: `IRPAPIError: 401 Unauthorized`
**Cause**: Invalid or missing API key
**Solution**: Verify `RISK_MODELER_API_KEY` environment variable is set correctly

#### Issue: `IRPWorkflowError: Job timed out`
**Cause**: Operation exceeded timeout period
**Solution**: Increase timeout parameter or check job status manually

#### Issue: `IRPReferenceDataError: Reference data not found`
**Cause**: Model profile, output profile, or other reference data doesn't exist
**Solution**: Use exact names from Risk Modeler UI, or list available options:

```python
profiles = irp_client.reference_data.get_model_profiles()
for p in profiles:
    print(f"{p['name']} (ID: {p['id']})")
```

#### Issue: `IRPValidationError: EDM name already exists`
**Cause**: Attempting to create duplicate EDM
**Solution**: Check existing EDMs first:

```python
existing = irp_client.edm.search_edms(filter=f'exposureName="{edm_name}"')
if existing:
    print(f"EDM '{edm_name}' already exists")
```

#### Issue: Jobs stuck in PENDING/QUEUED
**Cause**: Resource contention or API service issues
**Solution**: Check job status manually in Risk Modeler UI, or contact support

### Debug Logging

Enable detailed HTTP logging for troubleshooting:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('helpers.irp_integration')
logger.setLevel(logging.DEBUG)
```

## Integration with IRP Notebook Framework

The integration module is designed to work seamlessly with the batch/job system:

```python
from helpers import batch, job
from helpers.irp_integration import IRPClient

# Create batch in framework
batch_id = batch.create_batch(
    batch_type='portfolio_analysis',
    configuration_id=config_id,
    step_id=step_id
)

# Submit jobs via IRP Integration
irp_client = IRPClient()
for job_record in batch.get_batch_jobs(batch_id):
    config_data = job_record['job_configuration_data']

    result = irp_client.analysis.submit_portfolio_analysis_job(
        analysis_name=config_data['analysis_name'],
        portfolio_name=config_data['portfolio_name'],
        edm_name=config_data['edm_name'],
        model_profile_id=config_data['model_profile_id'],
        output_profile_id=config_data['output_profile_id']
    )

    # Update job with external job ID
    job.update_job_status(
        job_id=job_record['id'],
        status='SUBMITTED',
        external_job_id=result['jobId']
    )
```

## Next Steps

- Review [IRP_API_REFERENCE.md](IRP_API_REFERENCE.md) for detailed method documentation
- See [BATCH_JOB_SYSTEM.md](BATCH_JOB_SYSTEM.md) for batch processing patterns
- Check [CONFIGURATION_TRANSFORMERS.md](CONFIGURATION_TRANSFORMERS.md) for Excel-driven workflows

## Related Documentation

- [Design Document](DESIGN_DOCUMENT.md) - Overall system architecture
- [Batch & Job System](BATCH_JOB_SYSTEM.md) - Framework batch processing
- [Configuration Transformers](CONFIGURATION_TRANSFORMERS.md) - Excel configuration system
- [Testing Strategy](TESTING_STRATEGY.md) - Test patterns and quality gates
