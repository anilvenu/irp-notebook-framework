# IRP Integration API Reference

Complete API reference for the IRP Integration module (`workspace/helpers/irp_integration/`).

## Table of Contents

- [IRPClient (Main Entry Point)](#irpclient-main-entry-point)
- [EDMManager](#edmmanager)
- [PortfolioManager](#portfoliomanager)
- [AnalysisManager](#analysismanager)
- [ReferenceDataManager](#referencedatamanager)
- [TreatyManager](#treatymanager)
- [MRIImportManager](#mriimportmanager)
- [RDMManager](#rdmmanager)
- [JobManager](#jobmanager)
- [Exception Classes](#exception-classes)

---

## IRPClient (Main Entry Point)

The main facade class for accessing all IRP Integration functionality.

### Initialization

```python
from helpers.irp_integration import IRPClient

irp_client = IRPClient()
```

Reads configuration from environment variables:
- `RISK_MODELER_BASE_URL` - API base URL (default: https://api-euw1.rms-ppe.com)
- `RISK_MODELER_API_KEY` - Authentication key
- `RISK_MODELER_RESOURCE_GROUP_ID` - Resource group ID

### Manager Access

```python
irp_client.edm              # EDMManager
irp_client.portfolio        # PortfolioManager
irp_client.analysis         # AnalysisManager
irp_client.reference_data   # ReferenceDataManager
irp_client.treaty           # TreatyManager
irp_client.mri_import       # MRIImportManager
irp_client.rdm              # RDMManager
irp_client.job              # JobManager
```

---

## EDMManager

Exposure Data Management operations.

### search_edms

Search for EDMs with optional filtering and pagination.

```python
search_edms(
    filter: str = "",
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

**Parameters:**
- `filter` - OData filter string (e.g., `exposureName="MyEDM"`)
- `limit` - Maximum results per page (default: 100)
- `offset` - Pagination offset (default: 0)

**Returns:** List of EDM dictionaries with fields:
- `exposureId` - EDM ID
- `exposureName` - EDM name
- `exposureSetId` - Exposure set ID
- `databaseName` - Full database name
- `serverId` - Database server ID

**Example:**
```python
edms = irp_client.edm.search_edms(filter='exposureName="Book-2025-Q1"')
```

### search_edms_paginated

Fetch all EDMs across all pages automatically.

```python
search_edms_paginated(
    filter: str = ""
) -> List[Dict[str, Any]]
```

**Parameters:**
- `filter` - OData filter string

**Returns:** Complete list of all matching EDMs

### submit_create_edm_job

Submit job to create a new EDM.

```python
submit_create_edm_job(
    edm_name: str,
    server_name: str = "databridge-1"
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `edm_name` - Name for new EDM
- `server_name` - Database server name (default: "databridge-1")

**Returns:** Tuple of `(job_id, request_body)`

**Raises:**
- `IRPValidationError` - If parameters invalid
- `IRPAPIError` - If EDM creation fails

**Example:**
```python
job_id, request = irp_client.edm.submit_create_edm_job(
    edm_name="MyBook-2025",
    server_name="databridge-1"
)
```

### submit_create_edm_jobs

Submit multiple EDM creation jobs in batch.

```python
submit_create_edm_jobs(
    edm_data_list: List[Dict[str, Any]]
) -> List[int]
```

**Parameters:**
- `edm_data_list` - List of dicts, each containing:
  - `edm_name` (str)
  - `server_name` (str)

**Returns:** List of job IDs

### submit_upgrade_edm_data_version_job

Upgrade EDM data version.

```python
submit_upgrade_edm_data_version_job(
    edm_name: str,
    edm_version: str
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `edm_name` - EDM name to upgrade
- `edm_version` - Target version (e.g., "22")

**Returns:** Tuple of `(job_id, request_body)`

### delete_edm

Delete an EDM and all associated analyses.

```python
delete_edm(edm_name: str) -> Dict[str, Any]
```

**Parameters:**
- `edm_name` - Name of EDM to delete

**Returns:** Final job status dict

**Warning:** This deletes ALL analyses associated with the EDM!

### get_cedants_by_edm

Retrieve cedants for an EDM.

```python
get_cedants_by_edm(exposure_id: int) -> List[Dict[str, Any]]
```

**Parameters:**
- `exposure_id` - Exposure ID

**Returns:** List of cedant data

### get_lobs_by_edm

Retrieve lines of business (LOBs) for an EDM.

```python
get_lobs_by_edm(exposure_id: int) -> List[Dict[str, Any]]
```

**Parameters:**
- `exposure_id` - Exposure ID

**Returns:** List of LOB data

---

## PortfolioManager

Portfolio operations including creation, geocoding, and mapping.

### create_portfolio

Create a new portfolio within an EDM.

```python
create_portfolio(
    edm_name: str,
    portfolio_name: str,
    portfolio_number: str = "1",
    description: str = ""
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `edm_name` - EDM containing the portfolio
- `portfolio_name` - Name for new portfolio
- `portfolio_number` - Portfolio number (default: "1", max 20 chars)
- `description` - Optional description

**Returns:** Tuple of `(portfolio_id, request_body)`

**Example:**
```python
portfolio_id, request = irp_client.portfolio.create_portfolio(
    edm_name="MyBook-2025",
    portfolio_name="Commercial Property",
    portfolio_number="CPP-001",
    description="Q1 2025 commercial exposures"
)
```

### create_portfolios

Create multiple portfolios in batch.

```python
create_portfolios(
    portfolio_data_list: List[Dict[str, Any]]
) -> List[int]
```

**Parameters:**
- `portfolio_data_list` - List of dicts, each containing:
  - `edm_name` (str)
  - `portfolio_name` (str)
  - `portfolio_number` (str)
  - `description` (str)

**Returns:** List of portfolio IDs

### search_portfolios

Search portfolios within an EDM.

```python
search_portfolios(
    exposure_id: int,
    filter: str = "",
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

**Parameters:**
- `exposure_id` - EDM ID to search within
- `filter` - OData filter string
- `limit` - Max results per page
- `offset` - Pagination offset

**Returns:** List of portfolio dictionaries

### search_portfolios_paginated

Fetch all portfolios across all pages.

```python
search_portfolios_paginated(
    exposure_id: int,
    filter: str = ""
) -> List[Dict[str, Any]]
```

### submit_geohaz_job

Submit geocoding and hazard assignment job for a portfolio.

```python
submit_geohaz_job(
    portfolio_name: str,
    edm_name: str,
    version: str = "22.0",
    hazard_eq: bool = False,
    hazard_ws: bool = False,
    geocode_layer_options: Optional[Dict[str, Any]] = None,
    hazard_layer_options: Optional[Dict[str, Any]] = None
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `portfolio_name` - Name of portfolio to geocode
- `edm_name` - EDM containing portfolio
- `version` - Geocode version (default: "22.0")
- `hazard_eq` - Enable earthquake hazard (default: False)
- `hazard_ws` - Enable windstorm hazard (default: False)
- `geocode_layer_options` - Custom geocode options (optional)
- `hazard_layer_options` - Custom hazard options (optional)

**Returns:** Tuple of `(job_id, request_body)`

**Example:**
```python
job_id, request = irp_client.portfolio.submit_geohaz_job(
    portfolio_name="Commercial Property",
    edm_name="MyBook-2025",
    version="22.0",
    hazard_eq=True,
    hazard_ws=True
)
```

### poll_geohaz_job_to_completion

Poll geohaz job until completion.

```python
poll_geohaz_job_to_completion(
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

**Parameters:**
- `job_id` - Job ID to poll
- `interval` - Polling interval in seconds (default: 10)
- `timeout` - Timeout in seconds (default: 600000)

**Returns:** Final job status dict

### execute_portfolio_mapping

Execute SQL script to create sub-portfolios.

```python
execute_portfolio_mapping(
    portfolio_name: str,
    edm_name: str,
    import_file: str,
    cycle_type: str,
    connection_name: str = 'DATABRIDGE'
) -> Dict[str, Any]
```

**Parameters:**
- `portfolio_name` - Portfolio to map
- `edm_name` - EDM containing portfolio
- `import_file` - Import file identifier (used to locate SQL script)
- `cycle_type` - Cycle type (e.g., 'Quarterly', 'Annual') - determines SQL directory
- `connection_name` - SQL Server connection name (default: 'DATABRIDGE')

**Returns:** Dict containing:
- `status` - 'FINISHED' or 'SKIPPED'
- `message` - Result description
- `result_sets_count` - Number of result sets (if executed)
- `sql_script` - Script details
- `parameters` - SQL parameters used (if executed)

---

## AnalysisManager

Analysis submission, grouping, and results retrieval.

### submit_portfolio_analysis_job

Submit a portfolio analysis job (DLM or HD).

```python
submit_portfolio_analysis_job(
    edm_name: str,
    portfolio_name: str,
    job_name: str,
    analysis_profile_name: str,
    output_profile_name: str,
    event_rate_scheme_name: str,
    treaty_names: List[str],
    tag_names: List[str],
    currency: Dict[str, str] = None,
    skip_duplicate_check: bool = False,
    franchise_deductible: bool = False,
    min_loss_threshold: float = 1.0,
    treat_construction_occupancy_as_unknown: bool = True,
    num_max_loss_event: int = 1
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `edm_name` - EDM name
- `portfolio_name` - Portfolio to analyze
- `job_name` - Unique analysis name
- `analysis_profile_name` - Model profile name
- `output_profile_name` - Output profile name
- `event_rate_scheme_name` - Event rate scheme (required for DLM, optional for HD)
- `treaty_names` - List of treaty names to apply
- `tag_names` - List of tag names (auto-created if missing)
- `currency` - Currency config (optional, uses system default if None)
- `skip_duplicate_check` - Skip duplicate name check (for batch operations)
- `franchise_deductible` - Apply franchise deductible (default: False)
- `min_loss_threshold` - Minimum loss threshold (default: 1.0)
- `treat_construction_occupancy_as_unknown` - (default: True)
- `num_max_loss_event` - Number of max loss events (default: 1)

**Returns:** Tuple of `(job_id, request_body)`

**Example:**
```python
job_id, request = irp_client.analysis.submit_portfolio_analysis_job(
    edm_name="MyBook-2025",
    portfolio_name="Commercial Property",
    job_name="CPP-Q1-2025-EQ-CA",
    analysis_profile_name="US Earthquake HD 2024",
    output_profile_name="Standard Output Profile",
    event_rate_scheme_name="",  # Optional for HD
    treaty_names=["Treaty1", "Treaty2"],
    tag_names=["Q1-2025"]
)
```

### submit_analysis_grouping_job

Submit analysis grouping job to combine multiple analyses.

```python
submit_analysis_grouping_job(
    group_name: str,
    analysis_names: List[str],
    simulate_to_plt: bool = False,
    num_simulations: int = 50000,
    propagate_detailed_losses: bool = False,
    reporting_window_start: str = "01/01/2021",
    simulation_window_start: str = "01/01/2021",
    simulation_window_end: str = "12/31/2021",
    region_peril_simulation_set: List[Dict[str, Any]] = None,
    description: str = "",
    currency: Dict[str, str] = None,
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> Dict[str, Any]
```

**Parameters:**
- `group_name` - Name for analysis group
- `analysis_names` - List of analysis/group names to combine
- `simulate_to_plt` - Simulate to PLT (default: False, auto-enabled if needed)
- `num_simulations` - Number of simulations (default: 50000)
- `propagate_detailed_losses` - Propagate detailed losses (default: False)
- `reporting_window_start` - Reporting start date (default: "01/01/2021")
- `simulation_window_start` - Simulation start date (default: "01/01/2021")
- `simulation_window_end` - Simulation end date (default: "12/31/2021")
- `region_peril_simulation_set` - Region/peril config (auto-built if None)
- `description` - Group description
- `currency` - Currency config (uses default if None)
- `analysis_edm_map` - Map of analysis name → EDM name (for disambiguation)
- `group_names` - Set of known group names (for lookup disambiguation)
- `skip_missing` - Skip missing analyses instead of raising error

**Returns:** Dict containing:
- `job_id` - Job ID (or None if skipped)
- `skipped` - True if all items were missing
- `skipped_items` - List of missing item names
- `included_items` - List of included item names
- `http_request_body` - Request payload (if submitted)

**Example:**
```python
result = irp_client.analysis.submit_analysis_grouping_job(
    group_name="CPP-Q1-2025-AllPerils",
    analysis_names=["CPP-Q1-2025-EQ", "CPP-Q1-2025-WS"],
    analysis_edm_map={
        "CPP-Q1-2025-EQ": "MyBook-2025",
        "CPP-Q1-2025-WS": "MyBook-2025"
    }
)
```

### build_region_peril_simulation_set

Build regionPerilSimulationSet from analysis IDs for grouping.

```python
build_region_peril_simulation_set(
    analysis_ids: List[int]
) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_ids` - List of analysis/group IDs

**Returns:** List of region/peril simulation set entries for grouping request

**Note:** This is called automatically by `submit_analysis_grouping_job` when `region_peril_simulation_set` is None.

### search_analyses

Search analyses with filtering.

```python
search_analyses(
    filter: str = "",
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

**Parameters:**
- `filter` - OData filter string
- `limit` - Max results per page
- `offset` - Pagination offset

**Returns:** List of analysis result dictionaries

### search_analyses_paginated

Fetch all analyses across all pages.

```python
search_analyses_paginated(
    filter: str = ""
) -> List[Dict[str, Any]]
```

### get_analysis_by_id

Retrieve analysis details by ID.

```python
get_analysis_by_id(analysis_id: int) -> Dict[str, Any]
```

**Parameters:**
- `analysis_id` - Analysis ID

**Returns:** Analysis details dict

### get_analysis_by_name

Get analysis by name and EDM.

```python
get_analysis_by_name(
    analysis_name: str,
    edm_name: str
) -> Dict[str, Any]
```

**Parameters:**
- `analysis_name` - Analysis name
- `edm_name` - EDM name (required for uniqueness)

**Returns:** Analysis details dict

### delete_analysis

Delete an analysis by ID.

```python
delete_analysis(analysis_id: int) -> None
```

**Parameters:**
- `analysis_id` - Analysis ID to delete

### get_elt

Retrieve Event Loss Table (ELT) for an analysis.

```python
get_elt(
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_id` - Analysis ID
- `perspective_code` - 'GR' (Gross), 'GU' (Ground-Up), or 'RL' (Reinsurance)
- `exposure_resource_id` - Portfolio ID
- `filter` - Optional filter (e.g., "eventId IN (1, 2, 3)")
- `limit` - Max records to return
- `offset` - Pagination offset

**Returns:** List of ELT records

### get_ep

Retrieve Exceedance Probability (EP) curves for an analysis.

```python
get_ep(
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_id` - Analysis ID
- `perspective_code` - 'GR', 'GU', or 'RL'
- `exposure_resource_id` - Portfolio ID

**Returns:** List of EP curve data (OEP, AEP, CEP, TCE)

### get_stats

Retrieve statistics for an analysis.

```python
get_stats(
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_id` - Analysis ID
- `perspective_code` - 'GR', 'GU', or 'RL'
- `exposure_resource_id` - Portfolio ID

**Returns:** List of statistical metrics

### get_plt

Retrieve Period Loss Table (PLT) for an HD analysis.

```python
get_plt(
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_id` - Analysis ID (must be HD/PLT analysis)
- `perspective_code` - 'GR', 'GU', or 'RL'
- `exposure_resource_id` - Portfolio ID
- `filter` - Optional filter
- `limit` - Max records (default: 100000)
- `offset` - Pagination offset

**Returns:** List of PLT records

**Note:** PLT is only available for HD (High Definition) analyses.

### get_regions

Retrieve region/peril breakdown for an analysis or group.

```python
get_regions(analysis_id: int) -> List[Dict[str, Any]]
```

**Parameters:**
- `analysis_id` - Analysis or group ID

**Returns:** List of region dicts containing framework, peril codes, and simulation identifiers

---

## ReferenceDataManager

Reference data lookup and management.

### get_model_profiles

Get all model profiles.

```python
get_model_profiles() -> Dict[str, Any]
```

**Returns:** Dict containing model profile list

### get_model_profile_by_name

Get model profile by name.

```python
get_model_profile_by_name(profile_name: str) -> Dict[str, Any]
```

**Parameters:**
- `profile_name` - Model profile name

**Returns:** Model profile details dict

### get_output_profiles

Get all output profiles.

```python
get_output_profiles() -> List[Dict[str, Any]]
```

**Returns:** List of output profile dicts

### get_output_profile_by_name

Get output profile by name.

```python
get_output_profile_by_name(profile_name: str) -> List[Dict[str, Any]]
```

**Parameters:**
- `profile_name` - Output profile name

**Returns:** List of output profile dicts

### get_event_rate_schemes

Get all active event rate schemes.

```python
get_event_rate_schemes() -> Dict[str, Any]
```

**Returns:** Dict containing event rate scheme list

### get_event_rate_scheme_by_name

Get event rate scheme by name with optional filtering.

```python
get_event_rate_scheme_by_name(
    scheme_name: str,
    peril_code: str = None,
    model_region_code: str = None
) -> Dict[str, Any]
```

**Parameters:**
- `scheme_name` - Event rate scheme name
- `peril_code` - Peril code filter (optional)
- `model_region_code` - Model region code filter (optional)

**Returns:** Event rate scheme details dict

**Note:** Use peril_code and model_region_code when multiple schemes have the same name.

### get_analysis_currency

Get currency dict for analysis requests.

```python
get_analysis_currency() -> Dict[str, str]
```

**Returns:** Currency dict with:
- `asOfDate` - Effective date
- `code` - Currency code (e.g., "USD")
- `scheme` - Currency scheme (e.g., "RMS")
- `vintage` - Vintage code (e.g., "RL25")

### get_tag_ids_from_tag_names

Get or create tags by names and return IDs.

```python
get_tag_ids_from_tag_names(tag_names: List[str]) -> List[int]
```

**Parameters:**
- `tag_names` - List of tag names

**Returns:** List of tag IDs (creates tags if they don't exist)

### get_simulation_set_by_event_rate_scheme_id

Get simulation set by event rate scheme ID (for ELT analyses).

```python
get_simulation_set_by_event_rate_scheme_id(
    event_rate_scheme_id: int
) -> Dict[str, Any]
```

**Parameters:**
- `event_rate_scheme_id` - Event rate scheme ID

**Returns:** Simulation set dict with `id` field being simulationSetId

### get_pet_metadata_by_id

Get PET metadata by PET ID (for PLT/HD analyses).

```python
get_pet_metadata_by_id(pet_id: int) -> Dict[str, Any]
```

**Parameters:**
- `pet_id` - PET ID

**Returns:** PET metadata dict

### get_model_version_by_engine_version

Get model version string for an engine version.

```python
get_model_version_by_engine_version(
    engine_version: str
) -> str
```

**Parameters:**
- `engine_version` - Engine version (e.g., "HDv2.0", "RL23")

**Returns:** Model version string (e.g., "2.0", "23.0")

---

## TreatyManager

Treaty creation and management.

### create_treaty

Create a new treaty.

```python
create_treaty(
    edm_name: str,
    treaty_name: str,
    treaty_type: str,
    currency_name: str,
    attachment_basis: str,
    attachment_level: str,
    limit: float,
    participation: float = 1.0,
    treaty_number: str = "1",
    description: str = ""
) -> Tuple[int, Dict[str, Any]]
```

**Parameters:**
- `edm_name` - EDM containing treaty
- `treaty_name` - Treaty name
- `treaty_type` - Treaty type (e.g., "QS", "XL")
- `currency_name` - Currency name
- `attachment_basis` - Attachment basis type
- `attachment_level` - Attachment level type
- `limit` - Treaty limit amount
- `participation` - Participation percentage (default: 1.0)
- `treaty_number` - Treaty number (default: "1")
- `description` - Optional description

**Returns:** Tuple of `(treaty_id, request_body)`

### search_treaties

Search treaties within an EDM.

```python
search_treaties(
    exposure_id: int,
    filter: str = "",
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]
```

**Parameters:**
- `exposure_id` - EDM ID
- `filter` - OData filter string
- `limit` - Max results per page
- `offset` - Pagination offset

**Returns:** List of treaty dicts

---

## MRIImportManager

Multi-Risk Insurance data import via AWS S3.

### create_aws_bucket

Create AWS S3 bucket and get credentials.

```python
create_aws_bucket(edm_name: str) -> Dict[str, Any]
```

**Parameters:**
- `edm_name` - EDM name

**Returns:** Dict containing:
- `bucketName` - S3 bucket name
- `credentials` - AWS credentials (auto-decoded from base64)

### upload_csv_to_s3

Upload CSV file to S3 bucket.

```python
upload_csv_to_s3(
    bucket_name: str,
    file_name: str,
    file_content: bytes,
    credentials: Dict[str, Any]
) -> None
```

**Parameters:**
- `bucket_name` - S3 bucket name
- `file_name` - CSV file name
- `file_content` - File content as bytes
- `credentials` - AWS credentials from create_aws_bucket

### upload_mapping_json

Upload mapping configuration to S3.

```python
upload_mapping_json(
    bucket_name: str,
    mapping: Dict[str, Any],
    credentials: Dict[str, Any]
) -> None
```

**Parameters:**
- `bucket_name` - S3 bucket name
- `mapping` - Mapping configuration dict
- `credentials` - AWS credentials

### submit_mri_import_job

Submit MRI import job after files are uploaded.

```python
submit_mri_import_job(edm_name: str) -> int
```

**Parameters:**
- `edm_name` - EDM name

**Returns:** Job ID

---

## RDMManager

Risk Data Model export operations.

### export_analysis_to_rdm

Export analysis to RDM database.

```python
export_analysis_to_rdm(
    analysis_id: int,
    rdm_name: str,
    export_plt_loss: bool = True,
    export_hd_loss: bool = True
) -> Dict[str, Any]
```

**Parameters:**
- `analysis_id` - Analysis ID to export
- `rdm_name` - RDM database name
- `export_plt_loss` - Include PLT loss data (default: True)
- `export_hd_loss` - Include HD loss data (default: True)

**Returns:** Dict containing:
- `jobId` - Export job ID
- `http_request_body` - Request payload

### export_analysis_group_to_rdm

Export analysis group to RDM database.

```python
export_analysis_group_to_rdm(
    analysis_group_id: int,
    rdm_name: str,
    export_plt_loss: bool = True,
    export_hd_loss: bool = True
) -> Dict[str, Any]
```

**Parameters:**
- `analysis_group_id` - Analysis group ID to export
- `rdm_name` - RDM database name
- `export_plt_loss` - Include PLT loss data (default: True)
- `export_hd_loss` - Include HD loss data (default: True)

**Returns:** Dict containing job ID and request body

### delete_rdm

Delete an RDM database.

```python
delete_rdm(rdm_name: str) -> None
```

**Parameters:**
- `rdm_name` - RDM database name to delete

---

## JobManager

Job tracking and polling operations.

### poll_risk_data_job_to_completion

Poll a risk data job until completion or timeout.

```python
poll_risk_data_job_to_completion(
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> str
```

**Parameters:**
- `job_id` - Job ID to poll
- `interval` - Polling interval in seconds (default: 10)
- `timeout` - Timeout in seconds (default: 600000)

**Returns:** Final status string ('FINISHED', 'FAILED', or 'CANCELLED')

**Raises:**
- `IRPJobError` - If job times out

### poll_risk_data_job_batch_to_completion

Poll multiple risk data jobs until all complete.

```python
poll_risk_data_job_batch_to_completion(
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

**Parameters:**
- `job_ids` - List of job IDs
- `interval` - Polling interval in seconds (default: 20)
- `timeout` - Timeout in seconds (default: 600000)

**Returns:** List of final job status dicts for all jobs

---

## Exception Classes

All exceptions inherit from `IRPIntegrationError`.

### IRPIntegrationError

Base exception for all IRP integration errors.

```python
class IRPIntegrationError(Exception)
```

### IRPAPIError

HTTP/API request failures.

```python
class IRPAPIError(IRPIntegrationError)
```

**Raised when:**
- HTTP request fails
- API returns error response
- Response parsing fails

### IRPValidationError

Input validation failures.

```python
class IRPValidationError(IRPIntegrationError)
```

**Raised when:**
- Required parameter is empty/None
- Parameter value is invalid
- Parameter type is incorrect

### IRPWorkflowError

Workflow execution failures.

```python
class IRPWorkflowError(IRPIntegrationError)
```

**Raised when:**
- Workflow fails during execution
- Unexpected workflow state

### IRPJobError

Job tracking/polling failures.

```python
class IRPJobError(IRPIntegrationError)
```

**Raised when:**
- Job times out
- Job polling fails
- Job enters error state

### IRPReferenceDataError

Reference data lookup failures.

```python
class IRPReferenceDataError(IRPIntegrationError)
```

**Raised when:**
- Reference data not found
- Multiple matches when expecting one
- Reference data extraction fails

### IRPFileError

File operation failures.

```python
class IRPFileError(IRPIntegrationError)
```

**Raised when:**
- File upload fails
- File read fails
- File not found

---

## Status Enumerations

### Workflow Statuses

**In Progress:**
- `QUEUED`
- `PENDING`
- `RUNNING`
- `CANCEL_REQUESTED`
- `CANCELLING`

**Completed:**
- `FINISHED`
- `FAILED`
- `CANCELLED`

### Perspective Codes

Analysis perspective codes for result retrieval:

- `GR` - Gross
- `GU` - Ground-Up
- `RL` - Reinsurance Layer

---

## Common Patterns

### Error Handling

```python
from helpers.irp_integration import IRPClient
from helpers.irp_integration.exceptions import (
    IRPAPIError,
    IRPValidationError,
    IRPWorkflowError
)

irp_client = IRPClient()

try:
    job_id, _ = irp_client.edm.submit_create_edm_job("MyEDM", "server-1")
except IRPValidationError as e:
    print(f"Invalid input: {e}")
except IRPAPIError as e:
    print(f"API error: {e}")
except IRPWorkflowError as e:
    print(f"Workflow failed: {e}")
```

### Polling Pattern

```python
# Submit job
job_id, _ = irp_client.portfolio.submit_geohaz_job(
    portfolio_name="MyPortfolio",
    edm_name="MyEDM"
)

# Poll to completion
final_status = irp_client.portfolio.poll_geohaz_job_to_completion(
    job_id=job_id,
    interval=30,  # Poll every 30 seconds
    timeout=1800000  # 30 minute timeout
)

if final_status == 'FINISHED':
    print("Job completed successfully")
```

### Batch Operations

```python
# Submit multiple jobs
job_ids = irp_client.edm.submit_create_edm_jobs([
    {"edm_name": "Book1", "server_name": "server-1"},
    {"edm_name": "Book2", "server_name": "server-2"}
])

# Poll all jobs
results = irp_client.job.poll_risk_data_job_batch_to_completion(
    job_ids=job_ids,
    interval=10,
    timeout=600000
)

# Check results
for result in results:
    print(f"Job {result['id']}: {result['status']}")
```

---

## Related Documentation

- [IRP Integration Guide](IRP_INTEGRATION_GUIDE.md) - Setup, workflows, and best practices
- [Design Document](DESIGN_DOCUMENT.md) - Overall system architecture
- [Batch & Job System](BATCH_JOB_SYSTEM.md) - Framework batch processing
