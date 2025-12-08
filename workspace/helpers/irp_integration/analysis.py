"""
Analysis management operations.

Handles portfolio analysis submission, job tracking, and analysis group creation.
"""

import json
import time
from typing import Dict, List, Any, Optional
from .client import Client
from .constants import (
    CREATE_ANALYSIS_JOB, DELETE_ANALYSIS, GET_ANALYSIS_GROUPING_JOB,
    GET_ANALYSIS_JOB, GET_ANALYSIS_RESULT, CREATE_ANALYSIS_GROUP,
    SEARCH_ANALYSIS_JOBS, SEARCH_ANALYSIS_RESULTS,
    WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES,
    GET_ANALYSIS_ELT, GET_ANALYSIS_EP, GET_ANALYSIS_STATS, GET_ANALYSIS_PLT,
    PERSPECTIVE_CODES
)
from .exceptions import IRPAPIError, IRPJobError, IRPReferenceDataError, IRPValidationError
from .validators import validate_non_empty_string, validate_positive_int, validate_list_not_empty
from .utils import extract_id_from_location_header

class AnalysisManager:
    """Manager for analysis operations."""

    def __init__(
            self, 
            client: Client, 
            reference_data_manager: Optional[Any] = None, 
            treaty_manager: Optional[Any] = None,
            edm_manager: Optional[Any] = None,
            portfolio_manager: Optional[Any] = None
    ) -> None:
        """
        Initialize analysis manager.

        Args:
            client: IRP API client instance
            reference_data_manager: Optional ReferenceDataManager instance
        """
        self.client = client
        self._reference_data_manager = reference_data_manager
        self._treaty_manager = treaty_manager
        self._edm_manager = edm_manager
        self._portfolio_manager = portfolio_manager

    @property
    def reference_data_manager(self):
        """Lazy-loaded reference data manager to avoid circular imports."""
        if self._reference_data_manager is None:
            from .reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager
    
    @property
    def treaty_manager(self):
        """Lazy-loaded treaty manager to avoid circular imports."""
        if self._treaty_manager is None:
            from .treaty import TreatyManager
            self._treaty_manager = TreatyManager(self.client)
        return self._treaty_manager
    
    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager
    
    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager


    def get_analysis_by_id(self, analysis_id: int) -> Dict[str, Any]:
        """
        Retrieve analysis by ID.

        Args:
            analysis_id: Analysis ID

        Returns:
            Dict containing analysis details

        Raises:
            IRPValidationError: If analysis_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(analysis_id, "analysis_id")
        try:
            response = self.client.request('GET', GET_ANALYSIS_RESULT.format(analysisId=analysis_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get analysis {analysis_id}: {e}")


    def submit_portfolio_analysis_jobs(self, analysis_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple portfolio analysis jobs.

        Args:
            analysis_data_list: List of analysis job data dicts, each containing:
                - edm_name: str
                - portfolio_name: str
                - job_name: str
                - analysis_profile_name: str
                - output_profile_name: str
                - event_rate_scheme_name: str
                - treaty_names: List[str]
                - tag_names: List[str]

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If analysis_data_list is empty or invalid
            IRPAPIError: If analysis submission fails or duplicate analysis names exist
        """
        validate_list_not_empty(analysis_data_list, "analysis_data_list")

        # Pre-validate that no analysis names already exist
        analysis_names = list(a['job_name'] for a in analysis_data_list)
        for name in analysis_names:
            analysis_response = self.search_analyses(filter=f"analysisName = \"{name}\"")
            if len(analysis_response) > 0:
                raise IRPAPIError(f"Analysis with this name already exists: {name}")

        job_ids = []
        for analysis_data in analysis_data_list:
            try:
                job_id = self.submit_portfolio_analysis_job(
                    edm_name=analysis_data['edm_name'],
                    portfolio_name=analysis_data['portfolio_name'],
                    job_name=analysis_data['job_name'],
                    analysis_profile_name=analysis_data['analysis_profile_name'],
                    output_profile_name=analysis_data['output_profile_name'],
                    event_rate_scheme_name=analysis_data['event_rate_scheme_name'],
                    treaty_names=analysis_data['treaty_names'],
                    tag_names=analysis_data['tag_names'],
                    skip_duplicate_check=True  # Already validated above
                )
                job_ids.append(job_id)
            except KeyError as e:
                raise IRPAPIError(f"Missing analysis job data: {e}") from e

        return job_ids

    def submit_portfolio_analysis_job(
        self,
        edm_name: str,
        portfolio_name: str,
        job_name: str,
        analysis_profile_name: str,
        output_profile_name: str,
        event_rate_scheme_name: str,
        treaty_names: List[str],
        tag_names: List[str],
        currency: Dict[str, str] = None,
        skip_duplicate_check: bool = False
    ) -> int:
        """
        Submit portfolio analysis job (submits but doesn't wait).

        Args:
            edm_name: Name of the EDM (exposure database)
            portfolio_name: Name of the portfolio to analyze
            job_name: Name for analysis job (must be unique)
            analysis_profile_name: Model profile name
            output_profile_name: Output profile name
            event_rate_scheme_name: Event rate scheme name (required for DLM, optional for HD)
            treaty_names: List of treaty names to apply
            tag_names: List of tag names to apply
            currency: Optional currency configuration
            skip_duplicate_check: Skip checking if analysis name already exists (for batch operations)

        Returns:
            Workflow ID (Moody's job ID)

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails or EDM/portfolio not found
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(job_name, "job_name")
        validate_non_empty_string(analysis_profile_name, "analysis_profile_name")
        validate_non_empty_string(output_profile_name, "output_profile_name")
        # event_rate_scheme_name validation deferred - required for DLM but optional for HD

        # Check if analysis name already exists (unless skipped for batch operations)
        if not skip_duplicate_check:
            analysis_response = self.search_analyses(filter=f"analysisName = \"{job_name}\" AND exposureName = \"{edm_name}\"")
            if len(analysis_response) > 0:
                raise IRPAPIError(f"Analysis with name '{job_name}' already exists for EDM '{edm_name}'")

        # Look up EDM to get exposure_id
        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        # Look up portfolio to get portfolio_uri
        portfolios = self.portfolio_manager.search_portfolios(
            exposure_id=exposure_id,
            filter=f"portfolioName=\"{portfolio_name}\""
        )
        if len(portfolios) != 1:
            raise IRPAPIError(f"Expected 1 portfolio with name {portfolio_name}, found {len(portfolios)}")
        try:
            portfolio_uri = portfolios[0]['uri']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract portfolio URI for portfolio '{portfolio_name}': {e}"
            ) from e

        # Look up treaties by name
        if treaty_names:
            try:
                quoted = ", ".join(json.dumps(s) for s in treaty_names)
                filter_statement = f"treatyName IN ({quoted})"
                treaties_response = self.treaty_manager.search_treaties(
                    exposure_id=exposure_id,
                    filter=filter_statement
                )
            except Exception as e:
                raise IRPAPIError(f"Failed to search treaties with names {treaty_names}: {e}")

            if len(treaties_response) != len(treaty_names):
                raise IRPAPIError(f"Expected {len(treaty_names)} treaties, found {len(treaties_response)}")
            try:
                treaty_ids = [treaty['treatyId'] for treaty in treaties_response]
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract treaty IDs from treaty search response: {e}"
                ) from e
        else:
            treaty_ids = []

        # Look up reference data - model profile first to determine job type
        model_profile_response = self.reference_data_manager.get_model_profile_by_name(analysis_profile_name)
        output_profile_response = self.reference_data_manager.get_output_profile_by_name(output_profile_name)

        if model_profile_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Analysis profile '{analysis_profile_name}' not found")
        if len(output_profile_response) == 0:
            raise IRPReferenceDataError(f"Output profile '{output_profile_name}' not found")

        try:
            model_profile_id = model_profile_response['items'][0]['id']
            if "HD" in model_profile_response['items'][0]['softwareVersionCode']:
                job_type = "HD"
            else:
                job_type = "DLM"
        except (KeyError, IndexError, TypeError) as e:
            raise IRPReferenceDataError(
                f"Failed to extract model profile ID for '{analysis_profile_name}': {e}"
            ) from e

        try:
            output_profile_id = output_profile_response[0]['id']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPReferenceDataError(
                f"Failed to extract output profile ID for '{output_profile_name}': {e}"
            ) from e

        # Event rate scheme is required for DLM analyses but optional for HD
        event_rate_scheme_id = None
        if event_rate_scheme_name:
            event_rate_scheme_response = self.reference_data_manager.get_event_rate_scheme_by_name(event_rate_scheme_name)
            if event_rate_scheme_response.get('count', 0) == 0:
                raise IRPReferenceDataError(f"Event rate scheme '{event_rate_scheme_name}' not found")
            try:
                event_rate_scheme_id = event_rate_scheme_response['items'][0]['eventRateSchemeId']
            except (KeyError, IndexError, TypeError) as e:
                raise IRPReferenceDataError(
                    f"Failed to extract event rate scheme ID for '{event_rate_scheme_name}': {e}"
                ) from e
        elif job_type == "DLM":
            raise IRPReferenceDataError("Event rate scheme is required for DLM analyses")

        # Look up tag IDs
        try:
            tag_ids = self.reference_data_manager.get_tag_ids_from_tag_names(tag_names)
        except IRPAPIError as e:
            raise IRPAPIError(f"Failed to get tag ids for tag names {tag_names}: {e}")

        if currency is None:
            currency = self.reference_data_manager.get_analysis_currency()

        settings = {
            "name": job_name,
            "modelProfileId": model_profile_id,
            "outputProfileId": output_profile_id,
            "treatyIds": treaty_ids,
            "tagIds": tag_ids,
            "currency": currency
        }

        # Only include eventRateSchemeId for DLM analyses
        if event_rate_scheme_id is not None:
            settings["eventRateSchemeId"] = event_rate_scheme_id

        data = {
            "resourceUri": portfolio_uri,
            "resourceType": "portfolio",
            "type": job_type,
            "settings": settings
        }

        try:
            response = self.client.request('POST', CREATE_ANALYSIS_JOB, json=data)
            job_id = extract_id_from_location_header(response, "analysis job submission")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to submit analysis job '{job_name}' for portfolio {portfolio_name}: {e}")


    def submit_analysis_grouping_jobs(
        self,
        grouping_data_list: List[Dict[str, Any]],
        analysis_edm_map: Optional[Dict[str, str]] = None,
        group_names: Optional[set] = None
    ) -> List[int]:
        """
        Submit multiple analysis grouping jobs.

        Args:
            grouping_data_list: List of grouping data dicts, each containing:
                - group_name: str
                - analysis_names: List[str] (can include both analysis names and group names)
            analysis_edm_map: Optional mapping of analysis names to EDM names.
                Used to look up analyses by name + EDM (since analysis names are only
                unique within an EDM). If not provided, lookups use name only.
            group_names: Optional set of known group names. Items in this set are
                looked up as groups (by name only), all others are looked up as
                analyses (by name + EDM if mapping provided).

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If grouping_data_list is empty or invalid
            IRPAPIError: If grouping submission fails or analysis names not found
        """
        validate_list_not_empty(grouping_data_list, "grouping_data_list")

        job_ids = []
        for grouping_data in grouping_data_list:
            try:
                group_name = grouping_data['group_name']
                analysis_names = grouping_data['analysis_names']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing analysis job data: {e}"
                ) from e

            job_ids.append(self.submit_analysis_grouping_job(
                group_name=group_name,
                analysis_names=analysis_names,
                analysis_edm_map=analysis_edm_map,
                group_names=group_names
            ))

        return job_ids


    def submit_analysis_grouping_job(
        self,
        group_name: str,
        analysis_names: List[str],
        simulate_to_plt: bool = True,
        num_simulations: int = 50000,
        propagate_detailed_losses: bool = False,
        reporting_window_start: str = "01/01/2021",
        simulation_window_start: str = "01/01/2021",
        simulation_window_end: str = "12/31/2021",
        region_peril_simulation_set: List[Dict[str, Any]] = None,
        description: str = "",
        currency: Dict[str, str] = None,
        analysis_edm_map: Optional[Dict[str, str]] = None,
        group_names: Optional[set] = None
    ) -> int:
        """
        Submit analysis grouping job.

        Args:
            group_name: Name for analysis group
            analysis_names: List of names to include in the group (can be analyses or groups)
            simulate_to_plt: Whether to simulate to PLT (default: True)
            num_simulations: Number of simulations (default: 50000)
            propagate_detailed_losses: Whether to propagate detailed losses (default: False)
            reporting_window_start: Reporting window start date (default: "01/01/2021")
            simulation_window_start: Simulation window start date (default: "01/01/2021")
            simulation_window_end: Simulation window end date (default: "12/31/2021")
            region_peril_simulation_set: Region/peril simulation set (default: None)
            description: Group description (default: "")
            currency: Currency configuration (default: None, uses system default)
            analysis_edm_map: Optional mapping of analysis names to EDM names.
                Used to look up analyses by name + EDM (since analysis names are only
                unique within an EDM). If not provided, lookups use name only.
            group_names: Optional set of known group names. Items in this set are
                looked up as groups (by name only), all others are looked up as
                analyses (by name + EDM if mapping provided).

        Returns:
            Analysis group job ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails or analysis names not found
        """
        validate_non_empty_string(group_name, "group_name")
        validate_list_not_empty(analysis_names, "analysis_names")

        # Initialize defaults
        if analysis_edm_map is None:
            analysis_edm_map = {}
        if group_names is None:
            group_names = set()

        # Check if analysis group with this name already exists
        analysis_response = self.search_analyses(filter=f"analysisName = \"{group_name}\"")
        if len(analysis_response) > 0:
            raise IRPAPIError(f"Analysis Group with this name already exists: {group_name}")

        # Resolve analysis/group names to URIs
        analysis_uris = []
        for name in analysis_names:
            # Determine if this is a group name or an analysis name
            if name in group_names:
                # Group names are globally unique - search by name only
                analysis_response = self.search_analyses(filter=f"analysisName = \"{name}\"")
                if len(analysis_response) == 0:
                    raise IRPAPIError(f"Group with this name does not exist: {name}")
                if len(analysis_response) > 1:
                    raise IRPAPIError(f"Duplicate groups exist with name: {name}")
            else:
                # Analysis names - search by name + EDM if mapping provided
                edm_name = analysis_edm_map.get(name)
                if edm_name:
                    filter_str = f"analysisName = \"{name}\" AND exposureName = \"{edm_name}\""
                    analysis_response = self.search_analyses(filter=filter_str)
                    if len(analysis_response) == 0:
                        raise IRPAPIError(f"Analysis '{name}' not found for EDM '{edm_name}'")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Multiple analyses found with name '{name}' for EDM '{edm_name}'")
                else:
                    # Fallback to name-only search (legacy behavior)
                    analysis_response = self.search_analyses(filter=f"analysisName = \"{name}\"")
                    if len(analysis_response) == 0:
                        raise IRPAPIError(f"Analysis with this name does not exist: {name}")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Duplicate analyses exist with name: {name}.")

            try:
                analysis_uris.append(analysis_response[0]['uri'])
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract URI for '{name}': {e}"
                ) from e

        if currency is None:
            currency = self.reference_data_manager.get_analysis_currency()
        if region_peril_simulation_set is None:
            region_peril_simulation_set = []

        data = {
            "resourceType": "analyses",
            "resourceUris": analysis_uris,
            "settings": {
                "analysisName": group_name,
                "currency": currency,
                "simulateToPLT": simulate_to_plt,
                "propagateDetailedLosses": propagate_detailed_losses,
                "numOfSimulations": num_simulations,
                "reportingWindowStart": reporting_window_start,
                "simulationWindowStart": simulation_window_start,
                "simulationWindowEnd": simulation_window_end,
                "regionPerilSimulationSet": region_peril_simulation_set,
                "description": description
            }
        }

        try:
            response = self.client.request('POST', CREATE_ANALYSIS_GROUP, json=data)
            group_id = extract_id_from_location_header(response, "analysis group creation")
            return int(group_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to submit analysis group job '{group_name}': {e}")
        
    
    def get_analysis_grouping_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve analysis grouping job status by job ID.

        Args:
            job_id: Job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_ANALYSIS_GROUPING_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get analysis grouping job status for job ID {job_id}: {e}")


    def poll_analysis_grouping_job_to_completion(
        self,
        job_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll analysis grouping job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling analysis grouping job ID {job_id}")
            job_data = self.get_analysis_grouping_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            print(f"Job status: {status}; Percent complete: {progress}")
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Analysis grouping job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_analysis_grouping_job_batch_to_completion(
        self,
        job_ids: List[int],
        interval: int = 20,
        timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple analysis grouping jobs until all complete or timeout.

        Args:
            job_ids: List of job IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final job status details for all jobs

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If jobs time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(job_ids, "job_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch grouping job ids: {','.join(str(item) for item in job_ids)}")

            all_completed = False
            all_jobs = []
            for job_id in job_ids:
                workflow_response = self.get_analysis_grouping_job(job_id)
                all_jobs.append(workflow_response)
                try:
                    status = workflow_response['status']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Missing 'status' in workflow response for job ID {job_id}: {e}"
                    ) from e
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_jobs = []
                    break
                all_completed = True

            if all_completed:
                return all_jobs
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch grouping jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)


    def get_analysis_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve analysis job status by job ID.

        Args:
            job_id: Job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_ANALYSIS_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get analysis job status for job ID {job_id}: {e}")


    def poll_analysis_job_to_completion(
            self,
            job_id: int,
            interval: int = 10,
            timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll analysis job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling analysis job ID {job_id}")
            job_data = self.get_analysis_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            print(f"Job status: {status}; Percent complete: {progress}")
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Analysis job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def search_analysis_jobs(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search analysis jobs with optional filtering.

        Args:
            filter: Optional filter string (default: "")
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of analysis job dicts

        Raises:
            IRPAPIError: If search fails
        """
        params: Dict[str, Any] = {
            'limit': limit,
            'offset': offset
        }
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request('GET', SEARCH_ANALYSIS_JOBS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search analysis jobs : {e}")


    def poll_analysis_job_batch_to_completion(
            self,
            job_ids: List[int],
            interval: int = 20,
            timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple analysis jobs until all complete or timeout.

        Args:
            job_ids: List of job IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final job status details for all jobs

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If jobs time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(job_ids, "job_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch analysis ids: {','.join(str(item) for item in job_ids)}")

            # Fetch all workflows across all pages
            all_jobs = []
            offset = 0
            limit = 100
            while True:
                quoted = ", ".join(json.dumps(str(s)) for s in job_ids)
                filter_statement = f"jobId IN ({quoted})"
                analysis_response = self.search_analysis_jobs(
                    filter=filter_statement,
                    limit=limit,
                    offset=offset
                )
                all_jobs.extend(analysis_response)

                # Check if we've fetched all workflows
                if len(all_jobs) >= len(job_ids):
                    break

                # Move to next page
                offset += limit

            # Check if all workflows are completed
            all_completed = True
            for job in all_jobs:
                status = job.get('status', '')
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_completed = False
                    break

            if all_completed:
                return all_jobs

            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch analysis jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)


    def search_analyses(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search analysis results with optional filtering.

        Args:
            filter: Optional filter string (default: "")

        Returns:
            List of analysis result dicts

        Raises:
            IRPAPIError: If search fails
        """
        params = {}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request('GET', SEARCH_ANALYSIS_RESULTS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search analysis results : {e}")

    def get_analysis_by_name(self, analysis_name: str, edm_name: str) -> Dict[str, Any]:
        """
        Get an analysis by name and EDM name.

        Args:
            analysis_name: Name of the analysis
            edm_name: Name of the EDM (exposure database)

        Returns:
            Dict containing analysis details

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If analysis not found or multiple matches
        """
        validate_non_empty_string(analysis_name, "analysis_name")
        validate_non_empty_string(edm_name, "edm_name")

        filter_str = f'analysisName = "{analysis_name}" AND exposureName = "{edm_name}"'
        analyses = self.search_analyses(filter=filter_str)

        if len(analyses) == 0:
            raise IRPAPIError(f"Analysis '{analysis_name}' not found for EDM '{edm_name}'")
        if len(analyses) > 1:
            raise IRPAPIError(f"Multiple analyses found with name '{analysis_name}' for EDM '{edm_name}'")

        return analyses[0]

    def find_existing_analyses_from_job_configs(
        self,
        job_configs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Find which analysis job configurations already exist in Moody's.

        Used to validate analysis batch job configurations before submission.
        Each job config dict should have 'Analysis Name' and 'Database' keys
        (matching the Analysis Table / job configuration format).

        Args:
            job_configs: List of job configuration dicts, each containing:
                - 'Analysis Name': Name of the analysis
                - 'Database': Name of the EDM

        Returns:
            List of dicts for any matches found, each containing:
                - 'job_config': The original job configuration dict
                - 'analysis': The existing analysis from the API
        """
        existing = []

        for job_config in job_configs:
            analysis_name = job_config.get('Analysis Name')
            edm_name = job_config.get('Database')

            if not analysis_name or not edm_name:
                continue

            filter_str = f'analysisName = "{analysis_name}" AND exposureName = "{edm_name}"'
            analyses = self.search_analyses(filter=filter_str)

            if analyses:
                for analysis in analyses:
                    existing.append({
                        'job_config': job_config,
                        'analysis': analysis
                    })

        return existing

    def delete_analysis(self, analysis_id: int) -> None:
        """
        Delete an analysis by ID.

        Args:
            analysis_id: Analysis ID to delete

        Raises:
            IRPValidationError: If analysis_id is invalid
            IRPAPIError: If deletion fails
        """
        validate_positive_int(analysis_id, "analysis_id")

        try:
            self.client.request('DELETE', DELETE_ANALYSIS.format(analysisId=analysis_id))
            print(f"Deleted analysis ID: {analysis_id}")
        except Exception as e:
            raise IRPAPIError(f"Failed to delete analysis : {e}")

    def get_analysis_by_app_analysis_id(self, app_analysis_id: int) -> Dict[str, Any]:
        """
        Retrieve analysis by appAnalysisId (the ID used in the application/UI).

        Args:
            app_analysis_id: Application analysis ID (e.g., 35810)

        Returns:
            Dict containing analysisId and exposureResourceId

        Raises:
            IRPValidationError: If app_analysis_id is invalid
            IRPAPIError: If request fails or analysis not found
        """
        validate_positive_int(app_analysis_id, "app_analysis_id")

        try:
            filter_str = f"appAnalysisId={app_analysis_id}"
            results = self.search_analyses(filter=filter_str)
            if not results:
                raise IRPAPIError(f"No analysis found with appAnalysisId={app_analysis_id}")

            analysis = results[0]
            return {
                'analysisId': analysis.get('analysisId'),
                'exposureResourceId': analysis.get('exposureResourceId'),
                'analysisName': analysis.get('analysisName'),
                'engineType': analysis.get('engineType'),  # 'HD' or 'DLM'
                'raw': analysis
            }
        except IRPAPIError:
            raise
        except Exception as e:
            raise IRPAPIError(f"Failed to get analysis by appAnalysisId {app_analysis_id}: {e}")

    def _validate_perspective_code(self, perspective_code: str) -> None:
        """Validate perspective code is one of the allowed values."""
        if perspective_code not in PERSPECTIVE_CODES:
            raise IRPValidationError(
                f"Invalid perspective_code '{perspective_code}'. "
                f"Must be one of: {', '.join(PERSPECTIVE_CODES)}"
            )

    def get_elt(
        self,
        analysis_id: int,
        perspective_code: str,
        exposure_resource_id: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve Event Loss Table (ELT) for an analysis.

        Args:
            analysis_id: Analysis ID
            perspective_code: One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
            exposure_resource_id: Exposure resource ID (portfolio ID from analysis)

        Returns:
            List of ELT records containing eventId, positionValue, stdDevI, stdDevC, etc.

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(analysis_id, "analysis_id")
        validate_positive_int(exposure_resource_id, "exposure_resource_id")
        self._validate_perspective_code(perspective_code)

        params = {
            'perspectiveCode': perspective_code,
            'exposureResourceType': 'PORTFOLIO',
            'exposureResourceId': exposure_resource_id
        }

        try:
            response = self.client.request(
                'GET',
                GET_ANALYSIS_ELT.format(analysisId=analysis_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get ELT for analysis {analysis_id}: {e}")

    def get_ep(
        self,
        analysis_id: int,
        perspective_code: str,
        exposure_resource_id: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve EP (Exceedance Probability) metrics for an analysis.

        Args:
            analysis_id: Analysis ID
            perspective_code: One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
            exposure_resource_id: Exposure resource ID (portfolio ID from analysis)

        Returns:
            List of EP curve data (OEP, AEP, CEP, TCE curves)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(analysis_id, "analysis_id")
        validate_positive_int(exposure_resource_id, "exposure_resource_id")
        self._validate_perspective_code(perspective_code)

        params = {
            'perspectiveCode': perspective_code,
            'exposureResourceType': 'PORTFOLIO',
            'exposureResourceId': exposure_resource_id
        }

        try:
            response = self.client.request(
                'GET',
                GET_ANALYSIS_EP.format(analysisId=analysis_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get EP metrics for analysis {analysis_id}: {e}")

    def get_stats(
        self,
        analysis_id: int,
        perspective_code: str,
        exposure_resource_id: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve statistics for an analysis.

        Args:
            analysis_id: Analysis ID
            perspective_code: One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
            exposure_resource_id: Exposure resource ID (portfolio ID from analysis)

        Returns:
            List of statistical metrics

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(analysis_id, "analysis_id")
        validate_positive_int(exposure_resource_id, "exposure_resource_id")
        self._validate_perspective_code(perspective_code)

        params = {
            'perspectiveCode': perspective_code,
            'exposureResourceType': 'PORTFOLIO',
            'exposureResourceId': exposure_resource_id
        }

        try:
            response = self.client.request(
                'GET',
                GET_ANALYSIS_STATS.format(analysisId=analysis_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get statistics for analysis {analysis_id}: {e}")

    def get_plt(
        self,
        analysis_id: int,
        perspective_code: str,
        exposure_resource_id: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve Period Loss Table (PLT) for an analysis.

        Note: PLT is only available for HD (High Definition) analyses.

        Args:
            analysis_id: Analysis ID
            perspective_code: One of 'GR' (Gross), 'GU' (Ground-Up), 'RL' (Reinsurance Layer)
            exposure_resource_id: Exposure resource ID (portfolio ID from analysis)

        Returns:
            List of PLT records containing event dates, loss dates, and loss amounts

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(analysis_id, "analysis_id")
        validate_positive_int(exposure_resource_id, "exposure_resource_id")
        self._validate_perspective_code(perspective_code)

        params = {
            'perspectiveCode': perspective_code,
            'exposureResourceType': 'PORTFOLIO',
            'exposureResourceId': exposure_resource_id,
            'limit': 100000
        }

        try:
            response = self.client.request(
                'GET',
                GET_ANALYSIS_PLT.format(analysisId=analysis_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get PLT for analysis {analysis_id}: {e}")
