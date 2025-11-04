"""
Analysis management operations.

Handles portfolio analysis submission, job tracking, and analysis group creation.
"""

import json
import time
from typing import Dict, List, Any, Optional
from .client import Client
from .constants import CREATE_ANALYSIS_JOB, DELETE_ANALYSIS, GET_ANALYSIS_GROUPING_JOB, GET_ANALYSIS_JOB, GET_ANALYSIS_RESULT, CREATE_ANALYSIS_GROUP, SEARCH_ANALYSIS_JOBS, SEARCH_ANALYSIS_RESULTS, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES
from .exceptions import IRPAPIError, IRPJobError, IRPReferenceDataError
from .validators import validate_non_empty_string, validate_positive_int, validate_list_not_empty
from .utils import extract_id_from_location_header, build_analysis_currency_dict

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
        validate_list_not_empty(analysis_data_list, "analysis_data_list")

        analysis_names = list(a['job_name'] for a in analysis_data_list)
        for name in analysis_names:
            analysis_response = self.search_analyses(filter=f"analysisName = \"{name}\"")
            if (len(analysis_response) > 0):
                raise IRPAPIError(f"Analysis with this name already exists: {name}")


        job_ids = []
        for analysis_data in analysis_data_list:
            try:
                edm_name = analysis_data['edm_name']
                portfolio_name = analysis_data['portfolio_name']
                job_name = analysis_data['job_name']
                analysis_profile_name = analysis_data['analysis_profile_name']
                output_profile_name = analysis_data['output_profile_name']
                event_rate_scheme_name = analysis_data['event_rate_scheme_name']
                treaty_names = analysis_data['treaty_names']
                tag_names = analysis_data['tag_names']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing analysis job data: {e}"
                ) from e
            
            edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
            if (len(edms) != 1):
                raise Exception(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
            exposure_id = edms[0]['exposureId']

            portfolios = self.portfolio_manager.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
            if (len(portfolios) != 1):
                raise Exception(f"Expected 1 portfolio with name {portfolio_name}, found {len(portfolios)}")
            portfolio_uri = portfolios[0]['uri']

            job_ids.append(self.submit_portfolio_analysis_job(
                exposure_id=exposure_id,
                job_name=job_name,
                portfolio_uri=portfolio_uri,
                analysis_profile_name=analysis_profile_name,
                output_profile_name=output_profile_name,
                event_rate_scheme_name=event_rate_scheme_name,
                treaty_names=treaty_names,
                tag_names=tag_names
            ))

        return job_ids

    
    def submit_portfolio_analysis_job(
            self,
            exposure_id: int,
            job_name: str,
            portfolio_uri: str,
            analysis_profile_name: str,
            output_profile_name: str,
            event_rate_scheme_name: str,
            treaty_names: List[str],
            tag_names: List[str],
            currency: Dict[str, str] = None
    ) -> int:
        """
        Submit portfolio analysis job (submits but doesn't wait).

        Args:
            job_name: Name for analysis job
            portfolio_uri: URI of the portfolio to analyze
            analysis_profile_name: Model profile ID
            output_profile_name: Output profile ID
            event_rate_scheme_id: Event rate scheme ID
            treaty_ids: List of treaty IDs
            tag_ids: List of tag IDs

        Returns:
            Workflow ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(job_name, "job_name")
        validate_non_empty_string(portfolio_uri, "portfolio_uri")
        validate_non_empty_string(analysis_profile_name, "analysis_profile_name")
        validate_non_empty_string(output_profile_name, "output_profile_name")
        validate_non_empty_string(event_rate_scheme_name, "job_nevent_rate_scheme_nameame")

        try:
            quoted = ", ".join(json.dumps(s) for s in treaty_names)
            filter_statement = f"treatyName IN ({quoted})"
            treaties_response = self.treaty_manager.search_treaties(
                exposure_id=exposure_id, 
                filter=filter_statement
            )
        except Exception as e:
            raise IRPAPIError(f"Failed to search treaties with names {treaty_names} : {e}")
        
        if (len(treaties_response) != len(treaty_names)):
            raise IRPAPIError(f"Expected {len(treaty_names)} treaties, found {len(treaties_response)}")
        treaty_ids = [treaty['treatyId'] for treaty in treaties_response]

        model_profile_response = self.reference_data_manager.get_model_profile_by_name(analysis_profile_name)
        output_profile_response = self.reference_data_manager.get_output_profile_by_name(output_profile_name)
        event_rate_scheme_response = self.reference_data_manager.get_event_rate_scheme_by_name(event_rate_scheme_name)
        if model_profile_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Analysis profile '{analysis_profile_name}' not found")
        if len(output_profile_response) == 0:
            raise IRPReferenceDataError(f"Output profile '{output_profile_name}' not found")
        if event_rate_scheme_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Event rate scheme '{event_rate_scheme_name}' not found")

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

        try:
            event_rate_scheme_id = event_rate_scheme_response['items'][0]['eventRateSchemeId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPReferenceDataError(
                f"Failed to extract event rate scheme ID for '{event_rate_scheme_name}': {e}"
            ) from e

        try:
            tag_ids = self.reference_data_manager.get_tag_ids_from_tag_names(tag_names)
        except IRPAPIError as e:
            raise IRPAPIError(f"Failed to get tag ids for tag names {tag_names} : {e}")

        if currency is None:
            currency = build_analysis_currency_dict()

        data = {
            "resourceUri": portfolio_uri,
            "resourceType": "portfolio",
            "type": job_type,
            "settings": {
                "name": job_name,
                "modelProfileId": model_profile_id,
                "outputProfileId": output_profile_id,
                "eventRateSchemeId": event_rate_scheme_id,
                "treatyIds": treaty_ids,
                "tagIds": tag_ids,
                "currency": currency
            }
        }

        try:
            response = self.client.request('POST', CREATE_ANALYSIS_JOB, json=data)
            job_id = extract_id_from_location_header(response, "analysis job submission")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to submit analysis job '{job_name}' for portfolio {portfolio_uri}: {e}")


    def submit_analysis_grouping_jobs(self, grouping_data_list: List[Dict[str, Any]]) -> List[int]:
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
            
            analysis_uris = []
            for name in analysis_names:
                analysis_response = self.search_analyses(filter=f"analysisName = \"{name}\"")
                if (len(analysis_response) == 0):
                    raise IRPAPIError(f"Analysis with this name does not exist: {name}")
                if (len(analysis_response) > 1):
                    raise IRPAPIError(f"Duplicate analyses exist with name: {name}")
                analysis_uris.append(analysis_response[0]['uri'])
            
            analysis_response = self.search_analyses(filter=f"analysisName = \"{group_name}\"")
            if (len(analysis_response) > 0):
                raise Exception(f"Analysis Group with this name already exists: {name}")

            job_ids.append(self.submit_analysis_grouping_job(
                group_name=group_name,
                analysis_uris=analysis_uris
            ))

        return job_ids


    def submit_analysis_grouping_job(
            self,
            group_name: str,
            analysis_uris: List[str],
            simulate_to_plt: bool = True,
            num_simulations: int = 50000,
            propagate_detailed_losses: bool = False,
            reporting_window_start: str = "01/01/2021",
            simulation_window_start: str = "01/01/2021",
            simulation_window_end: str = "12/31/2021",
            region_peril_simulation_set: List[Dict[str, Any]] = None,
            description: str = "",
            currency: Dict[str, str] = None
    ) -> int:
        """
        Submit analysis grouping job.

        Args:
            group_name: Name for analysis group
            analysis_uris: List of analysis URIs to include in the group

        Returns:
            Analysis group job ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(group_name, "group_name")
        validate_list_not_empty(analysis_uris, "analysis_uris")

        if currency is None:
            currency = build_analysis_currency_dict()
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
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_ANALYSIS_GROUPING_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get analysis job status for job ID {job_id}: {e}")


    def poll_analysis_grouping_job_to_completion(
            self,
            job_id: int,
            interval: int = 10,
            timeout: int = 600000
    ) -> Dict[str, Any]:
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
                    status = workflow_response['status']
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
        params = {}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request('GET', SEARCH_ANALYSIS_RESULTS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search analysis results : {e}")
        

    def delete_analysis(self, analysis_id: int) -> None:
        validate_positive_int(analysis_id, "analysis_id")

        try:
            self.client.request('DELETE', DELETE_ANALYSIS.format(analysisId=analysis_id))
            print(f"Deleted analysis ID: {analysis_id}")
        except Exception as e:
            raise IRPAPIError(f"Failed to delete analysis : {e}")
