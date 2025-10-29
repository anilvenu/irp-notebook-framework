"""
Analysis management operations.

Handles portfolio analysis submission, job tracking, and analysis group creation.
"""

import json
from typing import Dict, List, Any, Optional
from .client import Client
from .constants import GET_ANALYSES, GET_PLATFORM_ANALYSES, ANALYZE_PORTFOLIO, CREATE_ANALYSIS_GROUP
from .exceptions import IRPReferenceDataError
from .validators import validate_non_empty_string, validate_positive_int, validate_list_not_empty
from .utils import extract_id_from_location_header, build_analysis_currency_dict

class AnalysisManager:
    """Manager for analysis operations."""

    def __init__(self, client: Client, reference_data_manager: Optional[Any] = None) -> None:
        """
        Initialize analysis manager.

        Args:
            client: IRP API client instance
            reference_data_manager: Optional ReferenceDataManager instance
        """
        self.client = client
        self._reference_data_manager = reference_data_manager

    @property
    def reference_data_manager(self):
        """Lazy-loaded reference data manager to avoid circular imports."""
        if self._reference_data_manager is None:
            from .reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager

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

        params = {"q": f"id={analysis_id}"}
        response = self.client.request('GET', GET_ANALYSES, params=params)
        return response.json()

    def get_analyses_by_ids(self, analysis_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Retrieve multiple analyses by IDs.

        Args:
            analysis_ids: List of analysis IDs

        Returns:
            Dict containing analysis list

        Raises:
            IRPValidationError: If analysis_ids is empty
            IRPAPIError: If request fails
        """
        validate_list_not_empty(analysis_ids, "analysis_ids")

        params = {'filter': f"appAnalysisId IN ({','.join(str(id) for id in analysis_ids)})"}
        response = self.client.request('GET', GET_PLATFORM_ANALYSES, params=params)
        return response.json()

    def submit_analysis_job(
        self,
        job_name: str,
        edm_name: str,
        portfolio_id: int,
        analysis_profile_name: str,
        output_profile_name: str,
        event_rate_scheme_name: str,
        treaty_ids: List[int],
        tag_names: List[str],
        *,
        global_analysis_settings: Optional[Dict[str, Any]] = None,
        currency: Optional[Dict[str, str]] = None
    ) -> int:
        """
        Submit analysis job using profile names (submits but doesn't wait).

        Args:
            job_name: Name for analysis job
            edm_name: Name of EDM datasource
            portfolio_id: Portfolio ID
            analysis_profile_name: Analysis profile name
            output_profile_name: Output profile name
            event_rate_scheme_name: Event rate scheme name
            treaty_ids: List of treaty IDs
            tag_names: List of tag names
            global_analysis_settings: Optional analysis settings dict
            currency: Optional currency dict (uses default if not provided)

        Returns:
            Workflow ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPReferenceDataError: If reference data not found
            IRPAPIError: If request fails
        """
        validate_non_empty_string(job_name, "job_name")
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_non_empty_string(analysis_profile_name, "analysis_profile_name")
        validate_non_empty_string(output_profile_name, "output_profile_name")
        validate_non_empty_string(event_rate_scheme_name, "event_rate_scheme_name")
        validate_list_not_empty(treaty_ids, "treaty_ids")
        validate_list_not_empty(tag_names, "tag_names")

        if global_analysis_settings is None:
            global_analysis_settings = {
                "franchiseDeductible": False,
                "minLossThreshold": "1.00",
                "treatConstructionOccupancyAsUnknown": True,
                "numMaxLossEvent": 1
            }

        if currency is None:
            currency = build_analysis_currency_dict()

        model_profile_response = self.reference_data_manager.get_model_profile_by_name(analysis_profile_name)
        output_profile_response = self.reference_data_manager.get_output_profile_by_name(output_profile_name)
        event_rate_scheme_response = self.reference_data_manager.get_event_rate_scheme_by_name(event_rate_scheme_name)
        tag_ids = self.reference_data_manager.get_tag_ids_from_tag_names(tag_names)

        if model_profile_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Analysis profile '{analysis_profile_name}' not found")
        if len(output_profile_response) == 0:
            raise IRPReferenceDataError(f"Output profile '{output_profile_name}' not found")
        if event_rate_scheme_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Event rate scheme '{event_rate_scheme_name}' not found")

        data = {
            "currency": currency,
            "edm": edm_name,
            "eventRateSchemeId": event_rate_scheme_response['items'][0]['eventRateSchemeId'],
            "exposureType": "PORTFOLIO",
            "id": portfolio_id,
            "modelProfileId": model_profile_response['items'][0]['id'],
            "outputProfileId": output_profile_response[0]['id'],
            "treaties": treaty_ids,
            "tagIds": tag_ids,
            "globalAnalysisSettings": global_analysis_settings,
            "jobName": job_name
        }

        print(json.dumps(data, indent=2))

        response = self.client.request('POST', ANALYZE_PORTFOLIO.format(portfolio_id=portfolio_id), json=data)
        workflow_id = extract_id_from_location_header(response, "analysis job submission")
        return int(workflow_id)
    
    def poll_analysis_job_batch(self, workflow_ids: List[int]) -> Dict[str, Any]:
        """
        Poll batch of analysis workflow jobs until completion.

        Args:
            workflow_ids: List of workflow IDs to poll

        Returns:
            Dict containing all workflow results

        Raises:
            IRPValidationError: If workflow_ids is empty
            IRPWorkflowError: If workflows time out
        """
        validate_list_not_empty(workflow_ids, "workflow_ids")

        response = self.client.poll_workflow_batch(workflow_ids)
        return response.json()

    def analyze_portfolio(
        self,
        job_name: str,
        edm_name: str,
        portfolio_id: int,
        model_profile_id: int,
        output_profile_id: int,
        event_rate_scheme_id: int,
        treaty_ids: List[int],
        *,
        global_analysis_settings: Optional[Dict[str, Any]] = None,
        currency: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Analyze portfolio using profile IDs (submits and waits for completion).

        Args:
            job_name: Name for analysis job
            edm_name: Name of EDM datasource
            portfolio_id: Portfolio ID
            model_profile_id: Model profile ID
            output_profile_id: Output profile ID
            event_rate_scheme_id: Event rate scheme ID
            treaty_ids: List of treaty IDs
            global_analysis_settings: Optional analysis settings dict
            currency: Optional currency dict (uses default if not provided)

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(job_name, "job_name")
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_positive_int(model_profile_id, "model_profile_id")
        validate_positive_int(output_profile_id, "output_profile_id")
        validate_positive_int(event_rate_scheme_id, "event_rate_scheme_id")
        validate_list_not_empty(treaty_ids, "treaty_ids")

        if global_analysis_settings is None:
            global_analysis_settings = {
                "franchiseDeductible": False,
                "minLossThreshold": "1.00",
                "treatConstructionOccupancyAsUnknown": True,
                "numMaxLossEvent": 1
            }

        if currency is None:
            currency = build_analysis_currency_dict()

        data = {
            "currency": currency,
            "edm": edm_name,
            "eventRateSchemeId": event_rate_scheme_id,
            "exposureType": "PORTFOLIO",
            "id": portfolio_id,
            "modelProfileId": model_profile_id,
            "outputProfileId": output_profile_id,
            "treaties": treaty_ids,
            "globalAnalysisSettings": global_analysis_settings,
            "jobName": job_name
        }

        response = self.client.execute_workflow('POST', ANALYZE_PORTFOLIO.format(portfolio_id=portfolio_id), json=data)
        return response.json()

    def execute_analysis(
        self,
        job_name: str,
        edm_name: str,
        portfolio_id: int,
        analysis_profile_name: str,
        output_profile_name: str,
        event_rate_scheme_name: str,
        treaty_ids: List[int]
    ) -> Dict[str, Any]:
        """
        Execute analysis using profile names (submits and waits for completion).

        This is a convenience method that looks up profile IDs by name
        and then calls analyze_portfolio.

        Args:
            job_name: Name for analysis job
            edm_name: Name of EDM datasource
            portfolio_id: Portfolio ID
            analysis_profile_name: Analysis profile name
            output_profile_name: Output profile name
            event_rate_scheme_name: Event rate scheme name
            treaty_ids: List of treaty IDs

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPReferenceDataError: If reference data not found
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(job_name, "job_name")
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_non_empty_string(analysis_profile_name, "analysis_profile_name")
        validate_non_empty_string(output_profile_name, "output_profile_name")
        validate_non_empty_string(event_rate_scheme_name, "event_rate_scheme_name")
        validate_list_not_empty(treaty_ids, "treaty_ids")

        model_profile_response = self.reference_data_manager.get_model_profile_by_name(analysis_profile_name)
        output_profile_response = self.reference_data_manager.get_output_profile_by_name(output_profile_name)
        event_rate_scheme_response = self.reference_data_manager.get_event_rate_scheme_by_name(event_rate_scheme_name)

        if model_profile_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Analysis profile '{analysis_profile_name}' not found")
        if len(output_profile_response) == 0:
            raise IRPReferenceDataError(f"Output profile '{output_profile_name}' not found")
        if event_rate_scheme_response.get('count', 0) == 0:
            raise IRPReferenceDataError(f"Event rate scheme '{event_rate_scheme_name}' not found")

        return self.analyze_portfolio(
            job_name,
            edm_name,
            portfolio_id,
            model_profile_response['items'][0]['id'],
            output_profile_response[0]['id'],
            event_rate_scheme_response['items'][0]['eventRateSchemeId'],
            treaty_ids
        )
    
    def create_analysis_group(
        self,
        analysis_ids: List[int],
        group_name: str,
        *,
        simulate_to_plt: bool = True,
        num_simulations: int = 50000,
        propogate_detailed_losses: bool = False,
        reporting_window_start: str = "01/01/2021",
        simulation_window_start: str = "01/01/2021",
        simulation_window_end: str = "12/31/2021",
        region_peril_simulation_set: Optional[List[Dict[str, Any]]] = None,
        description: str = "",
        currency: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create analysis group for aggregate portfolio analysis.

        Args:
            analysis_ids: List of analysis IDs to group
            group_name: Name for the analysis group
            simulate_to_plt: Enable PLT simulation (default: True)
            num_simulations: Number of simulations (default: 50000)
            propogate_detailed_losses: Propagate detailed losses (default: False)
            reporting_window_start: Reporting window start date (default: "01/01/2021")
            simulation_window_start: Simulation window start date (default: "01/01/2021")
            simulation_window_end: Simulation window end date (default: "12/31/2021")
            region_peril_simulation_set: Optional region/peril simulation set
            description: Group description (default: "")
            currency: Optional currency dict (uses default if not provided)

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_list_not_empty(analysis_ids, "analysis_ids")
        validate_non_empty_string(group_name, "group_name")
        validate_positive_int(num_simulations, "num_simulations")
        validate_non_empty_string(reporting_window_start, "reporting_window_start")
        validate_non_empty_string(simulation_window_start, "simulation_window_start")
        validate_non_empty_string(simulation_window_end, "simulation_window_end")

        if region_peril_simulation_set is None:
            region_peril_simulation_set = []

        if currency is None:
            currency = build_analysis_currency_dict()

        data = {
            "analysisIds": analysis_ids,
            "name": group_name,
            "currency": currency,
            "simulateToPLT": simulate_to_plt,
            "numOfSimulations": num_simulations,
            "propagateDetailedLosses": propogate_detailed_losses,
            "reportingWindowStart": reporting_window_start,
            "simulationWindowStart": simulation_window_start,
            "simulationWindowEnd": simulation_window_end,
            "regionPerilSimulationSet": region_peril_simulation_set,
            "description": description
        }

        print(json.dumps(data, indent=2))

        response = self.client.execute_workflow('POST', CREATE_ANALYSIS_GROUP, json=data)
        return response.json()
        
