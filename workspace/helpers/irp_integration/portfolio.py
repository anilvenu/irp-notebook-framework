"""
Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.
"""

import json
import time
from typing import Dict, Any, List, Optional
from .client import Client
from .constants import CREATE_PORTFOLIO, GET_GEOHAZ_JOB, SEARCH_PORTFOLIOS, GEOHAZ_PORTFOLIO, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES
from .exceptions import IRPAPIError, IRPJobError
from .validators import validate_list_not_empty, validate_non_empty_string, validate_positive_int
from .utils import extract_id_from_location_header

class PortfolioManager:
    """Manager for portfolio operations."""

    def __init__(self, client: Client, edm_manager: Optional[Any] = None) -> None:
        """
        Initialize portfolio manager.

        Args:
            client: IRP API client instance
        """
        self.client = client
        self._edm_manager = edm_manager

    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager

    
    def search_portfolios(self, exposure_id: int, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search portfolios within an exposure.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string for portfolio names

        Returns:
            Dict containing list of portfolios
        """
        validate_positive_int(exposure_id, "exposure_id")

        params = {}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request(
                'GET',
                SEARCH_PORTFOLIOS.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search portfolios for exposure ID '{exposure_id}': {e}")


    def create_portfolios(self, portfolio_data_list: List[Dict[str, Any]]) -> List[int]:
        validate_list_not_empty(portfolio_data_list, "portfolio_data_list")

        portfolio_ids = []
        for portfolio_data in portfolio_data_list:
            try:
                edm_name = portfolio_data['edm_name']
                portfolio_name = portfolio_data['portfolio_name']
                portfolio_number = portfolio_data['portfolio_number']
                description = portfolio_data['description']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing value in create portfolio data: {e}"
                ) from e
            
            portfolio_ids.append(self.create_portfolio(
                edm_name=edm_name,
                portfolio_name=portfolio_name,
                portfolio_number=portfolio_number,
                description=description
            ))
        
        return portfolio_ids


    def create_portfolio(
        self,
        edm_name: str,
        portfolio_name: str,
        portfolio_number: str = "1",
        description: str = ""
    ) -> int:
        """
        Create new portfolio in EDM.

        Args:
            exposure_id: ID of EDM datasource
            portfolio_name: Name for new portfolio
            portfolio_number: Portfolio number (default: "1")
            description: Portfolio description (default: "")

        Returns:
            Portfolio ID of created portfolio

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(portfolio_number, "portfolio_number")

        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if (len(edms) != 1):
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        exposure_id = edms[0]['exposureId']

        portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if (len(portfolios) > 0):
            raise IRPAPIError(f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name")

        data = {
            "portfolioName": portfolio_name,
            "portfolioNumber": portfolio_number[:20],
            "description": description,
        }

        try:
            response = self.client.request('POST', CREATE_PORTFOLIO.format(exposureId=exposure_id), json=data)
            portfolio_id = extract_id_from_location_header(response, "portfolio creation")
            return int(portfolio_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create portfolio '{portfolio_name}' in exposure id '{exposure_id}': {e}")


    def submit_geohaz_jobs(self, geohaz_data_list: List[Dict[str, Any]]) -> List[int]:
        validate_list_not_empty(geohaz_data_list, "geohaz_data_list")

        job_ids = []
        for geohaz_data in geohaz_data_list:
            try:
                edm_name = geohaz_data['edm_name']
                portfolio_name = geohaz_data['portfolio_name']
                version = geohaz_data['version']
                hazard_eq = geohaz_data['hazard_eq']
                hazard_ws = geohaz_data['hazard_ws']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing geohaz job data: {e}"
                ) from e
            
            edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
            if (len(edms) != 1):
                raise Exception(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
            try:
                exposure_id = edms[0]['exposureId']
            except (KeyError, TypeError, IndexError) as e:
                raise IRPAPIError(
                    f"Failed to extract exposure ID: {e}"
                ) from e

            portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
            if (len(portfolios) != 1):
                raise Exception(f"Expected 1 portfolio with name {portfolio_name}, found {len(portfolios)}")
            try:
                portfolio_uri = portfolios[0]['uri']
            except (KeyError, TypeError, IndexError) as e:
                raise IRPAPIError(
                    f"Failed to extract portfolio URI: {e}"
                ) from e
            
            job_ids.append(self.submit_geohaz_job(
                portfolio_uri=portfolio_uri,
                version=version,
                hazard_eq=hazard_eq,
                hazard_ws=hazard_ws
            ))

        return job_ids
        

    def submit_geohaz_job(self,
                          portfolio_uri: str,
                          version: str = "22.0",
                          hazard_eq: bool = False,
                          hazard_ws: bool = False,
                          geocode_layer_options: Optional[Dict[str, Any]] = None,
                          hazard_layer_options: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Execute geocoding and/or hazard operations on portfolio.

        Args:
            portfolio_uri: URI of the portfolio
            hazard_eq: Enable earthquake hazard (default: False)
            hazard_ws: Enable windstorm hazard (default: False)

        Returns:
            Job ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If workflow fails or times out
        """
        validate_non_empty_string(portfolio_uri, "portfolio_uri")
        if geocode_layer_options is None:
            geocode_layer_options = {
                "aggregateTriggerEnabled": "true",
                "geoLicenseType": "0",
                "skipPrevGeocoded": False
            }

        if hazard_layer_options is None:
            hazard_layer_options = {
                "overrideUserDef": False,
                "skipPrevHazard": False
            }

        data = {
            "resourceUri": portfolio_uri,
            "resourceType": "portfolio",
            "settings": {
                "layers": [
                    {
                        "type": "geocode",
                        "name": "geocode",
                        "engineType": "RL",
                        "version": version,
                        "layerOptions": geocode_layer_options
                    }
                ]
            }
        }

        if hazard_eq:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "earthquake",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        if hazard_ws:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "windstorm",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        try:
            response = self.client.request(
                'POST',
                GEOHAZ_PORTFOLIO,
                json=data
            )
            job_id = extract_id_from_location_header(response, "portfolio geohaz")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to execute geohaz for portfolio '{portfolio_uri}': {e}")
        
    
    def get_geohaz_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve geohaz job status by job ID.

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
            response = self.client.request('GET', GET_GEOHAZ_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get geohaz job status for job ID {job_id}: {e}")


    def poll_geohaz_job_to_completion(
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
            print(f"Polling GeoHaz job ID {job_id}")
            job_data = self.get_geohaz_job(job_id)
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
                    f"GeoHaz job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_geohaz_job_batch_to_completion(
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
            print(f"Polling batch geohaz job ids: {','.join(str(item) for item in job_ids)}")

            all_completed = False
            all_jobs = []
            for job_id in job_ids:
                workflow_response = self.get_geohaz_job(job_id)
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
                    f"Batch geohaz jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)