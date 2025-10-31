"""
EDM (Exposure Data Management) operations.

Handles datasource creation, duplication, deletion, and
associated data retrieval (cedants, LOBs).
"""

from typing import Dict, Any, List, Optional
from .client import Client
from .constants import SEARCH_DATABASE_SERVERS, SEARCH_EXPOSURE_SETS, CREATE_EXPOSURE_SET, SEARCH_EDMS, CREATE_EDM, UPGRADE_EDM_DATA_VERSION, DELETE_EDM, GET_CEDANTS, GET_LOBS
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_positive_int
from .utils import extract_id_from_location_header

class EDMManager:
    """Manager for EDM (Exposure Data Management) operations."""

    def __init__(self, client: Client, portfolio_manager: Optional[Any] = None) -> None:
        """
        Initialize EDM manager.

        Args:
            client: IRP API client instance
            portfolio_manager: Optional PortfolioManager instance
        """
        self.client = client
        self._portfolio_manager = portfolio_manager

    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager


    def search_database_servers(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search database servers.

        Args:
            filter: Optional filter string for server names

        Returns:
            Dict containing list of database servers
        """
        params = {}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_DATABASE_SERVERS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search database servers: {e}")


    def search_exposure_sets(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search exposure sets.

        Args:
            filter: Optional filter string for exposure set names

        Returns:
            Dict containing list of exposure sets
        """
        params = {}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_EXPOSURE_SETS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search exposure sets: {e}")
        
    
    def create_exposure_set(self, name: str) -> int:
        """
        Create a new exposure set.

        Args:
            name: Name of the exposure set

        Returns:
            The exposure set ID
        """
        validate_non_empty_string(name, "name")
        data = {"exposureSetName": name}
        try:
            response = self.client.request('POST', CREATE_EXPOSURE_SET, json=data)
            exposure_set_id = extract_id_from_location_header(response, "exposure set creation")
            return int(exposure_set_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create exposure set '{name}': {e}")


    def search_edms(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search EDMs (exposures).

        Args:
            filter: Optional filter string for EDM names

        Returns:
            Dict containing list of EDMs
        """
        params = {}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_EDMS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search EDMs: {e}")
        

    def submit_create_edm_job(self, exposure_set_id: int, edm_name: str, server_id: int) -> int:
        """
        Submit job to create a new EDM (exposure).

        Args:
            exposure_set_id: ID of the exposure set
            edm_name: Name of the EDM
            server_id: ID of the database server

        Returns:
            The EDM ID
        """
        validate_positive_int(exposure_set_id, "exposure_set_id")
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(server_id, "server_id")
        data = {
            "exposureName": edm_name,
            "serverId": server_id
        }
        try:
            response = self.client.request(
                'POST',
                CREATE_EDM.format(exposureSetId=exposure_set_id),
                json=data
            )
            job_id = extract_id_from_location_header(response, "EDM creation")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create EDM '{edm_name}': {e}")


    def submit_upgrade_edm_data_version_job(self, exposure_id: int, edm_version: str) -> int:
        """
        Submit job to upgrade EDM data version.

        Args:
            exposure_id: ID of the exposure (EDM)

        Returns:
            The job ID
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_non_empty_string(edm_version, "edm_version")
        try:
            data = {"edmDataVersion": edm_version}
            response = self.client.request(
                'POST',
                UPGRADE_EDM_DATA_VERSION.format(exposureId=exposure_id),
                json=data
            )
            job_id = extract_id_from_location_header(response, "EDM data version upgrade")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to upgrade EDM data version for exposure ID '{exposure_id}': {e}")


    def submit_delete_edm_job(self, exposure_id: int) -> int:
        """
        Submit job to delete an EDM (exposure).

        Args:
            exposure_id: ID of the exposure (EDM)

        Returns:
            The job ID
        """
        validate_positive_int(exposure_id, "exposure_id")
        try:
            response = self.client.request(
                'DELETE',
                DELETE_EDM.format(exposureId=exposure_id)
            )
            job_id = extract_id_from_location_header(response, "EDM deletion")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to delete EDM with exposure ID '{exposure_id}': {e}")


    def get_cedants_by_edm(self, exposure_id: int) -> Dict[str, Any]:
        """
        Retrieve cedants for an EDM.

        Args:
            exposure_id: Exposure ID

        Returns:
            Dict containing cedant list

        Raises:
            IRPValidationError: If exposure_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "edm_name")
        try:
            response = self.client.request('GET', GET_CEDANTS.format(exposureId=exposure_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get cedants for exposure ID '{exposure_id}': {e}")


    def get_lobs_by_edm(self, exposure_id: int) -> Dict[str, Any]:
        """
        Retrieve lines of business (LOBs) for an EDM.

        Args:
            edm_name: Name of EDM

        Returns:
            Dict containing LOB list

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "edm_name")
        try:
            response = self.client.request('GET', GET_LOBS.format(exposureId=exposure_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get LOBs for exposure ID '{exposure_id}': {e}")
