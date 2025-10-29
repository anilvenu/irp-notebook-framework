"""
EDM (Exposure Data Management) operations.

Handles datasource creation, duplication, deletion, and
associated data retrieval (cedants, LOBs).
"""

from typing import Dict, Any, Optional
from .client import Client
from .constants import GET_DATASOURCES, CREATE_DATASOURCE, DELETE_DATASOURCE, EXPORT_EDM, GET_CEDANTS, GET_LOBS
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string

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

    def get_all_edms(self) -> Dict[str, Any]:
        """
        Retrieve all EDMs (datasources).

        Returns:
            Dict containing list of EDMs

        Raises:
            IRPAPIError: If request fails
        """
        response = self.client.request('GET', GET_DATASOURCES)
        return response.json()

    def get_edm_by_name(self, edm_name: str) -> Dict[str, Any]:
        """
        Retrieve EDM by name.

        Args:
            edm_name: Name of the EDM datasource

        Returns:
            Dict containing EDM details

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")

        params = {"q": f"datasourceName={edm_name}"}
        response = self.client.request('GET', GET_DATASOURCES, params=params)
        return response.json()

    def create_edm(self, edm_name: str, server_name: str) -> Dict[str, Any]:
        """
        Create new EDM datasource.

        Args:
            edm_name: Name for the new datasource
            server_name: Server name for the datasource

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(server_name, "server_name")

        params = {
            "datasourcename": edm_name,
            "servername": server_name,
            "operation": "CREATE"
        }
        response = self.client.execute_workflow('POST', CREATE_DATASOURCE, params=params)
        return response.json()
    
    def duplicate_edm(
        self,
        source_edm_name: str,
        dest_edm_name: str = ""
    ) -> Dict[str, Any]:
        """
        Duplicate EDM with all portfolios and settings.

        Args:
            source_edm_name: Name of source EDM to duplicate
            dest_edm_name: Name for destination EDM (default: "np_{source_edm_name}")

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If source_edm_name is invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(source_edm_name, "source_edm_name")

        if not dest_edm_name:
            dest_edm_name = f"np_{source_edm_name}"

        portfolios_response = self.portfolio_manager.get_portfolios_by_edm_name(source_edm_name)
        portfolio_ids = [portfolio['id'] for portfolio in portfolios_response['searchItems']]

        data = {
            "createnew": True,
            "exportType": "EDM",
            "sourceDatasource": source_edm_name,
            "destinationDatasource": dest_edm_name,
            "exposureType": "PORTFOLIO",
            "exposureIds": portfolio_ids,
            "download": False,
            "exportFormat": "BAK",
            "exportOptions": {
                "exportAccounts": True,
                "exportLocations": True,
                "exportPerilDetailsInfo": True,
                "exportPolicies": True,
                "exportReinsuranceInfo": True,
                "exportTreaties": True
            },
            "preserveIds": True,
            "sqlVersion": 2019,
            "type": "ExposureExportInput"
        }

        response = self.client.execute_workflow('POST', EXPORT_EDM, json=data)
        return response.json()

    def upgrade_edm_version(self, edm_name: str) -> Dict[str, Any]:
        """
        Upgrade EDM data version.

        Args:
            edm_name: Name of EDM to upgrade

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(edm_name, "edm_name")

        params = {
            "datasourcename": edm_name,
            "operation": "EDM_DATA_UPGRADE"
        }
        response = self.client.execute_workflow('POST', CREATE_DATASOURCE, params=params)
        return response.json()

    def delete_edm(self, edm_name: str) -> Dict[str, Any]:
        """
        Delete EDM datasource.

        Args:
            edm_name: Name of EDM to delete

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(edm_name, "edm_name")

        response = self.client.execute_workflow('DELETE', DELETE_DATASOURCE.format(edm_name=edm_name))
        return response.json()

    def get_cedants_by_edm(self, edm_name: str) -> Dict[str, Any]:
        """
        Retrieve cedants for an EDM.

        Args:
            edm_name: Name of EDM

        Returns:
            Dict containing cedant list

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")

        params = {
            "fields": "id, name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_CEDANTS, params=params)
        return response.json()

    def get_lobs_by_edm(self, edm_name: str) -> Dict[str, Any]:
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
        validate_non_empty_string(edm_name, "edm_name")

        params = {
            "fields": "id, name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_LOBS, params=params)
        return response.json()
    