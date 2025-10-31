"""
Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.
"""

from typing import Dict, Any, Optional
from .client import Client
from .constants import CREATE_PORTFOLIO, PORTFOLIO_GEOHAZ, SEARCH_PORTFOLIOS
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_positive_int
from .utils import extract_id_from_location_header

class PortfolioManager:
    """Manager for portfolio operations."""

    def __init__(self, client: Client) -> None:
        """
        Initialize portfolio manager.

        Args:
            client: IRP API client instance
        """
        self.client = client

    
    def search_portfolios(self, exposure_id: int, filter: str = "") -> Dict[str, Any]:
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


    def create_portfolio(
        self,
        exposure_id: int,
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
        validate_positive_int(exposure_id, "exposure_id")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(portfolio_number, "portfolio_number")

        data = {
            "portfolioName": portfolio_name,
            "portfolioNumber": portfolio_number,
            "description": description,
        }

        try:
            response = self.client.request('POST', CREATE_PORTFOLIO.format(exposureId=exposure_id), json=data)
            portfolio_id = extract_id_from_location_header(response, "portfolio creation")
            return int(portfolio_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create portfolio '{portfolio_name}' in exposure id '{exposure_id}': {e}")


    def geohaz_portfolio(
        self,
        edm_name: str,
        portfolio_id: int,
        *,
        geocode: bool = True,
        hazard_eq: bool = False,
        hazard_ws: bool = False,
        engine_type: str = "RL",
        version: str = "22.0",
        geocode_layer_options: Optional[Dict[str, Any]] = None,
        hazard_layer_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute geocoding and/or hazard operations on portfolio.

        Args:
            edm_name: Name of EDM datasource
            portfolio_id: Portfolio ID
            geocode: Enable geocoding (default: True)
            hazard_eq: Enable earthquake hazard (default: False)
            hazard_ws: Enable windstorm hazard (default: False)
            engine_type: Engine type (default: "RL")
            version: Engine version (default: "22.0")
            geocode_layer_options: Geocoding options dict
            hazard_layer_options: Hazard options dict

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_non_empty_string(engine_type, "engine_type")
        validate_non_empty_string(version, "version")

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

        params = {"datasource": edm_name}
        data = []

        if geocode:
            data.append({
                "name": "geocode",
                "type": "geocode",
                "engineType": engine_type,
                "version": version,
                "layerOptions": geocode_layer_options,
            })

        if hazard_eq:
            data.append({
                "name": "earthquake",
                "type": "hazard",
                "engineType": engine_type,
                "version": version,
                "layerOptions": hazard_layer_options
            })

        if hazard_ws:
            data.append({
                "name": "windstorm",
                "type": "hazard",
                "engineType": engine_type,
                "version": version,
                "layerOptions": hazard_layer_options
            })

        try:
            response = self.client.execute_workflow(
                'POST',
                PORTFOLIO_GEOHAZ.format(portfolio_id=portfolio_id),
                params=params,
                json=data
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to execute geohaz for portfolio {portfolio_id} in EDM '{edm_name}': {e}")