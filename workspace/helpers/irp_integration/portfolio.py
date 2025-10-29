"""
Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.
"""

from typing import Dict, Any, Optional
from .client import Client
from .constants import CREATE_PORTFOLIO, GET_PORTFOLIOS, GET_PORTFOLIO_BY_ID, PORTFOLIO_GEOHAZ
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

    def create_portfolio(
        self,
        edm_name: str,
        portfolio_name: str,
        portfolio_number: str = "1",
        description: str = ""
    ) -> Dict[str, int]:
        """
        Create new portfolio in EDM.

        Args:
            edm_name: Name of EDM datasource
            portfolio_name: Name for new portfolio
            portfolio_number: Portfolio number (default: "1")
            description: Portfolio description (default: "")

        Returns:
            Dict with portfolio ID

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(portfolio_number, "portfolio_number")

        params = {"datasource": edm_name}
        data = {
            "name": portfolio_name,
            "number": portfolio_number,
            "description": description,
        }

        try:
            response = self.client.request('POST', CREATE_PORTFOLIO, params=params, json=data)
            portfolio_id = extract_id_from_location_header(response, "portfolio creation")
            return {'id': int(portfolio_id)}
        except Exception as e:
            raise IRPAPIError(f"Failed to create portfolio '{portfolio_name}' in EDM '{edm_name}': {e}")

    def get_portfolios_by_edm_name(self, edm_name: str) -> Dict[str, Any]:
        """
        Retrieve all portfolios for an EDM.

        Args:
            edm_name: Name of EDM datasource

        Returns:
            Dict containing portfolio list

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")

        params = {"datasource": edm_name}

        try:
            response = self.client.request('GET', GET_PORTFOLIOS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolios for EDM '{edm_name}': {e}")

    def get_portfolio_by_edm_name_and_id(
        self,
        edm_name: str,
        portfolio_id: int
    ) -> Dict[str, Any]:
        """
        Retrieve specific portfolio by ID.

        Args:
            edm_name: Name of EDM datasource
            portfolio_id: Portfolio ID

        Returns:
            Dict containing portfolio details

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")

        params = {"datasource": edm_name}

        try:
            response = self.client.request('GET', GET_PORTFOLIO_BY_ID.format(portfolio_id=portfolio_id), params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolio {portfolio_id} from EDM '{edm_name}': {e}")
    
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