"""
Treaty Manager for IRP Integration.

Handles treaty-related operations including creation, retrieval,
and Line of Business (LOB) assignments.
"""

from typing import Dict, List, Any, Optional
from .client import Client
from .constants import (
    GET_TREATIES,
    CREATE_TREATY,
    ASSIGN_TREATY_LOBS,
    GET_TREATY_TYPES,
    GET_TREATY_ATTACHMENT_BASES,
    GET_TREATY_ATTACHMENT_LEVELS
)
from .exceptions import IRPAPIError, IRPValidationError, IRPReferenceDataError
from .validators import validate_non_empty_string, validate_positive_int
from .utils import find_reference_data_by_name, extract_id_from_location_header


class TreatyManager:
    """Manager for treaty operations."""

    def __init__(self, client: Client, edm_manager=None, reference_data_manager=None):
        """
        Initialize Treaty Manager.

        Args:
            client: Client instance for API requests
            edm_manager: Optional EDMManager instance (lazy-loaded if None)
            reference_data_manager: Optional ReferenceDataManager instance (lazy-loaded if None)
        """
        self.client = client
        self._edm_manager = edm_manager
        self._reference_data_manager = reference_data_manager

    @property
    def edm_manager(self):
        """Lazy-load EDMManager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager

    @property
    def reference_data_manager(self):
        """Lazy-load ReferenceDataManager to avoid circular imports."""
        if self._reference_data_manager is None:
            from .reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager

    def get_treaties_by_edm(self, edm_name: str, limit: int = 100) -> Dict[str, Any]:
        """
        Get all treaties for an EDM.

        Args:
            edm_name: EDM name
            limit: Maximum number of treaties to return (default: 100)

        Returns:
            Dict with treaty search results

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(limit, "limit")

        params = {
            "datasource": edm_name,
            "limit": limit
        }

        try:
            response = self.client.request('GET', GET_TREATIES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get treaties: {e}")

    def get_treaty_types_by_edm(
        self,
        edm_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get available treaty types for an EDM.

        Args:
            edm_name: EDM name
            limit: Maximum number of types to return (default: 100)

        Returns:
            Dict with treaty types data

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(limit, "limit")

        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": limit
        }

        try:
            response = self.client.request('GET', GET_TREATY_TYPES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get treaty types: {e}")

    def get_treaty_attachment_bases_by_edm(
        self,
        edm_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get available treaty attachment bases for an EDM.

        Args:
            edm_name: EDM name
            limit: Maximum number to return (default: 100)

        Returns:
            Dict with attachment bases data

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(limit, "limit")

        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": limit
        }

        try:
            response = self.client.request('GET', GET_TREATY_ATTACHMENT_BASES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get treaty attachment bases: {e}")

    def get_treaty_attachment_levels_by_edm(
        self,
        edm_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get available treaty attachment levels for an EDM.

        Args:
            edm_name: EDM name
            limit: Maximum number to return (default: 100)

        Returns:
            Dict with attachment levels data

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(limit, "limit")

        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": limit
        }

        try:
            response = self.client.request('GET', GET_TREATY_ATTACHMENT_LEVELS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get treaty attachment levels: {e}")

    def create_treaty(
        self,
        edm_name: str,
        treaty_data: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Create treaty with provided data dict (low-level method).

        This method creates a treaty with provided data dict.
        For a simplified approach with automatic reference data lookup,
        use create_treaty_from_names() instead.

        Args:
            edm_name: Target EDM name
            treaty_data: Treaty data dict with all required fields

        Returns:
            Dict with 'id' (treaty ID)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If treaty creation fails
        """
        validate_non_empty_string(edm_name, "edm_name")

        if not isinstance(treaty_data, dict):
            raise IRPValidationError(
                f"treaty_data must be a dict, got {type(treaty_data).__name__}"
            )

        # Make a copy to avoid modifying the original
        data = treaty_data.copy()

        # Truncate treaty number if it exists
        if "treatyNumber" in data:
            data["treatyNumber"] = str(data["treatyNumber"])[:20]

        params = {"datasource": edm_name}

        try:
            response = self.client.request('POST', CREATE_TREATY, params=params, json=data)
            treaty_id = extract_id_from_location_header(response, "treaty creation response")
            return {'id': treaty_id}
        except IRPAPIError:
            raise
        except Exception as e:
            raise IRPAPIError(f"Failed to create treaty: {e}")

    def assign_lobs(
        self,
        edm_name: str,
        treaty_id: str,
        lob_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Assign Lines of Business (LOBs) to a treaty.

        Args:
            edm_name: EDM name
            treaty_id: Treaty ID
            lob_ids: List of LOB IDs to assign

        Returns:
            Response from LOB assignment

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If assignment fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(treaty_id, "treaty_id")

        if not isinstance(lob_ids, list):
            raise IRPValidationError(
                f"lob_ids must be a list, got {type(lob_ids).__name__}"
            )

        if not lob_ids:
            raise IRPValidationError("lob_ids cannot be empty")

        params = {"datasource": edm_name}

        # Build batch request data
        data = []
        for lob_id in lob_ids:
            body_value = {"id": lob_id}
            item = {
                "body": f"{body_value}",
                "method": "POST",
                "path": f"/{treaty_id}/lob"
            }
            data.append(item)

        try:
            response = self.client.request('POST', ASSIGN_TREATY_LOBS, params=params, json=data)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to assign LOBs to treaty: {e}")

    def create_treaty_from_names(
        self,
        edm_name: str,
        treaty_name: str,
        treaty_type_name: str,
        currency_name: str,
        attachment_basis_name: str,
        attachment_level_name: str,
        risk_limit: float,
        occur_limit: float,
        attach_pt: float,
        pcnt_covered: float,
        pcnt_placed: float,
        pcnt_ri_share: float,
        pcnt_retent: float,
        premium: float,
        num_of_reinst: int,
        reinst_charge: float,
        aggregate_limit: float,
        aggregate_deductible: float,
        priority: int,
        effect_date: str,
        expire_date: str,
        auto_assign_lobs: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create treaty with automatic reference data lookup (convenience method).

        This method handles:
        1. Fetching cedants from EDM
        2. Fetching LOBs from EDM
        3. Looking up treaty type by name
        4. Looking up attachment basis by name
        5. Looking up attachment level by name
        6. Looking up currency by name
        7. Creating treaty with all assembled data
        8. Optionally assigning LOBs to treaty

        Args:
            edm_name: Target EDM name
            treaty_name: Treaty name (truncated to 20 chars if needed)
            treaty_type_name: Treaty type name (e.g., "Working Excess")
            currency_name: Currency name (e.g., "US Dollar")
            attachment_basis_name: Attachment basis name (e.g., "Losses Occurring")
            attachment_level_name: Attachment level name (e.g., "Location")
            risk_limit: Risk limit amount
            occur_limit: Occurrence limit amount
            attach_pt: Attachment point amount
            pcnt_covered: Percent covered
            pcnt_placed: Percent placed
            pcnt_ri_share: Percent RI share
            pcnt_retent: Percent retention
            premium: Premium amount
            num_of_reinst: Number of reinstatements
            reinst_charge: Reinstatement charge
            aggregate_limit: Aggregate limit
            aggregate_deductible: Aggregate deductible
            priority: Priority
            effect_date: Effective date (ISO format)
            expire_date: Expiration date (ISO format)
            auto_assign_lobs: Automatically assign all LOBs to treaty (default: True)
            **kwargs: Additional treaty fields

        Returns:
            Dict with 'id' (treaty ID) and optionally 'lob_assignment' if auto_assign_lobs=True

        Raises:
            IRPValidationError: If parameters are invalid
            IRPReferenceDataError: If required reference data not found
            IRPAPIError: If API calls fail
        """
        # Validate required inputs
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(treaty_name, "treaty_name")
        validate_non_empty_string(treaty_type_name, "treaty_type_name")
        validate_non_empty_string(currency_name, "currency_name")
        validate_non_empty_string(attachment_basis_name, "attachment_basis_name")
        validate_non_empty_string(attachment_level_name, "attachment_level_name")
        validate_non_empty_string(effect_date, "effect_date")
        validate_non_empty_string(expire_date, "expire_date")

        # Fetch required reference data
        cedant_data = self.edm_manager.get_cedants_by_edm(edm_name)["searchItems"]
        if not cedant_data:
            raise IRPReferenceDataError(f"No cedants found in EDM '{edm_name}'")

        lob_data = self.edm_manager.get_lobs_by_edm(edm_name)["searchItems"]
        if not lob_data:
            raise IRPReferenceDataError(f"No LOBs found in EDM '{edm_name}'")

        # Look up treaty type
        treaty_type_data = self.get_treaty_types_by_edm(edm_name)
        treaty_type = find_reference_data_by_name(
            treaty_type_data["entityItems"]["values"],
            treaty_type_name,
            data_type="treaty type"
        )

        # Look up attachment basis
        attachment_basis_data = self.get_treaty_attachment_bases_by_edm(edm_name)
        attachment_basis = find_reference_data_by_name(
            attachment_basis_data["entityItems"]["values"],
            attachment_basis_name,
            data_type="attachment basis"
        )

        # Look up attachment level
        attachment_level_data = self.get_treaty_attachment_levels_by_edm(edm_name)
        attachment_level = find_reference_data_by_name(
            attachment_level_data["entityItems"]["values"],
            attachment_level_name,
            data_type="attachment level"
        )

        # Look up currency
        currency_data = self.reference_data_manager.get_currencies()
        currency = find_reference_data_by_name(
            currency_data["entityItems"]["values"],
            currency_name,
            data_type="currency"
        )

        # Build treaty data
        treaty_data = {
            "treatyNumber": treaty_name[:20],  # Truncate to 20 chars
            "treatyName": treaty_name,
            "treatyType": treaty_type,
            "riskLimit": risk_limit,
            "occurLimit": occur_limit,
            "attachPt": attach_pt,
            "cedant": cedant_data[0],
            "effectDate": effect_date,
            "expireDate": expire_date,
            "currency": {'code': currency['code'], 'name': currency['name']},
            "attachBasis": attachment_basis,
            "attachLevel": attachment_level,
            "pcntCovered": pcnt_covered,
            "pcntPlaced": pcnt_placed,
            "pcntRiShare": pcnt_ri_share,
            "pcntRetent": pcnt_retent,
            "premium": premium,
            "numOfReinst": num_of_reinst,
            "reinstCharge": reinst_charge,
            "aggregateLimit": aggregate_limit,
            "aggregateDeductible": aggregate_deductible,
            "priority": priority,
            "retentAmt": "",
            "isValid": True,
            "userId1": "",
            "userId2": "",
            "maolAmount": "",
            "lobs": lob_data,
            "tagIds": []
        }

        # Add any additional kwargs
        treaty_data.update(kwargs)

        # Create treaty
        treaty_response = self.create_treaty(edm_name, treaty_data)

        result = {
            'id': treaty_response['id'],
            'treaty_name': treaty_name
        }

        # Optionally assign LOBs
        if auto_assign_lobs:
            lob_ids = [lob['id'] for lob in lob_data]
            lob_assignment = self.assign_lobs(
                edm_name,
                treaty_response['id'],
                lob_ids
            )
            result['lob_assignment'] = lob_assignment
            result['lobs_assigned'] = len(lob_ids)

        return result
