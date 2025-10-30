"""
Reference data management operations.

Handles retrieval and creation of reference data including
model profiles, output profiles, event rate schemes, currencies, and tags.
"""

from typing import Dict, List, Any
from .client import Client
from .constants import GET_CURRENCIES, GET_TAGS, CREATE_TAG, GET_MODEL_PROFILES, GET_OUTPUT_PROFILES, GET_EVENT_RATE_SCHEME
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_list_not_empty
from .utils import extract_id_from_location_header, get_nested_field

class ReferenceDataManager:
    """Manager for reference data operations."""

    def __init__(self, client: Client) -> None:
        """
        Initialize reference data manager.

        Args:
            client: IRP API client instance
        """
        self.client = client

    def get_model_profiles(self) -> Dict[str, Any]:
        """
        Retrieve all model profiles.

        Returns:
            Dict containing model profile list

        Raises:
            IRPAPIError: If request fails
        """
        try:
            response = self.client.request('GET', GET_MODEL_PROFILES)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get model profiles: {e}")

    def get_model_profile_by_name(self, profile_name: str) -> Dict[str, Any]:
        """
        Retrieve model profile by name.

        Args:
            profile_name: Model profile name

        Returns:
            Dict containing model profile details

        Raises:
            IRPValidationError: If profile_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(profile_name, "profile_name")

        params = {'name': profile_name}

        try:
            response = self.client.request('GET', GET_MODEL_PROFILES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get model profile '{profile_name}': {e}")

    def get_output_profiles(self) -> List[Dict[str, Any]]:
        """
        Retrieve all output profiles.

        Returns:
            Dict containing output profile list

        Raises:
            IRPAPIError: If request fails
        """
        try:
            response = self.client.request('GET', GET_OUTPUT_PROFILES)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get output profiles: {e}")

    def get_output_profile_by_name(self, profile_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve output profile by name.

        Args:
            profile_name: Output profile name

        Returns:
            Dict containing output profile details

        Raises:
            IRPValidationError: If profile_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(profile_name, "profile_name")

        params = {'name': profile_name}

        try:
            response = self.client.request('GET', GET_OUTPUT_PROFILES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get output profile '{profile_name}': {e}")

    def get_event_rate_schemes(self) -> Dict[str, Any]:
        """
        Retrieve all active event rate schemes.

        Returns:
            Dict containing event rate scheme list

        Raises:
            IRPAPIError: If request fails
        """
        params = {'where': 'isActive=True'}

        try:
            response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get event rate schemes: {e}")

    def get_event_rate_scheme_by_name(self, scheme_name: str) -> Dict[str, Any]:
        """
        Retrieve event rate scheme by name.

        Args:
            scheme_name: Event rate scheme name

        Returns:
            Dict containing event rate scheme details

        Raises:
            IRPValidationError: If scheme_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(scheme_name, "scheme_name")

        params = {'where': f"eventRateSchemeName=\"{scheme_name}\""}

        try:
            response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get event rate scheme '{scheme_name}': {e}")

    def get_currencies(self) -> Dict[str, Any]:
        """
        Retrieve all available currencies.

        Returns:
            Dict containing currency list

        Raises:
            IRPAPIError: If request fails
        """
        params = {"fields": "code,name"}

        try:
            response = self.client.request('GET', GET_CURRENCIES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get currencies: {e}")

    def get_tag_by_name(self, tag_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve tag by name.

        Args:
            tag_name: Tag name

        Returns:
            List of dicts containing tag details

        Raises:
            IRPValidationError: If tag_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(tag_name, "tag_name")

        params = {
            "isActive": True,
            "filter": f"TAGNAME = '{tag_name}'"
        }

        try:
            response = self.client.request('GET', GET_TAGS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get tag '{tag_name}': {e}")

    def create_tag(self, tag_name: str) -> Dict[str, str]:
        """
        Create new tag.

        Args:
            tag_name: Tag name

        Returns:
            Dict with tag ID

        Raises:
            IRPValidationError: If tag_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(tag_name, "tag_name")

        data = {"tagName": tag_name}

        try:
            response = self.client.request('POST', CREATE_TAG, json=data)
            tag_id = extract_id_from_location_header(response, "tag creation")
            return {"id": tag_id}
        except Exception as e:
            raise IRPAPIError(f"Failed to create tag '{tag_name}': {e}")

    def get_tag_ids_from_tag_names(self, tag_names: List[str]) -> List[int]:
        """
        Get or create tags by names and return their IDs.

        This method will create tags if they don't already exist.

        Args:
            tag_names: List of tag names

        Returns:
            List of tag IDs

        Raises:
            IRPValidationError: If tag_names is empty
            IRPAPIError: If request fails
        """
        validate_list_not_empty(tag_names, "tag_names")

        tag_ids = []
        for tag_name in tag_names:
            tag_search_response = self.get_tag_by_name(tag_name)
            if len(tag_search_response) > 0:
                tag_id = get_nested_field(
                    tag_search_response, 0, 'tagId',
                    required=True,
                    context=f"tag search response for '{tag_name}'"
                )
                tag_ids.append(int(tag_id))
            else:
                created_tag = self.create_tag(tag_name)
                tag_id = get_nested_field(
                    created_tag, 'id',
                    required=True,
                    context=f"created tag response for '{tag_name}'"
                )
                tag_ids.append(int(tag_id))

        return tag_ids