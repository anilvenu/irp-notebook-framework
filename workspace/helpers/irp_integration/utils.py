"""
Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction,
and reference data lookup operations.
"""

import base64
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import requests
from .exceptions import IRPAPIError, IRPReferenceDataError


def get_workspace_root() -> Path:
    """
    Get workspace root directory, working in both VS Code and JupyterLab.

    Returns:
        Path to workspace directory
    """
    cwd = Path.cwd()

    # If we're in workspace or below it, navigate up to workspace
    if 'workspace' in cwd.parts:
        workspace_index = cwd.parts.index('workspace')
        return Path(*cwd.parts[:workspace_index + 1])

    # Otherwise check if workspace exists as subdirectory
    if (cwd / 'workspace').exists():
        return cwd / 'workspace'

    raise IRPAPIError(f"Cannot find workspace directory from {cwd}")


def get_location_header(
    response: requests.Response,
    error_context: str = "response"
) -> str:
    """
    Get Location header from response.

    Args:
        response: HTTP response object

    Returns:
        Location header value, or empty string if not found
    """
    if 'location' not in response.headers:
        raise IRPAPIError(
            f"Location header missing from {error_context}"
        )
    return response.headers.get('location', '')


def extract_id_from_location_header(
    response: requests.Response,
    error_context: str = "response",
) -> str:
    """
    Extract ID from Location header in HTTP response.

    Args:
        response: HTTP response object
        error_context: Context description for error message

    Returns:
        Extracted ID string

    Raises:
        IRPAPIError: If Location header is missing
    """
    location = get_location_header(response, error_context)
    resource_id = location.split('/')[-1]
    if not resource_id:
        raise IRPAPIError(
            f"Could not extract ID from Location header: {location}"
        )
    return resource_id


def extract_workflow_url_from_response(response: requests.Response) -> str:
    """
    Extract workflow URL from Location header.

    Args:
        response: HTTP response object

    Returns:
        Full workflow URL

    Raises:
        IRPAPIError: If Location header is missing
    """
    if 'location' not in response.headers:
        raise IRPAPIError(
            "Location header missing from workflow submission response"
        )
    return response.headers['location']


def decode_base64_field(encoded_value: str, field_name: str) -> str:
    """
    Decode a base64-encoded field value.

    Args:
        encoded_value: Base64-encoded string
        field_name: Field name for error message

    Returns:
        Decoded string

    Raises:
        IRPAPIError: If decoding fails
    """
    try:
        return base64.b64decode(encoded_value).decode("utf-8")
    except Exception as e:
        raise IRPAPIError(
            f"Failed to decode base64 field '{field_name}': {e}"
        )


def decode_mri_credentials(response_json: Dict[str, Any]) -> Dict[str, str]:
    """
    Decode base64 credentials from MRI import file credentials response.

    Args:
        response_json: Response JSON containing encoded credentials

    Returns:
        Dict with decoded credential fields

    Raises:
        IRPAPIError: If required fields missing or decoding fails
    """
    required_fields = ['accessKeyId', 'secretAccessKey', 'sessionToken', 's3Path', 's3Region']
    missing = [f for f in required_fields if f not in response_json]
    if missing:
        raise IRPAPIError(
            f"MRI credentials response missing fields: {', '.join(missing)}"
        )

    try:
        return {
            'aws_access_key_id': decode_base64_field(response_json['accessKeyId'], 'accessKeyId'),
            'aws_secret_access_key': decode_base64_field(response_json['secretAccessKey'], 'secretAccessKey'),
            'aws_session_token': decode_base64_field(response_json['sessionToken'], 'sessionToken'),
            's3_path': decode_base64_field(response_json['s3Path'], 's3Path'),
            's3_region': decode_base64_field(response_json['s3Region'], 's3Region')
        }
    except IRPAPIError:
        raise
    except Exception as e:
        raise IRPAPIError(f"Failed to decode MRI credentials: {e}")


def find_reference_data_by_name(
    data_list: List[Dict[str, Any]],
    target_name: str,
    name_field: str = "name",
    data_type: str = "reference data"
) -> Dict[str, Any]:
    """
    Find reference data item by name from a list.

    Args:
        data_list: List of reference data dicts
        target_name: Name to search for
        name_field: Field name containing the name (default: "name")
        data_type: Type description for error message

    Returns:
        Matching reference data dict

    Raises:
        IRPReferenceDataError: If item not found or list is empty
    """
    if not data_list:
        raise IRPReferenceDataError(
            f"No {data_type} available to search"
        )

    match = next(
        (item for item in data_list if item.get(name_field) == target_name),
        None
    )

    if match is None:
        available_names = [item.get(name_field, '<unnamed>') for item in data_list[:5]]
        names_str = ', '.join(available_names)
        if len(data_list) > 5:
            names_str += f", ... ({len(data_list) - 5} more)"

        raise IRPReferenceDataError(
            f"{data_type} '{target_name}' not found. Available: {names_str}"
        )

    return match


def validate_response_has_field(
    response_data: Dict[str, Any],
    field_name: str,
    context: str = "response"
) -> None:
    """
    Validate that response contains required field.

    Args:
        response_data: Response dict
        field_name: Required field name
        context: Context description for error message

    Raises:
        IRPAPIError: If field is missing
    """
    if field_name not in response_data:
        raise IRPAPIError(
            f"{context} missing required field '{field_name}'"
        )


def get_nested_field(
    data: Union[Dict[str, Any], List[Any]],
    *keys: Union[str, int],
    default: Any = None,
    required: bool = False,
    context: str = ""
) -> Any:
    """
    Safely get nested field from dict/list with proper error handling.

    This method supports both dict keys and list indices, allowing safe
    traversal of complex nested structures.

    Args:
        data: Source dict or list
        *keys: Sequence of keys/indices to traverse
               - String keys for dict access
               - Integer indices for list access
        default: Default value if field not found (only used if required=False)
        required: If True, raises IRPAPIError when field is missing
        context: Context description for error messages

    Returns:
        Extracted value or default

    Raises:
        IRPAPIError: If required=True and field is missing/None

    Examples:
        # Dict access
        value = get_nested_field(response, 'summary', 'exposureSetId')

        # List access
        id_val = get_nested_field(response, 'items', 0, 'id', required=True)

        # With default
        count = get_nested_field(response, 'count', default=0)

        # With context for better errors
        cedants = get_nested_field(
            response, 'searchItems',
            required=True,
            context="cedants response"
        )

    Note:
        For backward compatibility, you can still use dot notation with a
        single string argument (e.g., 'summary.exposureSetId'), but the
        new multi-argument style is preferred for list access and better
        error messages.
    """
    # Backward compatibility: support dot notation in single string
    if len(keys) == 1 and isinstance(keys[0], str) and '.' in keys[0]:
        keys = tuple(keys[0].split('.'))

    current = data
    path = []

    try:
        for key in keys:
            path.append(str(key))

            # Handle dict access
            if isinstance(current, dict):
                if not isinstance(key, str):
                    if required:
                        raise IRPAPIError(
                            f"Cannot use integer key '{key}' for dict access "
                            f"at '{'.'.join(path[:-1])}'"
                            f"{f' in {context}' if context else ''}"
                        )
                    return default
                if key not in current:
                    if required:
                        raise IRPAPIError(
                            f"Missing required key '{'.'.join(path)}'"
                            f"{f' in {context}' if context else ''}"
                        )
                    return default
                current = current[key]

            # Handle list/tuple access
            elif isinstance(current, (list, tuple)):
                if not isinstance(key, int):
                    if required:
                        raise IRPAPIError(
                            f"Cannot index list with non-integer key '{key}' "
                            f"at '{'.'.join(path[:-1])}'"
                            f"{f' in {context}' if context else ''}"
                        )
                    return default

                if key < 0 or key >= len(current):
                    if required:
                        raise IRPAPIError(
                            f"List index {key} out of range at '{'.'.join(path[:-1])}' "
                            f"(length: {len(current)})"
                            f"{f' in {context}' if context else ''}"
                        )
                    return default

                current = current[key]

            # Handle None or invalid types
            else:
                if required:
                    if current is None:
                        raise IRPAPIError(
                            f"Cannot access '{key}' on None value "
                            f"at '{'.'.join(path[:-1])}'"
                            f"{f' in {context}' if context else ''}"
                        )
                    raise IRPAPIError(
                        f"Cannot access key '{key}' on non-dict/list type "
                        f"{type(current).__name__} at '{'.'.join(path[:-1])}'"
                        f"{f' in {context}' if context else ''}"
                    )
                return default

            # Check if result is None when required
            if required and current is None:
                raise IRPAPIError(
                    f"Required value at '{'.'.join(path)}' is None"
                    f"{f' in {context}' if context else ''}"
                )

    except (KeyError, IndexError, TypeError) as e:
        if required:
            raise IRPAPIError(
                f"Failed to access '{'.'.join(path)}'"
                f"{f' in {context}' if context else ''}: {e}"
            ) from e
        return default

    return current


def build_analysis_currency_dict() -> Dict[str, str]:
    """
    Build default currency dict for analysis requests.

    Note: This is a temporary helper that will be replaced once
    currency data is gathered dynamically from APIs.

    Returns:
        Currency dict with default values
    """
    return {
        "asOfDate": "2018-11-15",
        "code": "USD",
        "scheme": "RMS",
        "vintage": "RL18.1"
    }


def extract_analysis_id_from_workflow_response(workflow: Dict[str, Any]) -> Optional[str]:
    """
    Extract analysis ID from workflow response.

    Args:
        workflow: Workflow response dict

    Returns:
        Analysis ID if found, None otherwise
    """
    return get_nested_field(workflow, 
                            'output', 
                            'analysisId', 
                            required=True, 
                            context='extract analysis id')
