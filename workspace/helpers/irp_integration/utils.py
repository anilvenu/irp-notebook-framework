"""
Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction,
and reference data lookup operations.
"""

import base64
from typing import Dict, List, Any, Optional
import requests
from .exceptions import IRPAPIError, IRPReferenceDataError


def extract_id_from_location_header(
    response: requests.Response,
    error_context: str = "response"
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
    if 'location' not in response.headers:
        raise IRPAPIError(
            f"Location header missing from {error_context}"
        )
    location = response.headers['location']
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


def parse_workflow_response(response: requests.Response) -> Dict[str, Any]:
    """
    Parse and validate workflow response structure.

    Args:
        response: HTTP response from workflow endpoint

    Returns:
        Parsed response dict

    Raises:
        IRPAPIError: If response structure is invalid
    """
    try:
        data = response.json()
    except Exception as e:
        raise IRPAPIError(f"Failed to parse workflow response as JSON: {e}")

    if not isinstance(data, dict):
        raise IRPAPIError(
            f"Workflow response must be dict, got {type(data).__name__}"
        )

    return data


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
    data: Dict[str, Any],
    field_path: str,
    default: Any = None
) -> Any:
    """
    Get nested field from dict using dot notation.

    Args:
        data: Dict to search
        field_path: Field path (e.g., "summary.exposureSetId")
        default: Default value if field not found

    Returns:
        Field value or default
    """
    fields = field_path.split('.')
    current = data

    for field in fields:
        if not isinstance(current, dict) or field not in current:
            return default
        current = current[field]

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


def extract_analysis_id_from_workflow(workflow: Dict[str, Any]) -> Optional[str]:
    """
    Extract analysis ID from workflow response.

    Args:
        workflow: Workflow response dict

    Returns:
        Analysis ID if found, None otherwise
    """
    return get_nested_field(workflow, 'output.analysisId')


def validate_workflow_status(
    workflow: Dict[str, Any],
    expected_status: str = 'FINISHED',
    context: str = "workflow"
) -> None:
    """
    Validate workflow completed with expected status.

    Args:
        workflow: Workflow response dict
        expected_status: Expected status value (default: 'FINISHED')
        context: Context description for error message

    Raises:
        IRPAPIError: If workflow status is not as expected
    """
    status = workflow.get('status')
    if status != expected_status:
        workflow_id = workflow.get('id', 'unknown')
        raise IRPAPIError(
            f"{context} {workflow_id} has unexpected status: {status} "
            f"(expected {expected_status})"
        )
