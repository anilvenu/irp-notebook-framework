"""
Input validation utilities for IRP Integration module.

Provides reusable validation functions that raise descriptive
IRPValidationError exceptions when validation fails.
"""

import os
from typing import Any, List
from .exceptions import IRPValidationError


def validate_non_empty_string(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty string.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty string
    """
    if not isinstance(value, str):
        raise IRPValidationError(
            f"{param_name} must be a string, got {type(value).__name__}"
        )
    if not value.strip():
        raise IRPValidationError(f"{param_name} cannot be empty")


def validate_positive_int(value: Any, param_name: str) -> None:
    """
    Validate that a value is a positive integer.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a positive integer
    """
    if not isinstance(value, int):
        raise IRPValidationError(
            f"{param_name} must be an integer, got {type(value).__name__}"
        )
    if value <= 0:
        raise IRPValidationError(
            f"{param_name} must be positive, got {value}"
        )


def validate_non_negative_number(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-negative number (int or float).

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-negative number
    """
    if not isinstance(value, (int, float)):
        raise IRPValidationError(
            f"{param_name} must be a number, got {type(value).__name__}"
        )
    if value < 0:
        raise IRPValidationError(
            f"{param_name} cannot be negative, got {value}"
        )


def validate_file_exists(file_path: str, param_name: str = "file_path") -> None:
    """
    Validate that a file exists at the given path.

    Args:
        file_path: Path to file
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If file does not exist
    """
    if not os.path.exists(file_path):
        raise IRPValidationError(
            f"{param_name} does not exist: {file_path}"
        )
    if not os.path.isfile(file_path):
        raise IRPValidationError(
            f"{param_name} is not a file: {file_path}"
        )


def validate_list_not_empty(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty list.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty list
    """
    if not isinstance(value, list):
        raise IRPValidationError(
            f"{param_name} must be a list, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise IRPValidationError(f"{param_name} cannot be empty")


def validate_dict_has_keys(value: Any, required_keys: List[str], param_name: str) -> None:
    """
    Validate that a dict contains all required keys.

    Args:
        value: Dictionary to validate
        required_keys: List of required key names
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a dict or missing required keys
    """
    if not isinstance(value, dict):
        raise IRPValidationError(
            f"{param_name} must be a dict, got {type(value).__name__}"
        )
    missing_keys = [key for key in required_keys if key not in value]
    if missing_keys:
        raise IRPValidationError(
            f"{param_name} missing required keys: {', '.join(missing_keys)}"
        )


def validate_workflow_id(workflow_id: Any, param_name: str = "workflow_id") -> None:
    """
    Validate a workflow ID (must be positive integer).

    Args:
        workflow_id: Workflow ID to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If workflow_id is invalid
    """
    validate_positive_int(workflow_id, param_name)


def validate_edm_name(edm_name: str) -> None:
    """
    Validate EDM name format.

    Args:
        edm_name: EDM name to validate

    Raises:
        IRPValidationError: If EDM name is invalid
    """
    validate_non_empty_string(edm_name, "edm_name")
    # Add additional EDM-specific validation if needed
    # e.g., length limits, character restrictions, etc.
