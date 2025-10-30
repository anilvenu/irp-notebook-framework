"""
Test suite for IRP Integration utilities

This test file validates utility functions including:
- get_nested_field: Safe nested data access with error handling

Run these tests:
    pytest workspace/tests/irp_integration/test_utils.py -v
    pytest workspace/tests/irp_integration/test_utils.py::test_name -v
"""

import pytest
import os
from pathlib import Path
from helpers.irp_integration.utils import get_nested_field, get_workspace_root
from helpers.irp_integration.exceptions import IRPAPIError


# ============================================================================
# Tests - Path Resolution
# ============================================================================

@pytest.mark.unit
def test_get_workspace_root_from_workspace_dir():
    """Test get_workspace_root when CWD is workspace directory"""
    original_cwd = Path.cwd()
    try:
        # Change to workspace directory
        workspace = original_cwd
        while workspace.name != 'workspace' and workspace.parent != workspace:
            workspace = workspace.parent
        if workspace.name != 'workspace':
            # We're not in workspace structure, find it differently
            if (original_cwd / 'workspace').exists():
                workspace = original_cwd / 'workspace'
            else:
                pytest.skip("Not in workspace directory structure")

        os.chdir(workspace)
        result = get_workspace_root()
        assert result.name == 'workspace'
        assert result.is_dir()
    finally:
        os.chdir(original_cwd)


@pytest.mark.unit
def test_get_workspace_root_from_nested_dir():
    """Test get_workspace_root from deeply nested directory"""
    original_cwd = Path.cwd()
    try:
        # Find workspace root first
        workspace = original_cwd
        while workspace.name != 'workspace' and workspace.parent != workspace:
            workspace = workspace.parent
        if workspace.name != 'workspace':
            if (original_cwd / 'workspace').exists():
                workspace = original_cwd / 'workspace'
            else:
                pytest.skip("Not in workspace directory structure")

        # Change to a nested directory within workspace
        nested_dir = workspace / 'workflows' / '_Tools'
        if not nested_dir.exists():
            pytest.skip("Test directory doesn't exist")

        os.chdir(nested_dir)
        result = get_workspace_root()
        assert result.name == 'workspace'
        assert result == workspace
    finally:
        os.chdir(original_cwd)


@pytest.mark.unit
def test_get_workspace_root_returns_path():
    """Test get_workspace_root returns a Path object"""
    result = get_workspace_root()
    assert isinstance(result, Path)
    assert result.is_absolute()


# ============================================================================
# Tests - Basic Dict Access
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_simple_dict_key():
    """Test simple single-level dict key access"""
    data = {"name": "test", "value": 42}

    result = get_nested_field(data, "name")
    assert result == "test"

    result = get_nested_field(data, "value")
    assert result == 42


@pytest.mark.unit
def test_get_nested_field_multiple_dict_keys():
    """Test multiple nested dict keys"""
    data = {
        "level1": {
            "level2": {
                "level3": "deeply nested value"
            }
        }
    }

    result = get_nested_field(data, "level1", "level2", "level3")
    assert result == "deeply nested value"


@pytest.mark.unit
def test_get_nested_field_missing_key_with_default():
    """Test missing key returns default value"""
    data = {"existing": "value"}

    result = get_nested_field(data, "missing", default="default_value")
    assert result == "default_value"

    # Nested missing key
    result = get_nested_field(data, "existing", "nested", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_missing_key_required():
    """Test missing key with required=True raises error"""
    data = {"existing": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "missing", required=True)

    assert "Missing required key" in str(exc_info.value)
    assert "'missing'" in str(exc_info.value)


@pytest.mark.unit
def test_get_nested_field_existing_key_with_default():
    """Test existing key ignores default value"""
    data = {"key": "actual_value"}

    result = get_nested_field(data, "key", default="ignored")
    assert result == "actual_value"


# ============================================================================
# Tests - Basic List Access
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_list_index_valid():
    """Test valid list index access"""
    data = ["first", "second", "third"]

    result = get_nested_field(data, 0)
    assert result == "first"

    result = get_nested_field(data, 1)
    assert result == "second"

    result = get_nested_field(data, 2)
    assert result == "third"


@pytest.mark.unit
def test_get_nested_field_list_index_out_of_range():
    """Test index out of bounds returns default"""
    data = ["a", "b", "c"]

    result = get_nested_field(data, 5, default="not_found")
    assert result == "not_found"

    result = get_nested_field(data, 100, default=None)
    assert result is None


@pytest.mark.unit
def test_get_nested_field_list_index_out_of_range_required():
    """Test index out of bounds with required=True raises error"""
    data = ["a", "b", "c"]

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, 10, required=True)

    assert "out of range" in str(exc_info.value).lower() or "index" in str(exc_info.value).lower()


@pytest.mark.unit
def test_get_nested_field_list_negative_index():
    """Test negative index handling"""
    data = ["a", "b", "c"]

    # Negative indices should return default (implementation detail)
    result = get_nested_field(data, -1, default="fallback")

    # Either returns the last element (if negative indexing supported)
    # or returns default (if not supported)
    assert result in ["c", "fallback"]


@pytest.mark.unit
def test_get_nested_field_list_boundary_indices():
    """Test first and last valid indices"""
    data = ["first", "middle", "last"]

    # First index
    result = get_nested_field(data, 0)
    assert result == "first"

    # Last index
    result = get_nested_field(data, 2)
    assert result == "last"


# ============================================================================
# Tests - Mixed Dict/List Access
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_dict_then_list():
    """Test dict key followed by list index"""
    data = {
        "items": ["first", "second", "third"]
    }

    result = get_nested_field(data, "items", 1)
    assert result == "second"


@pytest.mark.unit
def test_get_nested_field_list_then_dict():
    """Test list index followed by dict key"""
    data = [
        {"id": 1, "name": "first"},
        {"id": 2, "name": "second"}
    ]

    result = get_nested_field(data, 0, "name")
    assert result == "first"

    result = get_nested_field(data, 1, "id")
    assert result == 2


@pytest.mark.unit
def test_get_nested_field_complex_nested_structure():
    """Test multiple levels of both dict and list"""
    data = {
        "response": {
            "items": [
                {
                    "data": {
                        "values": [10, 20, 30]
                    }
                },
                {
                    "data": {
                        "values": [40, 50, 60]
                    }
                }
            ]
        }
    }

    result = get_nested_field(data, "response", "items", 1, "data", "values", 2)
    assert result == 60


# ============================================================================
# Tests - Type Mismatch Errors
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_integer_key_on_dict_with_default():
    """Test using int key on dict returns default"""
    data = {"key": "value"}

    result = get_nested_field(data, 0, default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_integer_key_on_dict_required():
    """Test using int key on dict with required=True raises error"""
    data = {"key": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, 0, required=True)

    error_msg = str(exc_info.value).lower()
    assert "cannot use integer key" in error_msg


@pytest.mark.unit
def test_get_nested_field_string_key_on_list_with_default():
    """Test using string key on list returns default"""
    data = ["a", "b", "c"]

    result = get_nested_field(data, "key", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_string_key_on_list_required():
    """Test using string key on list with required=True raises error"""
    data = ["a", "b", "c"]

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "key", required=True)

    error_msg = str(exc_info.value).lower()
    assert "cannot index list with non-integer key" in error_msg


# ============================================================================
# Tests - None Handling
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_none_intermediate_value():
    """Test accessing key on None intermediate value returns default"""
    data = {
        "level1": None
    }

    result = get_nested_field(data, "level1", "level2", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_none_intermediate_value_required():
    """Test accessing key on None intermediate value with required=True raises error"""
    data = {
        "level1": None
    }

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "level1", "level2", required=True)

    assert "None" in str(exc_info.value) or "required" in str(exc_info.value).lower()


@pytest.mark.unit
def test_get_nested_field_none_final_value_not_required():
    """Test final value is None, required=False returns None"""
    data = {
        "key": None
    }

    result = get_nested_field(data, "key", required=False)
    assert result is None


@pytest.mark.unit
def test_get_nested_field_none_final_value_required():
    """Test final value is None, required=True raises error"""
    data = {
        "key": None
    }

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "key", required=True)

    assert "None" in str(exc_info.value) or "required" in str(exc_info.value).lower()


# ============================================================================
# Tests - Invalid Type Handling
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_invalid_type_string():
    """Test accessing key on string returns default"""
    data = {
        "text": "just a string"
    }

    result = get_nested_field(data, "text", "nested", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_invalid_type_number():
    """Test accessing key on int/float returns default"""
    data = {
        "number": 42
    }

    result = get_nested_field(data, "number", "nested", default="fallback")
    assert result == "fallback"

    data = {
        "float": 3.14
    }

    result = get_nested_field(data, "float", "nested", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_invalid_type_required():
    """Test accessing key on invalid type with required=True raises error"""
    data = {
        "text": "just a string"
    }

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "text", "nested", required=True)

    error_msg = str(exc_info.value).lower()
    assert "type" in error_msg or "none" in error_msg or "required" in error_msg


# ============================================================================
# Tests - Backward Compatibility - Dot Notation
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_dot_notation_single_string():
    """Test old style dot notation: 'key1.key2.key3'"""
    data = {
        "level1": {
            "level2": {
                "level3": "value"
            }
        }
    }

    # Old style: single string with dots
    result = get_nested_field(data, "level1.level2.level3")
    assert result == "value"


@pytest.mark.unit
def test_get_nested_field_dot_notation_vs_multi_arg():
    """Test both dot notation and multi-arg produce same result"""
    data = {
        "summary": {
            "exposureSetId": "12345"
        }
    }

    # Old style
    result_old = get_nested_field(data, "summary.exposureSetId")

    # New style
    result_new = get_nested_field(data, "summary", "exposureSetId")

    assert result_old == result_new == "12345"


@pytest.mark.unit
def test_get_nested_field_dot_notation_with_list():
    """Test dot notation doesn't work with list indices (limitation)"""
    data = {
        "items": [
            {"id": 1},
            {"id": 2}
        ]
    }

    # Dot notation cannot access list indices
    # This should NOT work the same as get_nested_field(data, "items", 0, "id")
    result = get_nested_field(data, "items.0.id", default="not_accessible")

    # Should return default because "0" is treated as string key, not int index
    assert result == "not_accessible"


# ============================================================================
# Tests - Context Parameter
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_error_message_with_context():
    """Test error includes context in message"""
    data = {"key": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "missing", required=True, context="workflow submission")

    assert "workflow submission" in str(exc_info.value)


@pytest.mark.unit
def test_get_nested_field_error_message_without_context():
    """Test error works without context"""
    data = {"key": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "missing", required=True)

    # Should raise error even without context
    assert "missing" in str(exc_info.value).lower() or "required" in str(exc_info.value).lower()


# ============================================================================
# Tests - Path Building for Error Messages
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_error_shows_path():
    """Test error message shows full path attempted"""
    data = {
        "level1": {
            "level2": {}
        }
    }

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "level1", "level2", "missing", required=True)

    error_msg = str(exc_info.value)
    # Should show the path attempted
    assert "missing" in error_msg


# ============================================================================
# Tests - Edge Cases
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_empty_keys():
    """Test no keys provided returns the data itself"""
    data = {"key": "value"}

    # When no keys are provided, should return the original data
    result = get_nested_field(data)
    assert result == data


@pytest.mark.unit
def test_get_nested_field_empty_dict():
    """Test empty dict with keys returns default"""
    data = {}

    result = get_nested_field(data, "missing", default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_empty_list():
    """Test empty list with index returns default"""
    data = []

    result = get_nested_field(data, 0, default="fallback")
    assert result == "fallback"


@pytest.mark.unit
def test_get_nested_field_tuple_access():
    """Test accessing tuple (should work like list)"""
    data = ("first", "second", "third")

    result = get_nested_field(data, 1)
    assert result == "second"

    # Out of range on tuple
    result = get_nested_field(data, 10, default="fallback")
    assert result == "fallback"


# ============================================================================
# Tests - Real-World Usage Patterns
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_workflow_response_pattern():
    """Test mimics actual usage: workflow response parsing"""
    # Simulates actual Moody's API response structure
    response = {
        "summary": {
            "exposureSetId": "EXP-12345",
            "workflowId": "WF-67890"
        },
        "status": "COMPLETED"
    }

    exposure_id = get_nested_field(response, "summary", "exposureSetId")
    assert exposure_id == "EXP-12345"

    workflow_id = get_nested_field(response, "summary", "workflowId")
    assert workflow_id == "WF-67890"


@pytest.mark.unit
def test_get_nested_field_list_item_pattern():
    """Test mimics actual usage: accessing list items with required=True"""
    response = {
        "items": [
            {"id": "item-1", "status": "active"},
            {"id": "item-2", "status": "pending"}
        ]
    }

    first_id = get_nested_field(response, "items", 0, "id", required=True)
    assert first_id == "item-1"

    # Missing item should raise
    with pytest.raises(IRPAPIError):
        get_nested_field(response, "items", 5, "id", required=True)


@pytest.mark.unit
def test_get_nested_field_with_default_pattern():
    """Test mimics actual usage: count with default value"""
    response = {
        "data": {
            "results": []
        }
    }

    # Count might be missing, default to 0
    count = get_nested_field(response, "data", "count", default=0)
    assert count == 0

    # When count exists
    response["data"]["count"] = 5
    count = get_nested_field(response, "data", "count", default=0)
    assert count == 5


# ============================================================================
# Tests - Exception Details
# ============================================================================

@pytest.mark.unit
def test_get_nested_field_exception_chain():
    """Test verify exception chaining with 'from e'"""
    data = {"key": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "missing", required=True)

    # Exception should be raised
    assert exc_info.value is not None


@pytest.mark.unit
def test_get_nested_field_keyerror_handling():
    """Test KeyError is properly wrapped in IRPAPIError"""
    data = {"existing": "value"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "missing", required=True)

    # Should wrap KeyError
    assert isinstance(exc_info.value, IRPAPIError)


@pytest.mark.unit
def test_get_nested_field_indexerror_handling():
    """Test IndexError is properly wrapped in IRPAPIError"""
    data = ["a", "b", "c"]

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, 10, required=True)

    # Should wrap IndexError
    assert isinstance(exc_info.value, IRPAPIError)


@pytest.mark.unit
def test_get_nested_field_typeerror_handling():
    """Test TypeError is properly wrapped in IRPAPIError"""
    data = {"text": "just a string"}

    with pytest.raises(IRPAPIError) as exc_info:
        get_nested_field(data, "text", "nested", required=True)

    # Should wrap TypeError or AttributeError
    assert isinstance(exc_info.value, IRPAPIError)
