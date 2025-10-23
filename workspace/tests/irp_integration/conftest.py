"""
pytest configuration and fixtures for IRP Integration tests

This module provides fixtures specific to testing the irp_integration module:
- IRPClient instance
- Unique name generation for test resources
- Cleanup utilities

Note: Environment variables can be set before running tests if needed.
The Client class reads from os.environ with sensible defaults.
"""

import sys
import pytest
from pathlib import Path
from datetime import datetime

# Add workspace directory to Python path for imports
workspace_path = Path(__file__).parent.parent.parent.resolve()
workspace_path_str = str(workspace_path)
if workspace_path_str not in sys.path:
    sys.path.insert(0, workspace_path_str)


# ==============================================================================
# MODULE-LEVEL FIXTURES
# ==============================================================================

@pytest.fixture(scope="module")
def irp_client():
    """
    Create IRPClient instance for testing.

    The client is module-scoped to reuse across tests in the same file.
    The underlying Client reads configuration from environment variables:
    - RISK_MODELER_BASE_URL (default: 'https://api-euw1.rms-ppe.com')
    - RISK_MODELER_API_KEY (default: 'your_api_key')

    Set these environment variables before running tests to use real credentials.

    Returns:
        IRPClient: Configured client instance
    """
    from helpers.irp_integration import IRPClient
    return IRPClient()


# ==============================================================================
# FUNCTION-LEVEL FIXTURES
# ==============================================================================

@pytest.fixture
def unique_name():
    """
    Generate unique name for test resources to avoid conflicts.

    Uses timestamp to ensure uniqueness across test runs.
    Format: test_YYYYMMDD_HHMMSS_microseconds

    Returns:
        str: Unique name prefix for test resources
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"test_{timestamp}"


@pytest.fixture
def test_data_dir():
    """
    Get path to test data fixtures directory.

    Returns:
        Path: Path to fixtures directory containing test data files
    """
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def cleanup_edms():
    """
    Track EDMs created during tests and clean them up afterward.

    Usage:
        def test_something(irp_client, cleanup_edms):
            edm_name = "test_edm"
            irp_client.edm.create_edm(edm_name, "server")
            cleanup_edms.append(edm_name)  # Will be deleted after test

    Yields:
        list: List to append EDM names for cleanup
    """
    edm_names = []
    yield edm_names

    # Cleanup after test
    if edm_names:
        from helpers.irp_integration import IRPClient
        client = IRPClient()
        for edm_name in edm_names:
            try:
                print(f"\nCleaning up EDM: {edm_name}")
                client.edm.delete_edm(edm_name)
                print(f"✓ Deleted EDM: {edm_name}")
            except Exception as e:
                print(f"⚠ Warning: Failed to delete EDM '{edm_name}': {e}")


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_workflow_files_dir():
    """
    Get path to workflow files directory (for finding sample data).

    Returns:
        Path: Path to workflow files directory
    """
    return Path(__file__).parent.parent.parent / "workflows" / "_Tools" / "files" / "working_files"
