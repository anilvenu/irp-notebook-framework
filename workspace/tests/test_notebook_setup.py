"""
Tests for notebook_setup.py Module

Simple unit tests for the initialize_notebook_context function.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from helpers.notebook_setup import initialize_notebook_context


@pytest.mark.unit
def test_path_detection_from_active_directory():
    """Test path detection when CWD is in Active_ directory"""
    stage_dir = Path('/workspace/workflows/Active_Test/notebooks/Stage_03_Data_Import')

    with patch('helpers.notebook_setup.Path.cwd', return_value=stage_dir), \
         patch('helpers.notebook_setup.WorkContext') as mock_context_class, \
         patch('helpers.notebook_setup.Step') as mock_step_class:

        # Configure mocks
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        mock_step = MagicMock()
        mock_step.executed = False
        mock_step_class.return_value = mock_step

        # Call function
        context, step = initialize_notebook_context('Step_01_Test.ipynb')

        # Verify notebook path was constructed correctly
        expected_path = stage_dir / 'Step_01_Test.ipynb'
        mock_context_class.assert_called_once()
        call_args = mock_context_class.call_args[1]
        assert Path(call_args['notebook_path']) == expected_path


@pytest.mark.unit
def test_path_detection_no_workspace():
    """Test error handling when workspace directory doesn't exist"""
    home_dir = Path('/home/jovyan')

    with patch('helpers.notebook_setup.Path.cwd', return_value=home_dir), \
         patch('helpers.notebook_setup.Path.home', return_value=home_dir), \
         patch('pathlib.Path.exists', return_value=False):

        with pytest.raises(RuntimeError, match="Workspace directory not found"):
            initialize_notebook_context('Step_01_Test.ipynb')


@pytest.mark.unit
def test_step_not_executed_returns_step():
    """Test that function returns step when not executed"""
    stage_dir = Path('/workspace/workflows/Active_Test/notebooks/Stage_03_Data_Import')

    with patch('helpers.notebook_setup.Path.cwd', return_value=stage_dir), \
         patch('helpers.notebook_setup.WorkContext') as mock_context_class, \
         patch('helpers.notebook_setup.Step') as mock_step_class:

        # Configure mocks
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        mock_step = MagicMock()
        mock_step.executed = False
        mock_step_class.return_value = mock_step

        # Call function
        context, step = initialize_notebook_context('Step_01_Test.ipynb')

        # Verify returns
        assert context == mock_context
        assert step == mock_step


@pytest.mark.unit
def test_step_already_executed_allows_rerun():
    """Test that already-executed steps can be re-run without blocking"""
    stage_dir = Path('/workspace/workflows/Active_Test/notebooks/Stage_03_Data_Import')

    with patch('helpers.notebook_setup.Path.cwd', return_value=stage_dir), \
         patch('helpers.notebook_setup.WorkContext') as mock_context_class, \
         patch('helpers.notebook_setup.Step') as mock_step_class:

        # Configure mocks
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        mock_step = MagicMock()
        mock_step.executed = True  # Already executed
        mock_step.step_id = 123
        mock_step.status_message = "Step already run"
        mock_step_class.return_value = mock_step

        # Call function - should not raise or block
        context, step = initialize_notebook_context('Step_01_Test.ipynb')

        # Verify returns (no blocking, no forced start)
        assert context == mock_context
        assert step == mock_step
        # step.start should NOT be called by initialize_notebook_context
        mock_step.start.assert_not_called()
