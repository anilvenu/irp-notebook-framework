"""
Test suite for notebook executor failure handling.

Tests cover:
- Step run marked as FAILED when notebook crashes during chained execution
- Teams notification sent on notebook failure
- Various failure scenarios (execution error, timeout, unexpected error)
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_teams_env(monkeypatch):
    """Mock Teams environment variables."""
    monkeypatch.setenv('TEAMS_WEBHOOK_URL', 'https://webhook.test.com/teams')
    monkeypatch.setenv('TEAMS_NOTIFICATION_ENABLED', 'true')
    monkeypatch.setenv('TEAMS_DEFAULT_DASHBOARD_URL', 'https://dashboard.test.com')
    monkeypatch.setenv('TEAMS_DEFAULT_JUPYTERLAB_URL', 'https://jupyter.test.com')


@pytest.fixture
def sample_notebook_path(tmp_path):
    """Create a sample notebook path structure."""
    # Create directory structure like Active_TestCycle/notebooks/Stage_03_Import/
    cycle_dir = tmp_path / 'workflows' / 'Active_TestCycle' / 'notebooks' / 'Stage_03_Import'
    cycle_dir.mkdir(parents=True)

    notebook = cycle_dir / 'Step_01_Test.ipynb'
    notebook.write_text('{}')  # Empty notebook

    return notebook


# ============================================================================
# HANDLE NOTEBOOK FAILURE TESTS
# ============================================================================

@pytest.mark.unit
def test_handle_notebook_failure_marks_step_as_failed(mock_teams_env, sample_notebook_path):
    """Test that _handle_notebook_failure marks the step run as FAILED."""
    from helpers.notebook_executor import _handle_notebook_failure

    mock_context = MagicMock()
    mock_context.step_id = 123

    mock_last_run = {
        'id': 456,
        'status': 'ACTIVE',
        'run_num': 1
    }

    # Patch at the source modules since imports are inside the function
    with patch('helpers.context.WorkContext', return_value=mock_context), \
         patch('helpers.step.get_last_step_run', return_value=mock_last_run) as mock_get_run, \
         patch('helpers.step.update_step_run') as mock_update, \
         patch('helpers.teams_notification.TeamsNotificationClient') as mock_teams:

        mock_teams.return_value.send_error.return_value = True

        _handle_notebook_failure(sample_notebook_path, "Test error message")

        # Verify step was looked up
        mock_get_run.assert_called_once_with(123)

        # Verify step was marked as FAILED
        mock_update.assert_called_once()
        call_args = mock_update.call_args
        assert call_args[0][0] == 456  # run_id
        assert call_args[0][1] == 'FAILED'  # status
        assert 'Test error message' in call_args[1]['error_message']


@pytest.mark.unit
def test_handle_notebook_failure_skips_non_active_step(mock_teams_env, sample_notebook_path):
    """Test that already completed/failed steps are not updated."""
    from helpers.notebook_executor import _handle_notebook_failure

    mock_context = MagicMock()
    mock_context.step_id = 123

    # Step already COMPLETED
    mock_last_run = {
        'id': 456,
        'status': 'COMPLETED',
        'run_num': 1
    }

    with patch('helpers.context.WorkContext', return_value=mock_context), \
         patch('helpers.step.get_last_step_run', return_value=mock_last_run), \
         patch('helpers.step.update_step_run') as mock_update, \
         patch('helpers.teams_notification.TeamsNotificationClient') as mock_teams:

        mock_teams.return_value.send_error.return_value = True

        _handle_notebook_failure(sample_notebook_path, "Test error")

        # Should NOT update step (already completed)
        mock_update.assert_not_called()


@pytest.mark.unit
def test_handle_notebook_failure_sends_teams_notification(mock_teams_env, sample_notebook_path):
    """Test that Teams notification is sent on failure."""
    from helpers.notebook_executor import _handle_notebook_failure

    mock_context = MagicMock()
    mock_context.step_id = 123

    with patch('helpers.context.WorkContext', return_value=mock_context), \
         patch('helpers.step.get_last_step_run', return_value=None), \
         patch('helpers.teams_notification.TeamsNotificationClient') as mock_teams_class:

        mock_client = MagicMock()
        mock_teams_class.return_value = mock_client
        mock_client.send_error.return_value = True

        _handle_notebook_failure(sample_notebook_path, "Notebook crashed!")

        # Verify Teams notification was sent
        mock_client.send_error.assert_called_once()
        call_kwargs = mock_client.send_error.call_args[1]
        assert 'TestCycle' in call_kwargs['title']
        assert 'Step_01_Test' in call_kwargs['title']


@pytest.mark.unit
def test_handle_notebook_failure_truncates_long_error(mock_teams_env, sample_notebook_path):
    """Test that long error messages are truncated for step run."""
    from helpers.notebook_executor import _handle_notebook_failure

    mock_context = MagicMock()
    mock_context.step_id = 123

    mock_last_run = {
        'id': 456,
        'status': 'ACTIVE',
        'run_num': 1
    }

    long_error = "E" * 2000  # Very long error

    with patch('helpers.context.WorkContext', return_value=mock_context), \
         patch('helpers.step.get_last_step_run', return_value=mock_last_run), \
         patch('helpers.step.update_step_run') as mock_update, \
         patch('helpers.teams_notification.TeamsNotificationClient') as mock_teams:

        mock_teams.return_value.send_error.return_value = True

        _handle_notebook_failure(sample_notebook_path, long_error)

        # Verify error was truncated to 1000 chars
        call_args = mock_update.call_args
        assert len(call_args[1]['error_message']) == 1000


@pytest.mark.unit
def test_handle_notebook_failure_handles_context_error(mock_teams_env, sample_notebook_path):
    """Test that errors during context creation don't break notification."""
    from helpers.notebook_executor import _handle_notebook_failure

    with patch('helpers.context.WorkContext', side_effect=Exception("Context error")), \
         patch('helpers.teams_notification.TeamsNotificationClient') as mock_teams_class:

        mock_client = MagicMock()
        mock_teams_class.return_value = mock_client
        mock_client.send_error.return_value = True

        # Should not raise, notification should still be sent
        _handle_notebook_failure(sample_notebook_path, "Original error")

        # Teams notification should still be sent despite context error
        mock_client.send_error.assert_called_once()


# ============================================================================
# EXECUTE NOTEBOOK INTEGRATION TESTS
# ============================================================================

@pytest.mark.unit
def test_execute_notebook_calls_failure_handler_on_error(mock_teams_env, sample_notebook_path):
    """Test that execute_notebook calls _handle_notebook_failure on subprocess error."""
    from helpers.notebook_executor import execute_notebook
    import subprocess

    mock_error = subprocess.CalledProcessError(
        returncode=1,
        cmd=['jupyter', 'nbconvert'],
        output='stdout content',
        stderr='stderr content'
    )

    with patch('helpers.notebook_executor.subprocess.run', side_effect=mock_error), \
         patch('helpers.notebook_executor._handle_notebook_failure') as mock_handler:

        result = execute_notebook(sample_notebook_path)

        assert result['success'] is False
        mock_handler.assert_called_once()

        # Verify error message contains relevant info
        call_args = mock_handler.call_args[0]
        assert call_args[0] == sample_notebook_path
        assert 'Exit code: 1' in call_args[1]


@pytest.mark.unit
def test_execute_notebook_calls_failure_handler_on_timeout(mock_teams_env, sample_notebook_path):
    """Test that execute_notebook calls _handle_notebook_failure on timeout."""
    from helpers.notebook_executor import execute_notebook
    import subprocess

    mock_timeout = subprocess.TimeoutExpired(
        cmd=['jupyter', 'nbconvert'],
        timeout=3600
    )
    mock_timeout.stdout = 'partial output'
    mock_timeout.stderr = 'timeout stderr'

    with patch('helpers.notebook_executor.subprocess.run', side_effect=mock_timeout), \
         patch('helpers.notebook_executor._handle_notebook_failure') as mock_handler:

        result = execute_notebook(sample_notebook_path)

        assert result['success'] is False
        mock_handler.assert_called_once()

        call_args = mock_handler.call_args[0]
        assert 'timed out' in call_args[1]


@pytest.mark.unit
def test_execute_notebook_calls_failure_handler_on_unexpected_error(mock_teams_env, sample_notebook_path):
    """Test that execute_notebook calls _handle_notebook_failure on unexpected errors."""
    from helpers.notebook_executor import execute_notebook

    with patch('helpers.notebook_executor.subprocess.run', side_effect=RuntimeError("Unexpected!")), \
         patch('helpers.notebook_executor._handle_notebook_failure') as mock_handler:

        result = execute_notebook(sample_notebook_path)

        assert result['success'] is False
        mock_handler.assert_called_once()

        call_args = mock_handler.call_args[0]
        assert 'Unexpected' in call_args[1]


@pytest.mark.unit
def test_execute_notebook_success_does_not_call_failure_handler(mock_teams_env, sample_notebook_path):
    """Test that successful execution does not call _handle_notebook_failure."""
    from helpers.notebook_executor import execute_notebook

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = 'success'
    mock_result.stderr = ''

    with patch('helpers.notebook_executor.subprocess.run', return_value=mock_result), \
         patch('helpers.notebook_executor._handle_notebook_failure') as mock_handler:

        result = execute_notebook(sample_notebook_path)

        assert result['success'] is True
        mock_handler.assert_not_called()
