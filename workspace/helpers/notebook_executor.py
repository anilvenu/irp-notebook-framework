"""
Notebook execution engine for automatic step chaining.

This module provides functionality to programmatically execute Jupyter notebooks
using nbconvert, enabling automated workflow execution.
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def _send_failure_notification(notebook_path: Path, error: str) -> None:
    """
    Send Teams notification when notebook execution fails.

    Args:
        notebook_path: Path to the failed notebook
        error: Error message from the execution failure
    """
    try:
        from helpers.teams_notification import TeamsNotificationClient

        teams = TeamsNotificationClient()

        # Extract cycle/stage/step from path
        # e.g., .../Active_Demo-1126/notebooks/Stage_03_.../Step_01_...ipynb
        path_str = str(notebook_path)
        cycle_name = "Unknown"
        stage_name = "Unknown"
        step_name = notebook_path.stem

        if 'Active_' in path_str:
            parts = path_str.split('Active_')[1].split('/')
            cycle_name = parts[0].split('\\')[0]

        for part in notebook_path.parts:
            if part.startswith('Stage_'):
                stage_name = part
            if part.startswith('Step_'):
                step_name = part.replace('.ipynb', '')

        # Build action buttons with notebook link and dashboard
        actions = []
        base_url = os.environ.get('TEAMS_DEFAULT_JUPYTERLAB_URL', '')
        if base_url and 'workflows' in path_str:
            rel_path = path_str.split('workflows')[-1].lstrip('/\\')
            notebook_url = f"{base_url.rstrip('/')}/lab/tree/workflows/{rel_path}"
            actions.append({"title": "Open Notebook", "url": notebook_url})

        dashboard_url = os.environ.get('TEAMS_DEFAULT_DASHBOARD_URL', '')
        if dashboard_url:
            actions.append({"title": "View Dashboard", "url": dashboard_url})

        # Truncate error for notification (keep first 500 chars)
        error_summary = error[:500] + "..." if len(error) > 500 else error

        teams.send_error(
            title=f"[{cycle_name}] Notebook Failed: {step_name}",
            message=f"**Cycle:** {cycle_name}\n"
                    f"**Stage:** {stage_name}\n"
                    f"**Step:** {step_name}\n\n"
                    f"**Error:**\n{error_summary}",
            actions=actions if actions else None
        )

        logger.info(f"Sent failure notification for notebook: {notebook_path}")

    except Exception as e:
        # Don't let notification failure break the workflow
        logger.warning(f"Failed to send failure notification: {e}")


class NotebookExecutionError(Exception):
    """Raised when notebook execution fails."""
    pass


def execute_notebook(
    notebook_path: Path,
    timeout: int = 3600,
    cwd: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Execute a Jupyter notebook using nbconvert.

    This function runs a notebook in-place, preserving outputs and execution metadata.
    The notebook is executed in its parent directory to ensure proper relative path resolution.

    Args:
        notebook_path: Path to the notebook file to execute
        timeout: Maximum execution time in seconds (default: 3600 = 1 hour)
        cwd: Working directory for execution (default: notebook's parent directory)

    Returns:
        Dictionary with execution results:
        {
            'success': bool,
            'notebook_path': Path,
            'execution_time': float,
            'started_at': datetime,
            'completed_at': datetime,
            'stdout': str,
            'stderr': str,
            'error': str (if failed)
        }

    Raises:
        FileNotFoundError: If notebook file does not exist
        NotebookExecutionError: If execution fails
    """
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    # Use notebook's parent directory as working directory
    if cwd is None:
        cwd = notebook_path.parent

    # Build nbconvert command
    # --execute: Execute the notebook
    # --to notebook: Output as notebook format
    # --inplace: Overwrite the input notebook with output
    # --ExecutePreprocessor.timeout: Maximum execution time
    command = [
        'jupyter',
        'nbconvert',
        '--execute',
        '--to', 'notebook',
        '--inplace',
        '--ExecutePreprocessor.timeout', str(timeout),
        str(notebook_path.name)  # Use relative path in notebook's directory
    ]

    logger.info(f"Executing notebook: {notebook_path}")
    logger.debug(f"Command: {' '.join(command)}")
    logger.debug(f"Working directory: {cwd}")

    started_at = datetime.now()

    try:
        # Execute notebook
        result = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True  # Raise exception on non-zero exit code
        )

        completed_at = datetime.now()
        execution_time = (completed_at - started_at).total_seconds()

        logger.info(
            f"Notebook execution completed successfully: {notebook_path} "
            f"(execution time: {execution_time:.2f}s)"
        )

        return {
            'success': True,
            'notebook_path': notebook_path,
            'execution_time': execution_time,
            'started_at': started_at,
            'completed_at': completed_at,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'error': None
        }

    except subprocess.CalledProcessError as e:
        completed_at = datetime.now()
        execution_time = (completed_at - started_at).total_seconds()

        error_msg = (
            f"Notebook execution failed: {notebook_path}\n"
            f"Exit code: {e.returncode}\n"
            f"Stdout: {e.stdout}\n"
            f"Stderr: {e.stderr}"
        )

        logger.error(error_msg)

        # Send Teams notification for failure
        _send_failure_notification(notebook_path, error_msg)

        return {
            'success': False,
            'notebook_path': notebook_path,
            'execution_time': execution_time,
            'started_at': started_at,
            'completed_at': completed_at,
            'stdout': e.stdout,
            'stderr': e.stderr,
            'error': error_msg
        }

    except subprocess.TimeoutExpired as e:
        completed_at = datetime.now()
        execution_time = (completed_at - started_at).total_seconds()

        error_msg = (
            f"Notebook execution timed out after {timeout}s: {notebook_path}\n"
            f"Stdout: {e.stdout}\n"
            f"Stderr: {e.stderr}"
        )

        logger.error(error_msg)

        # Send Teams notification for timeout
        _send_failure_notification(notebook_path, error_msg)

        return {
            'success': False,
            'notebook_path': notebook_path,
            'execution_time': execution_time,
            'started_at': started_at,
            'completed_at': completed_at,
            'stdout': e.stdout if e.stdout else '',
            'stderr': e.stderr if e.stderr else '',
            'error': error_msg
        }

    except Exception as e:
        completed_at = datetime.now()
        execution_time = (completed_at - started_at).total_seconds()

        error_msg = f"Unexpected error executing notebook {notebook_path}: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Send Teams notification for unexpected error
        _send_failure_notification(notebook_path, error_msg)

        return {
            'success': False,
            'notebook_path': notebook_path,
            'execution_time': execution_time,
            'started_at': started_at,
            'completed_at': completed_at,
            'stdout': '',
            'stderr': '',
            'error': error_msg
        }


def execute_next_step(
    cycle_name: str,
    stage_num: int,
    step_num: int,
    notebook_path: Path,
    timeout: int = 3600
) -> Dict[str, Any]:
    """
    Execute the next step in a workflow chain.

    This is a convenience wrapper around execute_notebook that adds
    workflow-specific logging and context.

    Args:
        cycle_name: Name of the cycle
        stage_num: Stage number
        step_num: Step number
        notebook_path: Path to the notebook to execute
        timeout: Maximum execution time in seconds

    Returns:
        Execution results from execute_notebook()
    """
    logger.info(
        f"Auto-executing next step in chain: "
        f"{cycle_name} / Stage {stage_num:02d} / Step {step_num:02d}"
    )

    result = execute_notebook(notebook_path, timeout=timeout)

    if result['success']:
        logger.info(
            f"Step {step_num:02d} auto-execution completed successfully "
            f"(execution time: {result['execution_time']:.2f}s)"
        )
    else:
        logger.error(
            f"Step {step_num:02d} auto-execution failed: {result['error']}"
        )

    return result


def validate_nbconvert_available() -> bool:
    """
    Check if jupyter nbconvert is available in the environment.

    Returns:
        True if nbconvert is available, False otherwise
    """
    try:
        result = subprocess.run(
            ['jupyter', 'nbconvert', '--version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            logger.debug(f"nbconvert version: {result.stdout.strip()}")
            return True
        else:
            logger.warning(f"nbconvert check failed: {result.stderr}")
            return False
    except FileNotFoundError:
        logger.error("jupyter command not found in PATH")
        return False
    except Exception as e:
        logger.error(f"Error checking nbconvert availability: {e}")
        return False
