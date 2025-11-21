"""
Notebook execution engine for automatic step chaining.

This module provides functionality to programmatically execute Jupyter notebooks
using nbconvert, enabling automated workflow execution.
"""

import subprocess
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


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
