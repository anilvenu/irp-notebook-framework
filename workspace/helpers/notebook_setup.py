"""
Notebook Setup Helper

Provides utilities for initializing Jupyter notebooks with consistent
path detection, context initialization, and step tracking.
"""

import sys
from pathlib import Path
from typing import Tuple

from helpers.context import WorkContext
from helpers.step import Step, get_last_step_run
from helpers import ux


def initialize_notebook_context(
    notebook_filename: str,
    allow_rerun: bool = False
) -> Tuple[WorkContext, Step]:
    """
    Initialize notebook context and step tracking.

    This function handles:
    - Automatic path detection (works in both Active_* directories and /home/jovyan)
    - WorkContext initialization
    - Step tracking initialization with execution guards
    - sys.path configuration for workspace imports

    Args:
        notebook_filename: Name of the notebook file (e.g., 'Step_01_Submit_Create_EDM_Batch.ipynb')
        allow_rerun: If True, allows step to be re-executed automatically (default: False)
                     If False, raises SystemExit if step already executed

    Returns:
        Tuple of (WorkContext, Step) objects

    Raises:
        RuntimeError: If workspace or Active_ directory cannot be found
        SystemExit: If step already executed and allow_rerun=False

    Example:
        >>> # Standard execution - exits if already completed
        >>> context, step = initialize_notebook_context('Step_01_Submit_Create_EDM_Batch.ipynb')
        >>>
        >>> # Allow re-execution - automatically reruns if already completed
        >>> context, step = initialize_notebook_context('Step_01_Submit_Create_EDM_Batch.ipynb', allow_rerun=True)
    """
    # Determine the notebook's actual directory
    cwd = Path.cwd()

    if 'Active_' in str(cwd):
        # Working directory is set correctly, construct path to THIS notebook
        notebook_path = cwd / notebook_filename
    else:
        # Working directory is not set correctly (e.g., /home/jovyan)
        home = Path.home()
        workspace = home / 'workspace'

        if workspace.exists():
            workflows = workspace / 'workflows'
            active_dirs = list(workflows.glob('Active_*'))

            if active_dirs:
                # Use the first Active_ directory found
                # Extract stage directory from notebook filename path
                # Assumes notebook_filename might be just filename or include path
                if '/' in notebook_filename or '\\' in notebook_filename:
                    # Path provided, use as-is
                    notebook_path = active_dirs[0] / 'notebooks' / notebook_filename
                else:
                    # Just filename provided, need to infer stage directory
                    # This is a limitation - we need the stage directory
                    # For now, search for the notebook file
                    found = False
                    for stage_dir in (active_dirs[0] / 'notebooks').iterdir():
                        if stage_dir.is_dir() and stage_dir.name.startswith('Stage_'):
                            potential_path = stage_dir / notebook_filename
                            if potential_path.exists():
                                notebook_path = potential_path
                                found = True
                                break

                    if not found:
                        # Fall back to assuming Stage_03_Data_Import (common case)
                        # This maintains backward compatibility with existing code
                        notebook_path = active_dirs[0] / 'notebooks' / 'Stage_03_Data_Import' / notebook_filename
            else:
                raise RuntimeError("No Active_ cycle directory found in workspace/workflows/")
        else:
            raise RuntimeError("Workspace directory not found")

    print(f"Notebook path: {notebook_path}")

    # Add workspace to path
    workspace_path = notebook_path.parent.parent.parent.parent
    if str(workspace_path) not in sys.path:
        sys.path.insert(0, str(workspace_path))

    # Initialize context
    context = WorkContext(notebook_path=str(notebook_path))

    # Initialize step execution tracking
    step = Step(context)

    # Handle execution guards
    if step.executed:
        ux.warning("⚠ This step has already been executed")
        ux.info(f"Message: {step.status_message}")

        last_run = get_last_step_run(step.step_id)
        if last_run:
            ux.info(f"Last run: #{last_run['run_num']}")
            ux.info(f"Status: {last_run['status']}")
            if last_run['completed_ts']:
                ux.info(f"Completed: {last_run['completed_ts'].strftime('%Y-%m-%d %H:%M:%S')}")

        if allow_rerun:
            # Automatically re-run the step without prompting
            ux.info("Re-running step (allow_rerun=True)...")
            step.start(force=True)
        else:
            # Exit if step already executed and rerun not allowed
            ux.info("Step execution skipped (already completed)")
            raise SystemExit("Step already completed")

    # Register IPython exception handler to auto-fail step on uncaught exceptions
    _register_exception_handler(step)

    return context, step


def _register_exception_handler(step: Step) -> None:
    """
    Register IPython exception handler to automatically fail step on uncaught exceptions.

    This ensures that when any cell raises an exception, the step is marked as FAILED
    in the database and a Teams notification is sent.

    Args:
        step: Step object to fail on exception
    """
    try:
        from IPython import get_ipython
        ip = get_ipython()

        if ip is None:
            # Not running in IPython/Jupyter - skip registration
            return

        # Store original showtraceback method
        _original_showtraceback = ip.showtraceback

        def _on_exception(*args, **kwargs):
            """Handle uncaught exceptions by marking step as failed."""
            import sys

            # Get the current exception info
            exc_type, exc_value, exc_tb = sys.exc_info()

            # Mark step as failed if it has a run_id (was started)
            if step.run_id:
                error_msg = f"{exc_type.__name__}: {exc_value}" if exc_value else str(exc_type)
                step.fail(error_msg)

            # Call the original showtraceback to display the normal traceback
            _original_showtraceback(*args, **kwargs)

        # Replace the showtraceback method
        ip.showtraceback = _on_exception

    except ImportError:
        # IPython not available - skip registration
        pass
    except Exception as e:
        # Don't let handler registration failure break notebook initialization
        print(f"Warning: Failed to register exception handler: {e}")
