"""
Notebook Setup Helper

Provides utilities for initializing Jupyter notebooks with consistent
path detection, context initialization, and step tracking.
"""

import sys
from pathlib import Path
from typing import Tuple

from helpers.context import WorkContext
from helpers.step import Step


def initialize_notebook_context(
    notebook_filename: str
) -> Tuple[WorkContext, Step]:
    """
    Initialize notebook context and step tracking.

    This function handles:
    - Automatic path detection (works in both Active_* directories and /home/jovyan)
    - WorkContext initialization
    - Step tracking initialization
    - sys.path configuration for workspace imports

    Args:
        notebook_filename: Name of the notebook file (e.g., 'Step_01_Submit_Create_EDM_Batch.ipynb')

    Returns:
        Tuple of (WorkContext, Step) objects

    Raises:
        RuntimeError: If workspace or Active_ directory cannot be found

    Example:
        >>> context, step = initialize_notebook_context('Step_01_Submit_Create_EDM_Batch.ipynb')
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

    return context, step
