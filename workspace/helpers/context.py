"""
IRP Notebook Framework - Work Context
Automatically detects and manages workflow context from notebook path
"""

import re
import os
from pathlib import Path
from typing import Optional
from helpers.cycle import get_cycle_by_name, get_active_cycle, register_cycle
from helpers.stage import get_or_create_stage
from helpers.database import DatabaseError
from helpers.constants import NOTEBOOK_PATTERN, STAGE_PATTERN, CycleStatus

class WorkContextError(Exception):
    """Custom exception for context errors"""
    pass


class WorkContext:
    """
    Automatically creates workflow context from notebook path.
    
    Usage:
        context = WorkContext()
        
        # Access context properties
        print(context.cycle_name)  # e.g., "Q1_2024_Analysis"
        print(context.stage_num)   # e.g., 1
        print(context.step_num)    # e.g., 2
        print(context.step_id)     # Database ID of step
    """
    
    def __init__(self, notebook_path: str = None):
        """
        Initialize work context by parsing notebook path.

        Args:
            notebook_path: Full path to notebook (auto-detected if None)
        """

        # Get notebook path
        if notebook_path is None:
            # Use current working directory - JupyterLab sets this to the notebook's directory
            cwd = Path(os.getcwd())

            # Verify the directory structure is correct
            if 'Active_' not in str(cwd):
                raise WorkContextError(
                    f"Current working directory does not contain 'Active_' in path: {cwd}\n\n"
                    f"This usually means the notebook is being executed from the wrong location.\n"
                    f"Please ensure you opened the notebook from its correct location in:\n"
                    f"  .../workflows/Active_{{CycleName}}/notebooks/Stage_XX_{{StageName}}/\n\n"
                    f"JupyterLab Tip: Right-click the notebook in the file browser and select 'Open With' → 'Notebook'\n"
                    f"to ensure it opens in the correct working directory."
                )

            notebook_path = cwd
        else:
            notebook_path = Path(notebook_path)

        self.notebook_path = notebook_path
        
        # Parse path to extract context
        self._parse_path()
        
        # Get or create database entries
        self._ensure_database_entries()
        
    
    def _parse_path(self):
        """Parse notebook path to extract cycle, stage, and step information"""

        path_str = str(self.notebook_path)

        # Extract cycle name from path
        if 'Active_' not in path_str:
            raise WorkContextError(
                f"Notebook is not in an active cycle directory. Path: {path_str}"
            )

        # Extract cycle name
        parts = path_str.split('Active_')
        if len(parts) < 2:
            raise WorkContextError("Invalid path structure - cannot find cycle name")

        # Handle both Windows and Unix path separators
        cycle_part = parts[1].split('/')[0].split('\\')[0]
        self.cycle_name = cycle_part

        # Extract stage info from directory name
        stage_match = None
        for part in self.notebook_path.parts:
            match = re.match(STAGE_PATTERN, part)
            if match:
                stage_match = match
                break

        if not stage_match:
            raise WorkContextError(
                f"Cannot find stage directory in path: {path_str}"
            )

        self.stage_num = int(stage_match.group(1))
        self.stage_name = stage_match.group(2)

        # Extract step info from directory name if notebook_path is a directory
        # or from filename if it's a file
        if self.notebook_path.is_dir():
            # Path is a directory (current working directory in JupyterLab)
            # Look for any .ipynb file in this directory to parse the step info
            ipynb_files = list(self.notebook_path.glob("Step_*.ipynb"))
            if ipynb_files:
                # Use the first Step_*.ipynb file found to extract step info
                step_match = re.match(NOTEBOOK_PATTERN, ipynb_files[0].name)
                if not step_match:
                    raise WorkContextError(
                        f"Invalid notebook filename format: {ipynb_files[0].name}"
                    )
            else:
                # No Step_*.ipynb files - this might be in a stage directory
                # This shouldn't happen in normal workflow but provide a helpful error
                raise WorkContextError(
                    f"No Step_*.ipynb files found in directory: {self.notebook_path}"
                )
        else:
            # Path is a file
            step_match = re.match(NOTEBOOK_PATTERN, self.notebook_path.name)
            if not step_match:
                raise WorkContextError(
                    f"Invalid notebook filename format: {self.notebook_path.name}"
                )

        self.step_num = int(step_match.group(1))
        self.step_name = step_match.group(2)
        
        print(f"Context detected: {self.cycle_name} → Stage {self.stage_num} → Step {self.step_num}")
    
    
    def _ensure_database_entries(self):
        """Ensure cycle, stage, and step exist in database"""

        # Import here to avoid circular dependency
        from helpers.step import get_or_create_step

        try:
            # Get or create cycle
            cycle = get_cycle_by_name(self.cycle_name)

            if not cycle:
                # Check if there's an active cycle
                active = get_active_cycle()
                if active and active['cycle_name'] != self.cycle_name:
                    raise WorkContextError(
                        f"Active cycle '{active['cycle_name']}' exists, but notebook is in '{self.cycle_name}'"
                    )

                # Create cycle
                self.cycle_id = register_cycle(self.cycle_name)
                print(f"Created cycle: {self.cycle_name}")
            else:
                self.cycle_id = cycle['id']

                # Verify cycle is active
                if cycle['status'] != CycleStatus.ACTIVE:
                    raise WorkContextError(
                        f"Cycle '{self.cycle_name}' is {cycle['status']}, not active"
                    )

            # Get or create stage
            self.stage_id = get_or_create_stage(
                self.cycle_id,
                self.stage_num,
                self.stage_name
            )

            # Get or create step
            self.step_id = get_or_create_step(
                self.stage_id,
                self.step_num,
                self.step_name,
                str(self.notebook_path)
            )

            print(f"Database entries ready (step_id={self.step_id})")

        except DatabaseError as e:
            raise WorkContextError(f"Database error: {str(e)}")
    
    
    def get_info(self) -> dict:
        """Get all context information as dictionary"""
        return {
            'cycle_name': self.cycle_name,
            'cycle_id': self.cycle_id,
            'stage_num': self.stage_num,
            'stage_name': self.stage_name,
            'stage_id': self.stage_id,
            'step_num': self.step_num,
            'step_name': self.step_name,
            'step_id': self.step_id,
            'notebook_path': str(self.notebook_path)
        }
    
    
    def __repr__(self):
        return (f"WorkContext(cycle='{self.cycle_name}', "
                f"stage={self.stage_num}, step={self.step_num})")
    
    
    def __str__(self):
        return f"{self.cycle_name} → Stage {self.stage_num}: {self.stage_name} → Step {self.step_num}: {self.step_name}"

    @property
    def cycle_directory(self):
        """Get the cycle directory path"""
        # Navigate up from notebook_path to find the Active_{cycle_name} directory
        current = self.notebook_path if self.notebook_path.is_dir() else self.notebook_path.parent

        # Walk up the directory tree to find Active_{cycle_name}
        for parent in [current] + list(current.parents):
            if parent.name.startswith('Active_') and parent.name == f'Active_{self.cycle_name}':
                return parent

        # Shouldn't reach here if path parsing succeeded
        raise WorkContextError(f"Could not find cycle directory for {self.cycle_name}")


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

def get_context(notebook_path: str = None) -> WorkContext:
    """
    Convenience function to create WorkContext.
    
    Args:
        notebook_path: Full path to notebook (auto-detected if None)
    
    Returns:
        WorkContext object
    
    Example:
        context = get_context()
        print(context.cycle_name)
    """
    return WorkContext(notebook_path)