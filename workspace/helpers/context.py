"""
IRP Notebook Framework - Work Context
Automatically detects and manages workflow context from notebook path
"""

import re
from pathlib import Path
from typing import Optional
from helpers.cycle import get_cycle_by_name, get_active_cycle, register_cycle
from helpers.stage import get_or_create_stage
from helpers.database import DatabaseError
from helpers.constants import NOTEBOOK_PATTERN, STAGE_PATTERN, CycleStatus
from helpers.notebook import get_current_notebook_path

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
            notebook_path = get_current_notebook_path()  # pragma: no cover

        self.notebook_path = Path(notebook_path)
        
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
        
        cycle_part = parts[1].split('/')[0]
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
        
        # Extract step info from filename
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