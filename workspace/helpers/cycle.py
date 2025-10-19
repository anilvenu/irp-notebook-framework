"""
IRP Notebook Framework - Cycle Management
"""

import re
import shutil
from pathlib import Path
from typing import List, Any
from helpers import database as db
from .constants import (
    WORKFLOWS_PATH, TEMPLATE_PATH, ARCHIVE_PATH, SYSTEM_USER,
    CYCLE_NAME_RULES, NOTEBOOK_PATTERN, STAGE_PATTERN, CycleStatus
)


class CycleError(Exception):
    """Custom exception for cycle management errors"""
    pass

def delete_archived_cycles() -> int:
    """
    Delete all cycles in ARCHIVED status
    """
    query = """
        DELETE 
        FROM irp_cycle
        WHERE status = 'ARCHIVED'
    """
    
    return db.execute_command(query)


def validate_cycle_name(cycle_name: str) -> bool:
    """
    Validate cycle name against rules.
    
    Args:
        cycle_name: Proposed cycle name
    
    Returns:
        bool of is_valid
    """
    
    # Check length
    if len(cycle_name) < CYCLE_NAME_RULES['min_length']:
        print(f"Name too short (min {CYCLE_NAME_RULES['min_length']} chars)")
        return False
    
    if len(cycle_name) > CYCLE_NAME_RULES['max_length']:
        print(f"Name too long (max {CYCLE_NAME_RULES['max_length']} chars)")
        return False
    
    # Check allowed pattern
    if re.fullmatch(CYCLE_NAME_RULES['valid_pattern'], cycle_name) is None:
        print(f"Name must match pattern: {CYCLE_NAME_RULES['example']}")
        return False
    
    # Check if name already exists
    existing = db.get_cycle_by_name(cycle_name)
    if existing:
        print(f"Cycle '{cycle_name}' already exists")
        return False

    # Check if Active_ directory exists
    active_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"
    if active_dir.exists():
        print(f"Directory 'Active_{cycle_name}' already exists")
        return False

    # Check archive
    archive_dir = ARCHIVE_PATH / cycle_name
    if archive_dir.exists():
        print(f"Cycle '{cycle_name}' exists in archive")
        return False

    return True


def create_cycle(cycle_name: str) -> bool:
    """
    Create a new active cycle.
    
    This will:
    1. Archive any existing active cycle
    2. Create cycle in database
    3. Copy template to Active_<cycle_name>
    4. Register stages and steps
    
    Args:
        cycle_name: Name for the new cycle
    
    Returns:
        True if successful
    """
    try:
        # Validate name
        valid = validate_cycle_name(cycle_name)
        if not valid:
            raise CycleError('Cycle name validation failed')
        print("Cycle name validation passed...")

        # Check for existing active cycle
        active_cycle = db.get_active_cycle()
        
        if active_cycle:
            print(f"Archiving current cycle: {active_cycle['cycle_name']}")
            
            # Move directory to archive
            old_dir = WORKFLOWS_PATH / f"Active_{active_cycle['cycle_name']}"
            new_dir = ARCHIVE_PATH / active_cycle['cycle_name']
            
            if old_dir.exists():
                ARCHIVE_PATH.mkdir(exist_ok=True)
                shutil.move(str(old_dir), str(new_dir))
                print(f"   Moved to archive: {new_dir.name}")
            
            # Archive in database
            db.archive_cycle(active_cycle['id'])
        
        # Create new cycle in database
        print(f"\nCreating cycle: {cycle_name}")
        
        # Create directory from template
        new_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"
        
        if not TEMPLATE_PATH.exists():
            raise CycleError(f"Template directory not found: {TEMPLATE_PATH}")
        
        shutil.copytree(TEMPLATE_PATH, new_dir)
        print(f"Created directory: {new_dir}")

        # Dry run the stage and step registration
        _register_stages_and_steps(0, new_dir, apply=False)


        cycle_id = db.register_cycle(cycle_name)
        
        # Register stages and steps
        print('Registering stages and steps...')
        registered_count = _register_stages_and_steps(cycle_id, new_dir, apply=True)
        print(f"Registered {registered_count} stages with steps")
        
        print(f"\nCycle '{cycle_name}' created successfully")
        return True
        
    except Exception as e:
        print(f"\nFailed to create cycle: {str(e)}")
        return False

def get_stages_and_steps(notebooks_dir=None) -> List[dict]:
    """
    Gets stages and steps from Template directory structure.

    Args:
        notebooks_dir: Path to notebooks directory (defaults to TEMPLATE_PATH/notebooks)  
    Returns:
        List of stages and steps
    """
    
    # Default to template notebooks directory
    if notebooks_dir is None:
        notebooks_dir = TEMPLATE_PATH / "notebooks" # pragma: no cover
    
    if not notebooks_dir.exists():
        raise Exception(f"Template notebooks directory {notebooks_dir} missing")
    
    # Find all stage directories
    stage_dirs = sorted([d for d in notebooks_dir.iterdir() if d.is_dir() and re.match(STAGE_PATTERN, d.name)])

    results = []    
    for stage_dir in stage_dirs:
        # Parse stage info
        match = re.match(STAGE_PATTERN, stage_dir.name)
        if not match:
            continue # pragma: no cover
        
        stage_num = int(match.group(1))
        stage_name = match.group(2)
        
        # Find all step notebooks
        step_files = sorted([f for f in stage_dir.iterdir() if f.is_file() and re.match(NOTEBOOK_PATTERN, f.name)])
        
        for step_file in step_files:
            # Parse step info
            match = re.match(NOTEBOOK_PATTERN, step_file.name)
            if not match:
                continue # pragma: no cover
            step_num = int(match.group(1))
            step_name = match.group(2)
            
            results.append(
                {
                    "stage_id": stage_num, 
                    "stage_name": stage_name, 
                    "step_id": step_num, 
                    "step_name": step_name,
                    "notebook": notebooks_dir / stage_dir.name / step_file.name 
                    }
                    )
        
    return results


def _register_stages_and_steps(cycle_id: int, cycle_dir: Path, apply=False) -> int:
    """
    Register stages and steps from directory structure.
    
    Args:
        cycle_id: Database ID of cycle
        cycle_dir: Path to cycle directory
    
    Returns:
        Number of stages registered
    """
    
    notebooks_dir = cycle_dir / "notebooks"
    
    if not notebooks_dir.exists():
        return 0
    
    stage_count = 0
    
    # Find all stage directories
    stage_dirs = sorted([d for d in notebooks_dir.iterdir() if d.is_dir() and re.match(STAGE_PATTERN, d.name)])
    
    for stage_dir in stage_dirs:
        # Parse stage info
        match = re.match(STAGE_PATTERN, stage_dir.name)
        if not match:
            continue # pragma: no cover
        
        stage_num = int(match.group(1))
        stage_name = match.group(2)
        
        # Create stage
        if apply:
            stage_id = db.get_or_create_stage(cycle_id, stage_num, stage_name)
        else:
            print(f"Stage: {stage_num} - {stage_name}")
        
        # Find all step notebooks
        step_files = sorted([f for f in stage_dir.iterdir() if f.is_file() and re.match(NOTEBOOK_PATTERN, f.name)])
        
        for step_file in step_files:
            # Parse step info
            match = re.match(NOTEBOOK_PATTERN, step_file.name)
            if not match:
                continue # pragma: no cover
            step_num = int(match.group(1))
            step_name = match.group(2)
            
            # Create step
            if apply:
                db.get_or_create_step(
                    stage_id,
                    step_num,
                    step_name,
                    str(step_file)
                )
            else:
                print(f"Step: {step_num} - {step_name}")
        
        stage_count += 1
    
    return stage_count


def get_active_cycle_id() -> int:
    """
    Get the ID of the currently active cycle

    Uses schema from context (see db_context.py).

    Returns:
        Cycle ID of active cycle, or None if no active cycle
    """
    active_cycle = db.get_active_cycle()
    return active_cycle['id'] if active_cycle else None


def get_cycle_status() -> Any:
    """Get status of all cycles"""

    query = """
        SELECT
            id,
            cycle_name,
            status,
            created_ts,
            archived_ts
        FROM irp_cycle
        ORDER BY
            CASE status
                WHEN 'ACTIVE' THEN 1
                WHEN 'ARCHIVED' THEN 2
                ELSE 3
            END,
            created_ts DESC
    """

    return db.execute_query(query)


def archive_cycle_by_name(cycle_name: str) -> bool:
    """
    Archive a cycle by name.
    
    Args:
        cycle_name: Name of cycle to archive
    
    Returns:
        True if successful
    """
    
    try:
        cycle = db.get_cycle_by_name(cycle_name)
        
        if not cycle:
            print(f"Cycle '{cycle_name}' not found")
            return False
        
        if cycle['status'] == CycleStatus.ARCHIVED:
            print(f"Cycle '{cycle_name}' is already archived")
            return True
        
        # Archive in database
        db.archive_cycle(cycle['id'])
        
        # Move directory
        old_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"
        new_dir = ARCHIVE_PATH / cycle_name
        
        if old_dir.exists():
            ARCHIVE_PATH.mkdir(exist_ok=True)
            shutil.move(str(old_dir), str(new_dir))
        
        print(f"Cycle '{cycle_name}' archived")
        return True
        
    except Exception as e:
        print(f"Failed to archive cycle: {str(e)}")
        return False


def get_cycle_progress(cycle_name: str) -> Any:
    """Get progress for a cycle"""
    return db.get_cycle_progress(cycle_name)


def get_step_history(
    cycle_name: str,
    stage_num: int = None,
    step_num: int = None
) -> Any:
    """Get execution history for steps"""
    return db.get_step_history(cycle_name, stage_num, step_num)