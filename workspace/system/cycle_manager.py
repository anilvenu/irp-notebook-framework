"""
IRP Notebook Framework - Cycle Management
"""

import re
import shutil
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from . import database as db
from .config import (
    WORKFLOWS_PATH, TEMPLATE_PATH, ARCHIVE_PATH, SYSTEM_USER,
    CYCLE_NAME_RULES, NOTEBOOK_PATTERN, STAGE_PATTERN
)


class CycleError(Exception):
    """Custom exception for cycle management errors"""
    pass


def validate_cycle_name(cycle_name: str) -> Tuple[bool, str]:
    """
    Validate cycle name against rules.
    
    Args:
        cycle_name: Proposed cycle name
    
    Returns:
        Tuple of (is_valid, message)
    """
    
    # Check length
    if len(cycle_name) < CYCLE_NAME_RULES['min_length']:
        return False, f"Name too short (min {CYCLE_NAME_RULES['min_length']} chars)"
    
    if len(cycle_name) > CYCLE_NAME_RULES['max_length']:
        return False, f"Name too long (max {CYCLE_NAME_RULES['max_length']} chars)"
    
    # Check allowed characters
    if not re.match(CYCLE_NAME_RULES['allowed_chars'], cycle_name):
        return False, "Name can only contain letters, numbers, underscores, and hyphens"
    
    # Check forbidden prefixes
    for prefix in CYCLE_NAME_RULES['forbidden_prefixes']:
        if cycle_name.startswith(prefix):
            return False, f"Name cannot start with '{prefix}'"
    
    # Check if name already exists
    existing = db.get_cycle_by_name(cycle_name)
    if existing:
        return False, f"Cycle '{cycle_name}' already exists"
    
    # Check if Active_ directory exists
    active_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"
    if active_dir.exists():
        return False, f"Directory 'Active_{cycle_name}' already exists"
    
    # Check archive
    archive_dir = ARCHIVE_PATH / cycle_name
    if archive_dir.exists():
        return False, f"Cycle '{cycle_name}' exists in archive"
    
    return True, "Valid"


def create_cycle(cycle_name: str, created_by: str = None) -> bool:
    """
    Create a new active cycle.
    
    This will:
    1. Archive any existing active cycle
    2. Create cycle in database
    3. Copy template to Active_<cycle_name>
    4. Register stages and steps
    
    Args:
        cycle_name: Name for the new cycle
        created_by: User creating the cycle
    
    Returns:
        True if successful
    """
    
    if created_by is None:
        created_by = SYSTEM_USER
    
    try:
        # Validate name
        valid, message = validate_cycle_name(cycle_name)
        if not valid:
            raise CycleError(message)
        
        # Check for existing active cycle
        active_cycle = db.get_active_cycle()
        
        if active_cycle:
            print(f"Archiving current cycle: {active_cycle['cycle_name']}")
            
            # Archive in database
            db.archive_cycle(active_cycle['id'])
            
            # Move directory to archive
            old_dir = WORKFLOWS_PATH / f"Active_{active_cycle['cycle_name']}"
            new_dir = ARCHIVE_PATH / active_cycle['cycle_name']
            
            if old_dir.exists():
                ARCHIVE_PATH.mkdir(exist_ok=True)
                shutil.move(str(old_dir), str(new_dir))
                print(f"   Moved to archive: {new_dir.name}")
        
        # Create new cycle in database
        print(f"\nCreating cycle: {cycle_name}")
        cycle_id = db.create_cycle(cycle_name, created_by)
        
        # Create directory from template
        new_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"
        
        if not TEMPLATE_PATH.exists():
            raise CycleError(f"Template directory not found: {TEMPLATE_PATH}")
        
        shutil.copytree(TEMPLATE_PATH, new_dir)
        print(f"Created directory: {new_dir}")
        
        # Register stages and steps
        registered_count = _register_stages_and_steps(cycle_id, new_dir)
        print(f"Registered {registered_count} stages with steps")
        
        print(f"\nCycle '{cycle_name}' created successfully")
        return True
        
    except Exception as e:
        print(f"\nFailed to create cycle: {str(e)}")
        return False


def _register_stages_and_steps(cycle_id: int, cycle_dir: Path) -> int:
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
            continue
        
        stage_num = int(match.group(1))
        stage_name = match.group(2)
        
        # Create stage
        stage_id = db.get_or_create_stage(cycle_id, stage_num, stage_name)
        
        # Find all step notebooks
        step_files = sorted([f for f in stage_dir.iterdir() if f.is_file() and re.match(NOTEBOOK_PATTERN, f.name)])
        
        for step_file in step_files:
            # Parse step info
            match = re.match(NOTEBOOK_PATTERN, step_file.name)
            if not match:
                continue
            
            step_num = int(match.group(1))
            step_name = match.group(2)
            
            # Create step
            db.get_or_create_step(
                stage_id,
                step_num,
                step_name,
                str(step_file),
                is_idempotent=False
            )
        
        stage_count += 1
    
    return stage_count


def get_cycle_status() -> Any:
    """Get status of all cycles"""
    
    query = """
        SELECT 
            id,
            cycle_name,
            status,
            created_ts,
            archived_ts,
            created_by
        FROM irp_cycle
        ORDER BY 
            CASE status 
                WHEN 'active' THEN 1
                WHEN 'archived' THEN 2
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
        
        if cycle['status'] == 'archived':
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