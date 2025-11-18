"""
IRP Notebook Framework - Cycle Management

This module provides both CRUD operations and workflow functions for managing cycles.

LAYERS:
-------
- Layer 2 (CRUD): Simple create/read/update/delete operations (register_cycle, archive_cycle, etc.)
- Layer 3 (Workflows): Complex multi-step operations (create_cycle, archive_cycle_by_name, etc.)

TRANSACTION BEHAVIOR:
--------------------
- CRUD functions (Layer 2): Never manage transactions
- Workflow functions (Layer 3): May use transaction_context() when atomicity needed
"""

import re
import shutil
from pathlib import Path
from typing import List, Any, Optional, Dict
import pandas as pd
from helpers.database import execute_query, execute_insert, execute_command
from .constants import (
    WORKFLOWS_PATH, TEMPLATE_PATH, ARCHIVE_PATH, SYSTEM_USER,
    CYCLE_NAME_RULES, NOTEBOOK_PATTERN, STAGE_PATTERN, CycleStatus
)


class CycleError(Exception):
    """Custom exception for cycle management errors"""
    pass


# ============================================================================
# CYCLE CRUD OPERATIONS (Layer 2)
# ============================================================================

def get_active_cycle() -> Optional[Dict[str, Any]]:
    """
    Get the currently active cycle from the current schema.

    Returns the most recently created ACTIVE cycle. There should only be one
    active cycle at a time, but this ensures you get the latest if multiple exist.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Schema Behavior:
        - Uses current schema from context (get_current_schema())
        - To query a different schema, use set_schema() or schema_context()
        - Searches only within the current context schema

    Returns:
        Optional[Dict[str, Any]]: Dictionary with cycle fields or None
            {
                'id': int,           # Unique cycle ID
                'cycle_name': str,   # e.g., 'Analysis-2025-Q1'
                'status': str,       # 'ACTIVE' or 'ARCHIVED'
                'created_ts': datetime
            }
    """
    query = """
        SELECT id, cycle_name, status, created_ts
        FROM irp_cycle
        WHERE status = 'ACTIVE'
        ORDER BY created_ts DESC
        LIMIT 1
    """
    df = execute_query(query)
    return df.iloc[0].to_dict() if not df.empty else None


def get_cycle_by_name(cycle_name: str) -> Optional[Dict[str, Any]]:
    """
    Get cycle by its name from the current schema.

    Retrieves a cycle regardless of status (ACTIVE or ARCHIVED). Cycle names
    must be unique within a schema.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_name: Name of the cycle to retrieve (e.g., 'Analysis-2025-Q1')

    Schema Behavior:
        - Uses current schema from context
        - To query a different schema, use schema_context() first

    Returns:
        Optional[Dict[str, Any]]: Dictionary with cycle fields or None if not found
            {
                'id': int,
                'cycle_name': str,
                'status': str,         # 'ACTIVE' or 'ARCHIVED'
                'created_ts': datetime,
                'archived_ts': datetime | None
            }
    """
    query = """
        SELECT id, cycle_name, status, created_ts, archived_ts
        FROM irp_cycle
        WHERE cycle_name = %s
    """
    df = execute_query(query, (cycle_name,))
    return df.iloc[0].to_dict() if not df.empty else None


def register_cycle(cycle_name: str) -> int:
    """
    Create a new cycle with ACTIVE status.

    Creates a new analysis cycle in the current schema. The cycle is automatically
    set to ACTIVE status.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_name: Unique name for the cycle (e.g., 'Analysis-2025-Q1')
                   Should follow naming convention: 'Analysis-YYYY-QN'

    Schema Behavior:
        - Creates cycle in current schema from context
        - To create in different schema, use schema_context() or set_schema() first
        - Cycle names must be unique within their schema

    Returns:
        int: ID of the newly created cycle

    Raises:
        DatabaseError: If cycle name already exists in the schema
    """
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
    """
    return execute_insert(query, (cycle_name, CycleStatus.ACTIVE))


def archive_cycle_crud(cycle_id: int) -> bool:
    """
    Archive a cycle by setting its status to ARCHIVED.

    Marks a cycle as archived and records the archival timestamp. This does not
    delete any data - the cycle and all its stages/steps remain in the database.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_id: ID of the cycle to archive

    Schema Behavior:
        - Updates cycle in current schema from context
        - To update in different schema, use schema_context() or set_schema() first

    Returns:
        bool: True if cycle was archived, False if cycle_id not found
    """
    query = """
        UPDATE irp_cycle
        SET status = %s, archived_ts = NOW()
        WHERE id = %s
    """
    rows = execute_command(query, (CycleStatus.ARCHIVED, cycle_id))
    return rows > 0


def delete_cycle(cycle_id: int) -> bool:
    """
    Permanently delete a cycle and all associated data.

    WARNING: This is a hard delete that will remove ALL related records:
    - All batches and jobs (must be deleted first due to FK constraints)
    - All configurations
    - All stages, steps, and step runs
    - All associated execution history

    This operation CANNOT be undone. Consider using archive_cycle_by_name() instead
    for most use cases.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_id: ID of the cycle to delete

    Schema Behavior:
        - Deletes from current schema from context
        - To delete from different schema, use schema_context() or set_schema() first

    Returns:
        bool: True if cycle was deleted, False if cycle_id not found

    Example:
        # Typical use: Clean up test data
        with schema_context('test'):
            test_cycle = get_cycle_by_name('test-cycle')
            if test_cycle:
                delete_cycle(test_cycle['id'])
                print("Test data cleaned up")

    Notes:
        Use archive_cycle_by_name(): Safer alternative that preserves data
        Use get_cycle_by_name(): To verify before deleting
    """
    # Get all configuration IDs for this cycle
    config_query = """
        SELECT id FROM irp_configuration
        WHERE cycle_id = %s
    """
    config_df = execute_query(config_query, (cycle_id,))

    if not config_df.empty:
        config_ids = tuple(config_df['id'].tolist())

        # Get all batch IDs for these configurations
        if len(config_ids) == 1:
            batch_query = "SELECT id FROM irp_batch WHERE configuration_id = %s"
            batch_df = execute_query(batch_query, config_ids)
        else:
            batch_query = "SELECT id FROM irp_batch WHERE configuration_id IN %s"
            batch_df = execute_query(batch_query, (config_ids,))

        if not batch_df.empty:
            batch_ids = tuple(batch_df['id'].tolist())

            # Delete in order: irp_job -> irp_job_configuration -> irp_batch
            # (irp_job has CASCADE from irp_batch, but we'll be explicit)

            # Delete jobs first
            if len(batch_ids) == 1:
                execute_command("DELETE FROM irp_job WHERE batch_id = %s", batch_ids)
            else:
                execute_command("DELETE FROM irp_job WHERE batch_id IN %s", (batch_ids,))

            # Delete job configurations
            if len(batch_ids) == 1:
                execute_command("DELETE FROM irp_job_configuration WHERE batch_id = %s", batch_ids)
            else:
                execute_command("DELETE FROM irp_job_configuration WHERE batch_id IN %s", (batch_ids,))

            # Finally delete batches
            if len(batch_ids) == 1:
                execute_command("DELETE FROM irp_batch WHERE id = %s", batch_ids)
            else:
                execute_command("DELETE FROM irp_batch WHERE id IN %s", (batch_ids,))

    # Now delete the cycle (which will CASCADE to configurations, stages, steps, etc.)
    query = """
        DELETE FROM irp_cycle
        WHERE id = %s
    """
    rows = execute_command(query, (cycle_id,))
    return rows > 0


def get_cycle_progress(cycle_name: str) -> Any:
    """
    Get progress for all steps in a cycle.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_name: Name of the cycle

    Returns:
        DataFrame with progress information for all steps
    """
    query = """
        SELECT
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            sr.status as last_status,
            sr.run_num as last_run,
            sr.completed_ts as last_completed
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        LEFT JOIN LATERAL (
            SELECT status, run_num, completed_ts
            FROM irp_step_run
            WHERE step_id = st.id
            ORDER BY run_num DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE c.cycle_name = %s
        ORDER BY sg.stage_num, st.step_num
    """
    return execute_query(query, (cycle_name,))


def get_step_history(
    cycle_name: str,
    stage_num: int = None,
    step_num: int = None
) -> Any:
    """
    Get execution history for steps.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_name: Name of the cycle
        stage_num: Optional filter for specific stage
        step_num: Optional filter for specific step

    Returns:
        DataFrame with step execution history
    """
    base_query = """
        SELECT
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            sr.run_num,
            sr.status,
            sr.started_ts,
            sr.completed_ts,
            sr.started_by,
            EXTRACT(EPOCH FROM (sr.completed_ts - sr.started_ts)) as duration_seconds
        FROM irp_step_run sr
        INNER JOIN irp_step st ON sr.step_id = st.id
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        WHERE c.cycle_name = %s
    """

    params = [cycle_name]

    if stage_num is not None:
        base_query += " AND sg.stage_num = %s"
        params.append(stage_num)

    if step_num is not None:
        base_query += " AND st.step_num = %s"
        params.append(step_num)

    base_query += " ORDER BY sr.started_ts DESC"

    return execute_query(base_query, tuple(params))


# ============================================================================
# CYCLE WORKFLOW OPERATIONS (Layer 3)
# ============================================================================

def delete_archived_cycles() -> int:
    """
    Delete all cycles in ARCHIVED status.

    LAYER: 3 (Workflow)

    TRANSACTION BEHAVIOR:
        - Does NOT use transaction_context() (simple single-operation delete)
        - Could be wrapped in transaction_context() by caller if needed

    Returns:
        Number of cycles deleted
    """
    # Get all archived cycle IDs
    archived_query = """
        SELECT id FROM irp_cycle
        WHERE status = 'ARCHIVED'
    """
    archived_df = execute_query(archived_query)

    if archived_df.empty:
        return 0

    # Delete each archived cycle using the delete_cycle function
    # which properly handles all foreign key constraints
    count = 0
    for cycle_id in archived_df['id'].tolist():
        if delete_cycle(cycle_id):
            count += 1

    return count


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

    # Check for invalid characters (only allow alphanumeric, hyphens, and underscores)
    if not re.match(r'^[A-Za-z0-9_-]+$', cycle_name):
        print(f"Invalid characters in cycle name. Only alphanumeric, hyphens, and underscores allowed")
        return False

    # Check if name already exists
    existing = get_cycle_by_name(cycle_name)
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
        active_cycle = get_active_cycle()

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
            archive_cycle_crud(active_cycle['id'])

        # Create new cycle in database
        print(f"\nCreating cycle: {cycle_name}")

        # Create directory from template
        new_dir = WORKFLOWS_PATH / f"Active_{cycle_name}"

        if not TEMPLATE_PATH.exists():
            raise CycleError(f"Template directory not found: {TEMPLATE_PATH}")

        shutil.copytree(TEMPLATE_PATH, new_dir)
        print(f"Created directory: {new_dir}")

        # Create a /files/data subdirectory
        data_dir = new_dir / "files" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created data directory: {data_dir}")

        # Parse directory structure to validate it contains stages/steps
        print('\nValidating directory structure...')
        stages_steps = generate_stages_and_steps(new_dir)

        if not stages_steps:
            raise CycleError(f"No stages/steps found in {new_dir}/notebooks")

        # Count unique stages for reporting
        unique_stages = len(set((s['stage_num'], s['stage_name']) for s in stages_steps))
        print(f"Found {unique_stages} stage(s) with {len(stages_steps)} step(s)")

        # Display what will be registered
        for item in stages_steps:
            if item['step_num'] == 1:  # First step of each stage
                print(f"  Stage {item['stage_num']}: {item['stage_name']}")
            print(f"    Step {item['step_num']}: {item['step_name']}")

        # Create cycle in database
        cycle_id = register_cycle(cycle_name)

        # Register stages and steps in database
        print('\nRegistering stages and steps in database...')
        registered_count = register_stages_and_steps(cycle_id, stages_steps)
        print(f"Registered {registered_count} stage(s) with {len(stages_steps)} step(s)")
        
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


def generate_stages_and_steps(cycle_dir: Path) -> List[Dict[str, Any]]:
    """
    Parse directory structure to extract stages and steps information.

    This function does NOT touch the database - it only reads the filesystem.
    Use register_stages_and_steps() to write the parsed data to database.

    LAYER: 3 (Workflow - filesystem parsing)

    Args:
        cycle_dir: Path to cycle directory (e.g., Active_Analysis-2025-Q1)

    Returns:
        List of stage/step dictionaries:
        [
            {
                'stage_num': 1,
                'stage_name': 'Setup',
                'step_num': 1,
                'step_name': 'Initialize',
                'notebook_path': '/path/to/Step_01_Initialize.ipynb'
            },
            ...
        ]

    Raises:
        CycleError: If notebooks directory doesn't exist
    """
    notebooks_dir = cycle_dir / "notebooks"

    if not notebooks_dir.exists():
        raise CycleError(f"Notebooks directory not found: {notebooks_dir}")

    results = []

    # Find all stage directories
    stage_dirs = sorted([
        d for d in notebooks_dir.iterdir()
        if d.is_dir() and re.match(STAGE_PATTERN, d.name)
    ])

    for stage_dir in stage_dirs:
        # Parse stage info
        match = re.match(STAGE_PATTERN, stage_dir.name)
        if not match:
            continue  # pragma: no cover

        stage_num = int(match.group(1))
        stage_name = match.group(2)

        # Find all step notebooks
        step_files = sorted([
            f for f in stage_dir.iterdir()
            if f.is_file() and re.match(NOTEBOOK_PATTERN, f.name)
        ])

        for step_file in step_files:
            # Parse step info
            match = re.match(NOTEBOOK_PATTERN, step_file.name)
            if not match:
                continue  # pragma: no cover

            step_num = int(match.group(1))
            step_name = match.group(2)

            results.append({
                'stage_num': stage_num,
                'stage_name': stage_name,
                'step_num': step_num,
                'step_name': step_name,
                'notebook_path': str(step_file)
            })

    return results


def register_stages_and_steps(cycle_id: int, stages_steps: List[Dict[str, Any]]) -> int:
    """
    Register stages and steps in the database.

    Uses domain-specific CRUD functions from helpers.stage and helpers.step.
    This function writes to database based on parsed data from generate_stages_and_steps().

    LAYER: 3 (Workflow - orchestrates CRUD operations)

    Args:
        cycle_id: Database ID of the cycle
        stages_steps: List of stage/step dictionaries from generate_stages_and_steps()

    Returns:
        int: Number of stages registered

    Raises:
        CycleError: If registration fails
    """
    # Import domain-specific CRUD functions to avoid circular imports
    from helpers.stage import get_or_create_stage
    from helpers.step import get_or_create_step

    if not stages_steps:
        return 0

    # Group by stage to track unique stages
    stages_created = {}

    for item in stages_steps:
        stage_key = (item['stage_num'], item['stage_name'])

        # Create stage if we haven't seen it yet
        if stage_key not in stages_created:
            stage_id = get_or_create_stage(
                cycle_id=cycle_id,
                stage_num=item['stage_num'],
                stage_name=item['stage_name']
            )
            stages_created[stage_key] = stage_id
        else:
            stage_id = stages_created[stage_key]

        # Create step
        get_or_create_step(
            stage_id=stage_id,
            step_num=item['step_num'],
            step_name=item['step_name'],
            notebook_path=item['notebook_path']
        )

    return len(stages_created)


def get_active_cycle_id() -> int:
    """
    Get the ID of the currently active cycle

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Uses schema from context (see db_context.py).

    Returns:
        Cycle ID of active cycle, or None if no active cycle
    """
    active_cycle = get_active_cycle()
    return active_cycle['id'] if active_cycle else None


def get_cycle_status() -> Any:
    """
    Get status of all cycles.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Returns:
        DataFrame with all cycles ordered by status (ACTIVE first) and creation time
    """

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

    return execute_query(query)


def archive_cycle_by_name(cycle_name: str) -> bool:
    """
    Archive a cycle by name.

    LAYER: 3 (Workflow)

    TRANSACTION BEHAVIOR:
        - Does NOT use transaction_context() (filesystem + DB operations can't be atomic)
        - Archives in DB first, then moves directory

    Args:
        cycle_name: Name of cycle to archive

    Returns:
        True if successful
    """

    try:
        cycle = get_cycle_by_name(cycle_name)

        if not cycle:
            print(f"Cycle '{cycle_name}' not found")
            return False

        if cycle['status'] == CycleStatus.ARCHIVED:
            print(f"Cycle '{cycle_name}' is already archived")
            return True

        # Archive in database
        archive_cycle_crud(cycle['id'])

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