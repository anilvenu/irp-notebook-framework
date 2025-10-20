"""
Stage Management Operations

Low-level database operations for managing IRP stages.
Stages are part of the cycle → stage → step hierarchy.

ARCHITECTURE:
-------------
Layer 2 (CRUD): get_or_create_stage, get_stage_by_id, list_stages_for_cycle

TRANSACTION BEHAVIOR:
--------------------
- All CRUD functions (Layer 2) never manage transactions
- They are safe to call within or outside transaction_context()

All functions use the database schema from context (see db_context.py).

Usage:
    from helpers.stage import get_or_create_stage

    # Uses context schema (default 'public')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
"""

from typing import Optional
from helpers.database import execute_query, execute_insert, execute_scalar


class StageError(Exception):
    """Custom exception for stage-related errors"""
    pass


def get_or_create_stage(cycle_id: int, stage_num: int, stage_name: str) -> int:
    """
    Get existing stage or create new one.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Stages are identified uniquely by (cycle_id, stage_num).
    If a stage exists with the given cycle_id and stage_num, returns its ID.
    Otherwise, creates a new stage and returns the new ID.

    Args:
        cycle_id: ID of the parent cycle
        stage_num: Stage number within the cycle (1, 2, 3, etc.)
        stage_name: Human-readable name for the stage

    Returns:
        Stage ID (existing or newly created)

    Example:
        # Create or get stage 1 for cycle
        stage_id = get_or_create_stage(cycle_id, 1, 'Setup')

        # Create or get stage 2
        stage_id = get_or_create_stage(cycle_id, 2, 'Execution')
    """
    # Try to get existing
    query = "SELECT id FROM irp_stage WHERE cycle_id = %s AND stage_num = %s"
    stage_id = execute_scalar(query, (cycle_id, stage_num))

    if stage_id:
        return stage_id

    # Create new
    query = """
        INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
        VALUES (%s, %s, %s)
    """
    return execute_insert(query, (cycle_id, stage_num, stage_name))


def get_stage_by_id(stage_id: int) -> Optional[dict]:
    """
    Get stage information by ID.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        stage_id: ID of the stage

    Returns:
        Dictionary with stage information or None if not found

    Example:
        stage = get_stage_by_id(123)
        if stage:
            print(f"Stage {stage['stage_num']}: {stage['stage_name']}")
    """
    query = """
        SELECT id, cycle_id, stage_num, stage_name
        FROM irp_stage
        WHERE id = %s
    """
    df = execute_query(query, (stage_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def list_stages_for_cycle(cycle_id: int):
    """
    List all stages for a cycle.

    LAYER: 2 (CRUD)

    TRANSACTION BEHAVIOR:
        - Never manages transactions
        - Safe to call within or outside transaction_context()

    Args:
        cycle_id: ID of the cycle

    Returns:
        DataFrame with stage information, ordered by stage_num

    Example:
        stages = list_stages_for_cycle(cycle_id)
        for _, stage in stages.iterrows():
            print(f"Stage {stage['stage_num']}: {stage['stage_name']}")
    """
    query = """
        SELECT id, cycle_id, stage_num, stage_name
        FROM irp_stage
        WHERE cycle_id = %s
        ORDER BY stage_num
    """
    return execute_query(query, (cycle_id,))
