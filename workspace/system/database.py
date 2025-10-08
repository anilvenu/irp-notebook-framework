"""
IRP Notebook Framework - Database Operations
"""

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from .config import DB_CONFIG


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


# Create SQLAlchemy engine
def get_engine():
    """Get SQLAlchemy engine"""
    db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(db_url, poolclass=NullPool)


def _convert_query_params(query: str, params: tuple = None):
    """Convert %s style params to :param0 style for SQLAlchemy"""
    if not params:
        return query, {}
    
    import re
    param_dict = {}
    modified_query = query
    
    # Find all %s placeholders
    placeholders = list(re.finditer(r'%s', query))
    
    # Replace each %s with :param0, :param1, etc. (in reverse to maintain positions)
    for i, match in enumerate(reversed(placeholders)):
        param_name = f'param{len(placeholders) - i - 1}'
        param_dict[param_name] = params[len(placeholders) - i - 1]
        modified_query = modified_query[:match.start()] + f':{param_name}' + modified_query[match.end():]
    
    return modified_query, param_dict


def test_connection() -> bool:
    """Test database connectivity"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except:
        return False


def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame
    
    Args:
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        DataFrame with query results
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)
        
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql_query(text(converted_query), conn, params=param_dict)
        return df
    except Exception as e:
        raise DatabaseError(f"Query failed: {str(e)}")


def execute_scalar(query: str, params: tuple = None) -> Any:
    """
    Execute query and return single scalar value
    
    Args:
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        Single value from query
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)
        
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(converted_query), param_dict)
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Scalar query failed: {str(e)}")


def execute_command(query: str, params: tuple = None) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return rows affected
    
    Args:
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        Number of rows affected
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)
        
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            return result.rowcount
    except Exception as e:
        raise DatabaseError(f"Command failed: {str(e)}")


def execute_insert(query: str, params: tuple = None) -> int:
    """
    Execute INSERT and return new record ID
    
    Args:
        query: SQL INSERT query string
        params: Query parameters (optional)
    
    Returns:
        ID of newly inserted record
    """
    try:
        # Add RETURNING id if not present
        if "RETURNING" not in query.upper():
            query = query + " RETURNING id"
        
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)
        
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Insert failed: {str(e)}")


def init_database() -> bool:
    """
    Initialize database by running SQL script
    
    Returns:
        True if successful
    """
    try:
        from pathlib import Path
        
        # Read SQL initialization script
        sql_file = Path(__file__).parent / 'db' / 'init_database.sql'
        
        if not sql_file.exists():
            raise DatabaseError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_script = f.read()
        
        # Execute script
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()
        
        print("Database initialized successfully")
        return True
        
    except Exception as e:
        print(f"Database initialization failed: {str(e)}")
        return False

# ============================================================================
# CYCLE OPERATIONS
# ============================================================================

def get_active_cycle() -> Optional[Dict[str, Any]]:
    """Get the currently active cycle"""
    query = """
        SELECT id, cycle_name, status, created_ts, created_by, metadata
        FROM irp_cycle
        WHERE status = 'active'
        LIMIT 1
    """
    df = execute_query(query)
    return df.iloc[0].to_dict() if not df.empty else None


def get_cycle_by_name(cycle_name: str) -> Optional[Dict[str, Any]]:
    """Get cycle by name"""
    query = """
        SELECT id, cycle_name, status, created_ts, archived_ts, created_by, metadata
        FROM irp_cycle
        WHERE cycle_name = %s
    """
    df = execute_query(query, (cycle_name,))
    return df.iloc[0].to_dict() if not df.empty else None


def create_cycle(cycle_name: str, created_by: str, metadata: Dict = None) -> int:
    """Create new cycle"""
    query = """
        INSERT INTO irp_cycle (cycle_name, status, created_by, metadata)
        VALUES (%s, 'active', %s, %s)
    """
    import json
    return execute_insert(query, (cycle_name, created_by, json.dumps(metadata) if metadata else None))


def archive_cycle(cycle_id: int) -> bool:
    """Archive a cycle"""
    query = """
        UPDATE irp_cycle
        SET status = 'archived', archived_ts = NOW()
        WHERE id = %s
    """
    rows = execute_command(query, (cycle_id,))
    return rows > 0

# ============================================================================
# STAGE OPERATIONS
# ============================================================================

def get_or_create_stage(cycle_id: int, stage_num: int, stage_name: str) -> int:
    """Get existing stage or create new one"""
    
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

# ============================================================================
# STEP OPERATIONS
# ============================================================================

def get_or_create_step(
    stage_id: int,
    step_num: int,
    step_name: str,
    notebook_path: str = None,
    is_idempotent: bool = False
) -> int:
    """Get existing step or create new one"""
    
    # Try to get existing
    query = "SELECT id FROM irp_step WHERE stage_id = %s AND step_num = %s"
    step_id = execute_scalar(query, (stage_id, step_num))
    
    if step_id:
        return step_id
    
    # Create new
    query = """
        INSERT INTO irp_step (stage_id, step_num, step_name, notebook_path, is_idempotent)
        VALUES (%s, %s, %s, %s, %s)
    """
    return execute_insert(query, (stage_id, step_num, step_name, notebook_path, is_idempotent))


def get_step_info(step_id: int) -> Optional[Dict[str, Any]]:
    """Get step information"""
    query = """
        SELECT 
            st.id, st.step_num, st.step_name, st.notebook_path, st.is_idempotent,
            sg.stage_num, sg.stage_name,
            c.cycle_name, c.id as cycle_id
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        WHERE st.id = %s
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None

# ============================================================================
# STEP RUN OPERATIONS
# ============================================================================

def get_last_step_run(step_id: int) -> Optional[Dict[str, Any]]:
    """Get the most recent run for a step"""
    query = """
        SELECT id, run_number, status, started_ts, completed_ts, 
               started_by, error_message, output_data
        FROM irp_step_run
        WHERE step_id = %s
        ORDER BY run_number DESC
        LIMIT 1
    """
    df = execute_query(query, (step_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def create_step_run(step_id: int, started_by: str) -> Tuple[int, int]:
    """
    Create new step run
    
    Returns:
        Tuple of (run_id, run_number)
    """
    # Get next run number
    query = "SELECT COALESCE(MAX(run_number), 0) + 1 FROM irp_step_run WHERE step_id = %s"
    run_number = execute_scalar(query, (step_id,))
    
    # Create run
    query = """
        INSERT INTO irp_step_run (step_id, run_number, status, started_by)
        VALUES (%s, %s, 'running', %s)
    """
    run_id = execute_insert(query, (step_id, run_number, started_by))
    
    return run_id, run_number


def update_step_run(
    run_id: int,
    status: str,
    error_message: str = None,
    output_data: Dict = None
) -> bool:
    """Update step run with completion status"""
    import json
    
    query = """
        UPDATE irp_step_run
        SET status = %s,
            completed_ts = CASE WHEN %s IN ('completed', 'failed', 'skipped') THEN NOW() ELSE completed_ts END,
            error_message = %s,
            output_data = %s
        WHERE id = %s
    """
    
    rows = execute_command(query, (
        status,
        status,
        error_message,
        json.dumps(output_data) if output_data else None,
        run_id
    ))
    
    return rows > 0

# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_cycle_progress(cycle_name: str) -> pd.DataFrame:
    """Get progress for all steps in a cycle"""
    query = """
        SELECT 
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            st.is_idempotent,
            sr.status as last_status,
            sr.run_number as last_run,
            sr.completed_ts as last_completed
        FROM irp_step st
        INNER JOIN irp_stage sg ON st.stage_id = sg.id
        INNER JOIN irp_cycle c ON sg.cycle_id = c.id
        LEFT JOIN LATERAL (
            SELECT status, run_number, completed_ts
            FROM irp_step_run
            WHERE step_id = st.id
            ORDER BY run_number DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE c.cycle_name = %s
        ORDER BY sg.stage_num, st.step_num
    """
    return execute_query(query, (cycle_name,))


def get_step_history(cycle_name: str, stage_num: int = None, step_num: int = None) -> pd.DataFrame:
    """Get execution history for steps"""
    
    base_query = """
        SELECT 
            sg.stage_num,
            sg.stage_name,
            st.step_num,
            st.step_name,
            sr.run_number,
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