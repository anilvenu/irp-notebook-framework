"""
IRP Notebook Framework - Database Operations
"""

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from helpers.constants import DB_CONFIG, CycleStatus, StepStatus


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


# Create SQLAlchemy engine
def get_engine(schema: str = 'public'):
    """
    Get SQLAlchemy engine

    Args:
        schema: Database schema to use (default: 'public')

    Returns:
        SQLAlchemy engine
    """
    db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url, poolclass=NullPool)

    # Note: We set search_path per connection, not here
    return engine


def _set_schema(conn, schema: str):
    """Helper to set schema for a connection"""
    if schema and schema != 'public':
        conn.execute(text(f"SET search_path TO {schema}, public"))
    else:
        conn.execute(text("SET search_path TO public"))


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


def test_connection(schema: str = 'public') -> bool:
    """
    Test database connectivity

    Args:
        schema: Database schema to test (default: 'public')

    Returns:
        True if connection successful
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            _set_schema(conn, schema)
            conn.execute(text("SELECT 1"))
        return True
    except:
        return False


def execute_query(query: str, params: tuple = None, schema: str = 'public') -> pd.DataFrame:
    """
    Execute SELECT query and return results as DataFrame

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (default: 'public')

    Returns:
        DataFrame with query results
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_schema(conn, schema)
            df = pd.read_sql_query(text(converted_query), conn, params=param_dict)
        return df
    except Exception as e:
        raise DatabaseError(f"Query failed: {str(e)}")


def execute_scalar(query: str, params: tuple = None, schema: str = 'public') -> Any:
    """
    Execute query and return single scalar value

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (default: 'public')

    Returns:
        Single value from query
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_schema(conn, schema)
            result = conn.execute(text(converted_query), param_dict)
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Scalar query failed: {str(e)}")


def execute_command(query: str, params: tuple = None, schema: str = 'public') -> int:
    """
    Execute INSERT/UPDATE/DELETE and return rows affected

    Args:
        query: SQL query string
        params: Query parameters (optional)
        schema: Database schema to use (default: 'public')

    Returns:
        Number of rows affected
    """
    try:
        # Convert query params
        converted_query, param_dict = _convert_query_params(query, params)

        engine = get_engine()
        with engine.connect() as conn:
            _set_schema(conn, schema)
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            return result.rowcount
    except Exception as e:
        raise DatabaseError(f"Command failed: {str(e)}")


def execute_insert(query: str, params: tuple = None, schema: str = 'public') -> int:
    """
    Execute INSERT and return new record ID

    Args:
        query: SQL INSERT query string
        params: Query parameters (optional)
        schema: Database schema to use (default: 'public')

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
            _set_schema(conn, schema)
            result = conn.execute(text(converted_query), param_dict)
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        raise DatabaseError(f"Insert failed: {str(e)}")


def bulk_insert(query: str, params_list: List[tuple], jsonb_columns: List[int] = None, schema: str = 'public') -> List[int]:
    """
    Execute bulk INSERT and return list of new record IDs

    This function efficiently inserts multiple records in a single transaction.
    If any insert fails, the entire operation is rolled back.

    Args:
        query: SQL INSERT query string with placeholders (%s)
        params_list: List of tuples, each containing parameters for one insert
        jsonb_columns: Optional list of column indices (0-based) that contain JSONB data.
                      Dicts at these positions will be automatically converted to JSON strings.
        schema: Database schema to use (default: 'public')

    Returns:
        List of IDs for newly inserted records (in order)

    Example:
        # Basic insert
        query = "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)"
        params = [
            ('cycle1', 'ACTIVE', 'user1'),
            ('cycle2', 'ACTIVE', 'user2')
        ]
        ids = bulk_insert(query, params)

        # Insert with JSONB
        query = "INSERT INTO irp_cycle (cycle_name, status, metadata) VALUES (%s, %s, %s)"
        params = [
            ('cycle1', 'ACTIVE', {'key': 'value1'}),
            ('cycle2', 'ACTIVE', {'key': 'value2'})
        ]
        ids = bulk_insert(query, params, jsonb_columns=[2])

        # Test insert (using test schema)
        ids = bulk_insert(query, params, schema='test')
    """
    import json

    if not params_list:
        return []

    try:
        # Add RETURNING id if not present
        if "RETURNING" not in query.upper():
            query = query + " RETURNING id"

        # Process JSONB columns if specified
        processed_params = []
        for params in params_list:
            if jsonb_columns:
                # Convert to list for modification
                params_list_item = list(params)
                for col_idx in jsonb_columns:
                    if col_idx < len(params_list_item) and params_list_item[col_idx] is not None:
                        # Convert dict to JSON string if needed
                        if isinstance(params_list_item[col_idx], dict):
                            params_list_item[col_idx] = json.dumps(params_list_item[col_idx])
                processed_params.append(tuple(params_list_item))
            else:
                processed_params.append(params)

        engine = get_engine()
        inserted_ids = []

        with engine.connect() as conn:
            _set_schema(conn, schema)

            # Execute all inserts in a single transaction
            for params in processed_params:
                # Convert query params for each insert
                converted_query, param_dict = _convert_query_params(query, params)
                result = conn.execute(text(converted_query), param_dict)
                row = result.fetchone()
                if row:
                    inserted_ids.append(row[0])

            # Commit all inserts at once
            conn.commit()

        return inserted_ids

    except Exception as e:
        raise DatabaseError(f"Bulk insert failed: {str(e)}")


def init_database(schema: str = 'public', sql_file_name: str = 'init_database.sql') -> bool:
    """
    Initialize database by running SQL script

    Args:
        schema: Database schema to use (default: 'public')
        sql_file_name: Name of SQL initialization file (default: 'init_database.sql')

    Returns:
        True if successful
    """
    try:
        from pathlib import Path

        # Read SQL initialization script
        sql_file = Path(__file__).parent / 'db' / sql_file_name

        if not sql_file.exists():
            raise DatabaseError(f"SQL file not found: {sql_file}")

        with open(sql_file, 'r') as f:
            sql_script = f.read()

        # Execute script
        engine = get_engine()
        with engine.connect() as conn:
            # Create schema if not public
            if schema != 'public':
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                _set_schema(conn, schema)

            # Execute the SQL script
            conn.execute(text(sql_script))
            conn.commit()

        print(f"Database initialized successfully (schema: {schema})")
        return True

    except Exception as e:
        print(f"Database initialization failed: {str(e)}")
        return False

# ============================================================================
# CYCLE OPERATIONS
# ============================================================================

def get_active_cycle(schema: str = 'public') -> Optional[Dict[str, Any]]:
    """
    Get the currently active cycle

    Args:
        schema: Database schema to use (default: 'public')

    Returns:
        Dictionary with cycle information or None if no active cycle
    """
    query = """
        SELECT id, cycle_name, status, created_ts, created_by, metadata
        FROM irp_cycle
        WHERE status = 'ACTIVE'
        ORDER BY created_ts DESC
        LIMIT 1
    """
    df = execute_query(query, schema=schema)
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
        VALUES (%s, %s, %s, %s)
    """
    import json
    return execute_insert(query, (cycle_name, CycleStatus.ACTIVE, created_by, json.dumps(metadata) if metadata else None))


def archive_cycle(cycle_id: int) -> bool:
    """Archive a cycle"""
    query = """
        UPDATE irp_cycle
        SET status = %s, archived_ts = NOW()
        WHERE id = %s
    """
    rows = execute_command(query, (CycleStatus.ARCHIVED, cycle_id))
    return rows > 0

def delete_cycle(cycle_id: int) -> bool:
    """Hard delete a cycle"""
    query = """
        DELETE FROM irp_cycle
        WHERE id = %s
    """
    rows = execute_command(query, (cycle_id))
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
) -> int:
    """Get existing step or create new one"""
    
    # Try to get existing
    query = "SELECT id FROM irp_step WHERE stage_id = %s AND step_num = %s"
    step_id = execute_scalar(query, (stage_id, step_num))
    
    if step_id:
        return step_id
    
    # Create new
    query = """
        INSERT INTO irp_step (stage_id, step_num, step_name, notebook_path)
        VALUES (%s, %s, %s, %s)
    """
    return execute_insert(query, (stage_id, step_num, step_name, notebook_path))


def get_step_info(step_id: int) -> Optional[Dict[str, Any]]:
    """Get step information"""
    query = """
        SELECT 
            st.id, st.step_num, st.step_name, st.notebook_path,
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
        VALUES (%s, %s, 'RUNNING', %s)
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
            completed_ts = CASE WHEN %s IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN NOW() ELSE completed_ts END,
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