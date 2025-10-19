"""
Tests for Cycle, Stage, and Step Operations

Tests the cycle/stage/step hierarchy and database operations using schema context.
All operations use context from the test_schema fixture (no schema= parameters needed).
"""

import pytest
from helpers.database import (
    register_cycle, get_cycle_by_name, get_active_cycle, archive_cycle,
    get_or_create_stage, get_or_create_step,
    create_step_run, update_step_run, get_last_step_run, get_step_info
)
from helpers.stage import get_stage_by_id, list_stages_for_cycle
from helpers.constants import CycleStatus, StepStatus


# ==============================================================================
# CYCLE TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_create_cycle(test_schema):
    """Test creating a new cycle"""
    # Create cycle (no schema= parameter - uses context!)
    cycle_id = register_cycle('test_cycle_1')

    assert cycle_id > 0, "Cycle ID should be positive"

    # Verify cycle exists
    cycle = get_cycle_by_name('test_cycle_1')
    assert cycle is not None
    assert cycle['cycle_name'] == 'test_cycle_1'
    assert cycle['status'] == CycleStatus.ACTIVE


@pytest.mark.database
@pytest.mark.unit
def test_get_cycle_by_name(test_schema):
    """Test retrieving cycle by name"""
    # Create cycle
    cycle_id = register_cycle('test_cycle_2')

    # Retrieve by name
    cycle = get_cycle_by_name('test_cycle_2')

    assert cycle is not None
    assert cycle['id'] == cycle_id
    assert cycle['cycle_name'] == 'test_cycle_2'


@pytest.mark.database
@pytest.mark.unit
def test_get_cycle_by_name_not_found(test_schema):
    """Test retrieving non-existent cycle returns None"""
    cycle = get_cycle_by_name('nonexistent_cycle')
    assert cycle is None


@pytest.mark.database
@pytest.mark.unit
def test_get_active_cycle(test_schema):
    """Test retrieving the active cycle"""
    # Create a cycle
    cycle_id = register_cycle('test_cycle_3')

    # Should have active cycle
    active = get_active_cycle()
    assert active is not None
    assert active['id'] == cycle_id
    assert active['status'] == CycleStatus.ACTIVE
    assert active['cycle_name'] == 'test_cycle_3'


@pytest.mark.database
@pytest.mark.unit
def test_archive_cycle(test_schema):
    """Test archiving a cycle"""
    # Create cycle
    cycle_id = register_cycle('test_cycle_4')

    # Verify it's active first
    cycle_before = get_cycle_by_name('test_cycle_4')
    assert cycle_before['status'] == CycleStatus.ACTIVE

    # Archive it
    success = archive_cycle(cycle_id)
    assert success is True

    # Verify status changed to archived
    cycle_after = get_cycle_by_name('test_cycle_4')
    assert cycle_after['status'] == CycleStatus.ARCHIVED


@pytest.mark.database
@pytest.mark.integration
def test_multiple_cycles(test_schema):
    """Test creating multiple cycles (only one can be active)"""
    # Create first cycle
    cycle_id_1 = register_cycle('test_cycle_5')

    # Archive it
    archive_cycle(cycle_id_1)

    # Create second cycle
    cycle_id_2 = register_cycle('test_cycle_6')

    # Active should be the second one
    active = get_active_cycle()
    assert active['id'] == cycle_id_2

    # First should be archived
    cycle_1 = get_cycle_by_name('test_cycle_5')
    assert cycle_1['status'] == CycleStatus.ARCHIVED


# ==============================================================================
# STEP TESTS
# ==============================================================================
# Note: Stage tests removed - they are redundant with test_stage_module.py
# which tests the new stage.py module

@pytest.mark.database
@pytest.mark.unit
def test_create_step(test_schema):
    """Test creating a step"""
    cycle_id = register_cycle('test_cycle_10')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')

    # Create step
    step_id = get_or_create_step(stage_id, 1, 'Load Data', '/path/to/notebook.ipynb')

    assert step_id > 0

    # Verify step info
    step = get_step_info(step_id)
    assert step is not None
    assert step['step_num'] == 1
    assert step['step_name'] == 'Load Data'
    assert step['notebook_path'] == '/path/to/notebook.ipynb'
    assert step['stage_num'] == 1
    assert step['cycle_name'] == 'test_cycle_10'


@pytest.mark.database
@pytest.mark.unit
def test_get_or_create_step_existing(test_schema):
    """Test get_or_create_step returns existing step"""
    cycle_id = register_cycle('test_cycle_11')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')

    # Create step
    step_id_1 = get_or_create_step(stage_id, 1, 'Load Data')

    # Get same step (should return same ID)
    step_id_2 = get_or_create_step(stage_id, 1, 'Load Data')

    assert step_id_1 == step_id_2


@pytest.mark.database
@pytest.mark.unit
def test_get_step_info(test_schema):
    """Test getting complete step information"""
    cycle_id = register_cycle('test_cycle_12')
    stage_id = get_or_create_stage(cycle_id, 2, 'Processing')
    step_id = get_or_create_step(stage_id, 3, 'Transform', '/nb/transform.ipynb')

    step = get_step_info(step_id)

    assert step['id'] == step_id
    assert step['step_num'] == 3
    assert step['step_name'] == 'Transform'
    assert step['notebook_path'] == '/nb/transform.ipynb'
    assert step['stage_num'] == 2
    assert step['stage_name'] == 'Processing'
    assert step['cycle_name'] == 'test_cycle_12'
    assert step['cycle_id'] == cycle_id


# ==============================================================================
# STEP RUN TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_create_step_run(test_schema):
    """Test creating a step run"""
    cycle_id = register_cycle('test_cycle_13')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Create run
    run_id, run_num = create_step_run(step_id, 'test_user')

    assert run_id > 0
    assert run_num == 1  # First run


@pytest.mark.database
@pytest.mark.unit
def test_multiple_step_runs(test_schema):
    """Test creating multiple runs for same step"""
    cycle_id = register_cycle('test_cycle_14')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Create first run
    run_id_1, run_num_1 = create_step_run(step_id, 'user1')
    assert run_num_1 == 1

    # Create second run
    run_id_2, run_num_2 = create_step_run(step_id, 'user2')
    assert run_num_2 == 2

    # IDs should be different
    assert run_id_1 != run_id_2


@pytest.mark.database
@pytest.mark.unit
def test_update_step_run_completed(test_schema):
    """Test updating step run to completed"""
    cycle_id = register_cycle('test_cycle_15')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')
    run_id, run_num = create_step_run(step_id, 'test_user')

    # Update to completed
    output_data = {'rows_processed': 1000, 'status': 'success'}
    success = update_step_run(run_id, StepStatus.COMPLETED, output_data=output_data)

    assert success is True

    # Verify update
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED
    assert last_run['output_data']['rows_processed'] == 1000


@pytest.mark.database
@pytest.mark.unit
def test_update_step_run_failed(test_schema):
    """Test updating step run to failed"""
    cycle_id = register_cycle('test_cycle_16')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')
    run_id, run_num = create_step_run(step_id, 'test_user')

    # Update to failed
    success = update_step_run(run_id, StepStatus.FAILED, error_message='Connection timeout')

    assert success is True

    # Verify update
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.FAILED
    assert last_run['error_message'] == 'Connection timeout'


@pytest.mark.database
@pytest.mark.unit
def test_get_last_step_run(test_schema):
    """Test getting the most recent step run"""
    cycle_id = register_cycle('test_cycle_17')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # No runs yet
    last_run = get_last_step_run(step_id)
    assert last_run is None

    # Create first run
    run_id_1, _ = create_step_run(step_id, 'user1')
    update_step_run(run_id_1, StepStatus.COMPLETED)

    # Create second run
    run_id_2, _ = create_step_run(step_id, 'user2')
    update_step_run(run_id_2, StepStatus.FAILED, error_message='Test error')

    # Last run should be the second one
    last_run = get_last_step_run(step_id)
    assert last_run['id'] == run_id_2
    assert last_run['run_num'] == 2
    assert last_run['status'] == StepStatus.FAILED


# ==============================================================================
# HIERARCHY INTEGRATION TESTS
# ==============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_complete_hierarchy(test_schema):
    """Test creating complete cycle → stage → step → run hierarchy"""
    # Create cycle
    cycle_id = register_cycle('test_cycle_18')

    # Create stages
    stage_1_id = get_or_create_stage(cycle_id, 1, 'Setup')
    stage_2_id = get_or_create_stage(cycle_id, 2, 'Processing')

    # Create steps for stage 1
    step_1_1_id = get_or_create_step(stage_1_id, 1, 'Initialize')
    step_1_2_id = get_or_create_step(stage_1_id, 2, 'Validate')

    # Create steps for stage 2
    step_2_1_id = get_or_create_step(stage_2_id, 1, 'Transform')

    # Create runs
    run_1_1_id, _ = create_step_run(step_1_1_id, 'user')
    run_1_2_id, _ = create_step_run(step_1_2_id, 'user')
    run_2_1_id, _ = create_step_run(step_2_1_id, 'user')

    # Update runs
    update_step_run(run_1_1_id, StepStatus.COMPLETED)
    update_step_run(run_1_2_id, StepStatus.COMPLETED)
    update_step_run(run_2_1_id, StepStatus.ACTIVE)

    # Verify hierarchy
    cycle = get_active_cycle()
    assert cycle['cycle_name'] == 'test_cycle_18'

    stages = list_stages_for_cycle(cycle_id)
    assert len(stages) == 2

    step_info = get_step_info(step_2_1_id)
    assert step_info['cycle_name'] == 'test_cycle_18'
    assert step_info['stage_name'] == 'Processing'
    assert step_info['step_name'] == 'Transform'

    last_run = get_last_step_run(step_2_1_id)
    assert last_run['status'] == StepStatus.ACTIVE


@pytest.mark.database
@pytest.mark.integration
def test_schema_isolation(test_schema):
    """
    Test that schema context works correctly.

    This test verifies that operations use the test schema (from context)
    and not the 'public' schema.
    """
    # Create data in test schema
    cycle_id = register_cycle('test_isolation')

    # Verify it exists in this context
    cycle = get_cycle_by_name('test_isolation')
    assert cycle is not None
    assert cycle['cycle_name'] == 'test_isolation'

    # The test_schema fixture ensures this is in the test schema,
    # not in 'public' schema
    assert test_schema != 'public'


@pytest.mark.database
@pytest.mark.unit
def test_delete_cycle(test_schema):
    """Test permanently deleting a cycle"""
    from helpers.database import delete_cycle
    
    # Create cycle
    cycle_id = register_cycle('test_cycle_delete')
    
    # Verify it exists
    cycle_before = get_cycle_by_name('test_cycle_delete')
    assert cycle_before is not None
    assert cycle_before['id'] == cycle_id
    
    # Delete it
    success = delete_cycle(cycle_id)
    assert success is True
    
    # Verify it's gone
    cycle_after = get_cycle_by_name('test_cycle_delete')
    assert cycle_after is None


@pytest.mark.database
@pytest.mark.unit
def test_delete_cycle_not_found(test_schema):
    """Test deleting non-existent cycle returns False"""
    from helpers.database import delete_cycle
    
    # Try to delete a cycle that doesn't exist
    success = delete_cycle(99999)
    assert success is False


@pytest.mark.database
@pytest.mark.integration
def test_delete_cycle_cascade(test_schema):
    """Test that deleting a cycle cascades to stages and steps"""
    from helpers.database import delete_cycle
    
    # Create cycle with stages and steps
    cycle_id = register_cycle('test_cycle_cascade')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')
    run_id, run_num = create_step_run(step_id, 'test_user')
    
    # Verify data exists
    assert get_cycle_by_name('test_cycle_cascade') is not None
    assert get_step_info(step_id) is not None
    assert get_last_step_run(step_id) is not None
    
    # Delete cycle
    success = delete_cycle(cycle_id)
    assert success is True
    
    # Verify cycle is gone
    assert get_cycle_by_name('test_cycle_cascade') is None
    
    # Verify cascaded deletes (step and run should be gone)
    assert get_step_info(step_id) is None
    assert get_last_step_run(step_id) is None


# ============================================================================
# Tests - Database Initialization and Advanced Features
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_init_database_sql_file_not_found(test_schema):
    """Test init_database with non-existent SQL file"""
    from helpers.database import init_database, DatabaseError
    
    # Try to initialize with a non-existent SQL file
    result = init_database(schema=test_schema, sql_file_name='nonexistent.sql')
    
    # Should return False (error handled gracefully)
    assert result is False


@pytest.mark.database
@pytest.mark.integration
def test_get_step_history_with_stage_and_step_num(test_schema):
    """Test get_step_history with stage_num and step_num filters"""
    from helpers.database import get_step_history
    
    # Create test data
    cycle_id = register_cycle('test_cycle_history')
    stage_id_1 = get_or_create_stage(cycle_id, 1, 'Stage 1')
    stage_id_2 = get_or_create_stage(cycle_id, 2, 'Stage 2')
    
    step_id_1 = get_or_create_step(stage_id_1, 1, 'Step 1-1')
    step_id_2 = get_or_create_step(stage_id_1, 2, 'Step 1-2')
    step_id_3 = get_or_create_step(stage_id_2, 1, 'Step 2-1')
    
    # Create runs for each step
    run_id_1, _ = create_step_run(step_id_1, 'test_user')
    run_id_2, _ = create_step_run(step_id_2, 'test_user')
    run_id_3, _ = create_step_run(step_id_3, 'test_user')
    
    update_step_run(run_id_1, 'COMPLETED')
    update_step_run(run_id_2, 'COMPLETED')
    update_step_run(run_id_3, 'COMPLETED')
    
    # Test: Get all history
    all_history = get_step_history('test_cycle_history')
    assert len(all_history) == 3
    
    # Test: Filter by stage_num only (Lines 1509-1511)
    stage_1_history = get_step_history('test_cycle_history', stage_num=1)
    assert len(stage_1_history) == 2
    assert all(row['stage_num'] == 1 for _, row in stage_1_history.iterrows())
    
    # Test: Filter by stage_num AND step_num (Lines 1513-1515)
    specific_history = get_step_history('test_cycle_history', stage_num=1, step_num=2)
    assert len(specific_history) == 1
    assert specific_history.iloc[0]['stage_num'] == 1
    assert specific_history.iloc[0]['step_num'] == 2


@pytest.mark.database
@pytest.mark.integration
def test_execute_insert_with_returning_clause(test_schema):
    """Test execute_insert when query already has RETURNING clause"""
    from helpers.database import execute_insert
    
    # Create a cycle using query that already has RETURNING clause (Lines 941->945)
    query = """
        INSERT INTO irp_cycle (cycle_name, status)
        VALUES (%s, %s)
        RETURNING id
    """
    
    cycle_id = execute_insert(query, ('test_cycle_returning', 'ACTIVE'))
    
    # Verify it was inserted
    assert cycle_id is not None
    cycle = get_cycle_by_name('test_cycle_returning')
    assert cycle is not None
    assert cycle['id'] == cycle_id


@pytest.mark.database
@pytest.mark.integration
def test_bulk_insert_with_returning_clause(test_schema):
    """Test bulk_insert when query already has RETURNING clause"""
    from helpers.database import bulk_insert
    
    # Create cycle first
    cycle_id = register_cycle('test_cycle_bulk_returning')
    
    # Bulk insert stages with RETURNING clause already in query (Lines 996->1000)
    query = """
        INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
        VALUES (%s, %s, %s)
        RETURNING id
    """
    
    params = [
        (cycle_id, 1, 'Stage 1'),
        (cycle_id, 2, 'Stage 2'),
        (cycle_id, 3, 'Stage 3')
    ]
    
    ids = bulk_insert(query, params)
    
    # Verify all were inserted
    assert len(ids) == 3
    assert all(id is not None for id in ids)


@pytest.mark.database
@pytest.mark.integration
def test_bulk_insert_with_jsonb_columns_null_value(test_schema):
    """Test bulk_insert with JSONB columns containing None values"""
    from helpers.database import bulk_insert
    
    # Create cycle, stage, and step
    cycle_id = register_cycle('test_cycle_jsonb_null')
    stage_id = get_or_create_stage(cycle_id, 1, 'Stage 1')
    step_id = get_or_create_step(stage_id, 1, 'Step 1')
    
    # Bulk insert step runs with JSONB column, some with None values (Lines 1009->1008)
    query = """
        INSERT INTO irp_step_run (step_id, run_num, status, started_by, output_data)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    params = [
        (step_id, 1, 'COMPLETED', 'user1', {'key': 'value1'}),  # Has JSON data
        (step_id, 2, 'COMPLETED', 'user2', None),                # None - should skip conversion
        (step_id, 3, 'COMPLETED', 'user3', {'key': 'value3'})   # Has JSON data
    ]
    
    ids = bulk_insert(query, params, jsonb_columns=[4])
    
    # Verify all were inserted
    assert len(ids) == 3
    
    # Verify data
    from helpers.database import execute_query
    df = execute_query(
        "SELECT run_num, output_data FROM irp_step_run WHERE step_id = %s ORDER BY run_num",
        (step_id,)
    )
    
    assert len(df) == 3
    # First run should have JSON data
    assert df.iloc[0]['output_data'] is not None
    # Second run should have None
    assert df.iloc[1]['output_data'] is None
    # Third run should have JSON data
    assert df.iloc[2]['output_data'] is not None


@pytest.mark.database
@pytest.mark.integration
def test_bulk_insert_jsonb_column_out_of_range(test_schema):
    """Test bulk_insert with JSONB column index out of range"""
    from helpers.database import bulk_insert
    
    cycle_id = register_cycle('test_cycle_jsonb_range')
    
    # Insert stages with jsonb_columns pointing to non-existent column (Lines 1009->1008)
    query = """
        INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
        VALUES (%s, %s, %s)
    """
    
    params = [
        (cycle_id, 1, 'Stage 1'),
        (cycle_id, 2, 'Stage 2')
    ]
    
    # Specify JSONB column index that doesn't exist (index 10)
    # Should handle gracefully - index check prevents error
    ids = bulk_insert(query, params, jsonb_columns=[10])
    
    # Should still insert successfully
    assert len(ids) == 2
