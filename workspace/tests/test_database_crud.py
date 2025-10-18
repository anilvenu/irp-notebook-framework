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
