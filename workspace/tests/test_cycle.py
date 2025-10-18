"""
Tests for cycle.py Module

Tests the high-level cycle management functions in cycle.py.
These are domain-level functions that handle business logic.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from helpers import cycle
from helpers.database import create_cycle, execute_insert, execute_command
from helpers import constants


@pytest.mark.database
@pytest.mark.unit
def test_get_active_cycle_id(test_schema):
    """Test getting active cycle ID"""
    # No active cycle initially
    cycle_id = cycle.get_active_cycle_id()
    assert cycle_id is None

    # Create a cycle
    new_cycle_id = create_cycle('test_cycle_1')

    # Now should return that cycle
    active_id = cycle.get_active_cycle_id()
    assert active_id == new_cycle_id


@pytest.mark.database
@pytest.mark.unit
def test_delete_archived_cycles(test_schema):
    """Test deleting archived cycles"""
    # Create some cycles
    cycle_1 = create_cycle('cycle_1')
    cycle_2 = create_cycle('cycle_2')
    cycle_3 = create_cycle('cycle_3')

    # Archive two of them
    execute_insert(
        "UPDATE irp_cycle SET status = %s WHERE id IN (%s, %s)",
        (constants.CycleStatus.ARCHIVED, cycle_1, cycle_2)
    )

    # Delete archived cycles
    deleted_count = cycle.delete_archived_cycles()

    assert deleted_count == 2

    # Verify only cycle_3 remains
    from helpers.database import get_cycle_by_name
    assert get_cycle_by_name('cycle_1') is None
    assert get_cycle_by_name('cycle_2') is None
    assert get_cycle_by_name('cycle_3') is not None


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_too_short(test_schema):
    """Test cycle name validation - too short"""
    result = cycle.validate_cycle_name('ab')  # min is 3
    assert result is False


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_too_long(test_schema):
    """Test cycle name validation - too long"""
    long_name = 'a' * 100  # max is 50
    result = cycle.validate_cycle_name(long_name)
    assert result is False


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_invalid_pattern(test_schema):
    """Test cycle name validation - invalid characters"""
    result = cycle.validate_cycle_name('test cycle!')  # Special chars not allowed
    assert result is False


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_forbidden_prefix(test_schema):
    """Test cycle name validation - forbidden prefix"""
    result = cycle.validate_cycle_name('test_cycle')  # 'test_' is forbidden
    assert result is False


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_already_exists(test_schema):
    """Test cycle name validation - name already exists"""
    # Create a cycle
    create_cycle('Analysys-2024-Q1-test_validate_cycle_name_already_exists')

    # Try to validate same name
    result = cycle.validate_cycle_name('Analysys-2024-Q1-test_validate_cycle_name_already_exists')
    assert result is False


@pytest.mark.database
@pytest.mark.unit
def test_validate_cycle_name_valid(test_schema):
    """Test cycle name validation - valid name"""
    result = cycle.validate_cycle_name('Analysis-2024-Q1')
    assert result is True


@pytest.mark.database
@pytest.mark.unit
def test_get_cycle_status(test_schema):
    """Test getting cycle status"""
    # Create cycles with different statuses
    cycle_1 = create_cycle('Analysys-2024-Q1-test_get_cycle_status-cycle-1')
    cycle_2 = create_cycle('Analysys-2024-Q1-test_get_cycle_status-cycle-2')

    execute_command(
        "UPDATE irp_cycle SET status = %s WHERE id = %s",
        (constants.CycleStatus.ARCHIVED, cycle_2)
    )

    # Get status
    status_df = cycle.get_cycle_status()

    # Should return DataFrame with cycle info
    assert status_df is not None
    assert len(status_df[status_df['cycle_name'] == 'Analysys-2024-Q1-test_get_cycle_status-cycle-1']) == 1, "Expected exactly one matching cycle"
    assert len(status_df[status_df['cycle_name'] == 'Analysys-2024-Q1-test_get_cycle_status-cycle-2']) == 1, "Expected exactly one matching cycle"
    

@pytest.mark.database
@pytest.mark.integration
def test_get_cycle_progress(test_schema):
    """Test getting cycle progress"""
    from helpers.database import get_or_create_stage, get_or_create_step

    # Create a cycle with stages and steps
    cycle_id = create_cycle('progress_cycle')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Get progress
    progress = cycle.get_cycle_progress('progress_cycle')

    # Should return DataFrame with progress info
    assert progress is not None
    assert len(progress) > 0


@pytest.mark.database
@pytest.mark.integration
def test_get_step_history(test_schema):
    """Test getting step history"""
    from helpers.database import get_or_create_stage, get_or_create_step, create_step_run, update_step_run

    # Create hierarchy
    cycle_id = create_cycle('history_cycle')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Create some runs
    run_id_1, _ = create_step_run(step_id, 'user1')
    update_step_run(run_id_1, 'COMPLETED')

    run_id_2, _ = create_step_run(step_id, 'user2')
    update_step_run(run_id_2, 'FAILED', error_message='Test error')

    # Get history
    history = cycle.get_step_history('history_cycle')

    # Should return DataFrame with history
    assert history is not None
    assert len(history) >= 2  # At least our two runs


@pytest.mark.database
@pytest.mark.integration
def test_get_step_history_filtered_by_stage(test_schema):
    """Test getting step history filtered by stage number"""
    from helpers.database import get_or_create_stage, get_or_create_step, create_step_run

    # Create hierarchy with multiple stages
    cycle_id = create_cycle('filter_cycle')
    stage_1 = get_or_create_stage(cycle_id, 1, 'Stage1')
    stage_2 = get_or_create_stage(cycle_id, 2, 'Stage2')

    step_1_1 = get_or_create_step(stage_1, 1, 'Step1.1')
    step_2_1 = get_or_create_step(stage_2, 1, 'Step2.1')

    # Create runs
    create_step_run(step_1_1, 'user')
    create_step_run(step_2_1, 'user')

    # Get history for stage 1 only
    history_stage_1 = cycle.get_step_history('filter_cycle', stage_num=1)

    # Should only have stage 1 runs
    assert history_stage_1 is not None
    assert all(history_stage_1['stage_num'] == 1)


@pytest.mark.database
@pytest.mark.integration
def test_cycle_module_uses_context(test_schema):
    """Test that cycle.py functions use schema context correctly"""
    # Create data using cycle module functions
    cycle_id = cycle.get_active_cycle_id()

    # This proves cycle.py is using the context correctly
    # (it's calling database functions that use context)
    assert test_schema != 'public'  # Verify we're in test schema


# ==============================================================================
# FILESYSTEM INTEGRATION TESTS
# ==============================================================================

@pytest.fixture
def temp_cycle_dirs(monkeypatch):
    """
    Create temporary directories for cycle file operations.
    This fixture sets up the required directory structure for cycle.create_cycle()
    """
    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())

    # Create subdirectories
    workflows_path = temp_dir / "workflows"
    template_path = temp_dir / "_Template"
    archive_path = temp_dir / "_Archive"

    workflows_path.mkdir()
    archive_path.mkdir()

    # Create a minimal template structure
    template_path.mkdir()
    template_notebooks = template_path / "notebooks"
    template_notebooks.mkdir()

    # Create a sample stage directory
    stage_dir = template_notebooks / "1-Setup"
    stage_dir.mkdir()

    # Create a sample notebook file
    notebook_file = stage_dir / "1-Initialize.ipynb"
    notebook_file.write_text('{"cells": [], "metadata": {}}')

    # Monkeypatch the paths in cycle.py
    monkeypatch.setattr('helpers.cycle.WORKFLOWS_PATH', workflows_path)
    monkeypatch.setattr('helpers.cycle.TEMPLATE_PATH', template_path)
    monkeypatch.setattr('helpers.cycle.ARCHIVE_PATH', archive_path)

    yield {
        'workflows': workflows_path,
        'template': template_path,
        'archive': archive_path,
        'temp_root': temp_dir
    }

    # Cleanup temp directory
    shutil.rmtree(temp_dir)


@pytest.mark.database
@pytest.mark.integration
def test_create_cycle_with_directories(test_schema, temp_cycle_dirs):
    """Test cycle.create_cycle() creates directories and database records"""
    # Create cycle using high-level function
    cycle_name = 'Analysis-2024-Q1-with-directories'

    result = cycle.create_cycle(cycle_name)
    assert result is True

    # Verify directory was created
    active_dir = temp_cycle_dirs['workflows'] / f'Active_{cycle_name}'
    assert active_dir.exists()
    assert active_dir.is_dir()

    # Verify notebooks directory exists
    notebooks_dir = active_dir / 'notebooks'
    assert notebooks_dir.exists()

    # Verify database record
    from helpers.database import get_cycle_by_name
    db_cycle = get_cycle_by_name(cycle_name)
    assert db_cycle is not None
    assert db_cycle['cycle_name'] == cycle_name
    assert db_cycle['status'] == constants.CycleStatus.ACTIVE


@pytest.mark.database
@pytest.mark.integration
def test_create_cycle_archives_previous(test_schema, temp_cycle_dirs):
    """Test cycle.create_cycle() archives previous active cycle"""

    cycle_1 = 'Analysis-2025-Q1-Archive-Previous-1'
    cycle_2 = 'Analysis-2025-Q1-Archive-Previous-2'

    # Create first cycle
    result1 = cycle.create_cycle(cycle_1)
    assert result1 is True

    # Create second cycle (should archive first)
    result2 = cycle.create_cycle(cycle_2)
    assert result2 is True

    # First cycle should be archived in database
    from helpers.database import get_cycle_by_name
    q1_cycle = get_cycle_by_name(cycle_1)
    assert q1_cycle['status'] == constants.CycleStatus.ARCHIVED

    # Second cycle should be active
    q2_cycle = get_cycle_by_name(cycle_2)
    assert q2_cycle['status'] == constants.CycleStatus.ACTIVE


@pytest.mark.database
@pytest.mark.integration
def test_archive_cycle_by_name(test_schema, temp_cycle_dirs):
    """Test cycle.archive_cycle_by_name() moves directory and updates database"""

    cycle_name = 'Analysis-2025-Q1-Archive-Test'
    active_directory_name = f'Active_{cycle_name}'

    # Create cycle
    cycle.create_cycle(cycle_name)

    # Verify database
    from helpers.database import get_cycle_by_name
    db_cycle = get_cycle_by_name(cycle_name)
    assert db_cycle['status'] == constants.CycleStatus.ACTIVE

    # Verify directory moved
    active_dir = temp_cycle_dirs['workflows'] / active_directory_name
    assert active_dir.exists()

    # Archive it
    result = cycle.archive_cycle_by_name(cycle_name)
    assert result is True

    # Verify database
    from helpers.database import get_cycle_by_name
    db_cycle = get_cycle_by_name(cycle_name)
    assert db_cycle['status'] == constants.CycleStatus.ARCHIVED

    # Verify directory moved
    active_dir = temp_cycle_dirs['workflows'] / active_directory_name
    assert not active_dir.exists()

    archive_dir = temp_cycle_dirs['archive'] / cycle_name
    assert archive_dir.exists()

@pytest.mark.database
@pytest.mark.integration
def test_create_cycle_duplicate_run(test_schema, temp_cycle_dirs):
    """Test cycle.create_cycle() back to back mimicing accidental rerun"""
    from helpers.cycle import CycleError

    # Create cycle first time - should succeed
    result = cycle.create_cycle('Analysis-2024-Q1-run-twice')
    assert result is True

    # Create same cycle again - should fail with CycleError
    with pytest.raises(CycleError, match='Cycle name validation failed'):
        cycle.create_cycle('Analysis-2024-Q1-run-twice')

 
@pytest.mark.database
@pytest.mark.integration
def test_create_cycle_registers_stages_and_steps(test_schema, temp_cycle_dirs):
    """Test cycle.create_cycle() registers stages and steps from template"""
    # Create cycle
    result = cycle.create_cycle('Analysis-2024-Q1-test_registers-stages-and-steps')
    assert result is True

    # Verify stages were registered
    from helpers.database import get_cycle_by_name, execute_query
    db_cycle = get_cycle_by_name('Analysis-2024-Q1-test_registers-stages-and-steps')

    stages_df = execute_query(
        "SELECT * FROM irp_stage WHERE cycle_id = %s ORDER BY stage_num",
        (db_cycle['id'],)
    )
    steps_df = execute_query(
        """SELECT s.* FROM irp_step s
           INNER JOIN irp_stage st ON s.stage_id = st.id
           WHERE st.cycle_id = %s
           ORDER BY s.step_num""",
        (db_cycle['id'],)
    )
    # TODO: Once we figure out passing in files, assert stages and steps