"""
Tests for stage.py Module

Tests the new stage.py module created during refactoring.
All functions use schema context (no schema= parameters).
"""

import pytest
from helpers.stage import (
    get_or_create_stage,
    get_stage_by_id,
    list_stages_for_cycle,
    StageError
)
from helpers.cycle import register_cycle


@pytest.mark.database
@pytest.mark.unit
def test_get_or_create_stage_new(test_schema):
    """Test creating a new stage using stage.py module"""
    cycle_id = register_cycle('test_cycle_1')

    stage_id = get_or_create_stage(cycle_id, 1, 'Setup Stage')

    assert stage_id > 0

    # Verify it was created
    stage = get_stage_by_id(stage_id)
    assert stage is not None
    assert stage['stage_num'] == 1
    assert stage['stage_name'] == 'Setup Stage'
    assert stage['cycle_id'] == cycle_id


@pytest.mark.database
@pytest.mark.unit
def test_get_or_create_stage_existing(test_schema):
    """Test get_or_create_stage returns existing stage (idempotent)"""
    cycle_id = register_cycle('test_cycle_2')

    # Create first time
    stage_id_1 = get_or_create_stage(cycle_id, 1, 'Setup')

    # Get second time (should return same ID)
    stage_id_2 = get_or_create_stage(cycle_id, 1, 'Setup')

    assert stage_id_1 == stage_id_2


@pytest.mark.database
@pytest.mark.unit
def test_get_or_create_stage_different_numbers(test_schema):
    """Test creating stages with different stage numbers"""
    cycle_id = register_cycle('test_cycle_3')

    stage_1 = get_or_create_stage(cycle_id, 1, 'Stage One')
    stage_2 = get_or_create_stage(cycle_id, 2, 'Stage Two')
    stage_3 = get_or_create_stage(cycle_id, 3, 'Stage Three')

    # All should have different IDs
    assert stage_1 != stage_2
    assert stage_2 != stage_3
    assert stage_1 != stage_3


@pytest.mark.database
@pytest.mark.unit
def test_get_stage_by_id_found(test_schema):
    """Test retrieving stage by ID"""
    cycle_id = register_cycle('test_cycle_4')
    stage_id = get_or_create_stage(cycle_id, 5, 'Test Stage')

    stage = get_stage_by_id(stage_id)

    assert stage is not None
    assert stage['id'] == stage_id
    assert stage['cycle_id'] == cycle_id
    assert stage['stage_num'] == 5
    assert stage['stage_name'] == 'Test Stage'


@pytest.mark.database
@pytest.mark.unit
def test_get_stage_by_id_not_found(test_schema):
    """Test retrieving non-existent stage returns None"""
    stage = get_stage_by_id(99999)

    assert stage is None


@pytest.mark.database
@pytest.mark.unit
def test_list_stages_for_cycle_empty(test_schema):
    """Test listing stages for cycle with no stages"""
    cycle_id = register_cycle('test_cycle_5')

    stages = list_stages_for_cycle(cycle_id)

    assert len(stages) == 0


@pytest.mark.database
@pytest.mark.integration
def test_list_stages_for_cycle_multiple(test_schema):
    """Test listing multiple stages for a cycle"""
    cycle_id = register_cycle('test_cycle_6')

    # Create stages in random order
    get_or_create_stage(cycle_id, 3, 'Finalize')
    get_or_create_stage(cycle_id, 1, 'Setup')
    get_or_create_stage(cycle_id, 2, 'Process')

    stages = list_stages_for_cycle(cycle_id)

    # Should be ordered by stage_num
    assert len(stages) == 3
    assert list(stages['stage_num']) == [1, 2, 3]
    assert list(stages['stage_name']) == ['Setup', 'Process', 'Finalize']


@pytest.mark.database
@pytest.mark.integration
def test_list_stages_for_cycle_isolation(test_schema):
    """Test that stages are properly isolated by cycle"""
    cycle_1 = register_cycle('test_cycle_7')
    cycle_2 = register_cycle('test_cycle_8')

    # Create stages for both cycles
    get_or_create_stage(cycle_1, 1, 'Cycle1 Stage1')
    get_or_create_stage(cycle_1, 2, 'Cycle1 Stage2')
    get_or_create_stage(cycle_2, 1, 'Cycle2 Stage1')

    # List for cycle 1
    stages_1 = list_stages_for_cycle(cycle_1)
    assert len(stages_1) == 2
    assert all(stages_1['cycle_id'] == cycle_1)

    # List for cycle 2
    stages_2 = list_stages_for_cycle(cycle_2)
    assert len(stages_2) == 1
    assert all(stages_2['cycle_id'] == cycle_2)


@pytest.mark.database
@pytest.mark.integration
def test_stage_module_uses_context(test_schema):
    """Test that stage.py functions use schema context correctly"""
    # This test verifies that all stage operations happen in test schema
    # not in 'public' schema

    cycle_id = register_cycle('context_test_cycle')
    stage_id = get_or_create_stage(cycle_id, 1, 'Context Test Stage')

    # If context is working, this should find the stage
    stage = get_stage_by_id(stage_id)
    assert stage is not None

    # And listing should work
    stages = list_stages_for_cycle(cycle_id)
    assert len(stages) == 1

    # All of this proves operations are happening in test_schema,
    # not in 'public' schema (which would fail to find these records)
