"""
Tests for step.py Module

Tests the Step class in step.py which handles step execution lifecycle.
This is a high-level domain class for tracking step execution.
"""

import pytest
from helpers.step import Step, StepError
from helpers.database import create_cycle, get_or_create_stage, get_or_create_step, get_last_step_run
from helpers.constants import StepStatus


@pytest.fixture
def mock_context():
    """
    Mock WorkContext for testing outside Jupyter environment.

    WorkContext normally detects notebook path from IPython/Jupyter,
    which doesn't exist in pytest. This mock provides a minimal context.
    """
    class MockWorkContext:
        def __init__(self):
            self.step_id = None
            self.notebook_path = "/test/mock_notebook.ipynb"
            self.cycle_name = "test_cycle"
            self.stage_num = 1
            self.step_num = 1

        def __str__(self):
            return f"Cycle: {self.cycle_name}, Stage: {self.stage_num}, Step: {self.step_num}"

    return MockWorkContext


@pytest.mark.database
@pytest.mark.integration
def test_step_initialization(test_schema, mock_context):
    """Test Step class initialization"""
    # Create hierarchy
    cycle_id = create_cycle('test_cycle_1')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Create context
    context = mock_context()
    context.step_id = step_id

    # Initialize Step (auto-starts)
    step = Step(context)

    assert step.step_id == step_id
    assert step.run_id is not None
    assert step.run_num == 1
    assert step.executed is False


@pytest.mark.database
@pytest.mark.integration
def test_step_already_executed(test_schema, mock_context):
    """Test Step detects already executed steps"""
    # Create hierarchy
    cycle_id = create_cycle('test_cycle_2')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    # Create context and run step once
    context = mock_context()
    context.step_id = step_id

    step1 = Step(context)
    step1.complete()

    # Try to create another Step instance for same step
    step2 = Step(context)

    # Should detect it was already executed
    assert step2.executed is True
    assert 'already run' in step2.status_message.lower()


@pytest.mark.database
@pytest.mark.integration
def test_step_log(test_schema, mock_context):
    """Test Step logging functionality"""
    cycle_id = create_cycle('test_cycle_3')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Add logs
    step.log("Starting processing")
    step.log("Warning: Low memory", level="WARNING")
    step.log("Error occurred", level="ERROR")

    # Check logs were recorded
    assert len(step.logs) >= 3
    assert any('processing' in log['message'].lower() for log in step.logs)
    assert any(log['level'] == 'WARNING' for log in step.logs)
    assert any(log['level'] == 'ERROR' for log in step.logs)


@pytest.mark.database
@pytest.mark.integration
def test_step_checkpoint(test_schema, mock_context):
    """Test Step checkpoint functionality"""
    cycle_id = create_cycle('test_cycle_4')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Save checkpoints
    step.checkpoint({'records_processed': 100})
    step.checkpoint({'records_processed': 500})
    step.checkpoint({'records_processed': 1000})

    # Check checkpoints were saved
    assert len(step.checkpoints) == 3
    assert step.checkpoints[-1]['data']['records_processed'] == 1000


@pytest.mark.database
@pytest.mark.integration
def test_step_complete(test_schema, mock_context):
    """Test Step completion"""
    cycle_id = create_cycle('test_cycle_5')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Complete the step
    output_data = {'total_records': 1000, 'status': 'success'}
    step.complete(output_data)

    # Verify completion in database
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED
    assert last_run['output_data']['total_records'] == 1000
    assert '_meta' in last_run['output_data']


@pytest.mark.database
@pytest.mark.integration
def test_step_fail(test_schema, mock_context):
    """Test Step failure"""
    cycle_id = create_cycle('test_cycle_6')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Fail the step
    step.fail("Connection timeout error")

    # Verify failure in database
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.FAILED
    assert last_run['error_message'] == "Connection timeout error"


@pytest.mark.database
@pytest.mark.integration
def test_step_skip(test_schema, mock_context):
    """Test Step skipping"""
    cycle_id = create_cycle('test_cycle_7')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Skip the step
    step.skip("Data already loaded")

    # Verify skip in database
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.SKIPPED
    assert last_run['output_data']['skip_reason'] == "Data already loaded"


@pytest.mark.database
@pytest.mark.integration
def test_step_get_last_output(test_schema, mock_context):
    """Test getting last output from completed step"""
    cycle_id = create_cycle('test_cycle_8')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run - complete
    step1 = Step(context)
    step1.complete({'result': 'first_run', 'count': 100})

    # Second run - get last output
    # (This will fail to start because step already executed, but we can still get output)
    step2 = Step(context)
    last_output = step2.get_last_output()

    assert last_output is not None
    assert last_output['result'] == 'first_run'
    assert last_output['count'] == 100


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager(test_schema, mock_context):
    """Test Step as context manager"""
    cycle_id = create_cycle('test_cycle_9')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Use as context manager - auto completes on success
    with Step(context) as step:
        step.log("Processing data")
        step.checkpoint({'progress': 50})

    # Should auto-complete
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager_exception(test_schema, mock_context):
    """Test Step context manager with exception - auto fails"""
    cycle_id = create_cycle('test_cycle_10')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Use as context manager - raises exception
    try:
        with Step(context) as step:
            step.log("Processing data")
            raise ValueError("Something went wrong")
    except ValueError:
        pass  # Expected

    # Should auto-fail
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.FAILED
    assert 'Something went wrong' in last_run['error_message']


@pytest.mark.database
@pytest.mark.integration
def test_step_force_reexecution(test_schema, mock_context):
    """Test forcing re-execution of already executed step"""
    cycle_id = create_cycle('test_cycle_11')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First execution
    step1 = Step(context)
    step1.complete()

    # Second execution with force
    step2 = Step(context)
    step2.start(force=True)  # Force re-execution

    assert step2.run_num == 2  # Should be run #2
    step2.complete()

    # Verify both runs exist
    last_run = get_last_step_run(step_id)
    assert last_run['run_num'] == 2


@pytest.mark.database
@pytest.mark.integration
def test_step_module_uses_context(test_schema, mock_context):
    """Test that Step class uses schema context correctly"""
    # Create data in test schema
    cycle_id = create_cycle('context_cycle')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Step should work with context
    step = Step(context)
    step.complete()

    # Should find the run in test schema
    last_run = get_last_step_run(step_id)
    assert last_run is not None
    assert last_run['status'] == StepStatus.COMPLETED

    # This proves Step class is using context correctly
    assert test_schema != 'public'
