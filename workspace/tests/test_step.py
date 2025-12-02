"""
Tests for step.py Module

Tests the Step class in step.py which handles step execution lifecycle.
This is a high-level domain class for tracking step execution.
"""

import pytest
from helpers.step import Step, StepError, get_or_create_step, get_last_step_run
from helpers.cycle import register_cycle
from helpers.stage import get_or_create_stage
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
    cycle_id = register_cycle('test_cycle_1')
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
    cycle_id = register_cycle('test_cycle_2')
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
    cycle_id = register_cycle('test_cycle_3')
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
    cycle_id = register_cycle('test_cycle_4')
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
    cycle_id = register_cycle('test_cycle_5')
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
    cycle_id = register_cycle('test_cycle_6')
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
    cycle_id = register_cycle('test_cycle_7')
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
    cycle_id = register_cycle('test_cycle_8')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run - complete
    step1 = Step(context)
    step1.complete({'result': 'first_run', 'count': 100})

    # Second run - auto-starts with force=True (re-run)
    # Complete it with different output
    step2 = Step(context)
    step2.complete({'result': 'second_run', 'count': 200})

    # get_last_output should return the most recent COMPLETED run (run #2)
    last_output = step2.get_last_output()

    assert last_output is not None
    assert last_output['result'] == 'second_run'
    assert last_output['count'] == 200


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager(test_schema, mock_context):
    """Test Step as context manager"""
    cycle_id = register_cycle('test_cycle_9')
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
    cycle_id = register_cycle('test_cycle_10')
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
    cycle_id = register_cycle('test_cycle_11')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First execution
    step1 = Step(context)
    step1.complete()

    # Second execution - auto-starts as run #2 with force=True
    step2 = Step(context)
    assert step2.run_num == 2  # Auto-started as run #2

    # Calling start(force=True) again creates run #3
    step2.start(force=True)
    assert step2.run_num == 3  # Now run #3

    step2.complete()

    # Verify run #3 exists
    last_run = get_last_step_run(step_id)
    assert last_run['run_num'] == 3


@pytest.mark.database
@pytest.mark.integration
def test_step_module_uses_context(test_schema, mock_context):
    """Test that Step class uses schema context correctly"""
    # Create data in test schema
    cycle_id = register_cycle('context_cycle')
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


@pytest.mark.database
@pytest.mark.integration
def test_step_checkpoint_without_run_id(test_schema, mock_context):
    """Test that checkpoint raises error if step not started"""
    cycle_id = register_cycle('test_cycle_checkpoint_err')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run and complete the step
    step1 = Step(context)
    step1.complete()

    # Create a second Step instance (already executed)
    step2 = Step(context)

    # Manually clear run_id to simulate not started
    step2.run_id = None

    # Checkpoint should raise error
    with pytest.raises(StepError, match="Step not started"):
        step2.checkpoint({'data': 'test'})


@pytest.mark.database
@pytest.mark.integration
def test_step_complete_without_run_id(test_schema, mock_context):
    """Test that complete raises error if step not started"""
    cycle_id = register_cycle('test_cycle_complete_err')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run and complete the step
    step1 = Step(context)
    step1.complete()

    # Create a second Step instance (already executed)
    step2 = Step(context)

    # Manually clear run_id to simulate not started
    step2.run_id = None

    # Complete should raise error
    with pytest.raises(StepError, match="Step not started"):
        step2.complete()


@pytest.mark.database
@pytest.mark.integration
def test_step_skip_without_run_id(test_schema, mock_context):
    """Test that skip raises error if step not started"""
    cycle_id = register_cycle('test_cycle_skip_err')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run and complete the step
    step1 = Step(context)
    step1.complete()

    # Create a second Step instance (already executed)
    step2 = Step(context)

    # Manually clear run_id to simulate not started
    step2.run_id = None

    # Skip should raise error
    with pytest.raises(StepError, match="Step not started"):
        step2.skip("test reason")


@pytest.mark.database
@pytest.mark.integration
def test_step_fail_without_run_id(test_schema, mock_context):
    """Test that fail handles case when step never started"""
    cycle_id = register_cycle('test_cycle_fail_no_run')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First run and complete the step
    step1 = Step(context)
    step1.complete()

    # Create a second Step instance (already executed)
    step2 = Step(context)

    # Manually clear run_id to simulate not started
    step2.run_id = None

    # Fail should handle this gracefully (just logs, doesn't raise)
    step2.fail("test error message")
    # No exception should be raised


@pytest.mark.database
@pytest.mark.integration
def test_step_get_last_output_failed_run(test_schema, mock_context):
    """Test get_last_output returns None for failed run"""
    cycle_id = register_cycle('test_cycle_output_failed')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Run and fail
    step1 = Step(context)
    step1.fail("Something went wrong")

    # Get last output should return None for failed run
    step2 = Step(context)
    last_output = step2.get_last_output()

    assert last_output is None


@pytest.mark.database
@pytest.mark.integration
def test_step_get_last_output_no_run(test_schema, mock_context):
    """Test get_last_output returns None when no run exists"""
    cycle_id = register_cycle('test_cycle_no_run')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Don't run the step, just create context
    step = Step(context)

    # Since we auto-started, complete it first
    step.complete()

    # Now create a new step for a different step_id
    step_id_2 = get_or_create_step(stage_id, 2, 'Process Data')
    context.step_id = step_id_2

    step2 = Step(context)
    # Complete this one too
    step2.complete()

    # Now create step3 for a new step that has no completed runs
    step_id_3 = get_or_create_step(stage_id, 3, 'Final Data')
    context.step_id = step_id_3
    step3 = Step(context)
    step3.fail("error")  # Fail it instead of completing

    # Get last output should return None
    last_output = step3.get_last_output()
    assert last_output is None


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager_manual_start(test_schema, mock_context):
    """Test Step context manager when already manually started"""
    cycle_id = register_cycle('test_cycle_manual_start')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)
    # Step is already started via __init__

    # Using context manager should not start again
    with step as s:
        assert s.run_id is not None
        s.log("Processing")

    # Should auto-complete
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED


# ============================================================================
# Tests - Negative Test Coverage (Exception Paths)
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_step_start_already_executed_without_force(test_schema, mock_context):
    """Test that start raises error when step already executed without force flag"""
    cycle_id = register_cycle('test_cycle_start_error')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First execution
    step1 = Step(context)
    step1.complete()

    # Second execution - manually set executed flag and try to start without force
    step2 = Step(context)
    assert step2.executed is True

    # Try to call start() without force - should raise StepError (Line 100)
    with pytest.raises(StepError, match="Cannot execute step"):
        step2.start(force=False)


@pytest.mark.database
@pytest.mark.integration
def test_step_start_database_failure(test_schema, mock_context, monkeypatch):
    """Test that start handles database failure when creating step run"""
    cycle_id = register_cycle('test_cycle_start_db_fail')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First complete a step
    step1 = Step(context)
    step1.complete()

    # Mock create_step_run to fail
    from helpers import step as step_module
    original_create = step_module.create_step_run

    def failing_create(*args, **kwargs):
        raise Exception("Database connection lost")

    monkeypatch.setattr(step_module, 'create_step_run', failing_create)

    # Create a new step that hasn't been executed
    step_id_2 = get_or_create_step(stage_id, 2, 'Process Data')
    context.step_id = step_id_2

    # This should fail during __init__ -> start() (Lines 117-118)
    with pytest.raises(StepError, match="Failed to start step run"):
        step2 = Step(context)

    # Restore
    monkeypatch.setattr(step_module, 'create_step_run', original_create)


@pytest.mark.database
@pytest.mark.integration
def test_step_fail_database_update_failure(test_schema, mock_context, monkeypatch, capsys):
    """Test that fail handles database update failure gracefully"""
    cycle_id = register_cycle('test_cycle_fail_db_error')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Mock update_step_run to fail
    from helpers import step as step_module
    original_update = step_module.update_step_run

    def failing_update(*args, **kwargs):
        raise Exception("Database write failed")

    monkeypatch.setattr(step_module, 'update_step_run', failing_update)

    # Call fail - should catch exception and print error (Lines 241-242)
    step.fail("Test error message")

    # Restore
    monkeypatch.setattr(step_module, 'update_step_run', original_update)

    # Verify error message was printed
    captured = capsys.readouterr()
    assert "Failed to update step status" in captured.out
    assert "Database write failed" in captured.out


@pytest.mark.database
@pytest.mark.integration
def test_step_skip_database_update_failure(test_schema, mock_context, monkeypatch, capsys):
    """Test that skip handles database update failure gracefully"""
    cycle_id = register_cycle('test_cycle_skip_db_error')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    step = Step(context)

    # Mock update_step_run to fail
    from helpers import step as step_module
    original_update = step_module.update_step_run

    def failing_update(*args, **kwargs):
        raise Exception("Database write failed")

    monkeypatch.setattr(step_module, 'update_step_run', failing_update)

    # Call skip - should catch exception and print error (Lines 267-268)
    step.skip("Skipping for test")

    # Restore
    monkeypatch.setattr(step_module, 'update_step_run', original_update)

    # Verify error message was printed
    captured = capsys.readouterr()
    assert "Failed to skip step" in captured.out
    assert "Database write failed" in captured.out


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager_entry_without_run_id(test_schema, mock_context):
    """Test Step context manager __enter__ works with auto-started re-run"""
    cycle_id = register_cycle('test_cycle_ctx_entry')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # First execution
    step1 = Step(context)
    step1.complete()

    # Second execution - auto-starts with force=True (re-run)
    step2 = Step(context)
    assert step2.executed is True
    assert step2.run_id is not None  # Auto-started on re-run
    assert step2.run_num == 2  # Run #2

    # Using context manager should work normally since run_id is populated
    with step2 as s:
        assert s.run_id is not None
        assert s.run_num == 2

    # Context manager __exit__ should have completed the step
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager_exit_no_exception_with_run_id(test_schema, mock_context):
    """Test Step context manager __exit__ complete path when no exception"""
    cycle_id = register_cycle('test_cycle_ctx_exit_ok')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Use context manager - should auto-complete on success (Lines 293-294)
    with Step(context) as step:
        step.log("Processing data")
        assert step.run_id is not None

    # Verify step was completed
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.COMPLETED


@pytest.mark.database
@pytest.mark.integration
def test_step_context_manager_exit_with_exception_and_run_id(test_schema, mock_context):
    """Test Step context manager __exit__ fail path when exception occurs"""
    cycle_id = register_cycle('test_cycle_ctx_exit_fail')
    stage_id = get_or_create_stage(cycle_id, 1, 'Setup')
    step_id = get_or_create_step(stage_id, 1, 'Load Data')

    context = mock_context()
    context.step_id = step_id

    # Use context manager with exception (Lines 297-298)
    try:
        with Step(context) as step:
            assert step.run_id is not None
            raise ValueError("Intentional test error")
    except ValueError:
        pass  # Expected - exception is re-raised (Line 301)

    # Verify step was failed
    last_run = get_last_step_run(step_id)
    assert last_run['status'] == StepStatus.FAILED
    assert "Intentional test error" in last_run['error_message']
