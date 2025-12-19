"""
Tests for step chaining functionality.

This module tests the automatic execution of subsequent notebooks when batches complete.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from helpers.step_chain import (
    get_next_step_info,
    should_execute_next_step,
    get_chain_status,
    _build_notebook_path,
    STAGE_03_CHAIN
)
from helpers.notebook_executor import execute_notebook, execute_next_step, validate_nbconvert_available
from helpers.database import execute_insert, execute_query, execute_command
from helpers.constants import BatchStatus, CycleStatus, StepStatus


@pytest.fixture
def test_cycle(test_schema, request):
    """Create a test cycle for chaining tests."""
    # Archive any existing active cycles
    execute_command(
        "UPDATE irp_cycle SET status = 'ARCHIVED' WHERE status = 'ACTIVE'",
        schema=test_schema
    )

    # Create unique cycle name for each test using a counter to avoid duplicates
    test_name = request.node.name
    import time
    timestamp = str(int(time.time() * 1000))[-6:]  # Last 6 digits of timestamp
    cycle_name = f'TestChain-{test_name[:14]}-{timestamp}'  # Limit length

    # Create new active cycle
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        (cycle_name, CycleStatus.ACTIVE),
        schema=test_schema
    )

    return {
        'id': cycle_id,
        'name': cycle_name
    }


@pytest.fixture
def test_configuration(test_schema, test_cycle):
    """Create a test configuration."""
    # Need to use json.dumps for JSONB column
    config_data = json.dumps({'test': 'config'})

    config_id = execute_insert(
        """
        INSERT INTO irp_configuration (
            cycle_id,
            configuration_file_name,
            configuration_data,
            status,
            file_last_updated_ts
        )
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (test_cycle['id'], 'test_config.xlsx', config_data, 'ACTIVE'),
        schema=test_schema
    )

    return {'id': config_id}


@pytest.fixture
def test_step(test_schema, test_cycle):
    """Create a test step run in Stage 03."""
    # Create stage
    stage_id = execute_insert(
        """
        INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
        VALUES (%s, %s, %s)
        """,
        (test_cycle['id'], 3, 'Stage_03_Data_Import'),
        schema=test_schema
    )

    # Create step
    step_id = execute_insert(
        """
        INSERT INTO irp_step (stage_id, step_num, step_name)
        VALUES (%s, %s, %s)
        """,
        (stage_id, 1, 'Submit_Create_EDM_Batch'),
        schema=test_schema
    )

    # Create step run
    step_run_id = execute_insert(
        """
        INSERT INTO irp_step_run (step_id, run_num, status)
        VALUES (%s, %s, %s)
        """,
        (step_id, 1, StepStatus.ACTIVE),
        schema=test_schema
    )

    return {'id': step_run_id, 'stage_num': 3, 'step_num': 1}


@pytest.fixture
def test_batch(test_schema, test_configuration, test_step):
    """Create a test batch."""
    batch_id = execute_insert(
        """
        INSERT INTO irp_batch (
            batch_type,
            configuration_id,
            step_id,
            status
        )
        VALUES (%s, %s, %s, %s)
        """,
        ('EDM Creation', test_configuration['id'], test_step['id'], BatchStatus.ACTIVE),
        schema=test_schema
    )

    return {'id': batch_id, 'type': 'EDM Creation'}


class TestStepChainConfiguration:
    """Test step chain configuration and registry."""

    def test_stage_03_chain_has_8_steps(self):
        """Verify Stage 03 chain has all 8 steps configured."""
        assert len(STAGE_03_CHAIN) == 8
        for step_num in range(1, 9):
            assert step_num in STAGE_03_CHAIN

    def test_chain_step_structure(self):
        """Verify each chain step has required fields."""
        for step_num, config in STAGE_03_CHAIN.items():
            assert 'next_step' in config
            assert 'batch_type' in config
            assert 'wait_for' in config
            assert 'description' in config

    def test_chain_step_1_to_2(self):
        """Verify Step 1 chains to Step 2."""
        config = STAGE_03_CHAIN[1]
        assert config['next_step'] == 2
        assert config['batch_type'] == 'EDM Creation'
        assert config['wait_for'] == BatchStatus.COMPLETED

    def test_chain_step_7_to_8(self):
        """Verify Step 7 chains to Step 8."""
        config = STAGE_03_CHAIN[7]
        assert config['next_step'] == 8
        assert config['batch_type'] == 'Portfolio Mapping'
        assert config['wait_for'] == BatchStatus.COMPLETED

    def test_chain_step_8_is_final(self):
        """Verify Step 8 is the final step with no next step."""
        config = STAGE_03_CHAIN[8]
        assert config['next_step'] is None
        assert config['batch_type'] is None


class TestGetNextStepInfo:
    """Test get_next_step_info function."""

    def test_batch_not_found(self, test_schema):
        """Test with non-existent batch ID."""
        result = get_next_step_info(999999, schema=test_schema)
        assert result is None

    def test_cycle_not_active(self, test_schema, test_batch, test_cycle):
        """Test returns None when cycle is not ACTIVE."""
        # Archive the cycle
        execute_command(
            "UPDATE irp_cycle SET status = %s WHERE id = %s",
            (CycleStatus.ARCHIVED, test_cycle['id']),
            schema=test_schema
        )

        # Mark batch as completed
        execute_command(
            "UPDATE irp_batch SET status = %s WHERE id = %s",
            (BatchStatus.COMPLETED, test_batch['id']),
            schema=test_schema
        )

        result = get_next_step_info(test_batch['id'], schema=test_schema)
        assert result is None

    def test_batch_not_completed(self, test_schema, test_batch):
        """Test returns None when batch is not COMPLETED."""
        # Keep batch as ACTIVE
        result = get_next_step_info(test_batch['id'], schema=test_schema)
        assert result is None

    def test_stage_not_03(self, test_schema, test_cycle, test_configuration):
        """Test returns None for stages other than Stage 03."""
        # Create stage 02
        stage_id = execute_insert(
            """
            INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
            VALUES (%s, %s, %s)
            """,
            (test_cycle['id'], 2, 'Stage_02_Something'),
            schema=test_schema
        )

        # Create step in Stage 02
        step_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 1, 'Some_Step'),
            schema=test_schema
        )

        # Create step run
        step_run_id = execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step_id, 1, StepStatus.ACTIVE),
            schema=test_schema
        )

        # Create batch
        batch_id = execute_insert(
            """
            INSERT INTO irp_batch (batch_type, configuration_id, step_id, status)
            VALUES (%s, %s, %s, %s)
            """,
            ('Some Batch', test_configuration['id'], step_run_id, BatchStatus.COMPLETED),
            schema=test_schema
        )

        result = get_next_step_info(batch_id, schema=test_schema)
        assert result is None

    def test_batch_type_mismatch(self, test_schema, test_batch):
        """Test returns None when batch type doesn't match chain config."""
        # Update batch type to something unexpected
        execute_command(
            "UPDATE irp_batch SET batch_type = %s, status = %s WHERE id = %s",
            ('Wrong Type', BatchStatus.COMPLETED, test_batch['id']),
            schema=test_schema
        )

        result = get_next_step_info(test_batch['id'], schema=test_schema)
        assert result is None

    def test_final_step_no_next(self, test_schema, test_cycle, test_configuration):
        """Test returns None for final step (Step 08)."""
        # Create stage
        stage_id = execute_insert(
            """
            INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
            VALUES (%s, %s, %s)
            """,
            (test_cycle['id'], 3, 'Stage_03_Data_Import'),
            schema=test_schema
        )

        # Create step 08
        step_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 8, 'Control_Totals'),
            schema=test_schema
        )

        # Create step run
        step_run_id = execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step_id, 1, StepStatus.ACTIVE),
            schema=test_schema
        )

        # Create batch (Note: Step 8 has no batch_type in config, using placeholder)
        batch_id = execute_insert(
            """
            INSERT INTO irp_batch (batch_type, configuration_id, step_id, status)
            VALUES (%s, %s, %s, %s)
            """,
            ('Control Totals', test_configuration['id'], step_run_id, BatchStatus.COMPLETED),
            schema=test_schema
        )

        result = get_next_step_info(batch_id, schema=test_schema)
        assert result is None

    @patch('helpers.step_chain._build_notebook_path')
    def test_successful_next_step_info(self, mock_build_path, test_schema, test_batch, test_cycle):
        """Test returns correct next step info for valid completed batch."""
        # Mock notebook path
        mock_notebook_path = Path('/fake/path/Create_Base_Portfolios.ipynb')
        mock_build_path.return_value = mock_notebook_path

        # Mark batch as completed
        execute_command(
            "UPDATE irp_batch SET status = %s WHERE id = %s",
            (BatchStatus.COMPLETED, test_batch['id']),
            schema=test_schema
        )

        result = get_next_step_info(test_batch['id'], schema=test_schema)

        assert result is not None
        assert result['step_num'] == 2  # Next step after Step 01
        assert result['stage_num'] == 3
        assert result['current_step_num'] == 1
        assert result['notebook_path'] == mock_notebook_path
        assert result['cycle_name'] == test_cycle['name']
        assert 'Portfolio Creation' in result['description']


class TestShouldExecuteNextStep:
    """Test should_execute_next_step function."""

    @patch('helpers.step_chain._build_notebook_path')
    def test_should_execute_when_next_step_not_run(self, mock_build_path, test_schema, test_batch):
        """Test returns True when next step has not been executed."""
        mock_build_path.return_value = Path('/fake/path/notebook.ipynb')

        # Mark batch as completed
        execute_command(
            "UPDATE irp_batch SET status = %s WHERE id = %s",
            (BatchStatus.COMPLETED, test_batch['id']),
            schema=test_schema
        )

        result = should_execute_next_step(test_batch['id'], schema=test_schema)
        assert result is True

    @patch('helpers.step_chain._build_notebook_path')
    def test_should_execute_even_when_next_step_already_run(
        self, mock_build_path, test_schema, test_batch, test_cycle
    ):
        """Test returns True even when next step has already been executed.

        This allows re-running workflows after entity deletion - submit_batch
        will check entity existence and only resubmit jobs for missing entities.
        """
        mock_build_path.return_value = Path('/fake/path/notebook.ipynb')

        # Mark batch as completed
        execute_command(
            "UPDATE irp_batch SET status = %s WHERE id = %s",
            (BatchStatus.COMPLETED, test_batch['id']),
            schema=test_schema
        )

        # Get the existing stage
        stage_result = execute_query(
            "SELECT id FROM irp_stage WHERE cycle_id = %s AND stage_num = %s",
            (test_cycle['id'], 3),
            schema=test_schema
        )
        stage_id = stage_result.iloc[0]['id']

        # Create step 02
        step_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 2, 'Create_Base_Portfolios'),
            schema=test_schema
        )

        # Create a run for the next step (Step 02)
        execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step_id, 1, StepStatus.COMPLETED),
            schema=test_schema
        )

        # Note: should_execute_next_step now returns True even if the next step
        # was already executed. This allows re-running workflows after entity
        # deletion - submit_batch will check entity existence and only resubmit
        # jobs for missing entities.
        result = should_execute_next_step(test_batch['id'], schema=test_schema)
        assert result is True

    def test_should_not_execute_when_no_next_step(self, test_schema, test_batch):
        """Test returns False when get_next_step_info returns None."""
        # Keep batch as ACTIVE (won't meet COMPLETED requirement)
        result = should_execute_next_step(test_batch['id'], schema=test_schema)
        assert result is False


class TestGetChainStatus:
    """Test get_chain_status function."""

    def test_empty_chain_status(self, test_schema, test_cycle):
        """Test chain status with no steps executed."""
        result = get_chain_status(test_cycle['name'], schema=test_schema)

        assert len(result) == 8  # All 8 steps
        for step_status in result:
            assert step_status['step_status'] == 'NOT_STARTED'
            assert step_status['batch_status'] is None

    def test_chain_status_with_executed_steps(
        self, test_schema, test_cycle, test_configuration
    ):
        """Test chain status with some steps executed."""
        # Create stage
        stage_id = execute_insert(
            """
            INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
            VALUES (%s, %s, %s)
            """,
            (test_cycle['id'], 3, 'Stage_03_Data_Import'),
            schema=test_schema
        )

        # Create step 01
        step1_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 1, 'Submit_Create_EDM_Batch'),
            schema=test_schema
        )

        # Create Step 01 run
        step1_run_id = execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step1_id, 1, StepStatus.COMPLETED),
            schema=test_schema
        )

        # Create batch for Step 01
        execute_insert(
            """
            INSERT INTO irp_batch (batch_type, configuration_id, step_id, status)
            VALUES (%s, %s, %s, %s)
            """,
            ('EDM Creation', test_configuration['id'], step1_run_id, BatchStatus.COMPLETED),
            schema=test_schema
        )

        # Create step 02
        step2_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 2, 'Create_Base_Portfolios'),
            schema=test_schema
        )

        # Create Step 02 run
        execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step2_id, 1, StepStatus.ACTIVE),
            schema=test_schema
        )

        result = get_chain_status(test_cycle['name'], schema=test_schema)

        assert len(result) == 8

        # Step 1 should show as completed
        step1_status = result[0]
        assert step1_status['step_num'] == 1
        assert step1_status['step_status'] == StepStatus.COMPLETED
        assert step1_status['batch_status'] == BatchStatus.COMPLETED

        # Step 2 should show as active
        step2_status = result[1]
        assert step2_status['step_num'] == 2
        assert step2_status['step_status'] == StepStatus.ACTIVE

        # Remaining steps should be not started
        for i in range(2, 8):
            assert result[i]['step_status'] == 'NOT_STARTED'


class TestBuildNotebookPath:
    """Test _build_notebook_path function."""

    def test_build_path_step_01(self):
        """Test building path for Step 01."""
        # Note: This test will fail if directory structure doesn't exist
        # We'll mock the path resolution
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.glob') as mock_glob:
                # Mock stage directory
                mock_stage_dir = MagicMock()
                mock_stage_dir.__truediv__ = MagicMock(return_value=MagicMock())
                mock_glob.return_value = [mock_stage_dir]

                # Mock notebook file
                mock_notebook = Path('/fake/Stage_03/Submit_Create_EDM_Batch.ipynb')

                with patch.object(Path, 'glob', return_value=[mock_notebook]):
                    result = _build_notebook_path('Test-2025-Q1', 3, 1)
                    assert 'Submit_Create_EDM_Batch.ipynb' in str(result)

    def test_build_path_invalid_step(self):
        """Test building path with invalid step number raises error."""
        with pytest.raises(ValueError, match="No notebook configured"):
            _build_notebook_path('Test-2025-Q1', 3, 99)


class TestNotebookExecutor:
    """Test notebook execution functions."""

    def test_validate_nbconvert_available(self):
        """Test validation of nbconvert availability."""
        # This will actually check if nbconvert is installed
        # Result depends on environment
        result = validate_nbconvert_available()
        assert isinstance(result, bool)

    @patch('subprocess.run')
    def test_execute_notebook_success(self, mock_run):
        """Test successful notebook execution."""
        # Mock successful execution
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Execution successful"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        # Create a mock notebook path
        with patch('pathlib.Path.exists', return_value=True):
            notebook_path = Path('/fake/notebook.ipynb')

            result = execute_notebook(notebook_path)

            assert result['success'] is True
            assert result['notebook_path'] == notebook_path
            assert result['execution_time'] >= 0
            assert result['error'] is None

    @patch('subprocess.run')
    def test_execute_notebook_failure(self, mock_run):
        """Test failed notebook execution."""
        # Mock failed execution
        import subprocess
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=['jupyter', 'nbconvert'],
            output="Failed",
            stderr="Error occurred"
        )

        with patch('pathlib.Path.exists', return_value=True):
            notebook_path = Path('/fake/notebook.ipynb')

            result = execute_notebook(notebook_path)

            assert result['success'] is False
            assert result['error'] is not None
            assert 'failed' in result['error'].lower()

    @patch('subprocess.run')
    def test_execute_notebook_timeout(self, mock_run):
        """Test notebook execution timeout."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=['jupyter', 'nbconvert'],
            timeout=10
        )

        with patch('pathlib.Path.exists', return_value=True):
            notebook_path = Path('/fake/notebook.ipynb')

            result = execute_notebook(notebook_path, timeout=10)

            assert result['success'] is False
            assert 'timed out' in result['error'].lower()

    def test_execute_notebook_not_found(self):
        """Test execution with non-existent notebook."""
        notebook_path = Path('/nonexistent/notebook.ipynb')

        with pytest.raises(FileNotFoundError):
            execute_notebook(notebook_path)

    @patch('helpers.notebook_executor.execute_notebook')
    def test_execute_next_step(self, mock_execute):
        """Test execute_next_step wrapper function."""
        mock_result = {
            'success': True,
            'execution_time': 42.0,
            'error': None
        }
        mock_execute.return_value = mock_result

        result = execute_next_step(
            cycle_name='Test-2025-Q1',
            stage_num=3,
            step_num=2,
            notebook_path=Path('/fake/notebook.ipynb')
        )

        assert result == mock_result
        mock_execute.assert_called_once()


class TestIntegrationStepChaining:
    """Integration tests for complete step chaining workflow."""

    @patch('helpers.step_chain._build_notebook_path')
    @patch('helpers.notebook_executor.execute_notebook')
    def test_complete_chain_workflow(
        self, mock_execute, mock_build_path, test_schema, test_cycle, test_configuration
    ):
        """Test complete workflow: batch completion → chain detection → execution."""
        # Mock dependencies
        mock_build_path.return_value = Path('/fake/notebook.ipynb')
        mock_execute.return_value = {
            'success': True,
            'execution_time': 10.0,
            'error': None
        }

        # Create stage
        stage_id = execute_insert(
            """
            INSERT INTO irp_stage (cycle_id, stage_num, stage_name)
            VALUES (%s, %s, %s)
            """,
            (test_cycle['id'], 3, 'Stage_03_Data_Import'),
            schema=test_schema
        )

        # Create step 01
        step1_id = execute_insert(
            """
            INSERT INTO irp_step (stage_id, step_num, step_name)
            VALUES (%s, %s, %s)
            """,
            (stage_id, 1, 'Submit_Create_EDM_Batch'),
            schema=test_schema
        )

        # Create Step 01 run
        step1_run_id = execute_insert(
            """
            INSERT INTO irp_step_run (step_id, run_num, status)
            VALUES (%s, %s, %s)
            """,
            (step1_id, 1, StepStatus.ACTIVE),
            schema=test_schema
        )

        # Create batch
        batch_id = execute_insert(
            """
            INSERT INTO irp_batch (batch_type, configuration_id, step_id, status)
            VALUES (%s, %s, %s, %s)
            """,
            ('EDM Creation', test_configuration['id'], step1_run_id, BatchStatus.COMPLETED),
            schema=test_schema
        )

        # Check if should execute next step
        should_execute = should_execute_next_step(batch_id, schema=test_schema)
        assert should_execute is True

        # Get next step info
        next_step_info = get_next_step_info(batch_id, schema=test_schema)
        assert next_step_info is not None
        assert next_step_info['step_num'] == 2

        # Execute next step
        from helpers.notebook_executor import execute_next_step
        result = execute_next_step(
            cycle_name=next_step_info['cycle_name'],
            stage_num=next_step_info['stage_num'],
            step_num=next_step_info['step_num'],
            notebook_path=next_step_info['notebook_path']
        )

        assert result['success'] is True
        mock_execute.assert_called_once()
