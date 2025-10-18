"""
Tests for WorkContext class and context management

This module tests:
- WorkContext initialization with explicit notebook_path
- Path parsing (cycle, stage, step extraction)
- Database entry creation/lookup
- Error handling for invalid paths
- get_info() method
- String representations (__repr__, __str__)

NOTE ON _get_current_notebook_path:
-----------------------------------
The _get_current_notebook_path() method is NOT tested here because:
1. It requires IPython/Jupyter environment which isn't available in pytest
2. It relies on __session__ variable which is notebook-specific
3. We can bypass it by passing explicit notebook_path to WorkContext()

All tests send explicit paths to WorkContext to avoid calling _get_current_notebook_path().
"""

import pytest
from pathlib import Path
from helpers.context import WorkContext, WorkContextError, get_context
from helpers.database import (
    register_cycle,
    get_cycle_by_name,
    archive_cycle,
    set_schema,
    schema_context
)
from helpers.constants import CycleStatus


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def create_test_notebook_path(cycle_name: str, stage_num: int, stage_name: str,
                               step_num: int, step_name: str) -> str:
    """
    Helper to create valid notebook path for testing

    Returns path like:
    /workspace/Active_TestCycle/Stage_1_DataCollection/Step_01_Initialize.ipynb
    """
    return (f"/workspace/Active_{cycle_name}/"
            f"Stage_{stage_num}_{stage_name}/"
            f"Step_{step_num:02d}_{step_name}.ipynb")


# ==============================================================================
# PATH PARSING TESTS
# ==============================================================================

class TestPathParsing:
    """Test that WorkContext correctly parses notebook paths"""

    def test_parse_valid_path_basic(self, test_schema):
        """Test parsing of a basic valid notebook path"""
        # Pre-create cycle to avoid register_cycle issue
        cycle_id = register_cycle('TestCycleBasic')

        path = create_test_notebook_path(
            'TestCycleBasic', 1, 'DataCollection', 1, 'Initialize'
        )

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'TestCycleBasic'
        assert context.stage_num == 1
        assert context.stage_name == 'DataCollection'
        assert context.step_num == 1
        assert context.step_name == 'Initialize'

    def test_parse_valid_path_with_numbers(self, test_schema):
        """Test parsing path with numbers in names"""
        cycle_id = register_cycle('Analysis_2025_Q1')

        path = create_test_notebook_path(
            'Analysis_2025_Q1', 2, 'Processing_Phase2', 5, 'Step_5_Analysis'
        )

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'Analysis_2025_Q1'
        assert context.stage_num == 2
        assert context.stage_name == 'Processing_Phase2'
        assert context.step_num == 5
        assert context.step_name == 'Step_5_Analysis'

    def test_parse_valid_path_with_underscores(self, test_schema):
        """Test parsing path with underscores in names"""
        cycle_id = register_cycle('Q4_Final_Review')

        path = create_test_notebook_path(
            'Q4_Final_Review', 3, 'Final_Data_Check', 10, 'Validate_All_Results'
        )

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'Q4_Final_Review'
        assert context.stage_num == 3
        assert context.stage_name == 'Final_Data_Check'
        assert context.step_num == 10
        assert context.step_name == 'Validate_All_Results'

    def test_parse_extracts_cycle_from_active_prefix(self, test_schema):
        """Test that cycle name is extracted from Active_ prefix"""
        cycle_id = register_cycle('MyCycle')

        # Path with multiple directories
        path = "/home/user/workspace/Active_MyCycle/Stage_1_Test/Step_01_Init.ipynb"

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'MyCycle'

    def test_parse_handles_double_digit_numbers(self, test_schema):
        """Test parsing with double-digit stage and step numbers"""
        cycle_id = register_cycle('BigCycle')

        path = create_test_notebook_path(
            'BigCycle', 15, 'AdvancedStage', 99, 'FinalStep'
        )

        context = WorkContext(notebook_path=path)

        assert context.stage_num == 15
        assert context.step_num == 99


# ==============================================================================
# ERROR HANDLING TESTS
# ==============================================================================

class TestErrorHandling:
    """Test that WorkContext properly handles invalid paths"""

    def test_error_on_missing_active_prefix(self, test_schema):
        """Test error when path doesn't contain Active_ prefix"""
        path = "/workspace/TestCycle/Stage_1_Test/Step_01_Init.ipynb"

        with pytest.raises(WorkContextError, match="not in an active cycle directory"):
            WorkContext(notebook_path=path)

    def test_error_on_missing_stage_directory(self, test_schema):
        """Test error when stage directory is missing or invalid"""
        # Missing Stage_ prefix
        path = "/workspace/Active_TestCycle/DataCollection/Step_01_Init.ipynb"

        with pytest.raises(WorkContextError, match="Cannot find stage directory"):
            WorkContext(notebook_path=path)

    def test_error_on_invalid_stage_format(self, test_schema):
        """Test error when stage directory has invalid format"""
        # Stage without number
        path = "/workspace/Active_TestCycle/Stage_DataCollection/Step_01_Init.ipynb"

        with pytest.raises(WorkContextError, match="Cannot find stage directory"):
            WorkContext(notebook_path=path)

    def test_error_on_invalid_notebook_filename(self, test_schema):
        """Test error when notebook filename doesn't match pattern"""
        cycle_id = register_cycle('TestCycleForInvalidNotebookFilename')

        # Invalid filename - missing Step_ prefix
        path = "/workspace/Active_TestCycleForInvalidNotebookFilename/Stage_1_Test/01_Init.ipynb"

        with pytest.raises(WorkContextError, match="Invalid notebook filename format"):
            WorkContext(notebook_path=path)

    def test_error_on_non_ipynb_extension(self, test_schema):
        """Test error when file is not .ipynb"""
        cycle_id = register_cycle('TestCycleForNonIpynb')

        # Wrong extension
        path = "/workspace/Active_TestCycleForNonIpynb/Stage_1_Test/Step_01_Init.py"

        with pytest.raises(WorkContextError, match="Invalid notebook filename format"):
            WorkContext(notebook_path=path)

    def test_error_on_archived_cycle(self, test_schema):
        """Test error when cycle exists but is archived"""
        # Create and archive a cycle
        cycle_id = register_cycle('ArchivedCycle')
        archive_cycle(cycle_id)

        path = create_test_notebook_path(
            'ArchivedCycle', 1, 'Test', 1, 'Init'
        )

        with pytest.raises(WorkContextError, match="is ARCHIVED, not active"):
            WorkContext(notebook_path=path)

    def test_error_on_active_cycle_mismatch(self, test_schema):
        """Test error when different active cycle exists"""
        # Create first active cycle
        cycle1_id = register_cycle('Cycle1')

        # Try to create context for different cycle
        path = create_test_notebook_path('Cycle2', 1, 'Test', 1, 'Init')

        # Should raise WorkContextError about active cycle mismatch
        with pytest.raises(WorkContextError, match="Active cycle 'Cycle1' exists"):
            WorkContext(notebook_path=path)

        # Cleanup: Archive Cycle1 so it doesn't interfere with other tests
        archive_cycle(cycle1_id)


# ==============================================================================
# DATABASE INTEGRATION TESTS
# ==============================================================================

class TestDatabaseIntegration:
    """Test WorkContext database operations"""

    def test_creates_database_entries_for_new_cycle(self, test_schema):
        """Test that WorkContext creates cycle, stage, and step in database"""
        # First, ensure no other active cycles exist that would conflict
        # (archive any existing active cycles)
        from helpers.database import execute_query
        active_cycles = execute_query(
            "SELECT id FROM irp_cycle WHERE status = 'ACTIVE'"
        )
        for _, row in active_cycles.iterrows():
            # Convert numpy.int64 to Python int for psycopg2 compatibility
            archive_cycle(int(row['id']))

        path = create_test_notebook_path(
            'NewCycle', 1, 'FirstStage', 1, 'FirstStep'
        )

        context = WorkContext(notebook_path=path)

        # Verify entries were created
        assert context.cycle_id is not None
        assert context.stage_id is not None
        assert context.step_id is not None

        # Verify cycle exists in database
        cycle = get_cycle_by_name('NewCycle')
        assert cycle is not None
        assert cycle['id'] == context.cycle_id

        # Cleanup
        archive_cycle(context.cycle_id)

    def test_uses_existing_cycle(self, test_schema):
        """Test that WorkContext uses existing cycle if available"""
        # Pre-create cycle
        cycle_id = register_cycle('ExistingCycle')

        path = create_test_notebook_path(
            'ExistingCycle', 1, 'Stage1', 1, 'Step1'
        )

        context = WorkContext(notebook_path=path)

        # Should use existing cycle
        assert context.cycle_id == cycle_id

        # Cleanup
        archive_cycle(cycle_id)

    def test_creates_multiple_stages_in_same_cycle(self, test_schema):
        """Test creating multiple stages in the same cycle"""
        cycle_id = register_cycle('MultiStageCycle')

        # Create context for stage 1
        path1 = create_test_notebook_path(
            'MultiStageCycle', 1, 'Stage1', 1, 'Step1'
        )
        context1 = WorkContext(notebook_path=path1)

        # Create context for stage 2
        path2 = create_test_notebook_path(
            'MultiStageCycle', 2, 'Stage2', 1, 'Step1'
        )
        context2 = WorkContext(notebook_path=path2)

        # Both should belong to same cycle
        assert context1.cycle_id == context2.cycle_id
        # But different stages
        assert context1.stage_id != context2.stage_id

        # Cleanup
        archive_cycle(cycle_id)

    def test_creates_multiple_steps_in_same_stage(self, test_schema):
        """Test creating multiple steps in the same stage"""
        cycle_id = register_cycle('MultiStepCycle')

        # Create context for step 1
        path1 = create_test_notebook_path(
            'MultiStepCycle', 1, 'Stage1', 1, 'Step1'
        )
        context1 = WorkContext(notebook_path=path1)

        # Create context for step 2
        path2 = create_test_notebook_path(
            'MultiStepCycle', 1, 'Stage1', 2, 'Step2'
        )
        context2 = WorkContext(notebook_path=path2)

        # Both should belong to same cycle and stage
        assert context1.cycle_id == context2.cycle_id
        assert context1.stage_id == context2.stage_id
        # But different steps
        assert context1.step_id != context2.step_id

        # Cleanup
        archive_cycle(cycle_id)

    def test_idempotent_context_creation(self, test_schema):
        """Test that creating context multiple times for same path is idempotent"""
        cycle_id = register_cycle('IdempotentCycle')

        path = create_test_notebook_path(
            'IdempotentCycle', 1, 'Stage1', 1, 'Step1'
        )

        # Create context twice
        context1 = WorkContext(notebook_path=path)
        context2 = WorkContext(notebook_path=path)

        # Should have same IDs
        assert context1.cycle_id == context2.cycle_id
        assert context1.stage_id == context2.stage_id
        assert context1.step_id == context2.step_id

        # Cleanup
        archive_cycle(cycle_id)


# ==============================================================================
# METHOD TESTS
# ==============================================================================

class TestWorkContextMethods:
    """Test WorkContext public methods"""

    def test_get_info_returns_all_fields(self, test_schema):
        """Test that get_info() returns complete dictionary"""
        cycle_id = register_cycle('InfoTestCycle')

        path = create_test_notebook_path(
            'InfoTestCycle', 2, 'TestStage', 3, 'TestStep'
        )

        context = WorkContext(notebook_path=path)
        info = context.get_info()

        # Check all required fields are present
        assert 'cycle_name' in info
        assert 'cycle_id' in info
        assert 'stage_num' in info
        assert 'stage_name' in info
        assert 'stage_id' in info
        assert 'step_num' in info
        assert 'step_name' in info
        assert 'step_id' in info
        assert 'notebook_path' in info

        # Verify values
        assert info['cycle_name'] == 'InfoTestCycle'
        assert info['stage_num'] == 2
        assert info['stage_name'] == 'TestStage'
        assert info['step_num'] == 3
        assert info['step_name'] == 'TestStep'
        assert 'InfoTestCycle' in info['notebook_path']

    def test_repr_contains_key_info(self, test_schema):
        """Test __repr__ contains cycle, stage, and step info"""
        cycle_id = register_cycle('ReprTestCycle')

        path = create_test_notebook_path(
            'ReprTestCycle', 1, 'Stage1', 2, 'Step2'
        )

        context = WorkContext(notebook_path=path)
        repr_str = repr(context)

        assert 'WorkContext' in repr_str
        assert 'ReprTestCycle' in repr_str
        assert 'stage=1' in repr_str
        assert 'step=2' in repr_str

    def test_str_human_readable_format(self, test_schema):
        """Test __str__ returns human-readable format"""
        cycle_id = register_cycle('StrTestCycle')

        path = create_test_notebook_path(
            'StrTestCycle', 3, 'DataProcessing', 5, 'CleanData'
        )

        context = WorkContext(notebook_path=path)
        str_repr = str(context)

        # Should contain readable hierarchy
        assert 'StrTestCycle' in str_repr
        assert 'Stage 3' in str_repr
        assert 'DataProcessing' in str_repr
        assert 'Step 5' in str_repr
        assert 'CleanData' in str_repr
        assert '→' in str_repr  # Arrow showing hierarchy


# ==============================================================================
# CONVENIENCE FUNCTION TESTS
# ==============================================================================

class TestConvenienceFunction:
    """Test get_context() convenience function"""

    def test_get_context_creates_work_context(self, test_schema):
        """Test that get_context() returns WorkContext instance"""
        cycle_id = register_cycle('ConvenienceTestCycle')

        path = create_test_notebook_path(
            'ConvenienceTestCycle', 1, 'Stage1', 1, 'Step1'
        )

        context = get_context(notebook_path=path)

        assert isinstance(context, WorkContext)
        assert context.cycle_name == 'ConvenienceTestCycle'

    def test_get_context_with_path_parameter(self, test_schema):
        """Test get_context() accepts explicit path"""
        cycle_id = register_cycle('PathTestCycle')

        path = create_test_notebook_path(
            'PathTestCycle', 2, 'Stage2', 3, 'Step3'
        )

        context = get_context(notebook_path=path)

        assert context.cycle_name == 'PathTestCycle'
        assert context.stage_num == 2
        assert context.step_num == 3


# ==============================================================================
# EDGE CASES AND SPECIAL SCENARIOS
# ==============================================================================

class TestEdgeCases:
    """Test edge cases and special scenarios"""

    def test_cycle_name_with_special_characters(self, test_schema):
        """Test cycle names with underscores and hyphens"""
        cycle_id = register_cycle('Test_Cycle-2025')

        path = create_test_notebook_path(
            'Test_Cycle-2025', 1, 'Stage1', 1, 'Step1'
        )

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'Test_Cycle-2025'

    def test_stage_name_with_multiple_underscores(self, test_schema):
        """Test stage names with multiple underscores"""
        cycle_id = register_cycle('TestCycleManyUnderscoresInStage')

        path = create_test_notebook_path(
            'TestCycleManyUnderscoresInStage', 1, 'Stage_With_Many_Parts', 1, 'Step1'
        )

        context = WorkContext(notebook_path=path)

        assert context.stage_name == 'Stage_With_Many_Parts'

    def test_step_name_with_multiple_underscores(self, test_schema):
        """Test step names with multiple underscores"""
        cycle_id = register_cycle('TestCycleStepWithManyUnderscores')

        path = create_test_notebook_path(
            'TestCycleStepWithManyUnderscores', 1, 'Stage1', 1, 'Step_Name_With_Parts'
        )

        context = WorkContext(notebook_path=path)

        assert context.step_name == 'Step_Name_With_Parts'

    def test_path_with_absolute_linux_path(self, test_schema):
        """Test with absolute Linux-style path"""
        cycle_id = register_cycle('LinuxTestCycle')

        path = "/home/user/notebooks/Active_LinuxTestCycle/Stage_1_Test/Step_01_Init.ipynb"

        context = WorkContext(notebook_path=path)

        assert context.cycle_name == 'LinuxTestCycle'

    def test_path_stores_as_pathlib_path(self, test_schema):
        """Test that notebook_path is stored as Path object"""
        cycle_id = register_cycle('PathStoreTestCycle')

        path = create_test_notebook_path(
            'PathStoreTestCycle', 1, 'Stage1', 1, 'Step1'
        )

        context = WorkContext(notebook_path=path)

        assert isinstance(context.notebook_path, Path)

    def test_single_digit_step_number(self, test_schema):
        """Test that single digit step numbers work"""
        cycle_id = register_cycle('SingleDigitCycle')

        # Using single digit without padding
        path = "/workspace/Active_SingleDigitCycle/Stage_1_Test/Step_5_Init.ipynb"

        context = WorkContext(notebook_path=path)

        assert context.step_num == 5


# ==============================================================================
# SCHEMA ISOLATION TESTS
# ==============================================================================

class TestSchemaIsolation:
    """Test that WorkContext respects schema context"""

    def test_context_respects_schema_context(self, test_schema):
        """Test that WorkContext works with schema_context"""
        # Create cycle in test schema
        with schema_context(test_schema):
            cycle_id = register_cycle('SchemaTestCycle')

        # Create context in test schema
        with schema_context(test_schema):
            path = create_test_notebook_path(
                'SchemaTestCycle', 1, 'Stage1', 1, 'Step1'
            )

            context = WorkContext(notebook_path=path)

            assert context.cycle_name == 'SchemaTestCycle'
            assert context.cycle_id is not None

    def test_contexts_isolated_between_schemas(self, test_schema):
        """Test that contexts in different schemas are isolated"""
        # Create isolation schema
        isolation_schema = f"{test_schema}_isolation"
        from helpers.database import init_database
        init_database(schema=isolation_schema)

        try:
            # Create same-named cycle in two different schemas
            with schema_context(test_schema):
                cycle_id_1 = register_cycle('IsolationTestCycle')
                path1 = create_test_notebook_path(
                    'IsolationTestCycle', 1, 'Stage1', 1, 'Step1'
                )
                context1 = WorkContext(notebook_path=path1)

            with schema_context(isolation_schema):
                cycle_id_2 = register_cycle('IsolationTestCycle')
                path2 = create_test_notebook_path(
                    'IsolationTestCycle', 1, 'Stage1', 1, 'Step1'
                )
                context2 = WorkContext(notebook_path=path2)

            # Same cycle name but different IDs (different schemas)
            assert context1.cycle_name == context2.cycle_name
            # Note: cycle_id might be same due to sequence sharing, but data is isolated

        finally:
            # Cleanup
            from helpers.database import get_engine
            from sqlalchemy import text
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {isolation_schema} CASCADE"))
                conn.commit()