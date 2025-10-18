"""
Tests for constants module configuration and validation

This module tests:
- DB_CONFIG settings from environment variables
- _missing_config validation logic
- Status class completeness (all() method includes all values)
- Status class alignment with database enum types
- Regex patterns (NOTEBOOK_PATTERN, STAGE_PATTERN, CYCLE_NAME_RULES)
"""

import pytest
import os
import re
from helpers.database import execute_query
from helpers.constants import (
    DB_CONFIG,
    CycleStatus,
    StepStatus,
    BatchStatus,
    ConfigurationStatus,
    JobStatus,
    NOTEBOOK_PATTERN,
    STAGE_PATTERN,
    CYCLE_NAME_RULES
)


# ==============================================================================
# DB_CONFIG TESTS
# ==============================================================================

def test_db_config_from_environment():
    """Test that DB_CONFIG correctly loads from environment variables set in test.sh"""
    # test.sh sets these environment variables:
    # DB_SERVER=localhost
    # DB_PORT=5432
    # DB_NAME=test_db
    # DB_USER=test_user
    # DB_PASSWORD=test_pass

    assert DB_CONFIG['host'] == 'localhost', "DB_CONFIG['host'] should match DB_SERVER env var"
    assert DB_CONFIG['port'] == 5432, "DB_CONFIG['port'] should match DB_PORT env var"
    assert DB_CONFIG['database'] == 'test_db', "DB_CONFIG['database'] should match DB_NAME env var"
    assert DB_CONFIG['user'] == 'test_user', "DB_CONFIG['user'] should match DB_USER env var"
    assert DB_CONFIG['password'] == 'test_pass', "DB_CONFIG['password'] should match DB_PASSWORD env var"


def test_db_config_port_defaults_to_5432():
    """Test that DB_PORT defaults to 5432 if not set"""
    # Note: In actual test run, DB_PORT is set, but we're documenting the default behavior
    assert DB_CONFIG['port'] == 5432, "Port should be 5432 (standard PostgreSQL port)"


def test_db_config_types():
    """Test that DB_CONFIG values have correct types"""
    assert isinstance(DB_CONFIG['host'], str), "host should be string"
    assert isinstance(DB_CONFIG['port'], int), "port should be integer"
    assert isinstance(DB_CONFIG['database'], str), "database should be string"
    assert isinstance(DB_CONFIG['user'], str), "user should be string"
    assert isinstance(DB_CONFIG['password'], str), "password should be string"


def test_db_config_no_none_values():
    """Test that no DB_CONFIG values are None in test environment"""
    for key, value in DB_CONFIG.items():
        assert value is not None, f"DB_CONFIG['{key}'] should not be None in test environment"


# ==============================================================================
# _missing_config VALIDATION TESTS
# ==============================================================================

def test_missing_config_validation_would_catch_missing_vars():
    """
    Test that _missing_config validation logic catches missing variables

    Note: We can't actually trigger this in tests since constants.py would
    fail to import, but we can verify the logic is correct.
    """
    # The validation logic in constants.py:
    # _missing_config = [k for k, v in DB_CONFIG.items() if v is None and k != 'port']
    # if _missing_config:
    #     raise ValueError(...)

    # Simulate the logic
    test_config = {
        'host': None,
        'port': 5432,
        'database': 'test_db',
        'user': None,
        'password': 'pass'
    }

    missing = [k for k, v in test_config.items() if v is None and k != 'port']

    assert 'host' in missing, "Should detect missing host"
    assert 'user' in missing, "Should detect missing user"
    assert 'port' not in missing, "Port can be None (has default)"
    assert 'database' not in missing, "database is not missing"


def test_missing_config_allows_port_to_be_none():
    """Test that port is excluded from required variables (it has a default)"""
    test_config = {
        'host': 'localhost',
        'port': None,  # This is OK - port has default value
        'database': 'db',
        'user': 'user',
        'password': 'pass'
    }

    missing = [k for k, v in test_config.items() if v is None and k != 'port']

    assert len(missing) == 0, "Port should be allowed to be None"


# ==============================================================================
# STATUS CLASS TESTS - COMPLETENESS
# ==============================================================================

class TestStatusClassCompleteness:
    """Test that all status classes have complete all() methods"""

    def test_cycle_status_all_includes_all_values(self):
        """Test CycleStatus.all() includes all status values"""
        # Get all class attributes that are status values (uppercase constants)
        all_statuses = [
            getattr(CycleStatus, attr)
            for attr in dir(CycleStatus)
            if not attr.startswith('_') and attr.isupper()
        ]

        all_method_result = CycleStatus.all()

        assert len(all_statuses) == len(all_method_result), \
            "CycleStatus.all() should include all status constants"

        for status in all_statuses:
            assert status in all_method_result, \
                f"CycleStatus.all() should include {status}"

    def test_step_status_all_includes_all_values(self):
        """Test StepStatus.all() includes all status values"""
        all_statuses = [
            getattr(StepStatus, attr)
            for attr in dir(StepStatus)
            if not attr.startswith('_') and attr.isupper()
        ]

        all_method_result = StepStatus.all()

        assert len(all_statuses) == len(all_method_result), \
            "StepStatus.all() should include all status constants"

        for status in all_statuses:
            assert status in all_method_result, \
                f"StepStatus.all() should include {status}"

    def test_batch_status_all_includes_all_values(self):
        """Test BatchStatus.all() includes all status values"""
        all_statuses = [
            getattr(BatchStatus, attr)
            for attr in dir(BatchStatus)
            if not attr.startswith('_') and attr.isupper()
        ]

        all_method_result = BatchStatus.all()

        assert len(all_statuses) == len(all_method_result), \
            "BatchStatus.all() should include all status constants"

        for status in all_statuses:
            assert status in all_method_result, \
                f"BatchStatus.all() should include {status}"

    def test_configuration_status_all_includes_all_values(self):
        """Test ConfigurationStatus.all() includes all status values"""
        all_statuses = [
            getattr(ConfigurationStatus, attr)
            for attr in dir(ConfigurationStatus)
            if not attr.startswith('_') and attr.isupper()
        ]

        all_method_result = ConfigurationStatus.all()

        assert len(all_statuses) == len(all_method_result), \
            "ConfigurationStatus.all() should include all status constants"

        for status in all_statuses:
            assert status in all_method_result, \
                f"ConfigurationStatus.all() should include {status}"

    def test_job_status_all_includes_all_values(self):
        """Test JobStatus.all() includes all status values"""
        all_statuses = [
            getattr(JobStatus, attr)
            for attr in dir(JobStatus)
            if not attr.startswith('_') and attr.isupper()
        ]

        all_method_result = JobStatus.all()

        assert len(all_statuses) == len(all_method_result), \
            "JobStatus.all() should include all status constants"

        for status in all_statuses:
            assert status in all_method_result, \
                f"JobStatus.all() should include {status}"


# ==============================================================================
# STATUS CLASS vs DATABASE ENUM TESTS
# ==============================================================================

# Mapping of Status classes to their database enum type and table/column
STATUS_ENUM_MAP = {
    'CycleStatus': {
        'class': CycleStatus,
        'enum_type': 'cycle_status_enum',
        'sample_table': 'irp_cycle',
        'sample_column': 'status'
    },
    'StepStatus': {
        'class': StepStatus,
        'enum_type': 'step_status_enum',
        'sample_table': 'irp_step_run',
        'sample_column': 'status'
    },
    'BatchStatus': {
        'class': BatchStatus,
        'enum_type': 'batch_status_enum',
        'sample_table': 'irp_batch',
        'sample_column': 'status'
    },
    'ConfigurationStatus': {
        'class': ConfigurationStatus,
        'enum_type': 'configuration_status_enum',
        'sample_table': 'irp_configuration',
        'sample_column': 'status'
    },
    'JobStatus': {
        'class': JobStatus,
        'enum_type': 'job_status_enum',
        'sample_table': 'irp_job',
        'sample_column': 'status'
    }
}


def get_database_enum_values(enum_type_name: str, test_schema: str) -> list:
    """
    Helper function to query database for enum values

    Args:
        enum_type_name: Name of the PostgreSQL enum type
        test_schema: Schema to query

    Returns:
        List of enum values from database
    """
    # Query to get enum values from PostgreSQL
    query = """
        SELECT e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        WHERE t.typname = %s
        ORDER BY e.enumsortorder
    """

    from helpers.database import execute_query
    df = execute_query(query, (enum_type_name,), schema=test_schema)

    return df['enumlabel'].tolist() if not df.empty else []


class TestStatusClassDatabaseAlignment:
    """Test that Python status classes match database enum types"""

    def test_cycle_status_matches_database(self, test_schema):
        """Test CycleStatus values match cycle_status_enum in database"""
        config = STATUS_ENUM_MAP['CycleStatus']

        db_values = get_database_enum_values(config['enum_type'], test_schema)
        python_values = config['class'].all()

        assert set(python_values) == set(db_values), \
            f"CycleStatus.all() should match database enum {config['enum_type']}"

        # Also check individual constants
        for status in python_values:
            assert status in db_values, \
                f"CycleStatus.{status} not found in database enum"

    def test_step_status_matches_database(self, test_schema):
        """Test StepStatus values match step_status_enum in database"""
        config = STATUS_ENUM_MAP['StepStatus']

        db_values = get_database_enum_values(config['enum_type'], test_schema)
        python_values = config['class'].all()

        assert set(python_values) == set(db_values), \
            f"StepStatus.all() should match database enum {config['enum_type']}"

    def test_batch_status_matches_database(self, test_schema):
        """Test BatchStatus values match batch_status_enum in database"""
        config = STATUS_ENUM_MAP['BatchStatus']

        db_values = get_database_enum_values(config['enum_type'], test_schema)
        python_values = config['class'].all()

        assert set(python_values) == set(db_values), \
            f"BatchStatus.all() should match database enum {config['enum_type']}"

    def test_configuration_status_matches_database(self, test_schema):
        """Test ConfigurationStatus values match configuration_status_enum in database"""
        config = STATUS_ENUM_MAP['ConfigurationStatus']

        db_values = get_database_enum_values(config['enum_type'], test_schema)
        python_values = config['class'].all()

        assert set(python_values) == set(db_values), \
            f"ConfigurationStatus.all() should match database enum {config['enum_type']}"

    def test_job_status_matches_database(self, test_schema):
        """Test JobStatus values match job_status_enum in database"""
        config = STATUS_ENUM_MAP['JobStatus']

        db_values = get_database_enum_values(config['enum_type'], test_schema)
        python_values = config['class'].all()

        assert set(python_values) == set(db_values), \
            f"JobStatus.all() should match database enum {config['enum_type']}"


# ==============================================================================
# REGEX PATTERN TESTS - NOTEBOOK_PATTERN
# ==============================================================================

class TestNotebookPattern:
    """Test NOTEBOOK_PATTERN regex for extracting stage/step from notebook filenames"""

    def test_notebook_pattern_valid_filenames(self):
        """Test NOTEBOOK_PATTERN matches valid notebook filenames"""
        # Pattern: r'Step_(\d+)_(.+)\.ipynb'

        valid_cases = [
            ('Step_01_Initialize.ipynb', '01', 'Initialize'),
            ('Step_02_Load_Data.ipynb', '02', 'Load_Data'),
            ('Step_10_Process.ipynb', '10', 'Process'),
            ('Step_001_Start.ipynb', '001', 'Start'),
            ('Step_1_Simple.ipynb', '1', 'Simple'),
            ('Step_99_Final_Step.ipynb', '99', 'Final_Step'),
        ]

        for filename, expected_step_num, expected_name in valid_cases:
            match = re.match(NOTEBOOK_PATTERN, filename)
            assert match is not None, f"Should match valid filename: {filename}"
            assert match.group(1) == expected_step_num, \
                f"Step number should be {expected_step_num} for {filename}"
            assert match.group(2) == expected_name, \
                f"Step name should be {expected_name} for {filename}"

    def test_notebook_pattern_invalid_filenames(self):
        """Test NOTEBOOK_PATTERN rejects invalid filenames"""
        invalid_cases = [
            'step_01_lowercase.ipynb',  # lowercase 'step'
            'Step_01_Name.py',  # wrong extension
            'Step_01_Name.txt',  # wrong extension
            'Step_Name.ipynb',  # missing number
            '01_Name.ipynb',  # missing 'Step_'
            'Step_01.ipynb',  # missing name
            'Step__01_Name.ipynb',  # double underscore
            'Step_01_.ipynb',  # missing name after underscore
        ]

        for filename in invalid_cases:
            match = re.match(NOTEBOOK_PATTERN, filename)
            assert match is None, f"Should NOT match invalid filename: {filename}"

    def test_notebook_pattern_extracts_groups_correctly(self):
        """Test that pattern extracts both capture groups"""
        filename = 'Step_05_Data_Processing.ipynb'
        match = re.match(NOTEBOOK_PATTERN, filename)

        assert match is not None
        assert len(match.groups()) == 2, "Should capture exactly 2 groups"
        assert match.group(1) == '05', "First group should be step number"
        assert match.group(2) == 'Data_Processing', "Second group should be step name"


# ==============================================================================
# REGEX PATTERN TESTS - STAGE_PATTERN
# ==============================================================================

class TestStagePattern:
    """Test STAGE_PATTERN regex for extracting stage information"""

    def test_stage_pattern_valid_names(self):
        """Test STAGE_PATTERN matches valid stage names"""
        # Pattern: r'Stage_(\d+)_(.+)'

        valid_cases = [
            ('Stage_1_Data_Collection', '1', 'Data_Collection'),
            ('Stage_01_Initialize', '01', 'Initialize'),
            ('Stage_10_Processing', '10', 'Processing'),
            ('Stage_001_Start', '001', 'Start'),
            ('Stage_99_Final_Stage', '99', 'Final_Stage'),
        ]

        for stage_name, expected_stage_num, expected_name in valid_cases:
            match = re.match(STAGE_PATTERN, stage_name)
            assert match is not None, f"Should match valid stage name: {stage_name}"
            assert match.group(1) == expected_stage_num, \
                f"Stage number should be {expected_stage_num} for {stage_name}"
            assert match.group(2) == expected_name, \
                f"Stage name should be {expected_name} for {stage_name}"

    def test_stage_pattern_invalid_names(self):
        """Test STAGE_PATTERN rejects invalid stage names"""
        invalid_cases = [
            'stage_01_lowercase',  # lowercase 'stage'
            'Stage_Name',  # missing number
            '01_Stage_Name',  # missing 'Stage_'
            'Stage_01',  # missing name
            'Stage__01_Name',  # double underscore
        ]

        for stage_name in invalid_cases:
            match = re.match(STAGE_PATTERN, stage_name)
            assert match is None, f"Should NOT match invalid stage name: {stage_name}"


# ==============================================================================
# REGEX PATTERN TESTS - CYCLE_NAME_RULES
# ==============================================================================

class TestCycleNameValidation:
    """Test CYCLE_NAME_RULES valid_pattern regex"""

    def test_cycle_name_valid_pattern_matches_correct_format(self):
        """Test valid_pattern matches correct cycle name format"""
        # Pattern: r'^Analysis-20\d{2}-Q[1-4](-[\w-]+)?$'
        pattern = CYCLE_NAME_RULES['valid_pattern']

        valid_names = [
            'Analysis-2025-Q1',
            'Analysis-2025-Q2',
            'Analysis-2025-Q3',
            'Analysis-2025-Q4',
            'Analysis-2024-Q1',
            'Analysis-2099-Q4',
            'Analysis-2025-Q1-v1',
            'Analysis-2025-Q2-final',
            'Analysis-2025-Q3-test-run',
            'Analysis-2025-Q4-2024-11-15',
        ]

        for name in valid_names:
            match = re.match(pattern, name)
            assert match is not None, f"Should match valid cycle name: {name}"

    def test_cycle_name_valid_pattern_rejects_invalid_format(self):
        """Test valid_pattern rejects invalid cycle name format"""
        pattern = CYCLE_NAME_RULES['valid_pattern']

        invalid_names = [
            'Analysis-2025-Q5',  # Invalid quarter (Q5)
            'Analysis-2025-Q0',  # Invalid quarter (Q0)
            'Analysis-19-Q1',    # Wrong year format (not 20XX)
            'Analysis-1999-Q1',  # Wrong century
            'analysis-2025-Q1',  # Lowercase 'analysis'
            'ANALYSIS-2025-Q1',  # All uppercase
            'Analysis-2025-q1',  # Lowercase 'q'
            'Analysis-2025-Quarter1',  # Wrong quarter format
            'Cycle-2025-Q1',     # Wrong prefix
            '2025-Q1',           # Missing prefix
            'Analysis-2025',     # Missing quarter
            'Analysis-2025-Q1-'  # Trailing dash with no suffix
        ]

        for name in invalid_names:
            match = re.match(pattern, name)
            assert match is None, f"Should NOT match invalid cycle name: {name}"

    def test_cycle_name_rules_min_length(self):
        """Test CYCLE_NAME_RULES min_length constraint"""
        min_length = CYCLE_NAME_RULES['min_length']
        assert min_length == 3, "min_length should be 3"

        # Test that rule makes sense
        assert len('Analysis-2025-Q1') >= min_length, \
            "Valid cycle names should meet min_length requirement"

    def test_cycle_name_rules_max_length(self):
        """Test CYCLE_NAME_RULES max_length constraint"""
        max_length = CYCLE_NAME_RULES['max_length']
        assert max_length == 255, "max_length should be 255"

        # Test that normal names fit within limit
        assert len('Analysis-2025-Q1-with-very-long-suffix-name') < max_length, \
            "Normal cycle names should fit within max_length"

    def test_cycle_name_rules_forbidden_prefixes(self):
        """Test CYCLE_NAME_RULES forbidden_prefixes"""
        forbidden = CYCLE_NAME_RULES['forbidden_prefixes']

        assert 'Active_' in forbidden, "Should forbid 'Active_' prefix"

        # Test that forbidden names would be caught
        forbidden_names = [
            'Active_Analysis-2025-Q1',
            'Active_2025-Q1',
        ]

        for name in forbidden_names:
            # Check if name starts with any forbidden prefix
            has_forbidden = any(name.startswith(prefix) for prefix in forbidden)
            assert has_forbidden, f"Should detect forbidden prefix in: {name}"

    def test_cycle_name_rules_example_is_valid(self):
        """Test that CYCLE_NAME_RULES example is actually valid"""
        example = CYCLE_NAME_RULES['example']
        pattern = CYCLE_NAME_RULES['valid_pattern']

        # Example format: 'Analysis-2025-Q4 OR Analysis-2025-Q4-v1'
        # Extract individual examples
        examples = [e.strip() for e in example.split('OR')]

        for ex in examples:
            match = re.match(pattern, ex)
            assert match is not None, \
                f"Example '{ex}' should match the valid_pattern"


# ==============================================================================
# INTEGRATION TESTS
# ==============================================================================

class TestConstantsIntegration:
    """Integration tests for constants working together"""

    def test_step_status_terminal_subset_of_all(self):
        """Test that StepStatus.terminal() is a subset of all()"""
        all_statuses = StepStatus.all()
        terminal_statuses = StepStatus.terminal()

        for status in terminal_statuses:
            assert status in all_statuses, \
                f"Terminal status {status} should be in all()"

    def test_job_status_ready_for_submit_subset_of_all(self):
        """Test that JobStatus.ready_for_submit() is a subset of all()"""
        all_statuses = JobStatus.all()
        ready_statuses = JobStatus.ready_for_submit()

        for status in ready_statuses:
            assert status in all_statuses, \
                f"Ready status {status} should be in all()"

    def test_notebook_and_stage_patterns_consistent(self):
        """Test that notebook and stage patterns use consistent format"""
        # Both should use Stage_XX_Name format
        # Notebook adds Step_XX_Name.ipynb

        # Test that patterns use similar structure
        stage_match = re.match(STAGE_PATTERN, 'Stage_01_Test')
        step_match = re.match(NOTEBOOK_PATTERN, 'Step_01_Test.ipynb')

        assert stage_match is not None
        assert step_match is not None

        # Both extract number and name in same order
        assert stage_match.group(1) == '01'
        assert stage_match.group(2) == 'Test'
        assert step_match.group(1) == '01'
        assert step_match.group(2) == 'Test'
