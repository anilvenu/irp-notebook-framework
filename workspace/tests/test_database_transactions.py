"""
Test suite for transaction management (pytest version)

This test file validates the transaction_context() functionality including:
- Atomic multi-operation transactions
- Rollback on error
- Nested transaction prevention
- Schema support in transactions
- Integration with batch and job operations

All tests run in the 'test_database_transactions' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_database_transactions.py
    pytest workspace/tests/test_database_transactions.py -v
    pytest workspace/tests/test_database_transactions.py --preserve-schema
"""

import pytest
from datetime import datetime

from helpers.database import (
    transaction_context,
    execute_query,
    execute_insert,
    execute_command,
    execute_scalar,
    DatabaseError,
)


# ============================================================================
# Basic Transaction Tests
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_transaction_basic_commit(test_schema):
    """Test that operations within transaction are committed together"""

    with transaction_context(schema=test_schema):
        # Create two cycles in transaction
        cycle_id_1 = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('txn_cycle_1', 'ACTIVE'),
            schema=test_schema
        )

        cycle_id_2 = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('txn_cycle_2', 'ACTIVE'),
            schema=test_schema
        )

        # Both should have IDs
        assert cycle_id_1 is not None
        assert cycle_id_2 is not None

    # After transaction, both should be committed
    df = execute_query(
        "SELECT * FROM irp_cycle WHERE cycle_name LIKE 'txn_cycle_%' ORDER BY id",
        schema=test_schema
    )

    assert len(df) == 2
    assert df.iloc[0]['cycle_name'] == 'txn_cycle_1'
    assert df.iloc[1]['cycle_name'] == 'txn_cycle_2'


@pytest.mark.database
@pytest.mark.unit
def test_transaction_rollback_on_error(test_schema):
    """Test that transaction rolls back all operations on error"""

    # Count cycles before transaction
    before_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_cycle WHERE cycle_name LIKE 'rollback_test_%'",
        schema=test_schema
    )

    # Attempt transaction that will fail
    with pytest.raises(DatabaseError):
        with transaction_context(schema=test_schema):
            # First insert succeeds
            execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('rollback_test_1', 'ACTIVE'),
                schema=test_schema
            )

            # Second insert succeeds
            execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('rollback_test_2', 'ACTIVE'),
                schema=test_schema
            )

            # Duplicate key - will fail and rollback everything
            execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('rollback_test_1', 'ACTIVE'),  # Duplicate!
                schema=test_schema
            )

    # After rollback, no records should be inserted
    after_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_cycle WHERE cycle_name LIKE 'rollback_test_%'",
        schema=test_schema
    )

    assert after_count == before_count, \
        f"Transaction not rolled back - {after_count - before_count} records inserted"


@pytest.mark.database
@pytest.mark.unit
def test_transaction_with_query_operations(test_schema):
    """Test that SELECT queries work within transactions"""

    # Create initial data
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('query_test_cycle', 'ACTIVE'),
        schema=test_schema
    )

    with transaction_context(schema=test_schema):
        # Query within transaction
        df = execute_query(
            "SELECT * FROM irp_cycle WHERE id = %s",
            (cycle_id,),
            schema=test_schema
        )

        assert len(df) == 1
        assert df.iloc[0]['cycle_name'] == 'query_test_cycle'

        # Update within transaction
        execute_command(
            "UPDATE irp_cycle SET status = %s WHERE id = %s",
            ('ARCHIVED', cycle_id),
            schema=test_schema
        )

        # Query again within transaction to see uncommitted change
        df2 = execute_query(
            "SELECT status FROM irp_cycle WHERE id = %s",
            (cycle_id,),
            schema=test_schema
        )

        assert df2.iloc[0]['status'] == 'ARCHIVED'

    # After transaction, change should be committed
    final_status = execute_scalar(
        "SELECT status FROM irp_cycle WHERE id = %s",
        (cycle_id,),
        schema=test_schema
    )

    assert final_status == 'ARCHIVED'


@pytest.mark.database
@pytest.mark.unit
def test_transaction_with_scalar_operations(test_schema):
    """Test that scalar queries work within transactions"""

    with transaction_context(schema=test_schema):
        # Create cycle
        execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('scalar_test', 'ACTIVE'),
            schema=test_schema
        )

        # Use scalar query within transaction
        count = execute_scalar(
            "SELECT COUNT(*) FROM irp_cycle WHERE cycle_name = %s",
            ('scalar_test',),
            schema=test_schema
        )

        assert count == 1


@pytest.mark.database
@pytest.mark.unit
def test_nested_transaction_raises_error(test_schema):
    """Test that nested transactions raise an error"""

    with pytest.raises(DatabaseError) as exc_info:
        with transaction_context(schema=test_schema):
            # First transaction active

            # Try to start nested transaction - should fail
            with transaction_context(schema=test_schema):
                pass

    assert "nested transactions" in str(exc_info.value).lower()


# ============================================================================
# Integration Tests with Batch and Job Operations
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_transaction_atomic_batch_creation(test_schema):
    """Test that batch creation with multiple jobs is atomic"""
    from helpers.batch import create_batch, read_batch, get_batch_jobs
    from helpers.constants import ConfigurationStatus
    import json

    # Setup: Create cycle hierarchy
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('batch_txn_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'batch_txn_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'batch_txn_step'),
        schema=test_schema
    )

    # Create configuration directly (since create_configuration doesn't exist)
    config_data = {
        'batch_type': 'test_default',
        'items': ['A', 'B', 'C']
    }

    config_id = execute_insert(
        "INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts) VALUES (%s, %s, %s, %s, %s)",
        (cycle_id, 'test_batch_txn.json', json.dumps(config_data), ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    # Create batch (should be atomic)
    batch_id = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id,
        schema=test_schema
    )

    # Verify batch exists
    batch = read_batch(batch_id, schema=test_schema)
    assert batch['id'] == batch_id
    assert batch['batch_type'] == 'test_default'

    # Verify job was created (test_default transformer returns 1 job)
    jobs = get_batch_jobs(batch_id, schema=test_schema)
    assert len(jobs) == 1


@pytest.mark.database
@pytest.mark.integration
def test_transaction_rollback_batch_creation_on_error(test_schema):
    """Test that batch creation rolls back if job creation fails"""
    from helpers.batch import BatchError, create_batch
    from helpers.constants import ConfigurationStatus
    import json

    # Setup: Create cycle hierarchy
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('batch_rollback_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'batch_rollback_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'batch_rollback_step'),
        schema=test_schema
    )

    # Create configuration with invalid batch type
    config_data = {
        'batch_type': 'invalid_batch_type',
        'items': ['A', 'B']
    }

    config_id = execute_insert(
        "INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts) VALUES (%s, %s, %s, %s, %s)",
        (cycle_id, 'test_batch_rollback.json', json.dumps(config_data), ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    # Count batches before attempt
    before_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_batch WHERE step_id = %s",
        (step_id,),
        schema=test_schema
    )

    # Attempt to create batch with invalid type - should fail
    with pytest.raises(BatchError):
        create_batch(
            batch_type='invalid_type_that_does_not_exist',
            configuration_id=config_id,
            step_id=step_id,
            schema=test_schema
        )

    # After rollback, no batch should be created
    after_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_batch WHERE step_id = %s",
        (step_id,),
        schema=test_schema
    )

    assert after_count == before_count, \
        "Batch creation was not rolled back on error"


@pytest.mark.database
@pytest.mark.integration
def test_transaction_atomic_job_resubmission(test_schema, mock_irp_client):
    """Test that job resubmission (create new job + skip old job) is atomic"""
    from helpers.job import create_job_with_config, resubmit_job, read_job
    from helpers.constants import ConfigurationStatus
    import json

    # Setup: Create batch infrastructure
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('job_resubmit_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'job_resubmit_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'job_resubmit_step'),
        schema=test_schema
    )

    # Create configuration
    config_data = {'test': 'data'}
    config_id = execute_insert(
        "INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts) VALUES (%s, %s, %s, %s, %s)",
        (cycle_id, 'test_resubmit.json', json.dumps(config_data), ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    # Create batch
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'EDM Creation', 'INITIATED'),
        schema=test_schema
    )

    # Create original job
    job_config_data = {'original': 'config'}
    original_job_id = create_job_with_config(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data=job_config_data,
        schema=test_schema
    )

    # Resubmit with override
    override_config = {'Database': 'OverriddenDB', 'overridden': 'config', 'reason': 'testing'}
    new_job_id = resubmit_job(
        job_id=original_job_id,
        irp_client=mock_irp_client,
        batch_type='EDM Creation',
        job_configuration_data=override_config,
        override_reason='Testing transaction atomicity',
        schema=test_schema
    )

    # Verify original job is skipped
    original_job = read_job(original_job_id, schema=test_schema)
    assert original_job['skipped'] is True

    # Verify new job exists and is not skipped
    new_job = read_job(new_job_id, schema=test_schema)
    assert new_job['skipped'] is False
    assert new_job['parent_job_id'] == original_job_id


@pytest.mark.database
@pytest.mark.integration
def test_transaction_create_job_with_new_configuration(test_schema):
    """Test that creating job with new configuration is atomic"""
    from helpers.job import create_job_with_config
    from helpers.constants import ConfigurationStatus
    import json

    # Setup
    cycle_id = execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
        ('job_config_cycle', 'ACTIVE'),
        schema=test_schema
    )

    stage_id = execute_insert(
        "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
        (cycle_id, 1, 'job_config_stage'),
        schema=test_schema
    )

    step_id = execute_insert(
        "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
        (stage_id, 1, 'job_config_step'),
        schema=test_schema
    )

    # Create configuration
    config_data = {'master': 'config'}
    config_id = execute_insert(
        "INSERT INTO irp_configuration (cycle_id, configuration_file_name, configuration_data, status, file_last_updated_ts) VALUES (%s, %s, %s, %s, %s)",
        (cycle_id, 'test_job_config.json', json.dumps(config_data), ConfigurationStatus.VALID, datetime.now()),
        schema=test_schema
    )

    # Create batch
    batch_id = execute_insert(
        "INSERT INTO irp_batch (step_id, configuration_id, batch_type, status) VALUES (%s, %s, %s, %s)",
        (step_id, config_id, 'test_type', 'INITIATED'),
        schema=test_schema
    )

    # Count job_configurations before
    before_config_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_job_configuration WHERE batch_id = %s",
        (batch_id,),
        schema=test_schema
    )

    before_job_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_job WHERE batch_id = %s",
        (batch_id,),
        schema=test_schema
    )

    # Create job with new configuration (should be atomic)
    job_config_data = {'job_specific': 'config'}
    job_id = create_job_with_config(
        batch_id=batch_id,
        configuration_id=config_id,
        job_configuration_data=job_config_data,
        schema=test_schema
    )

    # After creation, both job_configuration and job should exist
    after_config_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_job_configuration WHERE batch_id = %s",
        (batch_id,),
        schema=test_schema
    )

    after_job_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_job WHERE batch_id = %s",
        (batch_id,),
        schema=test_schema
    )

    assert after_config_count == before_config_count + 1, \
        "Job configuration should be created"
    assert after_job_count == before_job_count + 1, \
        "Job should be created"


# ============================================================================
# Schema Support in Transactions
# ============================================================================

@pytest.mark.database
@pytest.mark.unit
def test_transaction_respects_schema_parameter(test_schema):
    """Test that transaction_context respects schema parameter"""

    # Create data in transaction with explicit schema
    with transaction_context(schema=test_schema):
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('schema_param_test', 'ACTIVE'),
            schema=test_schema
        )

    # Verify data exists in correct schema
    df = execute_query(
        "SELECT * FROM irp_cycle WHERE cycle_name = %s",
        ('schema_param_test',),
        schema=test_schema
    )

    assert len(df) == 1


@pytest.mark.database
@pytest.mark.unit
def test_transaction_without_schema_uses_context(test_schema):
    """Test that transaction_context without schema parameter uses current schema context"""
    from helpers.database import set_schema, get_current_schema

    # Save original schema
    original_schema = get_current_schema()

    try:
        # Set schema context
        set_schema(test_schema)

        # Create data in transaction without explicit schema
        with transaction_context():
            cycle_id = execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('context_schema_test', 'ACTIVE')
            )

        # Verify data exists in test schema
        df = execute_query(
            "SELECT * FROM irp_cycle WHERE cycle_name = %s",
            ('context_schema_test',),
            schema=test_schema
        )

        assert len(df) == 1

    finally:
        # Restore original schema
        set_schema(original_schema)


# ============================================================================
# Multi-Table Transaction Tests
# ============================================================================

@pytest.mark.database
@pytest.mark.integration
def test_transaction_multi_table_atomic(test_schema):
    """Test that multi-table operations in transaction are atomic"""

    with transaction_context(schema=test_schema):
        # Create cycle
        cycle_id = execute_insert(
            "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
            ('multi_table_cycle', 'ACTIVE'),
            schema=test_schema
        )

        # Create stage
        stage_id = execute_insert(
            "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
            (cycle_id, 1, 'multi_table_stage'),
            schema=test_schema
        )

        # Create step
        step_id = execute_insert(
            "INSERT INTO irp_step (stage_id, step_num, step_name) VALUES (%s, %s, %s)",
            (stage_id, 1, 'multi_table_step'),
            schema=test_schema
        )

    # After transaction, all should be committed
    cycle = execute_query(
        "SELECT * FROM irp_cycle WHERE id = %s",
        (cycle_id,),
        schema=test_schema
    )
    assert len(cycle) == 1

    stage = execute_query(
        "SELECT * FROM irp_stage WHERE id = %s",
        (stage_id,),
        schema=test_schema
    )
    assert len(stage) == 1

    step = execute_query(
        "SELECT * FROM irp_step WHERE id = %s",
        (step_id,),
        schema=test_schema
    )
    assert len(step) == 1


@pytest.mark.database
@pytest.mark.integration
def test_transaction_multi_table_rollback(test_schema):
    """Test that multi-table operations rollback together on error"""

    # Count records before
    before_cycle_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_cycle WHERE cycle_name = 'multi_rollback_cycle'",
        schema=test_schema
    )

    before_stage_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_stage",
        schema=test_schema
    )

    # Attempt multi-table transaction that will fail
    with pytest.raises(DatabaseError):
        with transaction_context(schema=test_schema):
            # Create cycle
            cycle_id = execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('multi_rollback_cycle', 'ACTIVE'),
                schema=test_schema
            )

            # Create stage
            stage_id = execute_insert(
                "INSERT INTO irp_stage (cycle_id, stage_num, stage_name) VALUES (%s, %s, %s)",
                (cycle_id, 1, 'multi_rollback_stage'),
                schema=test_schema
            )

            # Create duplicate cycle - will fail
            execute_insert(
                "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)",
                ('multi_rollback_cycle', 'ACTIVE'),  # Duplicate!
                schema=test_schema
            )

    # After rollback, no records should be inserted in any table
    after_cycle_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_cycle WHERE cycle_name = 'multi_rollback_cycle'",
        schema=test_schema
    )

    after_stage_count = execute_scalar(
        "SELECT COUNT(*) FROM irp_stage",
        schema=test_schema
    )

    assert after_cycle_count == before_cycle_count, \
        "Cycle creation was not rolled back"
    assert after_stage_count == before_stage_count, \
        "Stage creation was not rolled back"
