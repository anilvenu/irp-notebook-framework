# Proposal 002: Prevent Duplicate Batch Creation

**Status:** Proposed
**Date:** 2025-11-25
**Author:** System Analysis
**Priority:** High
**Effort:** ~3-4 hours

---

## Executive Summary

Re-running the Stage 01, Step 03 "Create Batches" notebook creates duplicate batches for the same configuration and batch types. This can lead to duplicate job submissions to Moody's, wasted API quota, and data integrity issues. This proposal recommends implementing both database-level constraints and application-level guards to prevent accidental duplicate batch creation while preserving the ability to intentionally recreate batches when needed.

---

## Problem Statement

### Current Issue

When a user accidentally re-runs the `Step_03_Create_Batches.ipynb` notebook, duplicate batches are created because:

1. **No database uniqueness constraint** on the `irp_batch` table
2. **No application-level duplicate check** in `create_batch()`
3. **Notebook allows re-execution** with `allow_rerun=True`
4. **User confirmation is insufficient** - just a simple yes/no without warning about existing batches

### Real-World Scenario

```
First Run (Intentional):
  - Step_03 identifies EDM_CREATION, PORTFOLIO_CREATION batch types
  - Creates Batch A (EDM_CREATION) with 3 jobs
  - Creates Batch B (PORTFOLIO_CREATION) with 5 jobs

Second Run (Accidental):
  - User re-runs notebook (cell outputs cleared, forgot they ran it)
  - Same batch types identified
  - Creates Batch C (EDM_CREATION) with 3 duplicate jobs  <-- PROBLEM
  - Creates Batch D (PORTFOLIO_CREATION) with 5 duplicate jobs  <-- PROBLEM

Result:
  - 4 batches exist instead of 2
  - If submitted, Moody's receives duplicate requests
  - Confusion about which batch is "correct"
  - Wasted API quota and potential data corruption
```

### Evidence in Code

**`workspace/helpers/batch.py` lines 252-379:**
- `create_batch()` has no duplicate checking logic
- No query for existing batches with same (configuration_id, batch_type)

**`workspace/helpers/db/init_database.sql` lines 91-103:**
```sql
CREATE TABLE irp_batch (
    id SERIAL PRIMARY KEY,
    step_id INTEGER NOT NULL,
    configuration_id INTEGER NOT NULL,
    batch_type VARCHAR(255) NOT NULL,
    status batch_status_enum DEFAULT 'INITIATED',
    -- NO UNIQUE CONSTRAINT on (configuration_id, batch_type)
);
```

**`Step_03_Create_Batches.ipynb` line 54:**
```python
context, step = initialize_notebook_context(
    'Step_03_Create_Batches.ipynb',
    allow_rerun=True  # <-- Allows unlimited re-runs
)
```

### Why This Matters

1. **Data Integrity**: Duplicate batches create confusion about which is authoritative
2. **API Costs**: Submitting duplicate jobs wastes Moody's API quota
3. **Operational Risk**: Users may submit wrong batch, causing data issues
4. **Audit Trail**: Difficult to track which batch was actually used
5. **Recovery Complexity**: Cleaning up duplicates requires manual database intervention

---

## Proposed Solution: Multi-Layer Protection

Implement protection at three levels:

### Layer 1: Database Constraint (Safety Net)

Add a partial unique constraint that prevents duplicate INITIATED/ACTIVE batches:

```sql
-- Prevent duplicate active batches for same configuration + batch_type
CREATE UNIQUE INDEX idx_batch_unique_active
ON irp_batch (configuration_id, batch_type)
WHERE status IN ('INITIATED', 'ACTIVE');
```

**Why Partial Index?**
- Allows historical completed/cancelled batches with same config+type
- Only prevents duplicates for batches that haven't finished
- Supports legitimate re-creation after batch completion/cancellation

### Layer 2: Application Check (User Experience)

Modify `create_batch()` to check for existing batches and provide options:

```python
def create_batch(
    batch_type: str,
    configuration_id: int,
    step_id: Optional[int] = None,
    allow_duplicate: bool = False,  # NEW PARAMETER
    schema: str = 'public'
) -> int:
    """
    Create a new batch for the given configuration.

    Args:
        allow_duplicate: If False (default), raises BatchError if an
                        INITIATED or ACTIVE batch already exists for
                        this configuration_id + batch_type combination.
                        If True, creates batch even if duplicate exists.
    """
    # ... existing validation ...

    # NEW: Check for existing active batches
    if not allow_duplicate:
        existing = get_active_batches_for_config(
            configuration_id,
            batch_type,
            schema=schema
        )
        if existing:
            raise BatchError(
                f"Active batch already exists for configuration {configuration_id} "
                f"and batch_type '{batch_type}': Batch ID {existing[0]['id']} "
                f"(status: {existing[0]['status']}). "
                f"Use allow_duplicate=True to create anyway, or cancel the existing batch."
            )

    # ... rest of creation logic ...
```

### Layer 3: Notebook UX (Prevention)

Update `Step_03_Create_Batches.ipynb` to:

1. **Check for existing batches** before showing creation preview
2. **Display warnings** if batches already exist
3. **Require explicit confirmation** to create duplicates

```python
# Section 3.5: Check for Existing Batches (NEW)
existing_batches = []
for batch_type in batch_types_to_create:
    batches = get_active_batches_for_config(config_id, batch_type)
    if batches:
        existing_batches.extend(batches)

if existing_batches:
    ux.warning(f"Found {len(existing_batches)} existing batch(es) for this configuration:")
    for batch in existing_batches:
        print(f"  - Batch {batch['id']}: {batch['batch_type']} ({batch['status']})")

    proceed_anyway = ux.yes_no(
        "Do you want to create ADDITIONAL batches? (This may create duplicates)"
    )
    if not proceed_anyway:
        print("Batch creation cancelled. Use existing batches or cancel them first.")
        # Skip to step completion
```

---

## Implementation Plan

### Phase 1: Add Helper Function (30 minutes)

**File:** `workspace/helpers/batch.py`

Add function to query existing active batches:

```python
def get_active_batches_for_config(
    configuration_id: int,
    batch_type: Optional[str] = None,
    schema: str = 'public'
) -> List[Dict[str, Any]]:
    """
    Get all INITIATED or ACTIVE batches for a configuration.

    Args:
        configuration_id: The configuration to check
        batch_type: Optional batch type filter
        schema: Database schema

    Returns:
        List of batch records with id, batch_type, status, created_ts
    """
    query = """
        SELECT id, batch_type, status, created_ts
        FROM irp_batch
        WHERE configuration_id = %s
          AND status IN ('INITIATED', 'ACTIVE')
    """
    params = [configuration_id]

    if batch_type:
        query += " AND batch_type = %s"
        params.append(batch_type)

    query += " ORDER BY created_ts DESC"

    return execute_query(query, tuple(params), schema=schema)
```

### Phase 2: Update create_batch() (45 minutes)

**File:** `workspace/helpers/batch.py`

1. Add `allow_duplicate` parameter (default `False`)
2. Add duplicate check before batch creation
3. Raise clear error with existing batch info
4. Update docstring with new behavior

### Phase 3: Add Database Constraint (30 minutes)

**File:** `workspace/helpers/db/init_database.sql`

Add partial unique index:

```sql
-- Prevent duplicate active batches for same configuration + batch_type
-- Uses partial index to only enforce uniqueness for non-completed batches
CREATE UNIQUE INDEX idx_batch_unique_active
ON irp_batch (configuration_id, batch_type)
WHERE status IN ('INITIATED', 'ACTIVE');
```

**Migration Script** (for existing databases):

```sql
-- Migration: Add unique constraint for active batches
-- Run after resolving any existing duplicates

-- First, check for existing duplicates
SELECT configuration_id, batch_type, COUNT(*) as count
FROM irp_batch
WHERE status IN ('INITIATED', 'ACTIVE')
GROUP BY configuration_id, batch_type
HAVING COUNT(*) > 1;

-- If duplicates exist, manually resolve before adding constraint
-- Then add the index:
CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_unique_active
ON irp_batch (configuration_id, batch_type)
WHERE status IN ('INITIATED', 'ACTIVE');
```

### Phase 4: Update Step_03 Notebook (1 hour)

**File:** `workspace/workflows/_Template/notebooks/Stage_01_Setup/Step_03_Create_Batches.ipynb`

1. Add new section after "Identify Batch Types" to check existing batches
2. Display warning banner if existing batches found
3. Show existing batch details (ID, status, job count, created time)
4. Add explicit "Create Additional Batches" confirmation
5. Pass `allow_duplicate=True` only if user explicitly confirms

### Phase 5: Add Batch Cancellation Helper (45 minutes)

**File:** `workspace/helpers/batch.py`

Add function to cancel a batch (useful for cleaning up duplicates):

```python
def cancel_batch(
    batch_id: int,
    reason: str = "Cancelled by user",
    schema: str = 'public'
) -> bool:
    """
    Cancel a batch and all its non-completed jobs.

    Only INITIATED or ACTIVE batches can be cancelled.

    Args:
        batch_id: The batch to cancel
        reason: Cancellation reason for audit trail
        schema: Database schema

    Returns:
        True if cancelled, False if already completed/cancelled

    Raises:
        BatchError: If batch not found
    """
    with transaction_context(schema=schema):
        # Get batch status
        batch = read_batch(batch_id, schema=schema)
        if not batch:
            raise BatchError(f"Batch {batch_id} not found")

        if batch['status'] not in [BatchStatus.INITIATED, BatchStatus.ACTIVE]:
            return False  # Already completed or cancelled

        # Cancel all non-completed jobs
        execute_command("""
            UPDATE irp_job
            SET status = 'CANCELLED',
                updated_ts = NOW()
            WHERE batch_id = %s
              AND status NOT IN ('FINISHED', 'FAILED', 'CANCELLED')
        """, (batch_id,), schema=schema)

        # Cancel batch
        execute_command("""
            UPDATE irp_batch
            SET status = 'CANCELLED',
                completed_ts = NOW(),
                updated_ts = NOW()
            WHERE id = %s
        """, (batch_id,), schema=schema)

        return True
```

### Phase 6: Update Tests (45 minutes)

**File:** `workspace/tests/test_batch.py`

Add tests for new functionality:

```python
def test_create_batch_duplicate_prevention(test_schema):
    """Test that duplicate batch creation is prevented by default."""
    # Setup
    cycle_id = register_cycle('test_cycle')
    config_id = create_test_configuration(cycle_id)
    step_id = create_test_step(cycle_id)

    # Create first batch - should succeed
    batch_id_1 = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id
    )
    assert batch_id_1 is not None

    # Try to create duplicate - should fail
    with pytest.raises(BatchError) as exc_info:
        create_batch(
            batch_type='test_default',
            configuration_id=config_id,
            step_id=step_id
        )
    assert "Active batch already exists" in str(exc_info.value)
    assert str(batch_id_1) in str(exc_info.value)

    # Cleanup
    archive_cycle(cycle_id)


def test_create_batch_allow_duplicate(test_schema):
    """Test that duplicates can be created with explicit flag."""
    # Setup
    cycle_id = register_cycle('test_cycle')
    config_id = create_test_configuration(cycle_id)
    step_id = create_test_step(cycle_id)

    # Create first batch
    batch_id_1 = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id
    )

    # Create duplicate with flag - should succeed
    batch_id_2 = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id,
        allow_duplicate=True
    )
    assert batch_id_2 is not None
    assert batch_id_2 != batch_id_1

    # Cleanup
    archive_cycle(cycle_id)


def test_create_batch_after_cancellation(test_schema):
    """Test that new batch can be created after cancelling existing one."""
    # Setup
    cycle_id = register_cycle('test_cycle')
    config_id = create_test_configuration(cycle_id)
    step_id = create_test_step(cycle_id)

    # Create and cancel first batch
    batch_id_1 = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id
    )
    cancel_batch(batch_id_1)

    # Create new batch - should succeed (no active batch exists)
    batch_id_2 = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id
    )
    assert batch_id_2 is not None

    # Cleanup
    archive_cycle(cycle_id)


def test_get_active_batches_for_config(test_schema):
    """Test querying active batches for a configuration."""
    # Setup
    cycle_id = register_cycle('test_cycle')
    config_id = create_test_configuration(cycle_id)
    step_id = create_test_step(cycle_id)

    # Initially no batches
    batches = get_active_batches_for_config(config_id)
    assert len(batches) == 0

    # Create batch
    batch_id = create_batch(
        batch_type='test_default',
        configuration_id=config_id,
        step_id=step_id
    )

    # Should find the batch
    batches = get_active_batches_for_config(config_id)
    assert len(batches) == 1
    assert batches[0]['id'] == batch_id

    # Filter by batch_type
    batches = get_active_batches_for_config(config_id, batch_type='test_default')
    assert len(batches) == 1

    batches = get_active_batches_for_config(config_id, batch_type='other_type')
    assert len(batches) == 0

    # Cleanup
    archive_cycle(cycle_id)
```

### Phase 7: Documentation (30 minutes)

1. Update `CLAUDE.md` with new `allow_duplicate` parameter
2. Update `docs/BATCH_JOB_SYSTEM.md` with duplicate prevention behavior
3. Add inline comments in Step_03 notebook explaining the guards

---

## Benefits

### Immediate Benefits

1. **Prevents Accidental Duplicates**: Default behavior blocks duplicate creation
2. **Clear User Feedback**: Error messages explain what exists and how to proceed
3. **Preserves Flexibility**: `allow_duplicate=True` enables intentional recreation
4. **Database Safety Net**: Constraint catches any edge cases missed by application

### Operational Benefits

1. **Reduced Confusion**: Users always know which batch is authoritative
2. **API Efficiency**: No wasted Moody's API calls for duplicate jobs
3. **Cleaner Audit Trail**: Each configuration has one active batch per type
4. **Easier Recovery**: `cancel_batch()` provides clean way to handle mistakes

### Long-Term Benefits

1. **Data Integrity**: Constraint ensures consistency even with direct DB access
2. **Code Quality**: Explicit duplicate handling improves maintainability
3. **User Trust**: System prevents common mistakes, building confidence

---

## Risks and Mitigation

### Risk 1: Breaking Existing Workflows

**Concern:** Users may have workflows that depend on creating multiple batches.

**Mitigation:**
- `allow_duplicate=True` preserves existing capability
- Default behavior is opt-out, not removed functionality
- Clear error messages guide users to the flag

### Risk 2: Database Migration Issues

**Concern:** Adding unique constraint to existing database with duplicates.

**Mitigation:**
- Migration script checks for duplicates first
- Duplicates must be resolved before constraint is added
- Provide `cancel_batch()` function for cleanup

### Risk 3: Test Failures

**Concern:** Existing tests may create duplicate batches.

**Mitigation:**
- Tests that need multiple batches will use `allow_duplicate=True`
- Most tests create single batch per configuration (no change needed)
- Estimate: ~5 tests may need minor updates

### Risk 4: Edge Case: Multiple Steps Creating Same Batch Type

**Concern:** Different steps might legitimately create same batch type.

**Mitigation:**
- Constraint is on (configuration_id, batch_type), not step_id
- Same configuration should not have duplicate batch types regardless of step
- If this is needed, use `allow_duplicate=True`

---

## Alternatives Considered

### Alternative 1: Step-Level Guard Only

Only add check in Step_03 notebook, no application/database changes.

**Pros:**
- Minimal code changes
- Quick to implement

**Cons:**
- Doesn't protect against direct `create_batch()` calls
- No safety net for programmatic usage
- Duplicates can still be created via other paths

**Decision:** Rejected - doesn't provide complete protection

### Alternative 2: Strict Database Constraint (No Partial Index)

Use full unique constraint on (configuration_id, batch_type).

**Pros:**
- Simpler constraint
- Maximum protection

**Cons:**
- Cannot recreate batch after completion
- Breaks legitimate "retry entire batch" workflows
- Historical data would prevent new batches

**Decision:** Rejected - too restrictive for operational needs

### Alternative 3: Automatic Cancellation of Old Batches

Automatically cancel existing batch when creating new one.

**Pros:**
- No duplicate ever exists
- Simple user experience

**Cons:**
- Data loss if old batch had partial progress
- No user awareness/consent
- Could cancel batch that was intentionally ACTIVE

**Decision:** Rejected - too aggressive, user should decide

### Alternative 4: Soft Warning Only (No Blocking)

Just show warning but allow creation to proceed.

**Pros:**
- Non-disruptive
- User always has control

**Cons:**
- Doesn't prevent the actual problem
- Users may dismiss warnings
- Still creates duplicates

**Decision:** Rejected - doesn't solve the core problem

---

## Success Criteria

This proposal is successful when:

1. **Default behavior blocks duplicates**: Running Step_03 twice shows existing batches and requires explicit confirmation
2. **Clear error messages**: `create_batch()` error includes existing batch ID and status
3. **Opt-in for duplicates**: `allow_duplicate=True` creates batch without error
4. **Database constraint active**: Direct SQL attempts to create duplicates fail
5. **Cancellation works**: Users can cancel batches to free up configuration+type combination
6. **All tests pass**: Existing functionality preserved, new tests added
7. **Documentation updated**: CLAUDE.md and BATCH_JOB_SYSTEM.md reflect new behavior

---

## Files Modified Summary

### New Functions

| File | Function | Purpose |
|------|----------|---------|
| `batch.py` | `get_active_batches_for_config()` | Query existing active batches |
| `batch.py` | `cancel_batch()` | Cancel batch and its jobs |

### Modified Functions

| File | Function | Change |
|------|----------|--------|
| `batch.py` | `create_batch()` | Add `allow_duplicate` parameter and check |

### Database Changes

| File | Change |
|------|--------|
| `init_database.sql` | Add partial unique index on `irp_batch` |

### Notebook Changes

| File | Change |
|------|--------|
| `Step_03_Create_Batches.ipynb` | Add existing batch check and warning |

### Test Changes

| File | Change |
|------|--------|
| `test_batch.py` | Add tests for duplicate prevention |

### Documentation Changes

| File | Change |
|------|--------|
| `CLAUDE.md` | Document `allow_duplicate` parameter |
| `BATCH_JOB_SYSTEM.md` | Add duplicate prevention section |

---

## Approval and Next Steps

**Recommendation:** Approve and implement

**Priority Justification:**
- High priority - prevents data integrity issues and API waste
- Medium risk - well-defined changes with clear rollback path
- High value - addresses real operational problem

**Next Steps After Approval:**
1. Create feature branch: `feature/prevent-duplicate-batches`
2. Implement phases 1-7
3. Run full test suite
4. Create PR with examples of error messages
5. Update proposal status to "Implemented" when merged

---

**Questions or Concerns?**

Please add comments or questions to this proposal document.
