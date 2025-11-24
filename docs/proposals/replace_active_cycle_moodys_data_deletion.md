# Proposal: Enhanced Replace Active Cycle with Moody's Data Deletion

## Problem

The current "Replace Active Cycle" notebook ([workflows/_Tools/Cycle Management/Replace Active Cycle.ipynb](../../workflows/_Tools/Cycle%20Management/Replace%20Active%20Cycle.ipynb)) deletes the active cycle from the database and recreates it from the template. However, it does **not account for any Moody's data** that may have been created as part of the cycle's workflow.

When a cycle has submitted batches and jobs to Moody's API (e.g., EDM creation, MRI imports, portfolio analysis), those external resources remain in Moody's systems even after the cycle is replaced locally. This creates:

1. **Orphaned Moody's Resources**: Jobs, portfolios, analyses, and other data objects remain in Moody's without corresponding local tracking
2. **Data Inconsistency**: Re-running the same cycle after replacement may conflict with existing Moody's data
3. **Cost Implications**: Moody's resources may consume storage/compute resources unnecessarily
4. **Audit Trail Loss**: No record that Moody's data should be cleaned up

## Current Behavior

The Replace Active Cycle notebook currently:

1. Deletes the active cycle directory (`Active_{cycle_name}/`)
2. Deletes the cycle from database (CASCADE deletes all related records including batches, jobs, configurations)
3. Creates fresh cycle from template with same name
4. Registers stages and steps

**What gets deleted**: Local filesystem + local database records (stages, steps, batches, jobs, configurations)

**What does NOT get deleted**: External Moody's data (portfolios, analyses, EDM objects, import jobs)

## Proposed Solution

Enhance the Replace Active Cycle notebook to give users the **option to delete Moody's data** before replacing the cycle.

### User Interface Flow

Add a new confirmation step before deletion:

```
⚠️ MOODY'S DATA CLEANUP

This cycle has submitted work to Moody's API. Do you want to delete the Moody's data?

Configuration found: config_123
  - 5 batches submitted
  - 23 jobs completed
  - Portfolios created: USEQ_Q4_2025, USHU_Q4_2025, CBHU_Q4_2025
  - Analyses executed: 8

Options:
  [1] Delete Moody's data AND replace cycle (recommended for full reset)
  [2] Keep Moody's data, only replace local cycle (use with caution)
  [3] Cancel operation

Your choice: _
```

### Implementation Approach

#### Step 1: Detect Moody's Data

Before deletion, query the cycle's configuration and batches to determine what Moody's data exists:

```python
from helpers.configuration import get_cycle_configuration
from helpers.batch import get_cycle_batches
from helpers.irp_integration.moodys_api import list_cycle_resources

# Get cycle configuration
config = get_cycle_configuration(active_cycle['id'])

if config:
    # Get all batches for this cycle
    batches = get_cycle_batches(active_cycle['id'])

    # Query Moody's API to list resources created by this cycle
    moodys_resources = list_cycle_resources(
        configuration_id=config['id'],
        include_portfolios=True,
        include_analyses=True,
        include_edm_objects=True
    )

    # Display summary to user
    display_moodys_summary(batches, moodys_resources)
```

#### Step 2: Conditional Deletion

If user chooses to delete Moody's data:

```python
from helpers.irp_integration.moodys_cleanup import delete_cycle_resources

if user_choice == "DELETE_MOODYS":
    ux.header("Deleting Moody's Data")

    try:
        # Use configuration metadata to identify and delete Moody's resources
        deletion_results = delete_cycle_resources(
            configuration_id=config['id'],
            cycle_name=active_cycle['cycle_name'],
            dry_run=False
        )

        ux.success(f"Deleted {deletion_results['portfolios_deleted']} portfolios")
        ux.success(f"Deleted {deletion_results['analyses_deleted']} analyses")
        ux.success(f"Deleted {deletion_results['edm_objects_deleted']} EDM objects")

    except MoodysAPIError as e:
        ux.error(f"Failed to delete Moody's data: {str(e)}")
        if not ux.yes_no("Continue with local cycle replacement anyway?"):
            raise Exception("User cancelled due to Moody's deletion failure")
```

#### Step 3: Proceed with Current Replacement Logic

After Moody's cleanup (if requested), proceed with existing replacement steps:
1. Delete cycle directory
2. Delete cycle from database
3. Create fresh cycle from template

### Configuration Metadata Requirements

The solution relies on the cycle's configuration containing metadata about created Moody's resources. The configuration should track:

```python
{
    "cycle_id": 123,
    "configuration_data": {
        "moodys_tracking": {
            "portfolios": [
                {"name": "USEQ_Q4_2025", "id": "port_abc123"},
                {"name": "USHU_Q4_2025", "id": "port_def456"}
            ],
            "edm_versions": ["EDM_2025_Q4_v1", "EDM_2025_Q4_v2"],
            "analysis_groups": ["Analysis_Group_Q4_2025"],
            "workspace_id": "workspace_789"
        }
    }
}
```

This tracking should be automatically updated by batch/job helpers when Moody's resources are created.

### Safety Considerations

1. **Dry Run Mode**: Always perform a dry-run first to show what would be deleted
2. **Confirmation Required**: Multiple confirmations before destructive operations
3. **Partial Failure Handling**: If some Moody's resources fail to delete, log errors but allow user to proceed
4. **Audit Logging**: Log all deletion operations for compliance/debugging
5. **Cascading Dependencies**: Warn about dependencies (e.g., can't delete portfolio if analyses depend on it)

## Implementation Components

### New Helper Modules

1. **`helpers/irp_integration/moodys_cleanup.py`**
   - `list_cycle_resources(configuration_id)` - Query Moody's for resources associated with cycle
   - `delete_cycle_resources(configuration_id, dry_run=True)` - Delete all Moody's data for a cycle
   - `delete_portfolio(portfolio_id)` - Delete specific portfolio
   - `delete_analysis(analysis_id)` - Delete specific analysis
   - `validate_deletion_safety(resource_ids)` - Check for dependencies before deletion

2. **Enhanced `helpers/configuration.py`**
   - Add methods to track Moody's resource IDs in configuration metadata
   - `track_moodys_resource(config_id, resource_type, resource_id)`
   - `get_moodys_resources(config_id)` - Retrieve tracked resources

3. **Enhanced `helpers/batch.py` and `helpers/job.py`**
   - Automatically track created Moody's resources in configuration when jobs succeed
   - Hook into job completion to update configuration metadata

### Modified Notebook

Update `workflows/_Tools/Cycle Management/Replace Active Cycle.ipynb`:

1. Add new cell after "Check Current Active Cycle" to detect Moody's data
2. Add new cell for Moody's deletion confirmation
3. Add new cell to execute Moody's cleanup (if requested)
4. Update warnings to mention Moody's data implications

## Testing Requirements

### Unit Tests

- Test `list_cycle_resources()` with various configuration states
- Test `delete_cycle_resources()` in dry-run mode
- Test configuration metadata tracking

### Integration Tests

- Create cycle with Moody's data, verify deletion removes all resources
- Test partial deletion failures (some resources fail to delete)
- Test replacement with "keep Moody's data" option
- Verify cascading deletion handles dependencies correctly

### End-to-End Test

1. Create cycle
2. Submit batches creating Moody's portfolios and analyses
3. Run Replace Active Cycle with Moody's deletion enabled
4. Verify all Moody's resources are deleted
5. Verify local cycle is replaced correctly

## Migration Path

### Phase 1: Foundation (Current Proposal)
- Implement Moody's cleanup helpers
- Add configuration metadata tracking
- Update Replace Active Cycle notebook

### Phase 2: Enhanced Tracking (Future)
- Add real-time Moody's resource dashboard
- Implement orphaned resource detection (resources without local tracking)
- Add bulk cleanup tools for multiple cycles

### Phase 3: Automation (Future)
- Automatic cleanup of Moody's data when archiving cycles
- Scheduled cleanup jobs for orphaned resources
- Cost tracking and reporting for Moody's resource usage

## Risks and Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Moody's API failure during deletion | Orphaned resources remain | Implement retry logic, manual cleanup procedures |
| Accidental deletion of active data | Data loss | Multiple confirmations, dry-run preview, audit logging |
| Incomplete resource tracking | Some resources not deleted | Implement orphaned resource detection tools |
| Moody's API rate limiting | Slow deletion process | Batch deletions, implement exponential backoff |

## Dependencies

- **Moody's API Access**: Requires delete permissions for portfolios, analyses, EDM objects
- **Configuration Tracking**: Requires configuration metadata schema to store Moody's resource IDs
- **API Client Library**: May need to enhance `helpers/irp_integration/moodys_api.py` with deletion endpoints

## Success Criteria

1. Users can successfully delete Moody's data when replacing a cycle
2. No orphaned Moody's resources after replacement (when deletion is selected)
3. Clear user feedback showing what will be deleted
4. Safe fallback if Moody's deletion fails (user can choose to proceed or abort)
5. All Moody's deletion operations are logged for audit purposes

---

**Priority**: Medium-High - Prevents resource accumulation and data inconsistency

**Effort**: Medium - Requires new Moody's API integration code and configuration tracking

**Risk**: Medium - Destructive operations require careful testing and safety mechanisms

**Dependencies**: Moody's API documentation, delete endpoint access, configuration metadata schema
