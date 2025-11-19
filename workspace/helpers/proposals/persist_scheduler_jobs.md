# Proposal: Persist JupyterLab Scheduler Jobs Across Docker Restarts

## Problem

Scheduled notebook jobs created using the `jupyterlab-scheduler` extension are **not persisted** after shutting down and restarting our Docker container. This causes all scheduled jobs to be lost, requiring manual recreation after every container restart.

## Impact

- **Loss of Automation**: Analysts must manually recreate all scheduled jobs after each container restart
- **Risk of Missed Executions**: Critical scheduled tasks (monitoring, reconciliation, reporting) may be forgotten
- **Poor Developer Experience**: Restarts during development or deployment disrupt workflow continuity
- **Production Risk**: Container restarts in production would lose all scheduling configurations

## Root Cause

The `jupyterlab-scheduler` extension stores job definitions in JupyterLab's configuration database, which is typically located in the container's filesystem at:

```
~/.jupyter/lab/workspaces/
~/.local/share/jupyter/
```

These directories are **not mounted as Docker volumes** in our current configuration, so any data written to them is lost when the container is destroyed.

## Proposed Solution

### Option 1: Add Volume Mount for Jupyter Configuration (Recommended)

Mount the Jupyter configuration directory as a Docker volume to persist scheduler data.

**Implementation**:

1. Update `docker-compose.yml` to add volume mount:
   ```yaml
   services:
     jupyter:
       volumes:
         - ./workspace:/home/jovyan/work
         - jupyter-config:/home/jovyan/.jupyter
         - jupyter-data:/home/jovyan/.local/share/jupyter

   volumes:
     postgres-data:
     jupyter-config:
     jupyter-data:
   ```

2. Benefits:
   - Preserves all JupyterLab settings (scheduler, extensions, preferences)
   - Simple implementation
   - Standard Docker pattern

3. Considerations:
   - Volume persists across container rebuilds
   - May need to clear volume if JupyterLab configuration becomes corrupted

### Option 2: Database-Backed Scheduler Storage

Configure `jupyterlab-scheduler` to store jobs in PostgreSQL instead of filesystem.

**Implementation**:

1. Check if `jupyterlab-scheduler` supports external database storage (needs investigation)
2. If supported, configure to use existing PostgreSQL container
3. Create tables for job definitions

**Benefits**:
- Centralized storage with existing database
- Can query/manage jobs via SQL
- Better integration with IRP framework

**Challenges**:
- Requires extension support (may not be available)
- More complex setup
- Need to verify compatibility

### Option 3: Hybrid Approach - Export/Import Scripts

Create helper scripts to export/import scheduler configurations.

**Implementation**:

1. Create export script: `./scripts/export_scheduler_jobs.sh`
   - Extracts scheduler config from Jupyter
   - Saves to `./workspace/config/scheduler_jobs.json`

2. Create import script: `./scripts/import_scheduler_jobs.sh`
   - Reads from `./workspace/config/scheduler_jobs.json`
   - Restores jobs via JupyterLab API

3. Update startup documentation to run import after container start

**Benefits**:
- Version-controllable job definitions
- Works regardless of extension capabilities

**Challenges**:
- Manual process (not automatic)
- Requires maintenance
- API compatibility concerns

## Recommended Approach

**Option 1 (Volume Mount)** is recommended because:

1. Simplest implementation with immediate results
2. Preserves all JupyterLab state, not just scheduler
3. Standard Docker practice
4. No dependency on extension features
5. Zero maintenance overhead

## Implementation Steps

1. Stop running containers: `./stop.sh`
2. Update `docker-compose.yml` with volume mounts
3. Start containers: `./start.sh`
4. Recreate existing scheduler jobs (one-time migration)
5. Test: Create a job, restart container, verify job persists
6. Update documentation in README.md

## Testing Checklist

- [ ] Create scheduled job in JupyterLab
- [ ] Stop container: `./stop.sh`
- [ ] Start container: `./start.sh`
- [ ] Verify scheduled job still exists
- [ ] Verify job executes on schedule
- [ ] Test container rebuild (not just restart)
- [ ] Verify other JupyterLab settings also persist

## Additional Considerations

### Security
- Ensure volume permissions are appropriate
- Consider backup strategy for persistent volumes

### Documentation Updates
- Update `README.md` with volume persistence details
- Document volume cleanup procedure if needed
- Add troubleshooting section for corrupted configuration

### Future Enhancements
- Automated backup of Jupyter configuration
- Configuration version control (export to git)
- Health checks for scheduler service

---

**Priority**: High - affects automation reliability and user experience

**Effort**: Low (Option 1) - Single docker-compose.yml change

**Risk**: Low - Standard Docker pattern with easy rollback
