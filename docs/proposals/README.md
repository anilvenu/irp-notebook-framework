# Improvement Proposals

This directory contains improvement proposals for the IRP Notebook Framework codebase.

## Purpose

Proposals document significant architectural changes, refactorings, or improvements that:
- Require substantial implementation effort (>1 hour)
- Affect multiple modules or components
- Change architectural patterns or design decisions
- Need review and discussion before implementation

## Proposal Format

Each proposal should include:

1. **Status** - Proposed, Approved, Rejected, Implemented, or Deferred
2. **Problem Statement** - What issue are we solving?
3. **Proposed Solution** - How do we solve it?
4. **Implementation Plan** - Step-by-step approach
5. **Benefits** - Why is this worth doing?
6. **Risks and Mitigation** - What could go wrong?
7. **Alternatives Considered** - What other options were evaluated?
8. **Success Criteria** - How do we know it worked?
9. **Timeline** - How long will it take?

## Naming Convention

Proposals are numbered sequentially:

```
NNN-short-description.md
```

Examples:
- `001-eliminate-circular-imports.md`
- `002-add-async-job-execution.md`
- `003-migrate-to-sqlalchemy.md`

## Proposal Lifecycle

```
Proposed → Under Review → Approved → In Progress → Implemented
                       ↓
                    Rejected/Deferred
```

### Status Definitions

- **Proposed** - Initial draft, open for discussion
- **Under Review** - Being evaluated by team
- **Approved** - Accepted, ready for implementation
- **In Progress** - Currently being implemented
- **Implemented** - Completed and merged
- **Rejected** - Not proceeding with this approach
- **Deferred** - Good idea, but not now

## How to Create a Proposal

1. **Copy template** (if available) or use existing proposal as guide
2. **Create new numbered file** in `docs/proposals/`
3. **Fill in all sections** thoroughly
4. **Commit and create PR** for discussion
5. **Update status** as proposal progresses

## Current Proposals

| ID | Title | Status | Priority | Effort |
|----|-------|--------|----------|--------|
| 001 | [Eliminate Circular Import Dependencies](001-eliminate-circular-imports.md) | Proposed | Medium | 2-3 hours |

## Implementation Notes

When implementing an approved proposal:

1. Create feature branch named after proposal: `refactor/001-eliminate-circular-imports`
2. Reference proposal in commit messages: `"Implement proposal 001: Split step.py into CRUD/workflow"`
3. Update proposal status to "In Progress" when starting
4. Update proposal status to "Implemented" when merged
5. Link to implementing PR/commit in proposal document

## Questions?

If you have questions about a proposal or the proposal process, add comments to the proposal document or reach out to the team.
