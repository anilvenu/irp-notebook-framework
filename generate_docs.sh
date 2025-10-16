#!/bin/bash
#
# Documentation Generator for IRP Notebook Framework
# Uses MkDocs with Material theme and mkdocstrings for API docs
#

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "======================================"
echo "IRP Documentation Generator (MkDocs)"
echo "======================================"

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if venv exists
if [ -d "venv" ]; then
    echo -e "${GREEN}✓${NC} Found venv directory"
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo -e "${GREEN}✓${NC} Found .venv directory"
    source .venv/bin/activate
else
    echo -e "${YELLOW}⚠${NC} No virtual environment found"
    echo "  To create one, run: python -m venv venv"
    echo "  Running with system Python..."
fi

echo ""
echo "Installing documentation dependencies..."
pip install -q mkdocs mkdocstrings[python] mkdocs-material pymdown-extensions

if [ $? -ne 0 ]; then
    echo -e "${RED}✗${NC} Failed to install dependencies"
    exit 1
fi

echo -e "${GREEN}✓${NC} Dependencies installed"

# Check if mkdocs.yml exists, if not create it
if [ ! -f "mkdocs.yml" ]; then
    echo ""
    echo "Creating mkdocs.yml configuration..."

    cat > mkdocs.yml << 'EOF'
site_name: IRP Notebook Framework Documentation
site_description: Documentation for the IRP Notebook Framework - A risk analysis cycle management system
site_author: IRP Team

theme:
  name: material
  palette:
    # Light mode
    - scheme: default
      primary: indigo
      accent: indigo
      toggle:
        icon: material/brightness-7
        name: Switch to dark mode
    # Dark mode
    - scheme: slate
      primary: indigo
      accent: indigo
      toggle:
        icon: material/brightness-4
        name: Switch to light mode
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.top
    - search.suggest
    - search.highlight
    - content.code.copy

plugins:
  - search
  - mkdocstrings:
      handlers:
        python:
          options:
            show_source: true
            show_root_heading: true
            show_root_full_path: false
            show_symbol_type_heading: true
            show_symbol_type_toc: true
            group_by_category: true
            members_order: source
            docstring_style: google
            docstring_section_style: spacy
            merge_init_into_class: true

markdown_extensions:
  - admonition
  - pymdownx.details
  - pymdownx.superfences
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.inlinehilite
  - pymdownx.snippets
  - pymdownx.tabbed:
      alternate_style: true
  - tables
  - toc:
      permalink: true

nav:
  - Home: index.md
  - User Guides:
    - Batch & Job System: BATCH_JOB_SYSTEM.md
    - Configuration Transformers: CONFIGURATION_TRANSFORMERS.md
    - Database Operations: BULK_INSERT.md
  - API Reference:
    - Overview: api/overview.md
    - Cycle Management: api/cycle.md
    - Configuration: api/configuration.md
    - Batch Management: api/batch.md
    - Job Management: api/job.md
    - Step Tracking: api/step.md
    - Database: api/database.md
    - Constants: api/constants.md

extra:
  version: 1.0.0
EOF

    echo -e "${GREEN}✓${NC} Created mkdocs.yml"
fi

# Create docs directory structure if it doesn't exist
mkdir -p docs/api

# Create index.md if it doesn't exist
if [ ! -f "docs/index.md" ]; then
    echo ""
    echo "Creating docs/index.md..."

    cat > docs/index.md << 'EOF'
# IRP Notebook Framework

Welcome to the IRP Notebook Framework documentation!

## Overview

The IRP Notebook Framework is a comprehensive system for managing risk analysis cycles with integrated batch job processing for Moody's workflows.

## Key Features

- **Cycle Management** - Create and manage risk analysis cycles with full lifecycle tracking
- **Configuration System** - Load and validate configuration from Excel files
- **Batch Processing** - Orchestrate multiple jobs with configurable transformers
- **Job Management** - Submit, track, and resubmit jobs with full audit trail
- **Reconciliation** - Automatic batch status reconciliation based on job states
- **Extensible** - Register custom transformers for different batch types

## Quick Start

### Create a Cycle

```python
from helpers.cycle import create_cycle

# Create new analysis cycle
create_cycle('Analysis-2025-Q1', created_by='analyst')
```

### Load Configuration

```python
from helpers.configuration import load_configuration_file

# Load configuration from Excel
config_id = load_configuration_file(
    cycle_id=1,
    excel_config_path='/path/to/config.xlsx',
    register=True
)
```

### Process Batch

```python
from helpers.batch import create_batch, submit_batch, recon_batch
from helpers.job import track_job_status

# Create and submit batch
batch_id = create_batch('portfolio_analysis', config_id, step_id)
submit_batch(batch_id)

# Track jobs (in production, this would be scheduled)
jobs = get_batch_jobs(batch_id)
for job in jobs:
    track_job_status(job['id'])

# Reconcile batch status
final_status = recon_batch(batch_id)
```

## Documentation Sections

### User Guides

- **[Batch & Job System](BATCH_JOB_SYSTEM.md)** - Comprehensive guide to batch processing, job management, and workflows
- **[Configuration Transformers](CONFIGURATION_TRANSFORMERS.md)** - How to create custom transformers
- **[Database Operations](BULK_INSERT.md)** - Bulk insert and performance optimization

### API Reference

Detailed API documentation for all modules:

- **[Cycle Management](api/cycle.md)** - Cycle lifecycle operations
- **[Configuration](api/configuration.md)** - Configuration loading and transformers
- **[Batch Management](api/batch.md)** - Batch creation and reconciliation
- **[Job Management](api/job.md)** - Job submission, tracking, and resubmission
- **[Step Tracking](api/step.md)** - Step execution tracking
- **[Database](api/database.md)** - Database utilities
- **[Constants](api/constants.md)** - Status enums and configuration

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Cycle                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Configuration (Master)                   │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │                  Batch                          │  │  │
│  │  │  ┌───────────────────────────────────────────┐  │  │  │
│  │  │  │      Job Configuration                    │  │  │  │
│  │  │  │  ┌─────────────────────────────────────┐  │  │  │  │
│  │  │  │  │           Job                       │  │  │  │  │
│  │  │  │  │  - Submit to Moody's                │  │  │  │  │
│  │  │  │  │  - Track status                     │  │  │  │  │
│  │  │  │  │  - Resubmit on failure              │  │  │  │  │
│  │  │  │  └─────────────────────────────────────┘  │  │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  │  ┌───────────────────────────────────────────┐  │  │  │
│  │  │  │      Batch Recon Log                      │  │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Status Lifecycles

### Batch Status
```
INITIATED → ACTIVE → COMPLETED
              ↓         ↓
           FAILED   CANCELLED
```

### Job Status
```
INITIATED → SUBMITTED → QUEUED → PENDING → RUNNING → COMPLETED
                                                  ↓
                                              FAILED
                                                  ↓
                                             CANCELLED
```

## Support

For issues, questions, or contributions, please refer to the project repository.
EOF

    echo -e "${GREEN}✓${NC} Created docs/index.md"
fi

# Create API documentation files
echo ""
echo "Creating API documentation files..."

# API Overview
cat > docs/api/overview.md << 'EOF'
# API Reference Overview

This section contains detailed API documentation for all modules in the IRP Notebook Framework.

## Module Organization

- **[cycle](cycle.md)** - Cycle creation and lifecycle management
- **[configuration](configuration.md)** - Configuration loading, validation, and transformers
- **[batch](batch.md)** - Batch creation, submission, and reconciliation
- **[job](job.md)** - Job submission, tracking, and resubmission
- **[step](step.md)** - Step execution tracking
- **[database](database.md)** - Database connection and query utilities
- **[constants](constants.md)** - Status enums and configuration constants

## Import Patterns

All modules follow explicit import patterns:

```python
# Import from specific modules
from helpers.batch import create_batch, submit_batch
from helpers.job import submit_job, track_job_status
from helpers.configuration import ConfigurationTransformer

# Import constants
from helpers.constants import BatchStatus, JobStatus
```

## Error Handling

Each module provides custom exception classes:

```python
from helpers.batch import BatchError
from helpers.job import JobError
from helpers.configuration import ConfigurationError
from helpers.cycle import CycleError
```

## Common Patterns

### Resource Reading
```python
batch = read_batch(batch_id)
job = read_job(job_id)
config = read_configuration(config_id)
```

### Status Updates
```python
update_batch_status(batch_id, BatchStatus.COMPLETED)
update_job_status(job_id, JobStatus.FAILED)
```

### Validation
All functions validate inputs before database operations and raise descriptive errors.
EOF

# Create individual API doc files
for module in cycle configuration batch job step database constants; do
    cat > docs/api/${module}.md << EOF
# helpers.${module}

::: helpers.${module}
    options:
      show_root_heading: true
      show_source: true
      members_order: source
EOF
done

echo -e "${GREEN}✓${NC} Created API documentation files"

# Build documentation
echo ""
echo -e "${BLUE}Building documentation...${NC}"
mkdocs build --clean

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓${NC} Documentation built successfully!"
    echo ""
    echo "Documentation is available at: ${BLUE}site/index.html${NC}"
    echo ""
    echo "To view documentation:"
    echo "  1. Run: ${BLUE}mkdocs serve${NC}"
    echo "  2. Open: ${BLUE}http://127.0.0.1:8000${NC}"
    echo ""
    echo "To serve documentation now, run:"
    echo "  ${BLUE}mkdocs serve${NC}"
else
    echo -e "${RED}✗${NC} Documentation build failed"
    exit 1
fi

# Deactivate venv if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi
