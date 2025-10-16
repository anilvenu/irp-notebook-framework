# IRP Notebook Framework Documentation

This directory contains documentation for the IRP Notebook Framework.

## Documentation Structure

```
docs/
├── index.md                      # Main landing page
├── BATCH_JOB_SYSTEM.md          # Comprehensive batch/job guide
├── CONFIGURATION_TRANSFORMERS.md # Transformer guide
├── BULK_INSERT.md               # Database operations guide
├── api/                         # Auto-generated API docs
│   ├── overview.md
│   ├── cycle.md
│   ├── configuration.md
│   ├── batch.md
│   ├── job.md
│   ├── step.md
│   ├── database.md
│   └── constants.md
└── README.md                    # This file
```

## Generating Documentation

### First Time Setup

```bash
# From project root
./generate_docs.sh
```

This will:
1. Install MkDocs and dependencies (mkdocs, mkdocstrings, mkdocs-material)
2. Create `mkdocs.yml` configuration
3. Generate all API documentation from docstrings
4. Build HTML documentation in `site/` directory

### View Documentation

```bash
# Serve locally with live reload
mkdocs serve

# Then open: http://127.0.0.1:8000
```

### Build Only (No Server)

```bash
mkdocs build
```

Output will be in `site/` directory.

### Clean Build

```bash
mkdocs build --clean
```

## Documentation Technologies

- **MkDocs** - Static site generator
- **Material Theme** - Modern, responsive theme
- **mkdocstrings** - Automatic API docs from Python docstrings
- **PyMdown Extensions** - Enhanced markdown features

## Writing Documentation

### Adding User Guides

1. Create markdown file in `docs/`
2. Add to `nav` section in `mkdocs.yml`
3. Run `mkdocs serve` to preview

### Updating API Docs

API documentation is auto-generated from Python docstrings. To update:

1. Edit docstrings in source code (`workspace/helpers/*.py`)
2. Run `./generate_docs.sh` to rebuild
3. Changes appear automatically in API reference

### Docstring Format

We use **Google-style** docstrings:

```python
def create_batch(
    batch_type: str,
    configuration_id: int,
    step_id: Optional[int] = None,
    schema: str = 'public'
) -> int:
    """
    Create a new batch with job configurations.

    Process:
        1. Validate configuration is VALID or ACTIVE
        2. Validate batch_type is registered
        3. Apply transformer to generate job configurations
        4. Create batch and jobs in transaction
        5. Submit batch

    Args:
        batch_type: Type of batch processing (must be registered)
        configuration_id: Master configuration ID
        step_id: Optional step ID (looked up if None)
        schema: Database schema (default: 'public')

    Returns:
        Batch ID

    Raises:
        BatchError: If validation fails or creation fails

    Example:
        ```python
        batch_id = create_batch(
            'portfolio_analysis',
            config_id=1,
            step_id=5
        )
        ```
    """
```

## Features

### Search
Full-text search across all documentation (top right in browser).

### Code Highlighting
Automatic syntax highlighting for Python, SQL, bash, etc.

### Dark Mode
Toggle between light/dark mode (top bar).

### Navigation
- Tabs for major sections
- Expandable sidebar
- "Back to top" button
- Breadcrumbs

### Mobile Responsive
Works on all devices.

## Deployment

### GitHub Pages

```bash
mkdocs gh-deploy
```

This builds and pushes to `gh-pages` branch.

### Custom Server

Copy `site/` directory to your web server:

```bash
rsync -avz site/ user@server:/var/www/docs/
```

## Troubleshooting

### "Module not found" errors

Make sure you're in the project root and venv is activated:

```bash
source venv/bin/activate
cd /path/to/irp-notebook-framework
./generate_docs.sh
```

### Docstring not showing

- Check docstring format (Google-style)
- Check indentation (must be consistent)
- Run `mkdocs build -v` for verbose output

### Live reload not working

```bash
# Kill existing server
pkill -f "mkdocs serve"

# Restart
mkdocs serve
```

## Links

- [MkDocs Documentation](https://www.mkdocs.org/)
- [Material Theme](https://squidfunk.github.io/mkdocs-material/)
- [mkdocstrings](https://mkdocstrings.github.io/)