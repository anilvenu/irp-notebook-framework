# Batch Viewer Demo

An interactive HTML viewer for batch job data from the IRP Notebook Framework.

## Quick Start

```bash
./demo/run_generator.sh
```

Open `demo/demo.html` in your browser to view the interactive batch viewer!

## Files

- **generate_batch_viewer.py** - Generates HTML from database
- **run_generator.sh** - Runs the generator with proper environment
- **CSV files** - Test data (cycles, stages, steps, batches, jobs, etc.)
- **demo.html** - Generated HTML viewer (open in browser)

## Test Scenarios

8 batch scenarios with different job states (101-108). Default: Batch 102 (mixed statuses).

## Database Setup

The script:
1. Creates `demo` schema
2. Loads tables from SQL files
3. Inserts CSV data
4. Generates HTML from reporting views

Database config in `run_generator.sh`:
```bash
export DB_SERVER="localhost"
export DB_NAME="test_db"
export DB_USER="test_user"
export DB_PASSWORD="test_pass"
```

## Customization

- Edit CSV files to change test data
- Modify `batch_id` in script to generate different batch
- Edit CSS in `generate_html()` function for styling

## Troubleshooting

**Connection error**: Check DB_SERVER in run_generator.sh  
**Line ending issues**: Run `/home/avenugopal/irp-notebook-framework/win-unix.sh demo/run_generator.sh`  
**CSV not found**: Ensure you're in project root

