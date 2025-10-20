#!/bin/bash
# Run batch viewer generator with proper environment
# Two-step process: 1) Prepare data, 2) Generate dashboards

# Get the script's directory and navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# Set database environment variables
export DB_SERVER="localhost"
export DB_PORT="5432"
export DB_NAME="test_db"
export DB_USER="test_user"
export DB_PASSWORD="test_pass"

# Activate virtual environment
source .venv/bin/activate

# Step 1: Prepare data
echo ""
echo "STEP 1: Preparing data..."
echo "=========================="
python demo/prepare_data.py
DATA_PREP_EXIT=$?

if [ $DATA_PREP_EXIT -ne 0 ]; then
    echo ""
    echo "✗ Data preparation failed. Aborting."
    exit 1
fi

# Step 2: Generate dashboards
echo ""
echo "STEP 2: Generating dashboards..."
echo "================================="
python demo/generate_dashboards.py
DASHBOARD_EXIT=$?

if [ $DASHBOARD_EXIT -ne 0 ]; then
    echo ""
    echo "✗ Dashboard generation failed."
    exit 1
fi

# Success
echo ""
echo "✓ Complete! Open the dashboard at:"
echo "$PROJECT_ROOT/demo/files/html_output/cycle/Analysis-2025-Q1/index.html"
