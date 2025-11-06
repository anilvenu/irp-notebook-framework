#!/bin/bash
# Run batch viewer generator with proper environment
# Loops through all demo sets: 1) Prepare data, 2) Generate dashboards

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

# Find all set directories
SETS=$(find demo -maxdepth 1 -type d -name "set_*" | sort)

if [ -z "$SETS" ]; then
    echo "✗ No demo sets found (looking for demo/set_* directories)"
    exit 1
fi

echo ""
echo "================================================================"
echo "Found demo sets:"
for SET_DIR in $SETS; do
    SET_NAME=$(basename "$SET_DIR")
    echo "  - $SET_NAME"
done
echo "================================================================"

# Process each set
for SET_DIR in $SETS; do
    SET_NAME=$(basename "$SET_DIR")

    echo ""
    echo "================================================================"
    echo "  Processing $SET_NAME"
    echo "================================================================"

    # Step 1: Prepare data for this set
    echo ""
    echo "STEP 1: Preparing data for $SET_NAME..."
    echo "========================================"
    python demo/prepare_data.py "$SET_NAME"
    DATA_PREP_EXIT=$?

    if [ $DATA_PREP_EXIT -ne 0 ]; then
        echo ""
        echo "✗ Data preparation failed for $SET_NAME. Continuing to next set..."
        continue
    fi

    # Step 2: Generate dashboards for this set
    echo ""
    echo "STEP 2: Generating dashboards for $SET_NAME..."
    echo "==============================================="
    python demo/generate_dashboards.py "$SET_NAME"
    DASHBOARD_EXIT=$?

    if [ $DASHBOARD_EXIT -ne 0 ]; then
        echo ""
        echo "✗ Dashboard generation failed for $SET_NAME. Continuing to next set..."
        continue
    fi

    echo ""
    echo "✓ $SET_NAME complete!"
    echo "  Dashboard: $PROJECT_ROOT/demo/$SET_NAME/html_output/cycle/Analysis-2025-Q1/index.html"
done

# Final summary
echo ""
echo "================================================================"
echo "✓ All sets processed!"
echo "================================================================"
echo ""
echo "Generated dashboards:"
for SET_DIR in $SETS; do
    SET_NAME=$(basename "$SET_DIR")
    DASHBOARD_PATH="$PROJECT_ROOT/demo/$SET_NAME/html_output/cycle/Analysis-2025-Q1/index.html"
    if [ -f "$DASHBOARD_PATH" ]; then
        echo "  ✓ $SET_NAME: $DASHBOARD_PATH"
    else
        echo "  ✗ $SET_NAME: (generation failed)"
    fi
done
