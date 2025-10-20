#!/bin/bash
# Run batch viewer generator with proper environment

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

# Activate virtual environment and run
source .venv/bin/activate
python demo/generate_batch_viewer.py

echo ""
echo "HTML file generated at:"
echo "$PROJECT_ROOT/demo/demo.html"
