#!/bin/bash
# Run pytest tests

echo "Setting environment variables" 
# Set TEST database connection
export DB_SERVER=localhost
export DB_PORT=5432
export DB_NAME=test_db
export DB_USER=test_user
export DB_PASSWORD=test_pass

echo "Activating .venv"
# Activate .venv
source .venv/bin/activate

# Set workspace path for imports
echo "$(pwd)/workspace" > $(python -c "import site; print(site.getsitepackages()[0])")/workspace.pth

# Run ALL pytest tests in a single command for unified results
# This excludes legacy test files
pytest workspace/tests/ -v "$@"