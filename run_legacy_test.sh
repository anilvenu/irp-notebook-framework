#!/bin/bash
# Run legacy test with proper environment setup

# Set TEST database connection
export DB_SERVER=localhost
export DB_PORT=5432
export DB_NAME=test_db
export DB_USER=test_user
export DB_PASSWORD=test_pass

# Activate .venv
source .venv/bin/activate

# Set workspace path for imports
echo "$(pwd)/workspace" > $(python -c "import site; print(site.getsitepackages()[0])")/workspace.pth

# Run legacy test
python workspace/tests/test_database_legacy.py
