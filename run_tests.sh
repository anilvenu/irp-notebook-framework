#!/bin/bash
#
# Test Runner for IRP Notebook Framework
# This script sets up the environment and runs database tests
#

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color


echo "============================================"
echo " "
echo "     _                                  _   "
echo "    / \   ___ ___ _   _ _ __ __ _ _ __ | |_ "
echo "   / _ \ / __/ __| | | |  __/ _  |  _ \\| __|"
echo "  / ___ \\\\__ \\__ \\ |_| | | | (_| | | | | |_ "
echo " /_/   \_\___/___/\__,_|_|  \__,_|_| |_|\__|"
echo " "
echo "IRP Notebook Framework Test Runner"
echo "============================================"



# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Set database connection to localhost for testing
export DB_SERVER=${DB_SERVER:-localhost}
export DB_PORT=${DB_PORT:-5432}
export DB_NAME=${DB_NAME:-irp_db}
export DB_USER=${DB_USER:-irp_user}
export DB_PASSWORD=${DB_PASSWORD:-irp_pass}

echo ""
echo "Database Configuration:"
echo "  Host: $DB_SERVER"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Check if venv exists - supports (venv) and (.venv)
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

# In docker, we are mounting the host /workspace (where we have all code) to /workspace on docker
# and we also set the PYTHONPATH to /workspace
# If we skip the step below, Python will look for packages in the project home and expect to see
# workspace.helper instead of helper. By setting the workspace path we are enabling packages
# under /workspace to be recognized. The scope of this is limited to the current virtual environment.
echo ""
echo "$(pwd)/workspace" > $(python -c "import site; print(f'{site.getsitepackages()[0]}/workspace.pth updated')")
python -c "import site; print(f'{site.getsitepackages()[0]}/workspace.pth updated')"


# Run the tests
echo ""
echo "Running database tests..."
python workspace/tests/test_database.py

echo ""
echo "Running configuration tests..."
python workspace/tests/test_configuration.py

echo ""
echo "Running batch management tests..."
python workspace/tests/test_batch.py

echo ""
echo "Running job management tests..."
python workspace/tests/test_job.py

echo ""
echo "Running batch/job integration tests..."
python workspace/tests/test_batch_job_integration.py

# Capture exit code
EXIT_CODE=$?

# Deactivate venv if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

#exit $EXIT_CODE
