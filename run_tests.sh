#!/bin/bash
#
# Test Runner for IRP Notebook Framework
# This script sets up the environment and runs database tests
#
# Usage: ./run_tests.sh [--preserve]
#   --preserve: Keep test schemas after tests for debugging
#

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse command line arguments
PRESERVE_FLAG=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --preserve)
            PRESERVE_FLAG="--preserve"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--preserve]"
            exit 1
            ;;
    esac
done

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

if [ -n "$PRESERVE_FLAG" ]; then
    echo -e "${YELLOW}⚠️  PRESERVE MODE: Test schemas will be kept for debugging${NC}"
fi

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Set TEST database connection for all tests
# This ensures tests NEVER touch production database (irp_db)
export DB_SERVER=localhost
export DB_PORT=5432
export DB_NAME=test_db
export DB_USER=test_user
export DB_PASSWORD=test_pass

echo ""
echo -e "${GREEN}TEST${NC} Database Configuration (Isolated from Production):"
echo "  Host: $DB_SERVER"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME (TEST DATABASE)"
echo "  User: $DB_USER (TEST USER)"
echo ""
echo -e "${YELLOW}NOTE:${NC} Tests run against 'test_db' - production 'irp_db' is protected"
echo ""

# Ensure test database exists in Docker postgres container
echo "Checking test database setup..."
CONTAINER_NAME="irp-postgres"

# Check if postgres container is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo -e "${RED}✗${NC} PostgreSQL container '$CONTAINER_NAME' is not running"
    echo "  Start it with: docker-compose up -d postgres"
    exit 1
fi

# Check if test_user exists, create if not
if ! docker exec $CONTAINER_NAME psql -U irp_user -d irp_db -tAc "SELECT 1 FROM pg_roles WHERE rolname='test_user'" | grep -q 1; then
    echo "  Creating test user..."
    docker exec $CONTAINER_NAME psql -U irp_user -d irp_db -c "CREATE USER test_user WITH PASSWORD 'test_pass';" > /dev/null 2>&1
    echo -e "  ${GREEN}✓${NC} Test user created"
else
    echo -e "  ${GREEN}✓${NC} Test user exists"
fi

# Check if test_db exists, create if not
if ! docker exec $CONTAINER_NAME psql -U irp_user -d irp_db -lqt | cut -d \| -f 1 | grep -qw test_db; then
    echo "  Creating test database..."
    docker exec $CONTAINER_NAME psql -U irp_user -d irp_db -c "CREATE DATABASE test_db OWNER test_user;" > /dev/null 2>&1
    docker exec $CONTAINER_NAME psql -U irp_user -d irp_db -c "GRANT ALL PRIVILEGES ON DATABASE test_db TO test_user;" > /dev/null 2>&1
    echo -e "  ${GREEN}✓${NC} Test database created"
else
    echo -e "  ${GREEN}✓${NC} Test database exists"
fi

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
python workspace/tests/test_database.py $PRESERVE_FLAG

echo ""
echo "Running configuration tests..."
python workspace/tests/test_configuration.py $PRESERVE_FLAG

echo ""
echo "Running batch management tests..."
python workspace/tests/test_batch.py $PRESERVE_FLAG

echo ""
echo "Running job management tests..."
python workspace/tests/test_job.py $PRESERVE_FLAG

echo ""
echo "Running batch/job integration tests..."
python workspace/tests/test_batch_job_integration.py $PRESERVE_FLAG

# Capture exit code
EXIT_CODE=$?

# Deactivate venv if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

#exit $EXIT_CODE
