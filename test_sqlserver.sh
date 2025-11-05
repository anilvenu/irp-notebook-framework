#!/bin/bash

set -e  # Exit on first error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}SQL Server Test Suite${NC}"
echo -e "${GREEN}========================================${NC}"

# Load environment variables
if [ -f .env.test ]; then
    set -a  # Automatically export all variables
    source .env.test
    set +a  # Disable automatic export
fi

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up SQL Server test container...${NC}"
    docker-compose -f docker-compose.sqlserver.yml down -v
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Start SQL Server container
echo -e "${YELLOW}Starting SQL Server test container...${NC}"
docker-compose -f docker-compose.sqlserver.yml up -d

# Wait for SQL Server to be healthy
echo -e "${YELLOW}Waiting for SQL Server to be ready...${NC}"
timeout 90 bash -c 'until docker inspect --format="{{.State.Health.Status}}" irp-sqlserver-test 2>/dev/null | grep -q "healthy"; do sleep 2; echo -n "."; done'
echo -e "${GREEN}SQL Server is ready!${NC}"

# Initialize test database
echo -e "${YELLOW}Initializing test database...${NC}"
MSYS_NO_PATHCONV=1 docker exec irp-sqlserver-test /opt/mssql-tools18/bin/sqlcmd \
    -S localhost \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -i /docker-entrypoint-initdb.d/init_sqlserver.sql

echo -e "${GREEN}Test database initialized${NC}"

# Check if pyodbc is installed
echo -e "${YELLOW}Checking pyodbc installation...${NC}"
if ! python -c "import pyodbc" 2>/dev/null; then
    echo -e "${YELLOW}pyodbc not found. Installing...${NC}"
    pip install pyodbc==5.1.0 -q
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ pyodbc installed successfully${NC}"
    else
        echo -e "${RED}✗ Failed to install pyodbc${NC}"
        echo -e "${YELLOW}Note: pyodbc requires ODBC drivers to be installed on the host system${NC}"
        echo -e "${YELLOW}For Windows: Install 'ODBC Driver 18 for SQL Server'${NC}"
        echo -e "${YELLOW}For Linux: Install unixodbc-dev and msodbcsql18${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓ pyodbc is available${NC}"
fi

# Run pytest with SQL Server marker
# Note: We need to override the --ignore setting from pytest.ini
echo -e "${YELLOW}Running SQL Server tests...${NC}"
python -m pytest workspace/tests/test_sqlserver.py -m sqlserver "$@"

# Capture exit code
TEST_EXIT_CODE=$?

# Report results
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "\n${GREEN}✓ All SQL Server tests passed${NC}"
else
    echo -e "\n${RED}✗ Some SQL Server tests failed${NC}"
fi

# cleanup() will run automatically due to trap
exit $TEST_EXIT_CODE
