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

echo "=================================="
echo "IRP Notebook Framework Test Runner"
echo "=================================="

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

# Check if venv exists
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

# Run the tests
echo ""
echo "Running database tests..."
python workspace/tests/test_database.py
echo "Running configuration tests..."
python workspace/tests/test_configuration.py

# Capture exit code
EXIT_CODE=$?

# Deactivate venv if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

#exit $EXIT_CODE
