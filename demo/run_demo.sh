#!/bin/bash

# IRP Dashboard API Launch Script
# Starts the FastAPI web server for the dashboard application

echo "========================================"
echo "IRP Dashboard API Server"
echo "========================================"

# Get the directory where this script is located (but don't cd into it)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Load environment variables from .env if it exists
#if [ -f "../.env" ]; then
#    echo "Loading environment from ../.env"
#    export $(grep -v '^#' ../.env | xargs)
#else
#    echo "Warning: ../.env file not found, using defaults"
#fi

# Set default database configuration if not already set
export DB_SERVER="${DB_SERVER:-localhost}"
export DB_SERVER="localhost"
export DB_NAME="${DB_NAME:-test_db}"
export DB_USER="${DB_USER:-test_user}"
export DB_PASSWORD="${DB_PASSWORD:-test_pass}"
export DB_PORT="${DB_PORT:-5432}"
export DB_SCHEMA="${DB_SCHEMA:-demo}"

# Set server port
export PORT="${PORT:-8000}"

echo ""
echo "Configuration:"
echo "  Database: ${DB_NAME}@${DB_SERVER}:${DB_PORT}"
echo "  Default Schema: ${DB_SCHEMA}"
echo "  Server Port: ${PORT}"
echo ""


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

# Install packages
pip install -r requirements.txt -q

# Install packages for test
pip install -r requirements-test.txt -q


# Check if required dependencies are installed
echo "Checking dependencies..."

DEPS_OK=true

if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "  ✗ FastAPI not installed"
    DEPS_OK=false
else
    echo "  ✓ FastAPI"
fi

if ! python3 -c "import uvicorn" 2>/dev/null; then
    echo "  ✗ Uvicorn not installed"
    DEPS_OK=false
else
    echo "  ✓ Uvicorn"
fi

if [ "$DEPS_OK" = false ]; then
    echo ""
    echo "Missing dependencies detected. Installing..."
    pip install -q -r "$SCRIPT_DIR/requirements-api.txt"

    if [ $? -eq 0 ]; then
        echo "  ✓ Dependencies installed successfully"
    else
        echo "  ✗ Failed to install dependencies"
        echo ""
        echo "Please install manually:"
        echo "  pip install -r $SCRIPT_DIR/requirements-api.txt"
        exit 1
    fi
else
    echo "  ✓ All dependencies OK"
fi
echo ""

# Trap Ctrl+C and cleanup
cleanup() {
    echo ""
    echo "Shutting down server..."
    pkill -f "uvicorn app.app:app"
}

trap cleanup SIGINT SIGTERM

# Start the FastAPI server (use subshell to avoid changing cwd)
echo "Starting IRP Dashboard API..."
echo "Open your browser to: http://localhost:${PORT}"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run uvicorn in a subshell so parent shell stays in current directory
(cd "$SCRIPT_DIR" && python3 -m uvicorn app.app:app --host 0.0.0.0 --port ${PORT} --reload) &
UVICORN_PID=$!

# Wait for the process
wait $UVICORN_PID