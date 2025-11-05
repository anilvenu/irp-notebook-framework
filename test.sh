#!/bin/bash
# Run pytest tests


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

# Install packages for test
pip install -r requirements-test.txt -q

# Set workspace path for imports
#echo "$(pwd)/workspace" > $(python -c "import site; print(site.getsitepackages()[0])")/workspace.pth
#python -c "import site; print(f'{site.getsitepackages()[0]}/workspace.pth updated')"

# Run ALL pytest tests in a single command for unified results
# SQL Server tests are excluded via pytest.ini (use ./test_sqlserver.sh for SQL Server tests)
pytest workspace/tests/ -v "$@"

# Deactivate venv if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi