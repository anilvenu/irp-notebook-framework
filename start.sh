#!/bin/bash

echo "============================================"
echo " "
echo "     _                                  _   "
echo "    / \   ___ ___ _   _ _ __ __ _ _ __ | |_ "
echo "   / _ \ / __/ __| | | |  __/ _  |  _ \\| __|"
echo "  / ___ \\\\__ \\__ \\ |_| | | | (_| | | | | |_ "
echo " /_/   \_\___/___/\__,_|_|  \__,_|_| |_|\__|"
echo " "
echo "   IRP Notebook Framework"
echo "   Starting Services..."
echo "============================================"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "ERROR: .env file missing from the root directory."
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

# Validate required environment variables
echo "Validating environment configuration..."
MISSING_VARS=()
REQUIRED_VARS=("DB_NAME" "DB_USER" "DB_PASSWORD" "DB_PORT" "RISK_MODELER_BASE_URL" "RISK_MODELER_API_KEY" "RISK_MODELER_RESOURCE_GROUP_ID")

for var in "${REQUIRED_VARS[@]}"; do
    if ! grep -q "^${var}=" .env || [ -z "$(grep "^${var}=" .env | cut -d'=' -f2)" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo "ERROR: Missing or empty required variables in .env:"
    for var in "${MISSING_VARS[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Please ensure .env contains all required database and risk modeler configuration"
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

echo "✓ Environment configuration valid"
echo ""

# Start Docker containers
docker-compose up -d

# Wait for services to be ready
echo ""
echo "Waiting for services to start..."
sleep 10

# Initialize database metastore if needed
echo ""
echo "Initializing database metastore..."
docker exec irp-notebook python3 -c "
import sys
import os
sys.path.insert(0, '/home/jovyan/workspace')
from helpers import database as db

schema = os.getenv('DB_SCHEMA', 'public')
print(f'Schema: {schema}')

# Check and initialize schema
if not db.table_exists('irp_cycle', schema):
    print(f'⚠  Initializing database schema...')
    if not db.init_database(schema):
        sys.exit(1)
else:
    print(f'✓ Database schema exists')

# Check and create reporting views
if not db.view_exists('v_irp_job', schema):
    print(f'⚠  Creating reporting views...')
    db.create_reporting_views(schema)
else:
    print(f'✓ Reporting views exist')
"
if [ $? -ne 0 ]; then
    echo ""
    echo "ERROR: Database initialization failed"
    echo "Fix the issue and run ./start.sh again"
    echo ""
else
    echo "✓ Database metastore ready"
fi
echo ""

# Check if services are running
if docker ps | grep -q irp-notebook && docker ps | grep -q irp-postgres; then
    echo ""
    echo "Services started successfully!"
    echo ""
    echo "=========================================="
    echo "   JupyterLab: http://localhost:8888"
    echo "   FastAPI Dashboard: http://localhost:8001"
    echo "   "
    echo "   PostgreSQL Server: localhost:5432"
    echo "=========================================="
    echo ""
    echo "To stop services, run: ./stop.sh"
else
    echo "Failed to start services. Check docker-compose logs."
    exit 1
fi