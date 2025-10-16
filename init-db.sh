#!/bin/bash
set -e

# This script initializes both production and test databases with separate users
# It runs when the PostgreSQL container starts for the first time

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Production database (irp_db) already created by POSTGRES_DB env var
    -- Production user (irp_user) already created by POSTGRES_USER env var
    SELECT 'Production database ready: irp_db (user: irp_user)';

    -- Create test user
    CREATE USER test_user WITH PASSWORD 'test_pass';

    -- Create test database
    CREATE DATABASE test_db OWNER test_user;

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE test_db TO test_user;

    SELECT 'Test database created: test_db (user: test_user)';
EOSQL

echo ""
echo "=========================================="
echo "Database initialization complete:"
echo "  Production DB: irp_db"
echo "  Production User: irp_user"
echo "  Test DB: test_db"
echo "  Test User: test_user"
echo "=========================================="
