-- ============================================================================
-- IRP Notebook Framework - User and Schema Security Setup
-- ============================================================================
-- This script creates separate database users and schemas to isolate
-- production (live) and test data with authentication-based access control.
--
-- Security Model:
-- - irp_live user: Access ONLY to 'live' schema
-- - irp_test user: Access ONLY to 'test' schema
-- - Forgotten schema parameters fail authentication instead of corrupting data
--
-- Usage:
--   psql -U postgres -d irp_db -f setup_users_and_schemas.sql
-- ============================================================================

-- Drop existing users if they exist (for clean setup)
DROP USER IF EXISTS irp_live;
DROP USER IF EXISTS irp_test;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS live;
CREATE SCHEMA IF NOT EXISTS test;

-- Create production user (live schema only)
CREATE USER irp_live WITH PASSWORD 'irp_g0_live';
GRANT CONNECT ON DATABASE irp_db TO irp_live;
GRANT USAGE ON SCHEMA live TO irp_live;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA live TO irp_live;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA live TO irp_live;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA live TO irp_live;

-- Set default schema for irp_live
ALTER USER irp_live SET search_path TO live;

-- Create test user (test schema only)
CREATE USER irp_test WITH PASSWORD 'g0test';
GRANT CONNECT ON DATABASE irp_db TO irp_test;
GRANT USAGE ON SCHEMA test TO irp_test;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test TO irp_test;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA test TO irp_test;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA test TO irp_test;

-- Set default schema for irp_test
ALTER USER irp_test SET search_path TO test;

-- Ensure future objects are also granted permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA live GRANT ALL ON TABLES TO irp_live;
ALTER DEFAULT PRIVILEGES IN SCHEMA live GRANT ALL ON SEQUENCES TO irp_live;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT ALL ON TABLES TO irp_test;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT ALL ON SEQUENCES TO irp_test;

-- Revoke access to other schemas for security
REVOKE ALL ON SCHEMA public FROM irp_live;
REVOKE ALL ON SCHEMA public FROM irp_test;
REVOKE ALL ON SCHEMA test FROM irp_live;
REVOKE ALL ON SCHEMA live FROM irp_test;

SELECT 'Database users and schemas configured successfully!' as message;
SELECT 'IMPORTANT: Change default passwords before deploying to production!' as warning;