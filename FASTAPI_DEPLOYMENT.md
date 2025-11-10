# FastAPI Deployment Guide

This document explains how to run the IRP Dashboard FastAPI application in both local development and Docker production modes.

## Overview

The FastAPI application supports dual deployment:

| Mode | Port | Database Host | Database | Schema | Command |
|------|------|---------------|----------|--------|---------|
| **Local Development** | 8000 | localhost | test_db | demo | `./demo/run_api.sh` |
| **Docker Production** | 8001 | postgres | irp_db | public | `docker compose up fastapi` |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Network (irp-network)              │
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐ │
│  │  Jupyter         │  │  FastAPI         │  │ PostgreSQL │ │
│  │  Container       │  │  Container       │  │ Container  │ │
│  │  (irp-notebook)  │  │  (irp-fastapi)   │  │ (postgres) │ │
│  │  Port: 8888      │  │  Port: 8001      │  │ Port: 5432 │ │
│  │                  │  │                  │  │            │ │
│  │  Schema: public  │  │  Schema: public  │  │ Database:  │ │
│  │                  │  │                  │  │ - irp_db   │ │
│  └──────────────────┘  └──────────────────┘  │ - test_db  │ │
│                                               └────────────┘ │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                     Host Machine (localhost)                  │
│                                                               │
│  ┌──────────────────┐                         ┌────────────┐ │
│  │  FastAPI App     │─────────────────────────│ PostgreSQL │ │
│  │  (demo/app/)     │  localhost:5432         │ (Docker)   │ │
│  │  Port: 8000      │                         │ Port: 5432 │ │
│  │                  │                         │            │ │
│  │  Schema: demo    │                         │ Database:  │ │
│  │                  │                         │ - test_db  │ │
│  └──────────────────┘                         └────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Local Development Mode

### Prerequisites
- PostgreSQL running and accessible at `localhost:5432`
- Database `test_db` exists with schema `demo`
- Python 3.11+ installed

### Running Locally

```bash
# From project root
./demo/run_api.sh
```

This will:
1. Set environment variables for local database
2. Check and install dependencies if needed
3. Start FastAPI on port 8000 with auto-reload
4. Connect to: `localhost:5432` → `test_db` → `demo` schema

### Access
- Dashboard: http://localhost:8000
- Health Check: http://localhost:8000/health
- API Docs: http://localhost:8000/docs

### Configuration
Environment variables are set in [demo/run_api.sh](demo/run_api.sh):

```bash
DB_SERVER=localhost
DB_NAME=test_db
DB_USER=test_user
DB_PASSWORD=test_pass
DB_PORT=5432
DB_SCHEMA=demo
PORT=8000
```

## Docker Production Mode

### Prerequisites
- Docker and Docker Compose installed
- `.env` file configured (see below)

### Running in Docker

```bash
# Start all services (PostgreSQL, Jupyter, FastAPI)
docker compose up

# Or start only FastAPI (and its dependency: PostgreSQL)
docker compose up fastapi

# Run in background
docker compose up -d fastapi
```

This will:
1. Build the FastAPI Docker image from [Dockerfile.fastapi](Dockerfile.fastapi)
2. Start PostgreSQL container if not running
3. Wait for PostgreSQL health check
4. Start FastAPI container on port 8001
5. Connect to: `postgres:5432` → `irp_db` → `public` schema

### Access
- Dashboard: http://localhost:8001
- Health Check: http://localhost:8001/health
- API Docs: http://localhost:8001/docs

### Configuration
Environment variables are set in [.env](.env) file:

```bash
# Production Database Configuration
DB_NAME=irp_db
DB_USER=irp_user
DB_PASSWORD=irp_pass
DB_SERVER=postgres
DB_PORT=5432
```

Additional variables are set in [docker-compose.yml](docker-compose.yml):
```yaml
DB_SCHEMA=public
PORT=8001
```

### Docker Services

The FastAPI service configuration in docker-compose.yml:

```yaml
fastapi:
  build:
    context: .
    dockerfile: Dockerfile.fastapi
  container_name: irp-fastapi
  ports:
    - "8001:8001"
  environment:
    - DB_SERVER=postgres
    - DB_PORT=${DB_PORT}
    - DB_NAME=${DB_NAME}
    - DB_USER=${DB_USER}
    - DB_PASSWORD=${DB_PASSWORD}
    - DB_SCHEMA=public
    - PORT=8001
  volumes:
    - ./workspace:/app/workspace:ro
  depends_on:
    postgres:
      condition: service_healthy
  networks:
    - irp-network
  restart: unless-stopped
```

## Running Both Modes Simultaneously

You can run both local and Docker deployments at the same time:

```bash
# Terminal 1: Start Docker FastAPI (port 8001)
docker compose up fastapi

# Terminal 2: Start Local FastAPI (port 8000)
./demo/run_api.sh
```

Access:
- Local (demo schema): http://localhost:8000
- Docker (public schema): http://localhost:8001

## Troubleshooting

### Local Mode Issues

**Problem**: Cannot connect to database
```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Check if test_db exists
psql -h localhost -U test_user -d test_db -c "SELECT current_database();"
```

**Problem**: Dependencies not installed
```bash
# Install manually
pip install -r demo/requirements-api.txt
```

### Docker Mode Issues

**Problem**: FastAPI container fails to start
```bash
# Check logs
docker compose logs fastapi

# Rebuild image
docker compose build fastapi --no-cache
```

**Problem**: Cannot connect to PostgreSQL
```bash
# Check PostgreSQL health
docker compose ps postgres

# Check if database exists
docker compose exec postgres psql -U irp_user -d irp_db -c "SELECT current_database();"
```

**Problem**: Port 8001 already in use
```bash
# Find what's using the port
lsof -i :8001

# Or change port in docker-compose.yml
ports:
  - "8002:8001"  # Maps host port 8002 to container port 8001
```

## Database Schema Management

The application uses the `helpers.database` module which supports dynamic schema selection:

- **Local Mode**: Uses `demo` schema (set via `DB_SCHEMA=demo`)
- **Docker Mode**: Uses `public` schema (set via `DB_SCHEMA=public`)

The schema is also configurable per-route via URL path:
- `http://localhost:8000/demo/` - Uses demo schema
- `http://localhost:8001/public/` - Uses public schema

## Development Workflow

### Making Changes

1. Edit code in `demo/app/` directory
2. Changes are immediately reflected:
   - **Local**: Auto-reload enabled via `--reload` flag
   - **Docker**: Rebuild and restart: `docker compose up --build fastapi`

### Shared Database Module

Both deployments use the shared `workspace/helpers/database.py` module:

- **Local**: Imports directly from workspace via PYTHONPATH
- **Docker**: Copies `workspace/helpers` into image at build time

After modifying `workspace/helpers/*`, rebuild Docker image:
```bash
docker compose build fastapi
docker compose up fastapi
```

## Environment Variables Reference

| Variable | Local Value | Docker Value | Purpose |
|----------|-------------|--------------|---------|
| `DB_SERVER` | localhost | postgres | Database host |
| `DB_NAME` | test_db | irp_db | Database name |
| `DB_USER` | test_user | irp_user | Database user |
| `DB_PASSWORD` | test_pass | irp_pass | Database password |
| `DB_PORT` | 5432 | 5432 | Database port |
| `DB_SCHEMA` | demo | public | Default schema |
| `PORT` | 8000 | 8001 | API server port |

## Files Reference

- **[Dockerfile.fastapi](Dockerfile.fastapi)** - FastAPI container image definition
- **[docker-compose.yml](docker-compose.yml)** - Docker services orchestration
- **[demo/run_api.sh](demo/run_api.sh)** - Local development startup script
- **[demo/app/app.py](demo/app/app.py)** - FastAPI application code
- **[.env](.env)** - Production environment variables
- **[workspace/helpers/database.py](workspace/helpers/database.py)** - Shared database module

## Summary

This dual deployment architecture provides:

✅ **Flexibility**: Run in local development or Docker production mode
✅ **Isolation**: Separate databases and schemas for different environments
✅ **Consistency**: Same application code for both deployments
✅ **No Conflicts**: Different ports allow simultaneous operation
✅ **Shared Code**: Common database helpers module
✅ **Easy Testing**: Quick switch between environments
