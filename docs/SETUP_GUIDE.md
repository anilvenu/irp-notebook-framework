# Setup Guide

This guide covers how to set up and run the IRP Notebook Framework.

## Prerequisites

- **Docker and Docker Compose** - Required for running all services
- **10GB+ RAM** available for containers
- **Available ports**: 8888, 5432, 8001
- **Bash shell** (Linux/Mac) or Git Bash/WSL2 (Windows)

## Quick Start

### 1. Create Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

### 2. Configure Required Variables

Edit `.env` and set the following required variables:

**Database (PostgreSQL):**
```
DB_NAME=irp_db
DB_USER=irp_user
DB_PASSWORD=your_secure_password
DB_PORT=5432
```

**Risk Modeler API:**
```
RISK_MODELER_BASE_URL=https://api-euw1.rms-ppe.com
RISK_MODELER_API_KEY=your_api_key
RISK_MODELER_RESOURCE_GROUP_ID=your_group_id
```

### 3. Start Services

```bash
# Make scripts executable (first time only)
chmod +x start.sh stop.sh test.sh

# Start all services
./start.sh
```

### 4. Access the Application

Open your browser to: **http://localhost:8888**

This opens JupyterLab where you can run workflow notebooks.

## Services

| Service | URL | Purpose |
|---------|-----|---------|
| JupyterLab | http://localhost:8888 | Main notebook interface |
| Dashboard | http://localhost:8001 | Monitoring and status |
| PostgreSQL | localhost:5432 | Workflow database |

## Stopping the Application

```bash
./stop.sh
```

This stops all containers but preserves your data. To remove all data including the database:

```bash
docker-compose down -v
```

## Optional Configuration

The following can be configured in `.env` if needed:

### SQL Server Connections

For connecting to external SQL Server databases:

```
MSSQL_DRIVER=ODBC Driver 18 for SQL Server
MSSQL_DATABRIDGE_SERVER=your-server.database.windows.net
MSSQL_DATABRIDGE_USER=your_username
MSSQL_DATABRIDGE_PASSWORD=your_password
```

### Teams Notifications

```
TEAMS_WEBHOOK_URL=your_webhook_url
TEAMS_NOTIFICATION_ENABLED=true
```

### Kerberos (Windows Authentication)

See `.env.example` for Kerberos configuration options when connecting to SQL Server with Windows Authentication.

## Troubleshooting

### "ERROR: .env file missing"

Create the `.env` file by copying from the example:
```bash
cp .env.example .env
```

### "Port already in use"

Another application is using port 8888, 5432, or 8001. Either stop the conflicting application or change the port in `docker-compose.yml`.

### "Docker daemon not running"

Start Docker Desktop (Windows/Mac) or the Docker service (Linux):
```bash
# Linux
sudo systemctl start docker
```

### "Database initialization failed"

Check that PostgreSQL container is healthy:
```bash
docker ps
docker logs irp-postgres
```

### Container fails to start

View container logs for details:
```bash
docker-compose logs jupyter
docker-compose logs postgres
```
