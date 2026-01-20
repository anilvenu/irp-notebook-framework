# Setup Guide

This guide covers how to set up and run the IRP Notebook Framework.

## Prerequisites

### Required Software

| Software | Version | Notes |
|----------|---------|-------|
| Docker | Latest | Docker Desktop (Windows/Mac) or Docker Engine (Linux) |
| Docker Compose | v2+ | Included with Docker Desktop |
| Git | Any | For cloning the repository |

### System Requirements

| Resource | Minimum | Notes |
|----------|---------|-------|
| RAM | 4GB | Available for Docker containers |
| Disk | 5GB | For Docker images and data volumes |

### Network Ports

The following ports must be available on your machine:

| Port | Service | Purpose |
|------|---------|---------|
| 8888 | JupyterLab | Main notebook interface |
| 8001 | FastAPI | Dashboard and monitoring |
| 5432 | PostgreSQL | Workflow database |

### What You DON'T Need to Install

The following are provided by Docker containers - no local installation required:

- PostgreSQL
- Python
- JupyterLab
- ODBC drivers

### External Services (Configuration Required)

Configuration is required for:

| Service | `.env` Variables |
|---------|-----------|
| Moody's Risk Modeler API | `RISK_MODELER_*` |
| SQL Server (Databridge) | `MSSQL_DATABRIDGE_*` |
| SQL Server (Assurant)- | `MSSQL_ASSURANT_*` |
| Microsoft Teams | `TEAMS_*` |

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd irp-notebook-framework
```

### 2. Create Environment File

```bash
cp .env.example .env
```

### 3. Configure Environment Variables

Edit `.env` and configure all required variables. See [Environment Configuration](#environment-configuration) below.

### 4. Start Services

```bash
# Make scripts executable (first time only, Linux/Mac)
chmod +x start.sh stop.sh test.sh

# Start all services
./start.sh
```

### 5. Access the Application

Open your browser to: **http://localhost:8888**

## Environment Configuration

Edit the `.env` file with your specific values. All sections below are required unless noted.

### PostgreSQL Database

These configure the local PostgreSQL container. You can use the defaults from `.env.example`:

```
DB_NAME=irp_db
DB_USER=irp_user
DB_PASSWORD=irp_pass
DB_SERVER=postgres
DB_PORT=5432
```

### Moody's Risk Modeler API

```
RISK_MODELER_BASE_URL=https://api-euw1.rms-ppe.com
RISK_MODELER_API_KEY=your_api_key
RISK_MODELER_RESOURCE_GROUP_ID=your_group_id
```

### SQL Server Connections

Connection details for external SQL Server databases:

```
# ODBC settings
MSSQL_DRIVER=ODBC Driver 18 for SQL Server
MSSQL_TRUST_CERT=yes
MSSQL_TIMEOUT=30

# Databridge connection
MSSQL_DATABRIDGE_AUTH_TYPE=SQL
MSSQL_DATABRIDGE_SERVER=your-server.database.windows.net
MSSQL_DATABRIDGE_USER=your_username
MSSQL_DATABRIDGE_PASSWORD=your_password

# Assurant connection
MSSQL_ASSURANT_AUTH_TYPE=SQL
MSSQL_ASSURANT_SERVER=your-server.database.windows.net
MSSQL_ASSURANT_USER=your_username
MSSQL_ASSURANT_PASSWORD=your_password
```

### Microsoft Teams Notifications

```
TEAMS_WEBHOOK_URL=your_webhook_url
TEAMS_NOTIFICATION_ENABLED=true
TEAMS_DEFAULT_DASHBOARD_URL=http://localhost:8001
TEAMS_DEFAULT_JUPYTERLAB_URL=http://localhost:8888
```

### Kerberos / Windows Authentication (If Required)

If your SQL Server uses Windows Authentication instead of SQL auth:

```
KERBEROS_ENABLED=true
KRB5_REALM=YOUR.DOMAIN.COM
KRB5_KDC=your-domain-controller.domain.com
KRB5_PRINCIPAL=svc_jupyter@YOUR.DOMAIN.COM
KRB5_KEYTAB=/etc/krb5/svc_jupyter.keytab
```

Additional setup steps for Kerberos:
1. Copy `config/krb5.conf.example` to `config/krb5.conf` and customize
2. Place your keytab file in `keytabs/` directory

## Services

| Service | URL | Purpose |
|---------|-----|---------|
| JupyterLab | http://localhost:8888 | Main notebook interface |
| Dashboard | http://localhost:8001 | Monitoring and status |
| PostgreSQL | localhost:5432 | Workflow database (internal) |

## Stopping the Application

```bash
./stop.sh
```

This stops all containers but preserves your data.

To remove all data including the database:

```bash
docker-compose down -v
```

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
