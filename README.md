# IRP Notebook Framework

A business-user-friendly Jupyter notebook framework for managing IRP (Internal Risk Processing) workflows, replacing complex orchestration systems with simple, visual notebooks.

✓
✗
⚠

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- 10GB+ RAM available for containers (PostgreSQL + SQL Server Express)
- Ports available: 8888 (Jupyter), 5432 (PostgreSQL), 1433 (SQL Server)

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd irp-notebook-framework
```

2. Add .env file

3. **Start the environment**
```bash
chmod +x start.sh stop.sh test.sh
./start.sh
```

4. **Access JupyterLab**
- URL: http://localhost:8888

This will launch JupyterLab and you should see the following directories

```
helper/
workflows/
``` 




## Project Structure

```
irp-notebook-framework/
├── docker-compose.yml      # Container orchestration
├── Dockerfile.jupyter      # Custom Jupyter image
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables
├── start.sh                # Start script
├── stop.sh                 # Stop script
├── test.sh                 # Stop script
└── workspace/              # Mounted workspace
    ├── helper/             # Helper components
    │   ├── db/             # Database scripts
    └── workflows/          # Workflow management
        ├── _Template/      # Master template
        ├── _Tools/         # Management tools
        ├── _Archive/       # Completed cycles
        └── Active_*        # Current / active cycle (copied from _Template)
```

## Workflow Concept

### Cycles
A **cycle** represents a complete workflow execution (e.g., "Q1_2024_Analysis"). Only one cycle can be active at a time.

### Stages
Each cycle contains multiple **stages** (e.g., Setup, Extract, Process, Submit, Monitor)

### Steps
Each stage contains numbered **steps** implemented as Jupyter notebooks

### Configuration

TODO

### Execution Flow
0. Initialize the database
1. Create a new cycle from template
2. Execute steps sequentially within each stage
3. Track progress automatically
4. Archive cycle when complete

## Key Components

### Helpers
Located in `workspace/helpers/`, these provide reusable functionality:

### Tool Notebooks
Located in `workspace/workflows/_Tools/`:

### Template Structure
The `_Template` directory contains the master template copied to each new cycle:

```
_Template/
├── notebooks/
│   ├── Stage_01_<stage name>>/
│   │   ├── Step_01_<step name>>.ipynb
│   │   ├── Step_02_<step name>>.ipynb
│   │   └── README.md
│   └── Stage_02_*/
├── files/
│   ├── excel_configuration/
│   └── data/
└── logs/
```

## Database Integration

The framework integrates with two database systems:

### PostgreSQL (Framework Metadata)
- **Purpose**: Stores workflow metadata, cycles, stages, steps, batches, jobs, configurations
- **Container**: `irp-postgres`
- **Port**: 5432
- **Database**: `irp_db` (production), `test_db` (testing)
- **Configuration**: Set via `DB_*` environment variables in `.env`

### SQL Server (Business Data)
- **Purpose**: External MSSQL databases for data extraction, queries, and operations
- **Test Container**: `irp-sqlserver` (SQL Server Express 2022)
- **Port**: 1433
- **Configuration**: Set via `MSSQL_*` environment variables in `.env`

### MSSQL Setup and Usage

The framework supports connections to multiple external MSSQL databases:

#### 1. Configure Connections

Edit `.env` file to add your MSSQL database connections:

```bash
# Production MSSQL Connection Example
MSSQL_AWS_DW_SERVER=your-server.company.com
MSSQL_AWS_DW_DATABASE=DataWarehouse
MSSQL_AWS_DW_USER=your_username
MSSQL_AWS_DW_PASSWORD=your_password

# Additional connections
MSSQL_ANALYTICS_SERVER=analytics-server.company.com
MSSQL_ANALYTICS_DATABASE=Analytics
MSSQL_ANALYTICS_USER=your_username
MSSQL_ANALYTICS_PASSWORD=your_password
```

#### 2. Create SQL Scripts

Store SQL scripts in `workspace/sql/` directory:

```sql
-- workspace/sql/extract_policies.sql
-- Extract policy data for a specific cycle
-- Parameters: {cycle_name}, {run_date}

SELECT p.policy_id, p.policy_name, p.premium
FROM policies p
WHERE p.cycle_name = {cycle_name}
  AND p.extraction_date = {run_date};
```

#### 3. Use in Notebooks

Execute queries and scripts from your Jupyter notebooks:

```python
from helpers.sqlserver import execute_query, execute_script_file

# Execute query with parameters
df = execute_query(
    "SELECT * FROM portfolios WHERE value > {min_value}",
    params={'min_value': 1000000},
    connection='AWS_DW'
)

# Execute SQL script from file
rows_affected = execute_script_file(
    'extract_policies.sql',
    params={'cycle_name': 'Q1-2025', 'run_date': '2025-01-15'},
    connection='AWS_DW'
)

print(f"Extracted {len(df)} portfolios")
print(f"Processed {rows_affected} policy records")
```

#### 4. Available Functions

- `execute_query(query, params, connection)` - SELECT queries → DataFrame
- `execute_scalar(query, params, connection)` - Single value queries
- `execute_command(query, params, connection)` - INSERT/UPDATE/DELETE
- `execute_query_from_file(file_path, params, connection)` - Query from SQL file
- `execute_script_file(file_path, params, connection)` - Execute SQL script
- `test_connection(connection)` - Verify database connectivity

See `workspace/sql/README.md` for detailed SQL script guidelines and best practices.

#### 5. Testing

The SQL Server Express container is available for local development and testing:

```bash
# Start all containers including SQL Server
./start.sh

# Test MSSQL integration
./test.sh workspace/tests/test_sqlserver.py -v

# Connect to SQL Server for debugging
docker exec -it irp-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P TestPass123!
```

## Database Schema

### PostgreSQL Tables

TODO