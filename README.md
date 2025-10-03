# IRP Notebook Framework

A business-user-friendly Jupyter notebook framework for managing IRP (Internal Risk Processing) workflows, replacing complex orchestration systems with simple, visual notebooks.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- 4GB+ RAM available for containers
- Port 8888 (Jupyter) and 1433 (SQL Server) available

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd irp-notebook-framework
```

2. **Start the environment**
```bash
chmod +x start.sh stop.sh
./start.sh
```

3. **Access JupyterLab**
- URL: http://localhost:8888
- Token: `irp2024`

4. **Initialize the system**
- Open `workflows/_Tools/Database_Admin.ipynb`
- Run the initialization cells
- Open `workflows/_Tools/Create_New_Cycle.ipynb`
- Create your first cycle

## 📁 Project Structure

```
irp-notebook-framework/
├── docker-compose.yml          # Container orchestration
├── Dockerfile.jupyter          # Custom Jupyter image
├── requirements.txt           # Python dependencies
├── .env                      # Environment variables
├── start.sh                  # Start script
├── stop.sh                   # Stop script
└── workspace/                # Mounted workspace
    ├── system/              # System components
    │   ├── db/             # Database scripts
    │   ├── helpers/        # Helper notebooks
    │   └── config/         # Configuration
    └── workflows/          # Workflow management
        ├── _Template/      # Master template
        ├── _Tools/         # Management tools
        ├── _Archive/       # Completed cycles
        └── Active_*        # Current active cycle
```

## 🔄 Workflow Concept

### Cycles
A **cycle** represents a complete workflow execution (e.g., "Q1_2024_Analysis"). Only one cycle can be active at a time.

### Stages
Each cycle contains multiple **stages** (e.g., Setup, Extract, Process, Submit, Monitor)

### Steps
Each stage contains numbered **steps** implemented as Jupyter notebooks

### Execution Flow
1. Create a new cycle from template
2. Execute steps sequentially within each stage
3. Track progress automatically
4. Archive cycle when complete

## 📚 Key Components

### Helper Notebooks
Located in `system/helpers/`, these provide reusable functionality:

- **00_Config.ipynb**: Configuration and constants
- **01_Database.ipynb**: Database connectivity
- **02_CycleManager.ipynb**: Cycle lifecycle management
- **03_StepTracker.ipynb**: Step execution tracking
- **04_Display.ipynb**: Clean output formatting

### Tool Notebooks
Located in `workflows/_Tools/`:

- **Create_New_Cycle.ipynb**: Create and initialize new cycles
- **Database_Admin.ipynb**: Database management and initialization
- **System_Status.ipynb**: View current status and progress

### Template Structure
The `_Template` directory contains the master template copied to each new cycle:

```
_Template/
├── notebooks/
│   ├── Stage_01_Setup/
│   │   ├── Step_01_Initialize.ipynb
│   │   ├── Step_02_Validate.ipynb
│   │   └── README.md
│   └── Stage_02_*/
├── files/
│   ├── excel_configuration/
│   └── extracted_configurations/
└── logs/
```

## 💾 Database Schema

### Core Tables
- **irp_cycle**: Cycle management
- **irp_stage**: Stage definitions
- **irp_step**: Step definitions
- **irp_step_run**: Execution history
- **irp_batch**: Batch job management (Phase 2)
- **irp_job**: Individual job tracking (Phase 2)

## 🛠️ Usage Guide

### Creating a New Cycle

1. Open `workflows/_Tools/Create_New_Cycle.ipynb`
2. Run cells to check current status
3. Enter a unique cycle name
4. Execute creation (archives previous active cycle)

### Running Steps

1. Navigate to `workflows/Active_<CycleName>/notebooks/`
2. Open stage folders in order
3. Execute step notebooks sequentially
4. Each notebook tracks its own execution

### Step Tracking

Each step notebook includes automatic tracking:

```python
step = StepTracker(
    cycle_name="{{CYCLE_NAME}}",
    stage_num=1,
    step_num=1,
    idempotent=False,  # Can this step be re-run?
    auto_start=True
)
```

- Non-idempotent steps can only run once
- Idempotent steps can be re-run safely
- All executions are logged

### Monitoring Progress

1. Open `workflows/_Tools/System_Status.ipynb`
2. View active cycle status
3. Check step completion progress
4. Review recent activity

## 🔧 Configuration

### Environment Variables (.env)
```
JUPYTER_TOKEN=irp2024
DB_NAME=IRP_DB
DB_USER=sa
DB_PASSWORD=IRP_Pass2024!
DB_SERVER=sqlserver
DB_PORT=1433
```

### Database Connection
The framework uses SQL Server with automatic initialization. Connection details are configured via environment variables.

## 📈 Phase 2: Batch Processing

The framework is designed to support batch job processing in Phase 2:

- Load configurations from Excel files
- Create batch jobs in database
- Submit to Moody's API
- Poll for status updates
- Track completion

## 🐛 Troubleshooting

### Database Connection Issues
1. Check if SQL Server container is running: `docker ps`
2. Verify credentials in `.env` file
3. Run initialization in `Database_Admin.ipynb`

### Missing Notebooks
1. Ensure template directory exists
2. Check if cycle was created properly
3. Verify file permissions

### Step Execution Errors
1. Check step status in `System_Status.ipynb`
2. Review error messages in step_run table
3. Mark step as idempotent if re-run needed

## 🚦 Best Practices

1. **Always check status** before creating new cycles
2. **Run steps sequentially** within stages
3. **Document changes** in step notebooks
4. **Archive completed cycles** regularly
5. **Back up database** before major operations

## 🔄 Migration to Databricks

The framework is designed for easy migration to Databricks:

1. **Database**: Point to Azure SQL Database
2. **Storage**: Use DBFS instead of local filesystem
3. **Notebooks**: Import directly into workspace
4. **Helpers**: Package as library
5. **Authentication**: Use Databricks secrets

## 📝 License

[Your License Here]

## 🤝 Support

For issues or questions:
1. Check the troubleshooting section
2. Review notebook documentation
3. Contact the development team