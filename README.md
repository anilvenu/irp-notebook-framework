# IRP Notebook Framework

A business-user-friendly Jupyter notebook framework for managing IRP (Internal Risk Processing) workflows, replacing complex orchestration systems with simple, visual notebooks.

## Quick Start

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

2. Add .env file

3. **Start the environment**
```bash
chmod +x start.sh stop.sh
./start.sh
```

4. **Access JupyterLab**
- URL: http://localhost:8888


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

## Database Schema

TODO