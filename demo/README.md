# Batch Viewer Demo

Interactive dashboards for batch job data from the IRP Notebook Framework.

## Two Modes Available

### 1. **Dynamic Web Application (NEW)** - Recommended

Real-time web dashboard with live database queries.

#### Quick Start

```bash
# Install API dependencies
pip install -r demo/requirements-api.txt

# Start the web server
./demo/run_api.sh
```

Open your browser to: **http://localhost:8000** for demo application.

#### Features

- **Real-time data** - No regeneration needed, always shows latest
- **Multi-schema support** - Switch between demo, test, prod schemas via URL
- **Cycle selector** - Browse all cycles with status indicators
- **Live filtering** - Search and filter jobs and configurations
- **Error transparency** - Expandable error details when issues occur
- **Manual refresh** - Refresh button to reload data

#### Configuration

Configure via environment variables in `.env`:

```bash
DB_SERVER=localhost
DB_NAME=irp_db
DB_USER=irp_user
DB_PASSWORD=irp_pass
DB_PORT=5432
DB_SCHEMA=demo      # Default schema
PORT=8000           # API server port
```

#### URL Structure

```
http://localhost:8000/                           # Home (uses default schema)
http://localhost:8000/{schema}/                  # Cycle selection
http://localhost:8000/{schema}/cycle/{name}      # Cycle dashboard
http://localhost:8000/{schema}/cycle/{name}/batch/{id}  # Batch details
```

**Examples:**
- `http://localhost:8000/demo/` - Demo schema cycles
- `http://localhost:8000/demo/cycle/Analysis-2025-Q1` - Cycle dashboard
- `http://localhost:8000/demo/cycle/Analysis-2025-Q1/batch/1` - Batch detail

#### Health Check

```bash
curl http://localhost:8000/health
```

---

### 2. **Static HTML Generation** - Legacy

Generates static HTML files from database snapshots.

#### Quick Start

```bash
./demo/run_generator.sh
```

Open `demo/files/html_output/cycle/{cycle_name}/index.html` in your browser.

#### Files

- **generate_dashboards.py** - Generates HTML from database
- **run_generator.sh** - Runs the generator with proper environment
- **CSV files** - Test data (cycles, stages, steps, batches, jobs, etc.)

---

## Database Setup

Both modes require:
1. PostgreSQL database with IRP schema
2. Tables created from `workspace/helpers/db/init_database.sql`
3. Reporting views from `workspace/helpers/db/reporting_views.sql`

The demo includes scripts to:
1. Create `demo` schema
2. Load tables from SQL files
3. Insert CSV test data

---

## Troubleshooting

### Web Application

**Connection error**: Check database settings in `.env`
```bash
export DB_SERVER="localhost"
export DB_NAME="irp_db"
export DB_USER="irp_user"
export DB_PASSWORD="irp_pass"
```

**Import errors**: Install dependencies
```bash
pip install -r demo/requirements-api.txt
```

**No cycles found**:
- Verify schema has data: `SELECT * FROM demo.irp_cycle;`
- Create a cycle using `workspace/workflows/_Tools/Cycle Management/New Cycle.ipynb`

**Views not found**: Install reporting views
```bash
psql -U irp_user -d irp_db -f workspace/helpers/db/reporting_views.sql
```

### Static HTML Generator

**Connection error**: Check DB_SERVER in run_generator.sh
**Line ending issues**: Run `./win-unix.sh demo/run_generator.sh`
**CSV not found**: Ensure you're in project root

---

## Development

### Project Structure

```
demo/
├── app/                       # FastAPI web application
│   ├── app.py                 # Main application file
│   ├── templates/             # Jinja2 HTML templates
│   │   ├── base.html
│   │   ├── home.html
│   │   ├── cycle_dashboard.html
│   │   └── batch_detail.html
│   └── static/                # CSS and JavaScript
│       ├── css/style.css
│       └── js/scripts.js
├── requirements-api.txt       # API dependencies
├── run_api.sh                 # Launch script
├── generate_dashboards.py     # Static HTML generator (legacy)
└── README.md                  # This file
```