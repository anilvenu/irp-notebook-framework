#!/usr/bin/env python3
"""
IRP Dashboard Web Application
FastAPI-based dynamic dashboard for viewing batch and cycle data
"""

import os
import sys
import traceback
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Add workspace to path so we can import helpers
workspace_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(workspace_path))

from helpers import database as db


# ============================================================================
# APPLICATION SETUP
# ============================================================================

app = FastAPI(
    title="IRP Dashboard",
    description="Interactive dashboard for IRP Notebook Framework batch and cycle data",
    version="1.0.0"
)

# Get default schema from environment
DEFAULT_SCHEMA = os.getenv('DB_SCHEMA', 'demo')

# Setup static files and templates
BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


# ============================================================================
# DATA SERVICE FUNCTIONS
# ============================================================================

def get_available_schemas() -> List[str]:
    """Get list of schemas in the database that contain IRP tables"""
    try:
        query = """
            SELECT DISTINCT table_schema
            FROM information_schema.tables
            WHERE table_name = 'irp_cycle'
            ORDER BY table_schema
        """
        df = db.execute_query(query)
        return df['table_schema'].tolist() if not df.empty else []
    except Exception as e:
        print(f"Error getting schemas: {e}")
        return [DEFAULT_SCHEMA]


def get_cycle_list(schema: str) -> List[Dict[str, Any]]:
    """Get list of all cycles in a schema"""
    query = f"""
        SELECT
            id,
            cycle_name,
            status,
            created_ts,
            archived_ts
        FROM {schema}.irp_cycle
        ORDER BY
            CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END,
            created_ts DESC
    """
    df = db.execute_query(query)

    if df.empty:
        return []

    # Convert timestamps to strings to avoid NaT issues in templates
    cycles = df.to_dict('records')
    for cycle in cycles:
        cycle['created_ts'] = format_timestamp(cycle.get('created_ts'))
        cycle['archived_ts'] = format_timestamp(cycle.get('archived_ts'))

    return cycles


def get_current_cycle(schema: str) -> Optional[Dict[str, Any]]:
    """Get the current (most recent active) cycle"""
    query = f"""
        SELECT
            id,
            cycle_name,
            status,
            created_ts
        FROM {schema}.irp_cycle
        WHERE status = 'ACTIVE'
        ORDER BY created_ts DESC
        LIMIT 1
    """
    df = db.execute_query(query)

    # If no active cycle, get most recent cycle
    if df.empty:
        query = f"""
            SELECT
                id,
                cycle_name,
                status,
                created_ts
            FROM {schema}.irp_cycle
            ORDER BY created_ts DESC
            LIMIT 1
        """
        df = db.execute_query(query)

    return df.to_dict('records')[0] if not df.empty else None


def get_cycle_batches(cycle_name: str, schema: str) -> List[Dict[str, Any]]:
    """Get all batches for a cycle with summary statistics"""
    # Use schema-qualified view name
    query = f"""
        SELECT
            vb.batch_id,
            vb.batch_type,
            vb.batch_status,
            vb.reporting_status,
            vb.created_ts,
            vb.submitted_ts,
            vb.completed_ts,
            vb.total_configs,
            vb.non_skipped_configs as active_configs,
            vb.fulfilled_configs,
            vb.unfulfilled_configs,
            vb.skipped_configs,
            vb.total_jobs,
            vb.finished_jobs,
            vb.failed_jobs,
            vb.error_jobs,
            vb.skipped_jobs,
            st.stage_name,
            s.step_name
        FROM {schema}.v_irp_batch vb
        JOIN {schema}.irp_configuration cfg ON vb.configuration_id = cfg.id
        JOIN {schema}.irp_cycle c ON cfg.cycle_id = c.id
        JOIN {schema}.irp_step s ON vb.step_id = s.id
        JOIN {schema}.irp_stage st ON s.stage_id = st.id
        WHERE c.cycle_name = %s
        ORDER BY vb.batch_id
    """
    df = db.execute_query(query, (cycle_name,))
    return df.to_dict('records') if not df.empty else []


def query_batch_data(batch_id: int, schema: str) -> Optional[Dict[str, Any]]:
    """Query all data for a specific batch (ported from generate_dashboards.py)"""

    # Get batch summary using v_irp_batch view for reporting_status
    summary_df = db.execute_query(f"""
        SELECT
            vb.batch_id,
            vb.batch_type,
            vb.batch_status,
            vb.reporting_status,
            c.cycle_name,
            st.stage_name,
            s.step_name,
            vb.created_ts,
            vb.submitted_ts,
            vb.completed_ts,
            vb.total_configs,
            vb.non_skipped_configs as active_configs,
            vb.fulfilled_configs,
            vb.unfulfilled_configs,
            vb.skipped_configs,
            vb.total_jobs,
            vb.finished_jobs,
            vb.skipped_jobs,
            (vb.total_jobs - COALESCE(vb.skipped_jobs, 0)) as active_jobs,
            (vb.total_jobs - COALESCE(vb.finished_jobs, 0) - COALESCE(vb.skipped_jobs, 0)) as unfinished_jobs
        FROM {schema}.v_irp_batch vb
        JOIN {schema}.irp_configuration cfg ON vb.configuration_id = cfg.id
        JOIN {schema}.irp_cycle c ON cfg.cycle_id = c.id
        JOIN {schema}.irp_step s ON vb.step_id = s.id
        JOIN {schema}.irp_stage st ON s.stage_id = st.id
        WHERE vb.batch_id = %s
    """, (batch_id,))

    if summary_df.empty:
        return None

    summary = summary_df.to_dict('records')[0]

    # Convert numeric fields to integers
    int_fields = ['total_configs', 'active_configs', 'fulfilled_configs', 'unfulfilled_configs',
                  'skipped_configs', 'total_jobs', 'finished_jobs', 'skipped_jobs',
                  'active_jobs', 'unfinished_jobs']
    for field in int_fields:
        if field in summary and summary[field] is not None and not pd.isna(summary[field]):
            summary[field] = int(summary[field])

    # Get jobs
    jobs_df = db.execute_query(f"""
        SELECT
            j.id,
            j.moodys_workflow_id,
            j.status,
            j.skipped,
            j.report_status,
            j.next_best_action,
            j.age_hours,
            j.is_successful,
            j.needs_attention,
            j.parent_job_id,
            j.job_configuration_id,
            j.overridden,
            j.override_reason_txt,
            j.submitted_ts,
            j.completed_ts,
            j.created_ts,
            jc.job_configuration_data,
            parent_j.status as parent_job_status,
            parent_j.moodys_workflow_id as parent_moodys_id
        FROM {schema}.v_irp_job j
        LEFT JOIN {schema}.irp_job_configuration jc ON j.job_configuration_id = jc.id
        LEFT JOIN {schema}.irp_job parent_j ON j.parent_job_id = parent_j.id
        WHERE j.batch_id = %s
        ORDER BY j.id
    """, (batch_id,))

    jobs = jobs_df.to_dict('records')

    # Convert numeric fields to integers for each job
    job_int_fields = ['id', 'job_configuration_id', 'parent_job_id']
    for job in jobs:
        for field in job_int_fields:
            if field in job and job[field] is not None and not pd.isna(job[field]):
                job[field] = int(job[field])

    # Get configurations with active job details
    configs_df = db.execute_query(f"""
        SELECT
            jc.id as config_id,
            jc.job_configuration_data,
            jc.skipped,
            jc.overridden,
            vjc.config_report_status,
            vjc.total_jobs,
            vjc.unsubmitted_jobs,
            vjc.finished_jobs,
            vjc.failed_jobs,
            vjc.cancelled_jobs,
            vjc.error_jobs,
            vjc.progress_percent,
            vjc.has_failures,
            vjc.has_errors,
            vjc.has_unsubmitted,
            (vjc.total_jobs - vjc.finished_jobs - vjc.failed_jobs - vjc.cancelled_jobs) as active_jobs
        FROM {schema}.v_irp_job_configuration vjc
        JOIN {schema}.irp_job_configuration jc ON vjc.job_configuration_id = jc.id
        WHERE vjc.batch_id = %s
        ORDER BY jc.id
    """, (batch_id,))

    configs = configs_df.to_dict('records')

    # Convert numeric fields to integers for each config
    config_int_fields = ['config_id', 'total_jobs', 'unsubmitted_jobs', 'finished_jobs',
                        'failed_jobs', 'cancelled_jobs', 'error_jobs', 'active_jobs']
    for config in configs:
        for field in config_int_fields:
            if field in config and config[field] is not None and not pd.isna(config[field]):
                config[field] = int(config[field])

    return {
        'summary': summary,
        'jobs': jobs,
        'configs': configs
    }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_error_details(exc: Exception) -> Dict[str, str]:
    """Format exception for display with summary and full details"""
    error_type = type(exc).__name__
    error_message = str(exc)
    error_traceback = traceback.format_exc()

    # Create a helpful summary
    summary = f"{error_type}: {error_message}"

    # Add hints for common errors
    hints = []
    if "does not exist" in error_message.lower():
        hints.append("💡 The requested resource was not found. Check if the schema has been initialized.")
    if "relation" in error_message.lower() and "does not exist" in error_message.lower():
        hints.append("💡 Make sure the reporting views are installed. Run workspace/helpers/db/reporting_views.sql on your schema.")
    if "connection" in error_message.lower():
        hints.append("💡 Check your database connection settings (DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD).")

    return {
        'summary': summary,
        'details': error_traceback,
        'hints': hints
    }


def format_timestamp(ts):
    """Format timestamp for display"""
    import pandas as pd

    if ts is None or str(ts) == 'None':
        return 'N/A'
    if isinstance(ts, str):
        return ts
    # Handle pandas NaT (Not a Time)
    if pd.isna(ts):
        return 'N/A'
    try:
        return ts.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, AttributeError):
        return 'N/A'


# ============================================================================
# ROUTES
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Redirect to default schema home page"""
    return RedirectResponse(url=f"/{DEFAULT_SCHEMA}/")


@app.get("/{schema}/", response_class=HTMLResponse)
async def home(request: Request, schema: str):
    """Home page with cycle selector"""
    try:
        cycles = get_cycle_list(schema)
        current_cycle = get_current_cycle(schema)
        available_schemas = get_available_schemas()

        return templates.TemplateResponse(
            "home.html",
            {
                "request": request,
                "schema": schema,
                "cycles": cycles,
                "current_cycle": current_cycle,
                "available_schemas": available_schemas,
                "error": None
            }
        )
    except Exception as e:
        error = format_error_details(e)
        return templates.TemplateResponse(
            "home.html",
            {
                "request": request,
                "schema": schema,
                "cycles": [],
                "current_cycle": None,
                "available_schemas": [schema],
                "error": error
            }
        )


@app.get("/{schema}/cycle/{cycle_name}", response_class=HTMLResponse)
async def cycle_dashboard(request: Request, schema: str, cycle_name: str):
    """Cycle dashboard showing all batches"""
    try:
        batches = get_cycle_batches(cycle_name, schema)

        return templates.TemplateResponse(
            "cycle_dashboard.html",
            {
                "request": request,
                "schema": schema,
                "cycle_name": cycle_name,
                "batches": batches,
                "error": None,
                "format_timestamp": format_timestamp
            }
        )
    except Exception as e:
        error = format_error_details(e)
        return templates.TemplateResponse(
            "cycle_dashboard.html",
            {
                "request": request,
                "schema": schema,
                "cycle_name": cycle_name,
                "batches": [],
                "error": error
            }
        )


@app.get("/{schema}/cycle/{cycle_name}/batch/{batch_id}", response_class=HTMLResponse)
async def batch_detail(request: Request, schema: str, cycle_name: str, batch_id: int):
    """Batch detail page showing jobs and configurations"""
    try:
        data = query_batch_data(batch_id, schema)

        if not data:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found in schema '{schema}'")

        return templates.TemplateResponse(
            "batch_detail.html",
            {
                "request": request,
                "schema": schema,
                "cycle_name": cycle_name,
                "batch_id": batch_id,
                "data": data,
                "error": None,
                "format_timestamp": format_timestamp
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        error = format_error_details(e)
        return templates.TemplateResponse(
            "batch_detail.html",
            {
                "request": request,
                "schema": schema,
                "cycle_name": cycle_name,
                "batch_id": batch_id,
                "data": None,
                "error": error
            }
        )


@app.get("/health")
async def health_check():
    """Health check endpoint with database info and view verification"""
    health_status = {
        "status": "healthy",
        "database": {
            "server": os.getenv("DB_SERVER", "unknown"),
            "name": os.getenv("DB_NAME", "unknown"),
            "user": os.getenv("DB_USER", "unknown"),
            "port": os.getenv("DB_PORT", "5432"),
            "schema": os.getenv("DB_SCHEMA", DEFAULT_SCHEMA)
        },
        "connection": "unknown",
        "views": {}
    }

    try:
        # Test database connection
        db.execute_query("SELECT 1")
        health_status["connection"] = "connected"

        # Check for required views in the schema
        schema = os.getenv("DB_SCHEMA", DEFAULT_SCHEMA)
        required_views = ['v_irp_batch', 'v_irp_job', 'v_irp_job_configuration']

        view_check_query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type = 'VIEW'
            AND table_name IN ('v_irp_batch', 'v_irp_job', 'v_irp_job_configuration')
        """

        views_df = db.execute_query(view_check_query)
        existing_views = views_df['table_name'].tolist() if not views_df.empty else []

        for view in required_views:
            health_status["views"][view] = {
                "exists": view in existing_views,
                "schema": schema
            }

        # Check if all required views exist
        all_views_exist = all(view in existing_views for view in required_views)
        if not all_views_exist:
            health_status["status"] = "degraded"
            health_status["warning"] = "Some required views are missing"

        return health_status

    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["connection"] = "disconnected"
        health_status["error"] = str(e)
        return health_status


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
