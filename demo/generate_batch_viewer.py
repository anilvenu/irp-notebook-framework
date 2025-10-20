#!/usr/bin/env python3
"""
Batch Viewer Demo Generator

This script:
1. Uses helpers.database module for all database operations
2. Loads test data from CSV files
3. Generates an interactive HTML viewer (demo.html)
"""

from curses import echo
import sys
from pathlib import Path
import json
import csv

# Add workspace to path so we can import helpers
workspace_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(workspace_path))

from helpers import database as db


SCHEMA = 'demo'


def initialize_schema():
    """Initialize demo schema with base tables and views"""
    print("\n" + "="*60)
    print("INITIALIZING DEMO SCHEMA")
    print("="*60)

    # Create schema
    try:
        db.execute_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        print(f"✓ Schema '{SCHEMA}' created/verified")
    except Exception as e:
        print(f"✗ Error creating schema: {e}")
        return False

    # Get paths to SQL files (use actual filesystem paths, not workspace_path from constants)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    init_sql = project_root / 'workspace' / 'helpers' / 'db' / 'init_database.sql'
    views_sql = project_root / 'workspace' / 'helpers' / 'db' / 'reporting_views.sql'

    # Execute initialization script
    try:
        print(f"Executing {init_sql}...")
        with open(init_sql, 'r') as f:
            sql_script = f.read()
        db.execute_command(sql_script, schema=SCHEMA)
        print(f"✓ Database tables created")
    except Exception as e:
        print(f"✗ Error executing init script: {e}")
        return False

    # Execute views script
    try:
        print(f"Executing {views_sql}...")
        with open(views_sql, 'r') as f:
            sql_script = f.read()
        db.execute_command(sql_script, schema=SCHEMA)
        print(f"✓ Reporting views created")
    except Exception as e:
        print(f"✗ Error executing views script: {e}")
        return False

    print("✓ Schema initialization complete")
    return True


def load_csv_data(csv_file, table_name):
    """Load data from CSV file into table"""
    csv_path = Path(__file__).parent / csv_file

    if not csv_path.exists():
        print(f"✗ CSV file not found: {csv_file}")
        return False

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print(f"⚠ No data in {csv_file}")
            return True

        # Get column names from first row
        columns = list(rows[0].keys())

        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Prepare data tuples
        data_tuples = []
        jsonb_columns = []

        for row in rows:
            values = []
            for i, col in enumerate(columns):
                val = row[col]

                # Handle NULL values
                if val == '' or val.lower() == 'null':
                    values.append(None)
                # Handle boolean values
                elif val.lower() in ('true', 'false'):
                    values.append(val.lower() == 'true')
                # Handle JSONB columns (detect by column name or content)
                elif col.endswith('_data') or (val.startswith('{') and val.endswith('}')):
                    values.append(val)  # Keep as string, will be converted
                    if i not in jsonb_columns:
                        jsonb_columns.append(i)
                else:
                    values.append(val)

            data_tuples.append(tuple(values))

        # Bulk insert
        ids = db.bulk_insert(query, data_tuples, jsonb_columns=jsonb_columns if jsonb_columns else None, schema=SCHEMA)
        print(f"✓ Loaded {len(ids)} rows into {table_name}")
        return True

    except Exception as e:
        print(f"✗ Error loading {csv_file}: {e}")
        import traceback
        traceback.print_exc()
        return False


def clear_existing_data():
    """Clear existing test data"""
    print("\n" + "="*60)
    print("CLEARING EXISTING DATA")
    print("="*60)

    try:
        db.execute_command(
            "TRUNCATE irp_job, irp_job_configuration, irp_batch, irp_step, irp_stage, irp_configuration, irp_cycle CASCADE",
            schema=SCHEMA
        )
        print("✓ Existing data cleared")
        return True
    except Exception as e:
        print(f"✗ Error clearing data: {e}")
        return False


def load_test_data():
    """Load all test data from CSV files"""
    print("\n" + "="*60)
    print("LOADING TEST DATA FROM CSV FILES")
    print("="*60)

    # Load in dependency order
    tables = [
        ('files/csv_data/cycles.csv', 'irp_cycle'),
        ('files/csv_data/stages.csv', 'irp_stage'),
        ('files/csv_data/steps.csv', 'irp_step'),
        ('files/csv_data/configurations.csv', 'irp_configuration'),
        ('files/csv_data/batches.csv', 'irp_batch'),
        ('files/csv_data/job_configurations.csv', 'irp_job_configuration'),
        ('files/csv_data/jobs.csv', 'irp_job'),
    ]

    for csv_file, table_name in tables:
        if not load_csv_data(csv_file, table_name):
            return False

    print("\n✓ All test data loaded successfully")
    return True


def query_batch_data(batch_id):
    """Query all data for a specific batch"""
    # Get batch summary
    summary_df = db.execute_query("""
        SELECT
            b.id as batch_id,
            b.batch_type,
            b.status as batch_status,
            c.cycle_name,
            st.stage_name,
            s.step_name,
            b.created_ts,
            b.submitted_ts,
            b.completed_ts,
            COUNT(DISTINCT jc.id) as total_configs,
            COUNT(DISTINCT jc.id) FILTER (WHERE NOT jc.skipped) as active_configs,
            COUNT(j.id) as total_jobs,
            COUNT(j.id) FILTER (WHERE NOT j.skipped) as active_jobs
        FROM irp_batch b
        JOIN irp_configuration cfg ON b.configuration_id = cfg.id
        JOIN irp_cycle c ON cfg.cycle_id = c.id
        JOIN irp_step s ON b.step_id = s.id
        JOIN irp_stage st ON s.stage_id = st.id
        LEFT JOIN irp_job_configuration jc ON b.id = jc.batch_id
        LEFT JOIN irp_job j ON jc.id = j.job_configuration_id
        WHERE b.id = %s
        GROUP BY b.id, b.batch_type, b.status, c.cycle_name, st.stage_name, s.step_name,
                 b.created_ts, b.submitted_ts, b.completed_ts
    """, (batch_id,), schema=SCHEMA)

    if summary_df.empty:
        print(f"✗ Batch {batch_id} not found")
        return None

    summary = summary_df.to_dict('records')[0]

    # Get jobs
    jobs_df = db.execute_query("""
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
        FROM v_irp_job j
        LEFT JOIN irp_job_configuration jc ON j.job_configuration_id = jc.id
        LEFT JOIN irp_job parent_j ON j.parent_job_id = parent_j.id
        WHERE j.batch_id = %s
        ORDER BY j.id
    """, (batch_id,), schema=SCHEMA)

    jobs = jobs_df.to_dict('records')

    # Get configurations with active job details
    configs_df = db.execute_query("""
        SELECT
            jc.id as config_id,
            jc.job_configuration_data,
            jc.skipped,
            jc.overridden,
            vjc.config_report_status,
            vjc.total_jobs,
            vjc.finished_jobs,
            vjc.failed_jobs,
            vjc.error_jobs,
            vjc.progress_percent,
            vjc.has_failures,
            vjc.has_errors,
            vjc.has_unsubmitted,
            -- Count active jobs (not terminal)
            (vjc.total_jobs - vjc.finished_jobs - vjc.failed_jobs - vjc.cancelled_jobs) as active_jobs
        FROM v_irp_job_configuration vjc
        JOIN irp_job_configuration jc ON vjc.job_configuration_id = jc.id
        WHERE vjc.batch_id = %s
        ORDER BY jc.id
    """, (batch_id,), schema=SCHEMA)

    configs = configs_df.to_dict('records')

    return {
        'summary': summary,
        'jobs': jobs,
        'configs': configs
    }


def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None or str(ts) == 'None':
        return 'N/A'
    if isinstance(ts, str):
        return ts
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def generate_html(batch_id, data):
    """Generate HTML for batch viewer"""
    summary = data['summary']
    jobs = data['jobs']
    configs = data['configs']

    # Convert data to JSON for JavaScript
    jobs_json = json.dumps(jobs, default=str)
    configs_json = json.dumps(configs, default=str)

    # Calculate statistics
    total_jobs = int(summary['total_jobs'])
    active_jobs = int(summary['active_jobs'])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Batch {batch_id} - {summary['batch_type']}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}

        .page-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
            margin-bottom: 30px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}

        .page-header h1 {{
            font-size: 32px;
            margin-bottom: 10px;
        }}

        .page-header .subtitle {{
            font-size: 16px;
            opacity: 0.9;
        }}

        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .info-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .info-card h3 {{
            font-size: 12px;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .info-card .value {{
            font-size: 24px;
            font-weight: 600;
            color: #333;
        }}

        .info-card .sub-value {{
            font-size: 14px;
            color: #888;
            margin-top: 5px;
        }}

        .status-badge {{
            display: inline-block;
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}

        .status-ACTIVE {{ background: #e3f2fd; color: #1976d2; }}
        .status-COMPLETED {{ background: #e8f5e9; color: #388e3c; }}
        .status-INITIATED {{ background: #fff3e0; color: #f57c00; }}
        .status-FAILED {{ background: #ffebee; color: #d32f2f; }}
        .status-CANCELLED {{ background: #f5f5f5; color: #757575; }}
        .status-ERROR {{ background: #ffebee; color: #c62828; }}
        .status-FINISHED {{ background: #e8f5e9; color: #2e7d32; }}
        .status-RUNNING {{ background: #e1f5fe; color: #0288d1; }}
        .status-QUEUED, .status-PENDING, .status-SUBMITTED {{ background: #f3e5f5; color: #7b1fa2; }}
        .status-SKIPPED {{ background: #fafafa; color: #616161; }}
        .status-UNSUBMITTED {{ background: #fff9c4; color: #f57f17; }}
        .status-FULFILLED {{ background: #e8f5e9; color: #2e7d32; }}
        .status-UNFULFILLED {{ background: #fff3e0; color: #f57c00; }}

        .tabs {{
            display: flex;
            gap: 0;
            margin-bottom: 0;
            background: white;
            border-radius: 8px 8px 0 0;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .tab {{
            flex: 1;
            padding: 15px 30px;
            cursor: pointer;
            background: white;
            border: none;
            font-size: 14px;
            font-weight: 600;
            color: #666;
            transition: all 0.2s;
            border-bottom: 3px solid transparent;
        }}

        .tab.active {{
            color: #667eea;
            border-bottom-color: #667eea;
            background: #fafafa;
        }}

        .tab:hover:not(.active) {{
            background: #f5f5f5;
        }}

        .tab-content {{
            display: none;
            background: white;
            padding: 20px;
            border-radius: 0 0 8px 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .tab-content.active {{
            display: block;
        }}

        .search-box {{
            width: 100%;
            padding: 12px;
            margin: 10px 0 20px 0;
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            font-size: 14px;
            transition: border-color 0.2s;
        }}

        .search-box:focus {{
            outline: none;
            border-color: #667eea;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
        }}

        th {{
            background: #fafafa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 12px;
            color: #666;
            border-bottom: 2px solid #e0e0e0;
            cursor: pointer;
            user-select: none;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        th:hover {{
            background: #f0f0f0;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 13px;
        }}

        tr:hover {{
            background: #fafafa;
        }}

        .json-preview {{
            max-width: 250px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            color: #666;
            cursor: help;
            padding: 4px 8px;
            background: #f5f5f5;
            border-radius: 4px;
        }}

        .age-badge {{
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 12px;
            background: #f5f5f5;
            color: #666;
            font-weight: 500;
        }}

        .needs-attention {{
            color: #d32f2f;
            font-weight: 600;
        }}

        .empty-state {{
            text-align: center;
            padding: 40px;
            color: #999;
        }}
    </style>
</head>
<body>
    <div class="page-header">
        <h1>Batch {batch_id}: {summary['batch_type']}</h1>
        <div class="subtitle">Cycle: {summary['cycle_name']} | Stage: {summary['stage_name']} | Step: {summary['step_name']}</div>
    </div>

    <div class="info-grid">
        <div class="info-card">
            <h3>Batch Status</h3>
            <div class="value">
                <span class="status-badge status-{summary['batch_status']}">{summary['batch_status']}</span>
            </div>
        </div>

        <div class="info-card">
            <h3>Jobs Summary</h3>
            <div class="value">{total_jobs} Total</div>
            <div class="sub-value">{active_jobs} active, {total_jobs - active_jobs} skipped</div>
        </div>

        <div class="info-card">
            <h3>Configurations</h3>
            <div class="value">{summary['total_configs']}</div>
            <div class="sub-value">{summary['active_configs']} active</div>
        </div>

        <div class="info-card">
            <h3>Created</h3>
            <div class="value" style="font-size: 16px;">{format_timestamp(summary['created_ts'])}</div>
        </div>

        <div class="info-card">
            <h3>Submitted</h3>
            <div class="value" style="font-size: 16px;">
                {format_timestamp(summary['submitted_ts'])}
            </div>
        </div>

        <div class="info-card">
            <h3>Completed</h3>
            <div class="value" style="font-size: 16px;">
                {format_timestamp(summary['completed_ts'])}
            </div>
        </div>
    </div>

    <div class="tabs">
        <button class="tab active" onclick="switchTab('jobs')">Jobs ({len(jobs)})</button>
        <button class="tab" onclick="switchTab('configurations')">Configurations ({len(configs)})</button>
    </div>

    <div id="jobs-content" class="tab-content active">
        <input type="text" class="search-box" id="jobSearch" placeholder="Search jobs by ID, Moody's ID, status, configuration..." onkeyup="filterTable('jobSearch', 'jobsTable')">
        <table id="jobsTable">
            <thead>
                <tr>
                    <th onclick="sortTable('jobsTable', 0)">ID ↕</th>
                    <th onclick="sortTable('jobsTable', 1)">Moody's Job ID ↕</th>
                    <th onclick="sortTable('jobsTable', 2)">Status ↕</th>
                    <th onclick="sortTable('jobsTable', 3)">Report Status ↕</th>
                    <th onclick="sortTable('jobsTable', 4)">Configuration ↕</th>
                    <th onclick="sortTable('jobsTable', 5)">Overridden ↕</th>
                    <th onclick="sortTable('jobsTable', 6)">Age (hrs) ↕</th>
                    <th onclick="sortTable('jobsTable', 7)">Next Action ↕</th>
                    <th onclick="sortTable('jobsTable', 8)">Parent Job ↕</th>
                </tr>
            </thead>
            <tbody id="jobsTableBody">
            </tbody>
        </table>
    </div>

    <div id="configurations-content" class="tab-content">
        <input type="text" class="search-box" id="configSearch" placeholder="Search configurations..." onkeyup="filterTable('configSearch', 'configsTable')">
        <table id="configsTable">
            <thead>
                <tr>
                    <th onclick="sortTable('configsTable', 0)">Config ID ↕</th>
                    <th onclick="sortTable('configsTable', 1)">Status ↕</th>
                    <th onclick="sortTable('configsTable', 2)">Configuration Data ↕</th>
                    <th onclick="sortTable('configsTable', 3)">Total Jobs ↕</th>
                    <th onclick="sortTable('configsTable', 4)">Active ↕</th>
                    <th onclick="sortTable('configsTable', 5)">Finished ↕</th>
                    <th onclick="sortTable('configsTable', 6)">Failed ↕</th>
                    <th onclick="sortTable('configsTable', 7)">Progress % ↕</th>
                    <th onclick="sortTable('configsTable', 8)">Flags ↕</th>
                    <th onclick="sortTable('configsTable', 9)">Skipped ↕</th>
                </tr>
            </thead>
            <tbody id="configsTableBody">
            </tbody>
        </table>
    </div>

    <script>
        const jobs = {jobs_json};
        const configs = {configs_json};

        function switchTab(tabName) {{
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

            event.target.classList.add('active');
            document.getElementById(tabName + '-content').classList.add('active');
        }}

        function renderJobs() {{
            const tbody = document.getElementById('jobsTableBody');
            if (jobs.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No jobs found</td></tr>';
                return;
            }}

            tbody.innerHTML = jobs.map(job => `
                <tr>
                    <td>${{job.id}}</td>
                    <td>${{job.moodys_workflow_id || '-'}}</td>
                    <td><span class="status-badge status-${{job.status}}">${{job.status}}</span></td>
                    <td><span class="status-badge status-${{job.report_status}}">${{job.report_status || '-'}}</span></td>
                    <td><div class="json-preview" title="${{JSON.stringify(job.job_configuration_data)}}">${{JSON.stringify(job.job_configuration_data)}}</div></td>
                    <td>${{job.overridden ? '✓ ' + (job.override_reason_txt || '') : '-'}}</td>
                    <td>${{job.age_hours ? '<span class="age-badge">' + Math.round(job.age_hours * 100) / 100 + 'h</span>' : '-'}}</td>
                    <td class="${{job.needs_attention ? 'needs-attention' : ''}}">${{job.next_best_action || '-'}}</td>
                    <td>${{job.parent_job_id || '-'}}</td>
                </tr>
            `).join('');
        }}

        function renderConfigs() {{
            const tbody = document.getElementById('configsTableBody');
            if (configs.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="10" class="empty-state">No configurations found</td></tr>';
                return;
            }}

            tbody.innerHTML = configs.map(config => {{
                // Build flags string
                const flags = [];
                if (config.has_failures) flags.push('<span style="color: #d32f2f;">⚠ Failures</span>');
                if (config.has_errors) flags.push('<span style="color: #c62828;">⚠ Errors</span>');
                if (config.has_unsubmitted) flags.push('<span style="color: #f57f17;">⚠ Unsubmitted</span>');
                const flagsHtml = flags.length > 0 ? flags.join('<br>') : '-';

                return `
                    <tr>
                        <td>${{config.config_id}}</td>
                        <td><span class="status-badge status-${{config.config_report_status}}">${{config.config_report_status}}</span></td>
                        <td><div class="json-preview" title="${{JSON.stringify(config.job_configuration_data)}}">${{JSON.stringify(config.job_configuration_data)}}</div></td>
                        <td>${{config.total_jobs}}</td>
                        <td style="color: #0288d1; font-weight: 600;">${{config.active_jobs || 0}}</td>
                        <td style="color: #2e7d32; font-weight: 600;">${{config.finished_jobs}}</td>
                        <td style="color: #d32f2f; font-weight: 600;">${{config.failed_jobs}}</td>
                        <td>${{Math.round(config.progress_percent * 100) / 100}}%</td>
                        <td>${{flagsHtml}}</td>
                        <td>${{config.skipped ? '✓' : '-'}}</td>
                    </tr>
                `;
            }}).join('');
        }}

        function sortTable(tableId, colIndex) {{
            const table = document.getElementById(tableId);
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            if (rows.length === 0 || rows[0].cells.length === 1) return; // Skip if empty state

            rows.sort((a, b) => {{
                const aVal = a.cells[colIndex].textContent.trim();
                const bVal = b.cells[colIndex].textContent.trim();
                return aVal.localeCompare(bVal, undefined, {{ numeric: true }});
            }});

            rows.forEach(row => tbody.appendChild(row));
        }}

        function filterTable(searchId, tableId) {{
            const input = document.getElementById(searchId);
            const filter = input.value.toLowerCase();
            const table = document.getElementById(tableId);
            const rows = table.querySelectorAll('tbody tr');

            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            }});
        }}

        // Initial render
        renderJobs();
        renderConfigs();
    </script>
</body>
</html>"""

    return html


def main():
    """Main execution"""
    print("Starting Prototype Generation...")

    # Initialize schema
    if not initialize_schema():
        print("\n✗ Failed to initialize schema")
        return 1

    # Clear existing data
    if not clear_existing_data():
        print("\n✗ Failed to clear existing data")
        return 1

    # Load test data from CSV
    if not load_test_data():
        print("\n✗ Failed to load test data")
        return 1

    # Get list of all batches
    batches_df = db.execute_query("SELECT id, batch_type FROM irp_batch ORDER BY id", schema=SCHEMA)

    if batches_df.empty:
        print("✗ No batches found")
        return 1

    batches = batches_df.to_dict('records')
    output_dir = Path(__file__).parent / 'files' / 'html_output'
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"✓Found {len(batches)} batches to process")

    generated_files = []

    for batch in batches:
        batch_id = batch['id']
        batch_type = batch['batch_type']

        print(f"\nProcessing batch {batch_id}: {batch_type}...")

        # Query data
        data = query_batch_data(batch_id)
        if not data:
            print(f"  ✗ Failed to query data for batch {batch_id}")
            continue

        # Generate HTML
        html_content = generate_html(batch_id, data)

        # Create filename: batch_102_In-Progress-Mixed-Statuses.html
        # Clean batch_type for filename (replace spaces and special chars)
        clean_batch_type = batch_type.replace(' ', '-').replace('/', '-')
        filename = f'batch_{batch_id}.html'
        output_file = output_dir / filename

        # Write to file
        with open(output_file, 'w') as f:
            f.write(html_content)

        generated_files.append(str(output_file))
        print(f"  ✓ Generated: {filename} for {batch_type}")

    print("\n" + "="*60)
    print("COMPLETE!")
    print("="*60)
    print(f"Generated {len(generated_files)} HTML files:")
    for file in generated_files:
        print(f"  - {Path(file).name}")
    print(f"\nOpen any file in your browser to view batch details")
    return 0


if __name__ == '__main__':
    sys.exit(main())
