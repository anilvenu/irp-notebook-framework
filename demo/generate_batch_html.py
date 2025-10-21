"""
Generate batch HTML using modular components
All enhancements implemented with reusable architecture
"""

import json
from html_components import (
    get_shared_css,
    generate_breadcrumb,
    generate_header,
    generate_card,
    generate_table,
    generate_tabs,
    get_shared_javascript
)


def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None or str(ts) == 'None':
        return 'N/A'
    if isinstance(ts, str):
        return ts
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def generate_batch_html(batch_id, data):
    """Generate HTML for batch viewer using modular components

    Args:
        batch_id: Batch ID
        data: Dictionary with 'summary', 'jobs', 'configs' keys

    Returns:
        Complete HTML string
    """
    summary = data['summary']
    jobs = data['jobs']
    configs = data['configs']

    # Convert data to JSON for JavaScript
    jobs_json = json.dumps(jobs, default=str)
    configs_json = json.dumps(configs, default=str)

    # Calculate statistics
    total_jobs = int(summary['total_jobs'])
    active_jobs = int(summary['active_jobs'])

    # Generate components
    breadcrumb = generate_breadcrumb(summary['cycle_name'], batch_id, summary['batch_type'])
    header = generate_header(
        title="Moody's Risk Modeling Dashboard",
        subtitle=f"Batch {batch_id}: {summary['batch_type']}"
    )

    # Info cards with enhancements
    cards = []

    # Card 1: Batch Status (use reporting_status for display)
    reporting_status = summary.get('reporting_status', summary.get('batch_status', 'UNKNOWN'))
    cards.append(generate_card(
        "Batch Status",
        f'<span class="status-badge status-{reporting_status}">{reporting_status}</span>'
    ))

    # Card 2: Jobs Summary
    cards.append(generate_card(
        "Jobs Summary",
        f"{total_jobs} Total",
        f"{active_jobs} active, {total_jobs - active_jobs} skipped"
    ))

    # Card 3: Configurations
    cards.append(generate_card(
        "Configurations",
        str(summary['total_configs']),
        f"{summary['active_configs']} active"
    ))

    # Card 4: Created + Stage/Step info (Enhancement #3)
    stage_step_info = f"{summary['stage_name']} › {summary['step_name']}"
    cards.append(generate_card(
        "Created",
        f'<span style="font-size: 14px;">{format_timestamp(summary["created_ts"])}</span>',
        stage_step_info
    ))

    # Card 5: Submitted
    cards.append(generate_card(
        "Submitted",
        f'<span style="font-size: 14px;">{format_timestamp(summary["submitted_ts"])}</span>'
    ))

    # Card 6: Completed
    cards.append(generate_card(
        "Completed",
        f'<span style="font-size: 14px;">{format_timestamp(summary["completed_ts"])}</span>'
    ))

    cards_html = f'<div class="info-grid">{"".join(cards)}</div>'

    # Tabs
    tabs = generate_tabs([
        {"id": "jobs", "label": "Jobs", "count": len(jobs)},
        {"id": "configurations", "label": "Configurations", "count": len(configs)}
    ])

    # Jobs table with updated column label (Enhancement #6)
    jobs_table = generate_table(
        table_id="jobsTable",
        columns=[
            {"label": "ID"},
            {"label": "Moody's Job ID"},
            {"label": "Status"},
            {"label": "Report Status"},
            {"label": "Configuration"},
            {"label": "Configuration Override"},  # Changed from "Overridden"
            {"label": "Age (hrs)"},
            {"label": "Next Action"},
            {"label": "Parent Job"}
        ],
        search_id="jobSearch"
    )

    # Configurations table with updated columns (Enhancement #7 & #8)
    # Dropped "Skipped", renamed "FLAGS" to "JOB ALERTS"
    configs_table = generate_table(
        table_id="configsTable",
        columns=[
            {"label": "Config ID"},
            {"label": "Status"},
            {"label": "Configuration Data"},
            {"label": "Total Jobs"},
            {"label": "Active"},
            {"label": "Finished"},
            {"label": "Failed"},
            {"label": "Progress %"},
            {"label": "Job Alerts"}  # Changed from "FLAGS", includes counts
        ],
        search_id="configSearch"
    )

    # Complete HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Batch {batch_id} - {summary['batch_type']}</title>
    <style>
{get_shared_css()}
    </style>
</head>
<body>
    {header}
    {breadcrumb}
    {cards_html}
    {tabs}

    <div id="jobs-content" class="tab-content active">
        {jobs_table}
    </div>

    <div id="configurations-content" class="tab-content">
        {configs_table}
    </div>

    <script>
{get_shared_javascript()}

        const jobs = {jobs_json};
        const configs = {configs_json};

        function renderJobs() {{
            const tbody = document.getElementById('jobsTableBody');
            if (jobs.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No jobs found</td></tr>';
                return;
            }}

            tbody.innerHTML = jobs.map((job, idx) => `
                <tr>
                    <td>${{job.id}}</td>
                    <td>${{job.moodys_workflow_id || '-'}}</td>
                    <td><span class="status-badge status-${{job.status}}">${{job.status}}</span></td>
                    <td><span class="status-badge status-${{job.report_status}}">${{job.report_status || '-'}}</span></td>
                    <td><div class="json-preview" onclick="showJsonTooltip(this, jobs[${{idx}}].job_configuration_data)">${{JSON.stringify(job.job_configuration_data)}}</div></td>
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
                tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No configurations found</td></tr>';
                return;
            }}

            tbody.innerHTML = configs.map((config, idx) => {{
                // Build alerts string with counts (Enhancement #8)
                const alerts = [];
                if (config.has_failures) {{
                    const count = config.failed_jobs || 0;
                    alerts.push(`<span class="alert-flag" style="color: #d32f2f;">⚠ Failures (${{count}})</span>`);
                }}
                if (config.has_errors) {{
                    const count = config.error_jobs || 0;
                    alerts.push(`<span class="alert-flag" style="color: #c62828;">⚠ Errors (${{count}})</span>`);
                }}
                if (config.has_unsubmitted) {{
                    // calculated in view based on INITIATED status
                    const unsubmitted = config.unsubmitted_jobs || 0;
                    alerts.push(`<span class="alert-flag" style="color: #f57f17;">⚠ Unsubmitted (${{unsubmitted}})</span>`);
                }}
                const alertsHtml = alerts.length > 0 ? alerts.join('<br>') : '-';

                return `
                    <tr>
                        <td>${{config.config_id}}</td>
                        <td><span class="status-badge status-${{config.config_report_status}}">${{config.config_report_status}}</span></td>
                        <td><div class="json-preview" onclick="showJsonTooltip(this, configs[${{idx}}].job_configuration_data)">${{JSON.stringify(config.job_configuration_data)}}</div></td>
                        <td>${{config.total_jobs}}</td>
                        <td style="color: #0288d1; font-weight: 600;">${{config.active_jobs || 0}}</td>
                        <td style="color: #2e7d32; font-weight: 600;">${{config.finished_jobs}}</td>
                        <td style="color: #d32f2f; font-weight: 600;">${{config.failed_jobs}}</td>
                        <td>${{Math.round(config.progress_percent * 100) / 100}}%</td>
                        <td>${{alertsHtml}}</td>
                    </tr>
                `;
            }}).join('');
        }}

        // Initial render
        renderJobs();
        renderConfigs();
    </script>
</body>
</html>"""

    return html
