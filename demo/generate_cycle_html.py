"""
Generate cycle dashboard HTML using modular components
Enhanced with summary cards and job alerts
"""

import json
from html_components import (
    get_shared_css,
    generate_breadcrumb,
    generate_header,
    generate_card,
    generate_table,
    get_shared_javascript
)


def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None or str(ts) == 'None':
        return 'N/A'
    if isinstance(ts, str):
        return ts
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def calculate_cycle_statistics(batches):
    """Calculate aggregated statistics for the cycle

    Args:
        batches: List of batch dictionaries with job data

    Returns:
        Dictionary with cycle-level statistics
    """
    stats = {
        'total_batches': len(batches),
        'completed_batches': 0,
        'not_completed_batches': 0,
        'total_jobs': 0,
        'finished_jobs': 0,
        'not_finished_jobs': 0,
        'error_jobs': 0,
        'failed_jobs': 0,
        'cancelled_jobs': 0,
        'skipped_jobs': 0,
        'total_configs': 0,
        'fulfilled_configs': 0,
        'unfulfilled_configs': 0,
        'skipped_configs': 0
    }

    # Aggregate from batches
    for batch in batches:
        # Batch counts (batch_status field now contains reporting_status)
        if batch['batch_status'] == 'COMPLETED':
            stats['completed_batches'] += 1
        else:
            stats['not_completed_batches'] += 1

        # Job counts
        stats['total_jobs'] += batch.get('total_jobs', 0)

        # Configuration counts
        stats['total_configs'] += batch.get('total_configs', 0)

        # Get enhanced stats from batch if available
        stats['fulfilled_configs'] += batch.get('fulfilled_configs', 0)
        stats['unfulfilled_configs'] += batch.get('unfulfilled_configs', 0)
        stats['skipped_configs'] += batch.get('skipped_configs', 0)
        stats['finished_jobs'] += batch.get('finished_jobs', 0)
        stats['failed_jobs'] += batch.get('failed_jobs', 0)
        stats['error_jobs'] += batch.get('error_jobs', 0)
        stats['skipped_jobs'] += batch.get('skipped_jobs', 0)

    # Calculate not finished jobs
    stats['not_finished_jobs'] = stats['total_jobs'] - stats['finished_jobs']

    # Determine cycle status
    stats['cycle_status'] = 'COMPLETED' if stats['not_completed_batches'] == 0 else 'INCOMPLETE'

    return stats


def enhance_batches_with_alerts(batches, cycle_name):
    """Query additional data to add job alerts to each batch

    Args:
        batches: List of batch dictionaries
        cycle_name: Name of the cycle

    Returns:
        Enhanced batches list with job_alerts field
    """
    # Import here to avoid circular dependency
    import sys
    from pathlib import Path
    workspace_path = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(workspace_path))
    from helpers import database as db

    SCHEMA = 'demo'

    # Get job statistics for all batches in this cycle
    for batch in batches:
        batch_id = batch['batch_id']

        # Query job alerts for this batch
        alerts_df = db.execute_query("""
            SELECT
                COUNT(*) FILTER (WHERE j.status = 'FAILED') as failed_count,
                COUNT(*) FILTER (WHERE j.status = 'ERROR') as error_count,
                COUNT(*) FILTER (WHERE j.status = 'CANCELLED') as cancelled_count,
                COUNT(*) FILTER (WHERE j.skipped = TRUE) as skipped_count
            FROM irp_job j
            WHERE j.batch_id = %s
        """, (batch_id,), schema=SCHEMA)

        if not alerts_df.empty:
            alert_data = alerts_df.to_dict('records')[0]

            # Build alert string with icons (matching batch page style)
            alerts = []
            if alert_data['failed_count'] > 0:
                alerts.append(f"⚠ Failures ({alert_data['failed_count']})")
            if alert_data['error_count'] > 0:
                alerts.append(f"⚠ Errors ({alert_data['error_count']})")
            if alert_data['cancelled_count'] > 0:
                alerts.append(f"⚠ Cancelled ({alert_data['cancelled_count']})")
            if alert_data['skipped_count'] > 0:
                alerts.append(f"⚠ Skipped ({alert_data['skipped_count']})")

            batch['job_alerts'] = ', '.join(alerts) if alerts else '-'
            batch['has_alerts'] = len(alerts) > 0
        else:
            batch['job_alerts'] = '-'
            batch['has_alerts'] = False

    return batches


def generate_cycle_dashboard_html(cycle_name, batches):
    """Generate cycle dashboard HTML with summary cards and enhanced table

    Args:
        cycle_name: Name of the cycle
        batches: List of batch dictionaries with batch data

    Returns:
        Complete HTML string
    """
    # Convert timestamps to strings
    for batch in batches:
        if batch['created_ts']:
            batch['created_ts'] = format_timestamp(batch['created_ts'])

    # Enhance batches with job alerts
    batches = enhance_batches_with_alerts(batches, cycle_name)

    # Calculate cycle statistics
    stats = calculate_cycle_statistics(batches)

    batches_json = json.dumps(batches, default=str)

    # Generate components
    breadcrumb = generate_breadcrumb(cycle_name)
    header = generate_header(
        title="Moody's Risk Modeling Dashboard",
        subtitle=f"Cycle: {cycle_name}"
    )

    # Summary Cards
    cards = []

    # Card 1: Cycle Status
    status_badge = f'<span class="status-badge status-{stats["cycle_status"].replace(" ", "_")}">{stats["cycle_status"]}</span>'
    cards.append(generate_card(
        "Cycle Status",
        status_badge
    ))

    # Card 2: Batches
    cards.append(generate_card(
        "Batches",
        str(stats['total_batches']),
        f"{stats['completed_batches']} completed, {stats['not_completed_batches']} not completed"
    ))

    # Card 3: Jobs
    cards.append(generate_card(
        "Jobs",
        str(int(stats['total_jobs'])),
        f"Finished: {int(stats['finished_jobs'])}, Not Finished: {int(stats['not_finished_jobs'])}"
    ))

    # Card 4: Configurations
    cards.append(generate_card(
        "Configurations",
        str(int(stats['total_configs'])),
        f"Fulfilled: {int(stats['fulfilled_configs'])}, Unfulfilled: {int(stats['unfulfilled_configs'])}, Skipped: {int(stats['skipped_configs'])}"
    ))

    cards_html = f'<div class="info-grid">{"".join(cards)}</div>'

    # Batch table with Job Alerts column
    batch_table = generate_table(
        table_id="batchTable",
        columns=[
            {"label": "Batch ID"},
            {"label": "Batch Type"},
            {"label": "Status"},
            {"label": "Total Jobs"},
            {"label": "Job Alerts"},
            {"label": "Created"}
        ],
        search_id=None  # No search for cycle dashboard
    )

    # Complete HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cycle Dashboard - {cycle_name}</title>
    <style>
{get_shared_css()}

        .batch-list {{
            background: white;
            border-radius: 6px;
            padding: 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        .batch-list h3 {{
            font-size: 14px;
            margin-bottom: 12px;
            color: #37474f;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 8px;
        }}

        .alert-cell {{
            font-size: 12px;
            color: #d32f2f;
        }}

        .status-INCOMPLETE {{
            background: #fff3e0;
            color: #f57c00;
        }}
    </style>
</head>
<body>
    {header}
    {breadcrumb}
    {cards_html}

    <div class="batch-list">
        <h3>Batches ({len(batches)})</h3>
        {batch_table}
    </div>

    <script>
{get_shared_javascript()}

        const batches = {batches_json};

        function renderBatches() {{
            const tbody = document.getElementById('batchTableBody');
            tbody.innerHTML = batches.map(batch => `
                <tr>
                    <td><a href="batch/${{batch.batch_id}}/index.html" class="batch-link">${{batch.batch_id}}</a></td>
                    <td>${{batch.batch_type}}</td>
                    <td><span class="status-badge status-${{batch.batch_status}}">${{batch.batch_status}}</span></td>
                    <td>${{batch.total_jobs}}</td>
                    <td class="${{batch.has_alerts ? 'alert-cell' : ''}}">${{batch.job_alerts}}</td>
                    <td>${{batch.created_ts}}</td>
                </tr>
            `).join('');
        }}

        renderBatches();
    </script>
</body>
</html>"""

    return html
