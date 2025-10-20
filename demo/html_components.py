"""
Reusable HTML components for batch viewer
Modular design with shared CSS and parameterized components
"""

import json


def get_shared_css():
    """Shared CSS for all HTML pages"""
    return """
        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: #f5f5f5;
            padding: 16px;
        }

        /* Breadcrumb Navigation */
        .breadcrumb {
            margin-bottom: 16px;
            font-size: 14px;
        }

        .breadcrumb a {
            color: #1976d2;
            text-decoration: none;
            font-weight: 500;
        }

        .breadcrumb a:hover {
            text-decoration: underline;
        }

        .breadcrumb-separator {
            margin: 0 8px;
            color: #999;
        }

        .breadcrumb-current {
            color: #666;
        }

        /* Page Header */
        .page-header {
            background: linear-gradient(135deg, #546e7a 0%, #37474f 100%);
            color: white;
            padding: 20px 24px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .page-header h1 {
            font-size: 16px;
            margin-bottom: 4px;
            font-weight: 500;
            opacity: 0.9;
        }

        .page-header h2 {
            font-size: 24px;
            font-weight: 600;
        }

        /* Info Cards Grid */
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }

        .info-card {
            background: white;
            padding: 16px;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .info-card h3 {
            font-size: 11px;
            color: #666;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .info-card .value {
            font-size: 20px;
            font-weight: 600;
            color: #333;
        }

        .info-card .sub-value {
            font-size: 13px;
            color: #888;
            margin-top: 4px;
        }

        /* Status Badges */
        .status-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-ACTIVE { background: #e3f2fd; color: #1976d2; }
        .status-COMPLETED { background: #e8f5e9; color: #388e3c; }
        .status-INITIATED { background: #fff3e0; color: #f57c00; }
        .status-FAILED { background: #ffebee; color: #d32f2f; }
        .status-CANCELLED { background: #f5f5f5; color: #757575; }
        .status-ERROR { background: #ffebee; color: #c62828; }
        .status-FINISHED { background: #e8f5e9; color: #2e7d32; }
        .status-RUNNING { background: #e1f5fe; color: #0288d1; }
        .status-QUEUED, .status-PENDING, .status-SUBMITTED { background: #f3e5f5; color: #7b1fa2; }
        .status-SKIPPED { background: #fafafa; color: #616161; }
        .status-UNSUBMITTED { background: #fff9c4; color: #f57f17; }
        .status-FULFILLED { background: #e8f5e9; color: #2e7d32; }
        .status-UNFULFILLED { background: #fff3e0; color: #f57c00; }

        /* Tabs */
        .tabs {
            display: flex;
            gap: 0;
            margin-bottom: 0;
            background: white;
            border-radius: 6px 6px 0 0;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .tab {
            flex: 1;
            padding: 12px 24px;
            cursor: pointer;
            background: white;
            border: none;
            font-size: 13px;
            font-weight: 600;
            color: #666;
            transition: all 0.2s;
            border-bottom: 3px solid transparent;
        }

        .tab.active {
            color: #546e7a;
            border-bottom-color: #546e7a;
            background: #fafafa;
        }

        .tab:hover:not(.active) {
            background: #f5f5f5;
        }

        /* Tab Content */
        .tab-content {
            display: none;
            background: white;
            padding: 16px;
            border-radius: 0 0 6px 6px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .tab-content.active {
            display: block;
        }

        /* Search Box */
        .search-box {
            width: 100%;
            padding: 10px;
            margin: 0 0 16px 0;
            border: 2px solid #e0e0e0;
            border-radius: 4px;
            font-size: 13px;
            transition: border-color 0.2s;
        }

        .search-box:focus {
            outline: none;
            border-color: #546e7a;
        }

        /* Tables */
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }

        th {
            background: #fafafa;
            padding: 10px;
            text-align: left;
            font-weight: 600;
            font-size: 11px;
            color: #666;
            border-bottom: 2px solid #e0e0e0;
            cursor: pointer;
            user-select: none;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        th:hover {
            background: #f0f0f0;
        }

        td {
            padding: 10px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 13px;
        }

        tr:hover {
            background: #fafafa;
        }

        /* Specialized cells */
        .json-preview {
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-family: 'Courier New', monospace;
            font-size: 11px;
            color: #666;
            cursor: help;
            padding: 4px 8px;
            background: #f5f5f5;
            border-radius: 3px;
        }

        .age-badge {
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 11px;
            background: #f5f5f5;
            color: #666;
            font-weight: 500;
        }

        .needs-attention {
            color: #d32f2f;
            font-weight: 600;
        }

        .empty-state {
            text-align: center;
            padding: 32px;
            color: #999;
        }

        .batch-link {
            color: #1976d2;
            text-decoration: none;
            font-weight: 600;
        }

        .batch-link:hover {
            text-decoration: underline;
        }

        .alert-flag {
            display: inline-block;
            margin-right: 8px;
            font-size: 12px;
        }
    """


def generate_breadcrumb(cycle_name, batch_id=None, batch_type=None):
    """Generate breadcrumb navigation

    Args:
        cycle_name: Name of the cycle
        batch_id: Optional batch ID (if on batch page)
        batch_type: Optional batch type (if on batch page)

    Returns:
        HTML string for breadcrumb
    """
    if batch_id:
        # Batch page breadcrumb: Cycle > Batch {id}
        return f"""
        <div class="breadcrumb">
            <a href="../../index.html">{cycle_name}</a>
            <span class="breadcrumb-separator">›</span>
            <span class="breadcrumb-current">Batch {batch_id} - {batch_type}</span>
        </div>
        """
    else:
        # Cycle page breadcrumb: Just cycle name
        return f"""
        <div class="breadcrumb">
            <span class="breadcrumb-current">{cycle_name}</span>
        </div>
        """


def generate_header(title, subtitle=None):
    """Generate page header component

    Args:
        title: Main title (e.g., "Cycle: Analysis-2025-Q1")
        subtitle: Optional subtitle (e.g., batch name)

    Returns:
        HTML string for header
    """
    subtitle_html = f"<h2>{subtitle}</h2>" if subtitle else ""

    return f"""
    <div class="page-header">
        <h1>Moody's Risk Modeling Dashboard</h1>
        {subtitle_html}
    </div>
    """


def generate_card(title, value, sub_value=None):
    """Generate info card component

    Args:
        title: Card title
        value: Main value (can be HTML string with badges, etc.)
        sub_value: Optional sub-value text

    Returns:
        HTML string for card
    """
    sub_value_html = f'<div class="sub-value">{sub_value}</div>' if sub_value else ""

    return f"""
    <div class="info-card">
        <h3>{title}</h3>
        <div class="value">{value}</div>
        {sub_value_html}
    </div>
    """


def generate_table(table_id, columns, search_id=None):
    """Generate table component structure

    Args:
        table_id: Unique ID for the table
        columns: List of column definitions [{"label": "ID", "sortable": True}, ...]
        search_id: Optional ID for search box

    Returns:
        HTML string for table with search box
    """
    search_html = ""
    if search_id:
        search_html = f'<input type="text" class="search-box" id="{search_id}" placeholder="Search..." onkeyup="filterTable(\'{search_id}\', \'{table_id}\')">'

    headers = []
    for i, col in enumerate(columns):
        sortable = col.get('sortable', True)
        onclick = f'onclick="sortTable(\'{table_id}\', {i})"' if sortable else ''
        arrow = ' ↕' if sortable else ''
        headers.append(f'<th {onclick}>{col["label"]}{arrow}</th>')

    return f"""
    {search_html}
    <table id="{table_id}">
        <thead>
            <tr>
                {''.join(headers)}
            </tr>
        </thead>
        <tbody id="{table_id}Body">
        </tbody>
    </table>
    """


def generate_tabs(tabs):
    """Generate tabs component

    Args:
        tabs: List of tab definitions [{"id": "jobs", "label": "Jobs", "count": 10}, ...]

    Returns:
        HTML string for tabs
    """
    tab_buttons = []
    for i, tab in enumerate(tabs):
        active = "active" if i == 0 else ""
        count = f" ({tab['count']})" if 'count' in tab else ""
        tab_buttons.append(f'<button class="tab {active}" onclick="switchTab(\'{tab["id"]}\')">{ tab["label"]}{count}</button>')

    return f"""
    <div class="tabs">
        {''.join(tab_buttons)}
    </div>
    """


def get_shared_javascript():
    """Shared JavaScript functions for all pages"""
    return """
    function switchTab(tabName) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

        event.target.classList.add('active');
        document.getElementById(tabName + '-content').classList.add('active');
    }

    function sortTable(tableId, colIndex) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        if (rows.length === 0 || rows[0].cells.length === 1) return; // Skip if empty state

        rows.sort((a, b) => {
            const aVal = a.cells[colIndex].textContent.trim();
            const bVal = b.cells[colIndex].textContent.trim();
            return aVal.localeCompare(bVal, undefined, { numeric: true });
        });

        rows.forEach(row => tbody.appendChild(row));
    }

    function filterTable(searchId, tableId) {
        const input = document.getElementById(searchId);
        const filter = input.value.toLowerCase();
        const table = document.getElementById(tableId);
        const rows = table.querySelectorAll('tbody tr');

        rows.forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(filter) ? '' : 'none';
        });
    }
    """
